"""담당: 지인 (Agent3) — 개선안 생성 raw 파이프라인.

순수 Python + 타입 힌트, 프레임워크 미사용. 각 단계를 독립 함수로 분리해
나중에 LangGraph 노드(graph.py의 _route/_retrieve/_generate/_verify/_fallback)로
그대로 옮길 수 있게 한다. 그 전까지는 service.generate_recommendation()이
run()을 직접 호출한다.

흐름(docs/agent3_logic.md §1·§4-3):
    retrieve_context → route_proposal_type → generate_proposal
        → evaluate (최대 3시도) → assemble

route_proposal_type이 핵심 판단 지점이다 — copy_draft(상세페이지 문구 수정)와
image_guide(촬영 가이드) 중 뭘 쓸지, **우리 코드가 규칙으로 정하지 않고 LLM이 tool
호출로 직접 고른다**(core/llm_client.py의 choose_tool(), tool_choice="required").
이게 workflow와 agent를 가르는 지점 — 판단을 코드가 하면 workflow, 모델이 하면
agent. retrieve_context가 copy_draft/image_guide 후보 근거를 둘 다 미리 가져와
모델에게 보여주는 이유도 이것 때문이다(라우팅 전엔 어느 쪽이 뽑힐지 모른다).

주의 — image_guide(정상 개선안 타입, 여기)와 fallback_guide_v1.md(근거없음 경로,
grounding이 MAX_RETRY번 실패했을 때 타는 별도 개념)는 다르다. 헷갈리지 말 것.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.constants import MAX_RETRY, SIMILAR_CASE_TOP_N
from app.core.exceptions import EvidenceNotFoundError
from app.core.llm_client import get_llm_client
from app.core.schemas import (
    Citation,
    DetectionAlert,
    DetectionConfidence,
    Evaluator,
    EvaluatorChecks,
    HitlStatus,
    Proposal,
    ProposalType,
    Recommendation,
    RecommendationConfidence,
    RecommendedAction,
)
from app.core.vectordb import (
    get_detail_pages,
    get_documents,
    get_rejection_reasons,
    query_documents,
)
from app.recommendation.grounding import has_evidence, verify_grounding

NO_DETAIL_TEXT = "정보 없음"
"""상세페이지 미등록/빈 값 표기(§4-1·§4-5) — 근거없음 경로를 유발하는 값이라 상수로 뺀다."""

ACTIONABLE_TEXT_MARKERS = ("하세요", "해보세요", "바랍니다", "권장", "검토", "확인", "진행", "추가")
"""proposed_text가 실행 가능한 안내문인지 판정하는 키워드 휴리스틱(actionability, §2 방법5).
팀 합의된 기준이 따로 없어 자체 설계한 임시 규칙 — 필요시 조정."""

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
COPY_DRAFT_PROMPT_PATH = PROMPTS_DIR / "copy_draft_v1.md"
IMAGE_GUIDE_PROMPT_PATH = PROMPTS_DIR / "image_guide_v1.md"
ROUTING_PROMPT_PATH = PROMPTS_DIR / "route_proposal_type_v1.md"
FALLBACK_GUIDE_PROMPT_PATH = PROMPTS_DIR / "fallback_guide_v1.md"

COPY_DRAFT_TOOL: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "use_copy_draft",
        "description": (
            "상세페이지 문구를 수정하는 개선안을 제안한다. "
            "상세페이지 원문에 실제로 고칠 근거가 있을 때 선택."
        ),
        "parameters": {
            "type": "object",
            "properties": {"reason": {"type": "string", "description": "이 도구를 고른 근거 한 문장"}},
            "required": ["reason"],
        },
    },
}

IMAGE_GUIDE_TOOL: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "use_image_guide",
        "description": (
            "촬영/이미지 보완 가이드를 제안한다. "
            "원인이 사진·조명·이미지 표현처럼 촬영 결과물 문제일 때 선택."
        ),
        "parameters": {
            "type": "object",
            "properties": {"reason": {"type": "string", "description": "이 도구를 고른 근거 한 문장"}},
            "required": ["reason"],
        },
    },
}

ROUTING_TOOLS = [COPY_DRAFT_TOOL, IMAGE_GUIDE_TOOL]

_TOOL_NAME_TO_PROPOSAL_TYPE = {
    "use_copy_draft": ProposalType.COPY_DRAFT,
    "use_image_guide": ProposalType.IMAGE_GUIDE,
}

_CONFIDENCE_RANK = {
    RecommendationConfidence.LOW: 0,
    RecommendationConfidence.MEDIUM: 1,
    RecommendationConfidence.HIGH: 2,
}

_DETECTION_CONFIDENCE_CAP = {
    DetectionConfidence.HIGH: RecommendationConfidence.HIGH,
    DetectionConfidence.MEDIUM: RecommendationConfidence.MEDIUM,
    DetectionConfidence.LOW: RecommendationConfidence.LOW,
    DetectionConfidence.NOT_APPLICABLE: RecommendationConfidence.LOW,
}
"""탐지확신도→개선안확신도 상한 매핑(§5-1). should_generate 게이트 통과분은 실무적으로
HIGH/MEDIUM만 들어오지만, LOW/NOT_APPLICABLE도 방어적으로 가장 낮은 상한을 매핑해둔다."""

MAX_ATTEMPTS = MAX_RETRY + 1


def should_generate(alert: DetectionAlert) -> bool:
    """트리거 게이트: recommended_action이 "개선안 생성"인 alert만 통과."""
    return alert.recommended_action == RecommendedAction.GENERATE_RECOMMENDATION


def retrieve_context(alert: DetectionAlert) -> dict:
    """근거 조회 — copy_draft/image_guide 후보 근거를 둘 다 미리 가져온다.

    라우팅이 LLM 몫(route_proposal_type)이라, retrieve_context는 어느 쪽이 뽑힐지
    모른 채로 두 후보를 같이 준비해서 모델에게 보여준다:

    - detail_text: 컬렉션1(상세페이지) get(정확 필터) — 임베딩 안 거침. 미등록
      SKU/빈 값이면 NO_DETAIL_TEXT(§4-5).
    - cs_summary: image_guide용 근거. raw_text 조회 경로가 아직 없어(분류 워커·
      ClassifiedItem 연동 전) root_cause 통계로 만든 요약으로 대체.

    컬렉션2(과거·반려 사례)는 공통, 0건이면 similar_case=None(§4-2 — 반려 적재
    전엔 항상 0건이라 정상 상태).
    """
    detail_text = _get_detail_page_text(alert)
    cs_summary = _summarize_cs_evidence(alert)

    rejection_reasons = get_rejection_reasons()
    query_text = alert.root_cause.label if alert.root_cause else alert.main_aspect.value
    similar_rows = query_documents(
        rejection_reasons,
        query_text=query_text,
        n_results=SIMILAR_CASE_TOP_N,
        where={"aspect": alert.main_aspect.value},
    )
    similar_case = similar_rows[0]["document"] if similar_rows else None

    return {"detail_text": detail_text, "cs_summary": cs_summary, "similar_case": similar_case}


def _get_detail_page_text(alert: DetectionAlert) -> str:
    """컬렉션1(상세페이지) get — 임베딩 안 거침. 미등록 SKU/빈 값이면 NO_DETAIL_TEXT(§4-5)."""
    detail_pages = get_detail_pages()
    detail_rows = get_documents(
        detail_pages,
        where={
            "$and": [
                {"product_group_id": alert.product_group_id},
                {"channel": alert.channel.value},
                {"aspect": alert.main_aspect.value},
            ]
        },
    )
    return detail_rows[0]["document"] if detail_rows else NO_DETAIL_TEXT


def _summarize_cs_evidence(alert: DetectionAlert) -> str:
    """image_guide용 근거 — CS 원문 대신 root_cause 통계로 만든 요약(§4-3).

    raw_text 조회 경로가 아직 없다(분류 워커·ClassifiedItem 연동 전). 있으면 이
    함수만 실제 CS 원문 인용으로 교체하면 되고, 그 외 코드는 안 건드려도 된다.
    """
    if alert.root_cause is None:
        return NO_DETAIL_TEXT
    return f"CS {alert.root_cause.total}건 중 {alert.root_cause.count}건이 '{alert.root_cause.label}' 관련 언급"


async def route_proposal_type(alert: DetectionAlert, context: dict) -> ProposalType:
    """개선안 도구 라우팅 — LLM이 tool 호출로 직접 판단(§4-3).

    copy_draft/image_guide 두 tool과 실제 근거(상세페이지 원문 + CS 요약)를 모델에게
    보여주고, tool_choice="required"로 반드시 둘 중 하나를 호출하게 한다. 판단
    로직을 규칙으로 코드에 박지 않는 게 핵심 — 규칙 기반으로 되돌리면 workflow로
    후퇴한다.
    """
    root_cause_label = alert.root_cause.label if alert.root_cause else "미상"
    prompt = ROUTING_PROMPT_PATH.read_text(encoding="utf-8").format(
        aspect=alert.main_aspect.value,
        root_cause_label=root_cause_label,
        detail_text=context["detail_text"],
        cs_summary=context["cs_summary"],
    )

    result = await get_llm_client().choose_tool(
        prompt, tools=ROUTING_TOOLS, trace_key=f"alert_id={alert.alert_id}"
    )
    return _TOOL_NAME_TO_PROPOSAL_TYPE[result["name"]]


async def generate_proposal(
    alert: DetectionAlert, proposal_type: ProposalType, context: dict
) -> Proposal:
    """개선안 생성 — OpenAI 호출(core/llm_client.py 경유), 슬롯 채우기 방식.

    current_text·proposed_text·rationale 셋 다 LLM이 채운다. current_text는 LLM이
    근거 원문에서 "인용"한 것이라고 주장하는 문구 — LLM 자기신고를 그대로 믿지 않고
    evaluate()가 실제 근거(context)와 문자 그대로 대조해 사후 검증한다(§4-3). type
    (route_proposal_type 결과)·target_field·detailpage_grounded는 alert/context에서
    이미 알고 있어 LLM에 맡기지 않는다.
    """
    root_cause_label = alert.root_cause.label if alert.root_cause else "미상"
    anomaly = f"{alert.channel.value} · {alert.main_aspect.value} 이상 (원인: {root_cause_label})"
    rejection_reasons = context.get("similar_case") or "없음"

    if proposal_type == ProposalType.COPY_DRAFT:
        evidence_text = context["detail_text"]
        prompt = COPY_DRAFT_PROMPT_PATH.read_text(encoding="utf-8").format(
            anomaly=anomaly, detail_pages=evidence_text, rejection_reasons=rejection_reasons
        )
    else:
        evidence_text = context["cs_summary"]
        prompt = IMAGE_GUIDE_PROMPT_PATH.read_text(encoding="utf-8").format(
            anomaly=anomaly, cs_summary=evidence_text, rejection_reasons=rejection_reasons
        )

    response = await get_llm_client().complete_json(prompt, trace_key=f"alert_id={alert.alert_id}")

    return Proposal(
        type=proposal_type,
        target_field=alert.main_aspect,
        current_text=response["current_text"],
        proposed_text=response["proposed_text"],
        rationale=response["rationale"],
        detailpage_grounded=(proposal_type == ProposalType.COPY_DRAFT and evidence_text != NO_DETAIL_TEXT),
    )


async def generate_fallback_proposal(alert: DetectionAlert, proposal_type: ProposalType) -> Proposal:
    """근거없음 경로 — grounding이 MAX_RETRY번 실패했을 때만 호출된다(§2 방법1).

    특정 인용 없이 일반 가이드 문구로 대체한다. current_text는 NO_DETAIL_TEXT로
    고정 — "근거를 특정하지 못했다"를 그대로 반영한다. evaluate()를 다시 태우지
    않는다(run() 참고) — 애초에 근거가 없다고 선언하는 경로라 재검증할 인용 자체가
    없다.
    """
    root_cause_label = alert.root_cause.label if alert.root_cause else "미상"
    anomaly = f"{alert.channel.value} · {alert.main_aspect.value} 이상 (원인: {root_cause_label})"

    prompt = FALLBACK_GUIDE_PROMPT_PATH.read_text(encoding="utf-8").format(anomaly=anomaly)
    response = await get_llm_client().complete_json(prompt, trace_key=f"alert_id={alert.alert_id}")

    return Proposal(
        type=proposal_type,
        target_field=alert.main_aspect,
        current_text=NO_DETAIL_TEXT,
        proposed_text=response["proposed_text"],
        rationale=response["rationale"],
        detailpage_grounded=False,
    )


def _is_consistent_with_root_cause(rationale: str, alert: DetectionAlert) -> bool:
    """rationale이 실제 진단된 원인 라벨을 근거로 삼고 있는지(자기일관성, §2 방법4).

    grounding은 통과해도(current_text는 진짜 인용) rationale이 엉뚱한 사유를 댈 수
    있다 — 이 경우를 잡는다. has_evidence()의 정규화+부분일치 로직을 재사용해서
    rationale 안에 root_cause.label이 실제로 언급됐는지 확인한다. root_cause가
    없으면(원칙적으로 게이트에서 걸러지지만 방어적으로) 검사 대상이 없으니 통과 처리.
    """
    if alert.root_cause is None:
        return True
    return has_evidence(alert.root_cause.label, rationale)


def _is_actionable(proposed_text: str) -> bool:
    """proposed_text가 실행 가능한 안내문 형태인지(actionability, §2 방법5).

    ACTIONABLE_TEXT_MARKERS 키워드 휴리스틱 — "느낌"이나 "확신도" 같은 관찰만 있고
    행동 지시가 없는 문장을 거른다.
    """
    return any(marker in proposed_text for marker in ACTIONABLE_TEXT_MARKERS)


def evaluate(proposal: Proposal, alert: DetectionAlert, context: dict, attempt: int = 1) -> Evaluator:
    """Evaluator 검증 — 3기준 실제 판정(§2·§4-3).

    - grounding: proposal.current_text(LLM이 "이게 근거다"라고 주장한 인용)가 실제
      근거(context["detail_text"] 또는 context["cs_summary"], §4-3 도구별 분리)에
      있는지 grounding.py로 대조. 없는 내용을 인용했다고 우기면 실패한다.
    - consistency: rationale이 실제 원인 라벨(alert.root_cause.label)을 근거로
      삼고 있는지 — grounding은 통과했는데 사유가 엉뚱한 경우를 잡는다.
    - actionability: proposed_text가 행동 지시 형태인지 키워드로 확인.

    attempt는 run()의 재시도 루프가 몇 번째 시도인지 넘겨준다(1부터 시작).
    """
    evidence_text = context["detail_text"] if proposal.type == ProposalType.COPY_DRAFT else context["cs_summary"]

    failure_reasons: list[str] = []

    try:
        verify_grounding(proposal.current_text, evidence_text)
        grounding_ok = True
    except EvidenceNotFoundError as exc:
        grounding_ok = False
        failure_reasons.append(str(exc))

    consistency_ok = _is_consistent_with_root_cause(proposal.rationale, alert)
    if not consistency_ok:
        failure_reasons.append(
            f"rationale이 원인 라벨({alert.root_cause.label if alert.root_cause else '미상'})을 근거로 삼지 않음"
        )

    actionability_ok = _is_actionable(proposal.proposed_text)
    if not actionability_ok:
        failure_reasons.append("proposed_text가 실행 가능한 안내문 형태가 아님")

    return Evaluator(
        passed=grounding_ok and consistency_ok and actionability_ok,
        attempts=attempt,
        checks=EvaluatorChecks(
            grounding=grounding_ok, consistency=consistency_ok, actionability=actionability_ok
        ),
        failure_reason="; ".join(failure_reasons) if failure_reasons else None,
    )


def score_confidence(
    proposal: Proposal, context: dict, alert: DetectionAlert
) -> tuple[RecommendationConfidence, str, bool]:
    """개선안 확신도 산정(§4-4) + 탐지 확신도 캡핑(§5-1).

    베이스 라벨은 (상세페이지 근거 유무 + 유사사례 유무) 가중합 — 초기엔 규칙 기반
    등가(§4-4, 승인·반려 데이터 쌓이면 재보정 예정): 둘 다 있으면 높음, 하나만
    있으면 중간, 둘 다 없으면 낮음.

    그 다음 alert.detection_confidence로 캡핑한다(§5-1) — 탐지 확신도가 중간이면
    개선안도 중간이 상한(높음 표시 금지). should_generate 게이트
    (recommended_action=="개선안 생성") 통과분은 실무적으로 detection_confidence가
    높음/중간만 들어온다.

    Returns:
        (최종 확신도, 사람이 읽을 한 줄 사유, 캡핑으로 실제 강등됐는지)
    """
    has_detail_grounding = proposal.detailpage_grounded
    has_similar_case = context.get("similar_case") is not None

    if has_detail_grounding and has_similar_case:
        base = RecommendationConfidence.HIGH
    elif has_detail_grounding or has_similar_case:
        base = RecommendationConfidence.MEDIUM
    else:
        base = RecommendationConfidence.LOW

    cap = _DETECTION_CONFIDENCE_CAP[alert.detection_confidence]
    final = base if _CONFIDENCE_RANK[base] <= _CONFIDENCE_RANK[cap] else cap
    capped = final != base

    reason = (
        f"상세페이지 근거 {'있음' if has_detail_grounding else '없음'} + "
        f"유사 사례 {'있음' if has_similar_case else '없음'} → {base.value}"
    )
    if capped:
        reason += f" (탐지 확신도 {alert.detection_confidence.value}로 {final.value} 캡핑)"

    return final, reason, capped


def assemble(
    alert: DetectionAlert,
    proposal: Proposal,
    evaluator: Evaluator,
    context: dict,
) -> Recommendation:
    """단계 결과 조립 → Recommendation. hitl은 항상 대기 상태로 시작."""
    confidence, confidence_reason, capped = score_confidence(proposal, context, alert)

    return Recommendation(
        recommendation_id=f"REC-{uuid.uuid4().hex[:12]}",
        alert_id=alert.alert_id,
        created_at=datetime.now(timezone.utc),
        proposal=proposal,
        citations=[
            Citation(inquiry_id=inquiry_id, quote="")
            for inquiry_id in alert.evidence.inquiry_ids[:1]
        ],
        evaluator=evaluator,
        similar_case=context.get("similar_case"),
        recommendation_confidence=confidence,
        confidence_reason=confidence_reason,
        capped_by_detection=capped,
        hitl_status=HitlStatus.PENDING,
        hitl_feedback=None,
    )


async def run(alert: DetectionAlert) -> Recommendation | None:
    """오케스트레이터: 트리거 게이트 → 근거조회 → 라우팅(LLM) → (생성→검증) 최대 3회
    → (그래도 실패하면) 근거없음 경로 → 조립.

    route_proposal_type()은 alert 하나당 1회만 — 재시도는 "같은 도구로 다시 생성"이지
    "도구를 바꿔서 다시 판단"이 아니다. MAX_ATTEMPTS를 다 써도 grounding이 안 되면
    generate_fallback_proposal()로 넘어간다(§2 방법1) — 억지로 근거 있는 척 넘기지
    않는다. LLM 호출 있는 함수는 async로 통일한다(협업 규칙 5).
    """
    if not should_generate(alert):
        return None

    context = retrieve_context(alert)
    proposal_type = await route_proposal_type(alert, context)

    attempt = 1
    proposal = await generate_proposal(alert, proposal_type, context)
    evaluator = evaluate(proposal, alert, context, attempt=attempt)
    while not evaluator.passed and attempt < MAX_ATTEMPTS:
        attempt += 1
        proposal = await generate_proposal(alert, proposal_type, context)
        evaluator = evaluate(proposal, alert, context, attempt=attempt)

    if not evaluator.passed:
        proposal = await generate_fallback_proposal(alert, proposal_type)
        evaluator = Evaluator(
            passed=True,
            attempts=MAX_ATTEMPTS,
            checks=EvaluatorChecks(grounding=False, consistency=True, actionability=True),
            failure_reason=f"근거를 찾지 못해 일반 가이드로 대체(MAX_RETRY={MAX_RETRY} 소진)",
        )

    return assemble(alert, proposal, evaluator, context)


def record_hitl_outcome(alert: DetectionAlert, recommendation: Recommendation) -> None:
    """승인/반려 결과를 컬렉션2(과거·반려 사례)에 적재(§4-2).

    Agent3는 hitl_status를 판단하지도, Recommendation을 저장하지도 않는다 — 그
    소유자는 Spring Boot다(graph.py HITL 메모 참고, "지금 정하지 말고 5주차 연동
    때 정하라"고 팀이 이미 합의함). 이 함수는 그 결정이 끝난 뒤 호출되는
    사이드이펙트 하나뿐이다: 다음 유사 케이스 생성 때 "이런 개선안은 반려/승인
    됐었다"를 참고할 수 있도록 결과를 인덱싱한다.

    승인·반려 둘 다 적재 대상이다(§4-2: "승인 또는 반려된 개선안 1건 = 문서 1건").
    id는 recommendation_id로 결정적 — 같은 건에 대해 재호출해도 upsert라 중복 없음.

    Raises:
        ValueError: alert/recommendation이 서로 다른 건을 가리키거나, hitl_status가
            아직 PENDING(결정 전)이라 적재할 결과가 없는 경우.
    """
    if recommendation.alert_id != alert.alert_id:
        raise ValueError(
            f"recommendation.alert_id({recommendation.alert_id})가 "
            f"alert.alert_id({alert.alert_id})와 다릅니다"
        )
    if recommendation.hitl_status == HitlStatus.PENDING:
        raise ValueError("hitl_status가 아직 결정되지 않았습니다(PENDING) — 적재 대상 아님")

    root_cause_label = alert.root_cause.label if alert.root_cause else "미상"
    proposed_text = recommendation.proposal.proposed_text if recommendation.proposal else ""
    document = f"{root_cause_label} {alert.main_aspect.value} {proposed_text}"

    outcome = "반려" if recommendation.hitl_status == HitlStatus.REJECTED else "승인"
    decided_at = (
        recommendation.hitl_feedback.processed_at
        if recommendation.hitl_feedback
        else datetime.now(timezone.utc)
    )

    metadata: dict[str, Any] = {
        "channel": alert.channel.value,
        "aspect": alert.main_aspect.value,
        "root_cause_label": root_cause_label,
        "outcome": outcome,
        "decided_at": decided_at.isoformat(),
    }

    rejection_reason = (
        recommendation.hitl_feedback.rejection_reason if recommendation.hitl_feedback else None
    )
    if rejection_reason is not None:
        if rejection_reason.reason_code is not None:
            metadata["rejection_reason_code"] = rejection_reason.reason_code.value
        if rejection_reason.reason_text is not None:
            metadata["rejection_reason_text"] = rejection_reason.reason_text

    get_rejection_reasons().upsert(
        ids=[recommendation.recommendation_id],
        documents=[document],
        metadatas=[metadata],
    )
