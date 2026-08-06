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

from langsmith import traceable

from app.core.constants import MAX_RETRY, SIMILAR_CASE_TOP_N
from app.core.exceptions import EvidenceNotFoundError, LlmParseError
from app.core.llm_client import get_llm_client
from app.core.schemas import (
    DetectionAlert,
    DetectionConfidence,
    Evaluator,
    EvaluatorChecks,
    HitlStatus,
    LinkedCSInquiry,
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

ETC_LABEL = "기타"
"""원인 라벨이 "기타"면 확신도 상한을 중간으로 캡핑한다(팀 §4-3 도구선택표). 라우팅
(어떤 tool을 쓸지)은 그대로 LLM이 판단하고, 확신도만 후처리로 깎는다 — score_confidence 참고."""

SCOPE_LIMIT_LABELS = ("실물_염색_편차", "실제_원단_문제")
"""텍스트도 사진도 해결 못하는 실제 상품/공급 단계 문제(팀 §4-3 도구선택표 스코프 한계
예외). LLM 호출 없이 고정 문구로 대체하고 확신도는 낮음으로 못박는다 — 생성해봐야
지어내는 것밖에 안 되는 케이스라 애초에 LLM을 안 부른다."""

SCOPE_LIMIT_PROPOSED_TEXT = "상품 자체 또는 공급 단계 문제일 수 있습니다. 실물 상태와 공급처를 확인해보세요."

MAX_ATTEMPTS = MAX_RETRY + 1

assert MAX_ATTEMPTS <= 3, (
    "core/schemas.py의 Evaluator.attempts는 Field(ge=1, le=3)로 상한이 3 고정이다. "
    "MAX_RETRY를 올려서 MAX_ATTEMPTS가 3을 넘으면 assemble()에서 ValidationError가 "
    "터진다 — schemas.py도 같이 안 고치면 MAX_RETRY를 올리지 말 것."
)

RETRY_TEMPERATURES = (0.0, 0.4, 0.7)
"""재시도 회차별 temperature. 1차는 재현성 위해 0.0, 이후엔 올려서 같은 프롬프트라도
다른 답이 나올 여지를 준다 — 실패 피드백과 같이 써야 재시도가 실질적으로 의미 있다.
MAX_ATTEMPTS보다 짧으면 마지막 값을 반복 사용(run()의 인덱싱 참고)."""


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


@traceable
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
    tool_name = result["name"]
    if tool_name not in _TOOL_NAME_TO_PROPOSAL_TYPE:
        raise LlmParseError(
            f"모델이 알 수 없는 tool을 호출함: {tool_name!r} [alert_id={alert.alert_id}]"
        )
    return _TOOL_NAME_TO_PROPOSAL_TYPE[tool_name]


@traceable
async def generate_proposal(
    alert: DetectionAlert,
    proposal_type: ProposalType,
    context: dict,
    *,
    previous_failure: str | None = None,
    temperature: float = 0.0,
) -> Proposal:
    """개선안 생성 — OpenAI 호출(core/llm_client.py 경유), 슬롯 채우기 방식.

    current_text·proposed_text·rationale 셋 다 LLM이 채운다. current_text는 LLM이
    근거 원문에서 "인용"한 것이라고 주장하는 문구 — LLM 자기신고를 그대로 믿지 않고
    evaluate()가 실제 근거(context)와 문자 그대로 대조해 사후 검증한다(§4-3). type
    (route_proposal_type 결과)·target_field·detailpage_grounded는 alert/context에서
    이미 알고 있어 LLM에 맡기지 않는다.

    previous_failure/temperature는 run()의 재시도 전용 — temperature=0.0 고정에
    같은 프롬프트를 그대로 재요청하면 거의 같은 답이 나와서 재시도가 사실상 무의미
    했던 버그를 고친다(2026-07-27). 실패 이유를 프롬프트에 실제로 알려줘야 LLM이
    진짜 다른 시도를 할 수 있다.
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

    if previous_failure:
        prompt += (
            "\n\n## 이전 시도 피드백\n"
            f"직전 시도가 다음 이유로 검증에 실패했습니다: {previous_failure}\n"
            "이번엔 이 문제를 피해서 다시 작성하세요 — 인용은 근거 원문에 실제로 있는 문구만 "
            "그대로 사용하고, rationale에는 원인 분류 라벨을 명확히 언급하세요."
        )

    response = await get_llm_client().complete_json(
        prompt, trace_key=f"alert_id={alert.alert_id}", temperature=temperature
    )

    return Proposal(
        type=proposal_type,
        target_field=alert.main_aspect,
        current_text=response["current_text"],
        proposed_text=response["proposed_text"],
        rationale=response["rationale"],
        detailpage_grounded=(proposal_type == ProposalType.COPY_DRAFT and evidence_text != NO_DETAIL_TEXT),
    )


@traceable
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


def _build_scope_limit_proposal(alert: DetectionAlert) -> Proposal:
    """스코프 한계 원인(SCOPE_LIMIT_LABELS) 전용 — LLM 호출 없이 고정 문구(§4-3).

    텍스트·이미지 어느 쪽으로도 해결 안 되는 원인이라고 표에 이미 정해져 있어서,
    LLM한테 뭘 만들라고 시켜봐야 근거 없이 지어내는 것밖에 안 된다. type은 copy_draft로
    고정(§4-3 표기 그대로), current_text는 인용할 근거가 없으므로 NO_DETAIL_TEXT.
    """
    return Proposal(
        type=ProposalType.COPY_DRAFT,
        target_field=alert.main_aspect,
        current_text=NO_DETAIL_TEXT,
        proposed_text=SCOPE_LIMIT_PROPOSED_TEXT,
        rationale=f"원인 분류: {alert.root_cause.label} — 텍스트·이미지로 해결 불가능한 상품/공급 단계 문제로 판단(§4-3)",
        detailpage_grounded=False,
    )


def _is_consistent_with_root_cause(rationale: str, alert: DetectionAlert) -> bool:
    """rationale이 실제 진단된 원인 라벨을 근거로 삼고 있는지(자기일관성, §2 방법4).

    grounding은 통과해도(current_text는 진짜 인용) rationale이 엉뚱한 사유를 댈 수
    있다 — 이 경우를 잡는다. root_cause가 없으면(원칙적으로 게이트에서 걸러지지만
    방어적으로) 검사 대상이 없으니 통과 처리.

    라벨을 "_"로 쪼갠 조각 단위로 확인한다(예: "사진_색감_오차" → 사진/색감/오차) —
    라벨 문자열을 통째로 has_evidence()에 넣으면 실패한다. LLM은 자연스러운 문장으로
    풀어쓰지("사진 색감이 다르게 촬영되어") 라벨을 언더스코어째로 그대로 베끼지 않고,
    그러면 정규화를 거쳐도 "_"가 살아있는 라벨 원문과 안 겹쳐서 항상 False가 나왔다
    (2026-07-27 버그 발견·수정 — 실제 API 호출 전에 정적으로 재현 확인함). 조각 절반
    이상이 rationale에 있으면 통과로 본다.
    """
    if alert.root_cause is None:
        return True

    segments = [segment for segment in alert.root_cause.label.split("_") if segment]
    if not segments:
        return has_evidence(alert.root_cause.label, rationale)

    matched = sum(1 for segment in segments if has_evidence(segment, rationale))
    return matched >= (len(segments) + 1) // 2


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
    proposal: Proposal, context: dict, alert: DetectionAlert, evaluator: Evaluator
) -> tuple[RecommendationConfidence, str, bool]:
    """개선안 확신도 산정(§4-4) + 캡핑 2단계(§4-3·§5-1).

    베이스 라벨은 **근거(필수) + 보강 2축**으로 정한다:
      - 근거 없음                       → 낮음
      - 근거 있음 + 보강 0개            → 중간
      - 근거 있음 + 보강 1개 이상       → 높음
    보강 축은 원인 일관성(root_cause.consistent)과 유사사례 유무다.

    근거를 필수로 둔 이유: 근거 없이 원인만 일관돼도 본문은 검증되지 않은 문장이라
    확신도를 올릴 근거가 못 된다. 근거없음 경로가 여기로 떨어져 낮음을 유지한다.

    원인 일관성을 축으로 넣은 이유: 원인이 흩어져 있으면(consistent=False) 개선안이
    다수가 아닌 원인을 고칠 위험이 있다. 유사사례 축은 컬렉션2(HITL 실적)가 쌓이기
    전까지 항상 False라, 이 축이 없으면 "높음"이 구조적으로 나올 수 없었다.

    그 위에 캡핑을 두 번 거친다 — 라우팅(어떤 tool을 쓸지)은 그대로 LLM이 판단하고,
    이 두 규칙은 결과 확신도만 후처리로 깎는 안전장치다(§4-3 도구선택표):
    1. 원인 라벨이 "기타"면 상한을 중간으로 캡핑.
    2. SCOPE_LIMIT_LABELS(실물_염색_편차·실제_원단_문제)는 위 계산을 다 건너뛰고
       무조건 낮음으로 확정 — 텍스트·이미지 어느 쪽으로도 해결 안 되는 케이스라
       확신도를 매길 근거 자체가 없다.

    마지막으로 alert.detection_confidence로 한 번 더 캡핑한다(§5-1) — 탐지
    확신도가 중간이면 개선안도 중간이 상한(높음 표시 금지).

    Returns:
        (최종 확신도, 사람이 읽을 한 줄 사유, 캡핑으로 실제 강등됐는지)
    """
    if alert.root_cause and alert.root_cause.label in SCOPE_LIMIT_LABELS:
        return (
            RecommendationConfidence.LOW,
            (
                f"원인 '{alert.root_cause.label}'은 텍스트·이미지로 해결 불가능한 상품/공급 "
                "문제로 판단해 확신도 낮음 고정(§4-3)"
            ),
            True,
        )

    # 근거 축은 evaluator 결과를 쓴다 — proposal.detailpage_grounded 는 copy_draft 전용이라
    # (generate_proposal 참고) image_guide 는 항상 False 여서 확신도가 구조적으로 낮음에
    # 고정됐다. evaluate()가 이미 타입별로 옳은 근거를 대조하므로(copy_draft→상세페이지,
    # image_guide→CS) 그 판정을 그대로 재사용하면 타입 분기를 다시 만들 필요가 없다.
    has_grounding = evaluator.checks.grounding

    # 보강 축 — 근거가 있을 때만 확신도를 끌어올린다.
    has_consistent_cause = alert.root_cause is not None and alert.root_cause.consistent
    has_similar_case = context.get("similar_case") is not None

    # 근거는 필수 조건이다. 근거 없이 원인만 일관돼도 개선안 본문은 허공에 뜬 것이라
    # 확신할 수 없다 — 근거없음 경로(generate_fallback_proposal)가 여기로 떨어진다.
    if not has_grounding:
        raw_base = RecommendationConfidence.LOW
    elif has_consistent_cause or has_similar_case:
        raw_base = RecommendationConfidence.HIGH
    else:
        raw_base = RecommendationConfidence.MEDIUM

    reason = (
        f"근거 검증 {'통과' if has_grounding else '실패'} + "
        f"원인 일관 {'있음' if has_consistent_cause else '없음'} + "
        f"유사 사례 {'있음' if has_similar_case else '없음'} → {raw_base.value}"
    )
    applied_caps: list[str] = []

    is_etc_label = alert.root_cause is not None and alert.root_cause.label == ETC_LABEL
    etc_cap = RecommendationConfidence.MEDIUM if is_etc_label else RecommendationConfidence.HIGH
    base = raw_base if _CONFIDENCE_RANK[raw_base] <= _CONFIDENCE_RANK[etc_cap] else etc_cap
    if base != raw_base:
        applied_caps.append(f"원인 '{ETC_LABEL}'로 {base.value} 상한 캡핑(§4-3)")

    detection_cap = _DETECTION_CONFIDENCE_CAP[alert.detection_confidence]
    final = base if _CONFIDENCE_RANK[base] <= _CONFIDENCE_RANK[detection_cap] else detection_cap
    if final != base:
        applied_caps.append(f"탐지 확신도 {alert.detection_confidence.value}으로 {final.value} 캡핑")

    if applied_caps:
        reason += " (" + "; ".join(applied_caps) + ")"

    return final, reason, final != raw_base


def assemble(
    alert: DetectionAlert,
    proposal: Proposal,
    evaluator: Evaluator,
    context: dict,
) -> Recommendation:
    """단계 결과 조립 → Recommendation. hitl은 항상 대기 상태로 시작."""
    confidence, confidence_reason, capped = score_confidence(proposal, context, alert, evaluator)

    return Recommendation(
        # TODO(2026-08-03): 같은 alert_id가 재처리되면(백엔드 재시도·배치 재실행)
        # 문구가 다른 개선안이 중복 저장된다. 백엔드가 alert_id 유니크 키로 upsert.
        recommendation_id=f"REC-{uuid.uuid4().hex[:12]}",
        alert_id=alert.alert_id,
        created_at=datetime.now(timezone.utc),
        proposal=proposal,
        # citations는 CS 원문 인용용이다(evidence.inquiry_ids 중 실제로 인용한 문의).
        # 빈 리스트가 정직한 값 — quote=""인 Citation은 "인용이 있다"는 오해를 만든다.
        # TODO(2026-08-03): 조회 경로는 원본 DB(cs·reviews) 직접 읽기로 확정.
        # item_id ↔ cs/reviews PK 연결만 확인되면 실제 인용으로 채울 것(§4-3).
        citations=[],
        evaluator=evaluator,
        similar_case=context.get("similar_case"),
        recommendation_confidence=confidence,
        confidence_reason=confidence_reason,
        capped_by_detection=capped,
        hitl_status=HitlStatus.PENDING,
        hitl_feedback=None,
    )


@traceable
async def run(alert: DetectionAlert) -> Recommendation | None:
    """오케스트레이터: 트리거 게이트 → 근거조회 → 라우팅(LLM) → (생성→검증) 최대 3회
    → (그래도 실패하면) 근거없음 경로 → 조립.

    route_proposal_type()은 alert 하나당 1회만 — 재시도는 "같은 도구로 다시 생성"이지
    "도구를 바꿔서 다시 판단"이 아니다. MAX_ATTEMPTS를 다 써도 grounding이 안 되면
    generate_fallback_proposal()로 넘어간다(§2 방법1) — 억지로 근거 있는 척 넘기지
    않는다. LLM 호출 있는 함수는 async로 통일한다(협업 규칙 5).

    SCOPE_LIMIT_LABELS(§4-3 스코프 한계)면 라우팅·생성 둘 다 건너뛰고 고정 문구로
    바로 조립한다 — LLM한테 물어봐도 답이 안 바뀌는 케이스라 호출 자체를 안 한다.
    """
    if not should_generate(alert):
        return None

    if alert.root_cause and alert.root_cause.label in SCOPE_LIMIT_LABELS:
        proposal = _build_scope_limit_proposal(alert)
        evaluator = Evaluator(
            passed=True,
            attempts=1,
            checks=EvaluatorChecks(
                grounding=False,  # 대조할 근거 자체가 없음 — fallback_guide와 동일하게 정직히 기록
                consistency=_is_consistent_with_root_cause(proposal.rationale, alert),
                actionability=_is_actionable(proposal.proposed_text),
            ),
            failure_reason="스코프 한계 원인이라 근거 검증 대상 자체가 없음(§4-3)",
        )
        return assemble(alert, proposal, evaluator, {"similar_case": None})

    context = retrieve_context(alert)
    proposal_type = await route_proposal_type(alert, context)

    attempt = 1
    proposal = await generate_proposal(
        alert, proposal_type, context, temperature=RETRY_TEMPERATURES[0]
    )
    evaluator = evaluate(proposal, alert, context, attempt=attempt)
    while not evaluator.passed and attempt < MAX_ATTEMPTS:
        attempt += 1
        temperature = RETRY_TEMPERATURES[min(attempt - 1, len(RETRY_TEMPERATURES) - 1)]
        proposal = await generate_proposal(
            alert,
            proposal_type,
            context,
            previous_failure=evaluator.failure_reason,
            temperature=temperature,
        )
        evaluator = evaluate(proposal, alert, context, attempt=attempt)

    if not evaluator.passed:
        proposal = await generate_fallback_proposal(alert, proposal_type)
        evaluator = Evaluator(
            passed=True,
            attempts=MAX_ATTEMPTS,
            checks=EvaluatorChecks(
                grounding=False,  # 근거 자체를 안 씀 — 정직한 기록
                # consistency/actionability는 예전에 evaluate()가 스텁이던 시절 True로
                # 하드코딩해뒀던 게 evaluate() 실판정 구현 이후에도 안 바뀌고 남아있던
                # 버그였다(2026-07-27 발견·수정) — LLM 호출 없이 계산 가능해서 실제로 돌린다.
                consistency=_is_consistent_with_root_cause(proposal.rationale, alert),
                actionability=_is_actionable(proposal.proposed_text),
            ),
            failure_reason=f"근거를 찾지 못해 일반 가이드로 대체(MAX_RETRY={MAX_RETRY} 소진)",
        )

    return assemble(alert, proposal, evaluator, context)


async def generate_for_alert(
    alert: DetectionAlert,
    inquiries: list[LinkedCSInquiry],
) -> Recommendation | None:
    """배치 진입점 — 알림 1건에 개선안 1건. `run()`을 감싸고 예외를 삼킨다.

    `run()`과 달리 **예외를 던지지 않는다.** 배치가 알림 20건을 도는 중에 1건이 터져도
    나머지가 발행돼야 하고, 개선안 실패가 알림 발행까지 막으면 안 된다 — 실패하면
    None 을 돌려주고 payload 의 `recommendation` 이 null 로 나간다.

    게이트(`recommended_action != "개선안 생성"`)면 LLM 을 부르지 않고 None 이다.
    호출부가 `should_generate()`로 미리 걸러도 되고(dry-run 호출 수 추정), 안 걸러도
    결과는 같다.

    Args:
        alert: 탐지 알림.
        inquiries: `alert.evidence.inquiry_ids` 로 원본 DB(`cs`·`reviews`)에서 조인해 온
            CS 원문. `citations` 를 채우는 유일한 재료다.
            ⚠️ 지금은 받아만 두고 쓰지 않는다 — `ClassifiedItem.item_id` 와 두 테이블 PK
            의 연결이 미확인(백엔드 C4)이라 인용을 못 만든다. 가짜로 채우지 않는다.

    Returns:
        개선안. 게이트가 닫혔거나 생성이 실패하면 None.

    Raises:
        NotImplementedError: 미구현. **구현 후에는 아무 예외도 던지지 않는다.**
    """
    raise NotImplementedError("generate_for_alert 미구현 — Agent3 배치 진입점 작업 중")


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
    # §4-2 스펙: "원인 라벨 + CS 요약 + 개선안 본문". 예전엔 CS 요약 대신 aspect를 넣는
    # 실수가 있었다(2026-07-27 발견·수정) — _summarize_cs_evidence()로 실제 CS 요약을 쓴다.
    cs_summary = _summarize_cs_evidence(alert)
    document = f"{root_cause_label} {cs_summary} {proposed_text}"

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
