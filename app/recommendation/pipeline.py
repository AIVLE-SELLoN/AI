"""담당: 지인 (Agent3) — 개선안 생성 파이프라인.

순수 Python + 타입 힌트로만 짠다. LangGraph 를 검토했다가 접었다 — HITL 이
`/generate`·`/hitl` 두 개의 독립 stateless 호출이라 interrupt·checkpointer 가 푸는
"실행 중 상태를 멈췄다 재개" 문제 자체가 없고, 그래프로 옮길 대상도 선형 파이프라인
+ 재시도 루프 하나뿐이다. 단계를 독립 함수로 쪼갠 실익은 프레임워크 이식이 아니라
단계별 단위 테스트다.

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

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from langsmith import traceable

from app.core.constants import CS_QUOTE_TOP_N, MAX_RETRY, SIMILAR_CASE_TOP_N
from app.core.exceptions import EvidenceNotFoundError, LlmParseError
from app.core.ids import build_recommendation_id
from app.core.llm_client import get_llm_client
from app.core.schemas import (
    Citation,
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
    TENANT_METADATA_KEY,
    current_tenant,
    get_detail_pages,
    get_documents,
    get_rejection_reasons,
    query_documents,
    scoped_document_id,
    upsert_documents,
)
from app.recommendation.grounding import has_evidence, verify_grounding

logger = logging.getLogger(__name__)

NO_DETAIL_TEXT = "정보 없음"
"""상세페이지 미등록/빈 값 표기 — 근거없음 경로를 유발하는 값이라 상수로 뺀다."""

ACTIONABLE_TEXT_MARKERS = ("하세요", "해보세요", "바랍니다", "권장", "검토", "확인", "진행", "추가")
"""proposed_text가 실행 가능한 안내문인지 판정하는 키워드 휴리스틱(actionability).
팀 합의된 기준이 따로 없어 자체 설계한 임시 규칙 — 필요시 조정."""

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
COPY_DRAFT_PROMPT_PATH = PROMPTS_DIR / "copy_draft_v1.md"
IMAGE_GUIDE_PROMPT_PATH = PROMPTS_DIR / "image_guide_v2.md"
ROUTING_PROMPT_PATH = PROMPTS_DIR / "route_proposal_type_v2.md"
FALLBACK_GUIDE_PROMPT_PATH = PROMPTS_DIR / "fallback_guide_v1.md"

PROMPT_HEADER_SEPARATOR = "---"
"""프롬프트 파일의 머리말(개발자용)과 본문(모델에게 보낼 것)을 가르는 줄."""


def load_prompt(path: Path) -> str:
    """프롬프트 파일에서 **모델에게 보낼 본문만** 잘라낸다.

    프롬프트 md 는 맨 위에 개발자용 머리말을 둔다 — 제목, "구버전 삭제 금지" 관례,
    v1 대비 무엇이 왜 바뀌었는지. 파일을 통째로 읽어 보내면 그게 다 지시문에 섞인다.

    실제로 두 가지가 문제였다:
      1. 토큰. `route_proposal_type_v2.md` 는 머리말이 전체의 45% 다.
      2. **더 나쁜 건 내용이다.** 변경 이력에 "예전엔 요약을 되풀이하면 통과했다"
         같은 과거 결함 설명이 들어가는데, 그건 모델에게 우회로를 알려주는 것이다.

    그래서 첫 `---` 줄을 경계로 앞은 버린다. 구분선이 없으면 전체를 보낸다(안전한
    쪽) — 새 프롬프트를 머리말 없이 써도 깨지지 않게 하기 위해서다.

    첫 번째 구분선만 본다. 본문 안에 `---` 를 또 쓰면 그 뒤만 남는 게 아니라
    **첫 줄 기준으로만 잘리므로** 본문은 온전하다. 다만 머리말 안에 `---` 를 두 번
    쓰면 첫 번째에서 잘려 나머지 머리말이 본문으로 딸려 들어간다.
    """
    raw = path.read_text(encoding="utf-8")
    lines = raw.splitlines()  # CRLF 체크아웃에서도 동작해야 한다
    for index, line in enumerate(lines):
        if line.strip() == PROMPT_HEADER_SEPARATOR:
            return "\n".join(lines[index + 1 :]).strip()
    return raw.strip()


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
"""탐지확신도→개선안확신도 상한 매핑. should_generate 게이트 통과분은 실무적으로
HIGH/MEDIUM만 들어오지만, LOW/NOT_APPLICABLE도 방어적으로 가장 낮은 상한을 매핑해둔다."""

ETC_LABEL = "기타"
"""원인 라벨이 "기타"면 확신도 상한을 중간으로 캡핑한다(팀 도구선택표). 라우팅
(어떤 tool을 쓸지)은 그대로 LLM이 판단하고, 확신도만 후처리로 깎는다 — score_confidence 참고."""

SCOPE_LIMIT_LABELS = ("실물_염색_편차", "실제_원단_문제")
"""텍스트도 사진도 해결 못하는 실제 상품/공급 단계 문제(팀 도구선택표 스코프 한계
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


def retrieve_context(
    alert: DetectionAlert,
    inquiries: Sequence[LinkedCSInquiry] = (),
) -> dict:
    """근거 조회 — copy_draft/image_guide 후보 근거를 둘 다 미리 가져온다.

    라우팅이 LLM 몫(route_proposal_type)이라, retrieve_context는 어느 쪽이 뽑힐지
    모른 채로 두 후보를 같이 준비해서 모델에게 보여준다:

    - detail_text: 컬렉션1(상세페이지) get(정확 필터) — 임베딩 안 거침. 미등록
      SKU/빈 값이면 NO_DETAIL_TEXT.
    - cs_quotes: image_guide 의 **근거 원문**. `evidence.inquiry_ids` 로 조회한 실제
      고객 문의다. 없으면 NO_DETAIL_TEXT.
    - cs_summary: root_cause 통계 한 줄. **근거가 아니라 맥락**이다.

    **cs_quotes 와 cs_summary 를 한 문자열로 합치지 말 것.** cs_summary 는 우리가
    f-string 으로 만든 문장이라, grounding 대조 대상에 넣으면 LLM 이 그 문장을
    되풀이하는 것만으로 통과한다 — 검증이 자기 자신을 대조하는 꼴이 된다.
    **evaluate() 가 대조하는 건 cs_quotes 하나뿐이다.**

    컬렉션2(과거·반려 사례)는 공통이고, 0건이면 similar_case=None 이다 — 반려 적재
    전에는 항상 0건이라 정상 상태다.
    """
    detail_text = _get_detail_page_text(alert)
    cs_quotes = _collect_cs_quotes(inquiries)
    cs_summary = _summarize_cs_evidence(alert)

    rejection_reasons = get_rejection_reasons()
    query_text = alert.root_cause.label if alert.root_cause else alert.main_aspect.value
    tenant = current_tenant()
    # 회사 축을 반드시 같이 좁힌다 — `aspect` 만으로 좁히면 **다른 회사의 반려 사례**가
    #    similar_case 로 새어 나온다(`vectordb.current_tenant` docstring). 문서 ID 에
    #    회사 접두어가 붙어도 이 필터가 없으면 조회는 그대로 뚫린다 — 둘은 짝이다.
    similar_rows = query_documents(
        rejection_reasons,
        query_text=query_text,
        n_results=SIMILAR_CASE_TOP_N,
        where={
            "$and": [
                {"aspect": alert.main_aspect.value},
                {TENANT_METADATA_KEY: tenant},
            ]
        },
    )
    similar_case = similar_rows[0]["document"] if similar_rows else None

    return {
        "detail_text": detail_text,
        "cs_quotes": cs_quotes,
        "cs_summary": cs_summary,
        "similar_case": similar_case,
    }


def _get_detail_page_text(alert: DetectionAlert) -> str:
    """컬렉션1(상세페이지) get — 임베딩 안 거침. 미등록 SKU/빈 값이면 NO_DETAIL_TEXT."""
    detail_pages = get_detail_pages()
    tenant = current_tenant()
    # 회사 축을 반드시 같이 좁힌다 — `product_group_id` 가 회사별 시퀀스라 A사 P001 과
    #    B사 P001 이 구분되지 않는다. 이 필터가 없으면 **다른 회사 상세페이지**를 개선안의
    #    인용 근거로 쓰고, 그 문장이 `citations` 에 박제돼 셀러 화면까지 나간다.
    #    시딩 ID·metadata 와 셋이 짝이다(`scripts/seed_vectordb.py`).
    detail_rows = get_documents(
        detail_pages,
        where={
            "$and": [
                {TENANT_METADATA_KEY: tenant},
                {"product_group_id": alert.product_group_id},
                {"channel": alert.channel.value},
                {"aspect": alert.main_aspect.value},
            ]
        },
    )
    if detail_rows:
        return detail_rows[0]["document"]

    _log_detail_page_miss(alert, detail_pages, tenant)
    return NO_DETAIL_TEXT


def _log_detail_page_miss(alert: DetectionAlert, collection: Any, tenant: str) -> None:
    """0건의 사유를 셋으로 가른다 — 조치가 전부 다르기 때문이다.

    | 사유 | 조치 | 수준 |
    |---|---|---|
    | 컬렉션이 통째로 비었다 | 시딩 실행 | WARNING |
    | 이 회사(`company_id`) 문서가 0건이다 | **시딩 실행** | WARNING |
    | 이 상품만 없다 | 상품 등록 | INFO |

    `.chroma/` 가 gitignore 라 시딩(`scripts/seed_vectordb.py`) 전 로컬은 비어 있는데,
    그 상태가 "상세페이지 미등록" 과 **같은 모양(조회 0건)** 이라 구분이 안 됐다.
    가운데 줄은 회사 축이 만든 사유다 — 축 없이 시딩한 컬렉션은 조회 필터가 전건을
    걸러내는데 `count()` 는 504 라 첫 줄에 안 걸린다. 조치는 시딩인데 INFO 로 떨어지면
    상품 등록 쪽을 파게 된다.

    **`--reset` 은 안내하지 않는다.** 가운데 줄에는 런타임에서 구분할 수 없는 두 상태가
    겹쳐 있다 — 구형 문서만 있는 경우와, 다른 회사는 정상인데 이 회사만 아직 없는 경우.
    후자에서 실행하면 **다른 회사 문서와 HITL 반려 이력까지 지운다.** 일반 시딩으로
    복구되므로 파괴적 명령을 안내할 이유가 없다.

    **그 두 상태를 여기서 가르려 하지 말 것.** 알림별이 아니라 컬렉션 전체의 성질이라
    미스마다 재계산하는 게 틀렸고, 핫 패스라 전수를 못 봐 표본으로 어림잡게 된다.
    정확한 판별은 `scripts/seed_vectordb.py` 가 시딩 직후 전수로 한다.

    로그만 남기고 예외는 안 던진다 — 근거 0건 처리는 `run()` 이 이미 한다. 여기서 막으면
    그 경로가 두 벌이 된다.
    """
    if collection.count() == 0:
        logger.warning(
            "[상세페이지 컬렉션 비어 있음] 시딩이 안 된 환경일 수 있습니다 — "
            "`python scripts/seed_vectordb.py` 확인. alert=%s",
            alert.alert_id,
        )
        return

    # 임베딩을 안 타는 get 이고 limit=1 이라 비용이 사실상 없다.
    if not get_documents(collection, where={TENANT_METADATA_KEY: tenant}, limit=1):
        logger.warning(
            "[상세페이지 회사 문서 0건] 컬렉션에 문서는 있는데 company_id=%s 것이 "
            "하나도 없습니다 — 회사 축 없이 시딩됐거나 이 회사가 처음입니다. "
            "둘 다 `python scripts/seed_vectordb.py` 로 해결됩니다. "
            "⚠️ `--reset` 은 쓰지 마세요: 다른 회사 문서와 컬렉션2(반려 이력)까지 지웁니다. "
            "alert=%s",
            tenant,
            alert.alert_id,
        )
        return

    logger.info(
        "[상세페이지 미등록] company_id=%s product_group_id=%s channel=%s aspect=%s alert=%s",
        tenant,
        alert.product_group_id,
        alert.channel.value,
        alert.main_aspect.value,
        alert.alert_id,
    )


def quotable_inquiries(inquiries: Sequence[LinkedCSInquiry]) -> list[LinkedCSInquiry]:
    """프롬프트에 실을 문의 = 인용 후보. **두 곳이 반드시 같은 목록을 봐야 한다.**

    `_collect_cs_quotes`(모델에게 보여줄 것)와 `_build_citations`(인용을 역추적할 것)가
    다르게 고르면 "프롬프트엔 실렸는데 인용 대상엔 없는" 문의가 생긴다. 한 곳으로 묶어
    갈라질 수 없게 한다.

    빈 원문을 거른 **뒤** 자른다. 순서를 바꾸면 앞쪽에 빈 게 섞였을 때 근거가 그만큼 준다.
    """
    return [inquiry for inquiry in inquiries if inquiry.raw_text.strip()][:CS_QUOTE_TOP_N]


def evidence_for(proposal_type: ProposalType, context: dict) -> str:
    """도구별 근거 원문을 고른다 — copy_draft→상세페이지 / image_guide→CS 원문.

    `generate_proposal`(무엇을 보여줄지)·`evaluate`(무엇과 대조할지)·`run`(근거가
    있는지)이 **같은 함수를 쓴다.** 셋이 각자 슬롯을 고르면 "프롬프트엔 A 를 주고
    검증은 B 와 하는" 어긋남이 생기는데, 그게 바로 이번에 고친 자기참조 버그의 모양이다.

    image_guide 는 `cs_quotes`(고객 원문)다. `cs_summary`(우리가 만든 통계 문장)를
    돌려주면 안 된다 — retrieve_context docstring 참고.
    """
    return context["detail_text"] if proposal_type == ProposalType.COPY_DRAFT else context["cs_quotes"]


def _collect_cs_quotes(inquiries: Sequence[LinkedCSInquiry]) -> str:
    """image_guide 의 근거 원문 — 실제 고객 문의를 그대로 이어 붙인다.

    앞에서부터 CS_QUOTE_TOP_N 건까지만 싣는다. 원문이 빈 항목은 버린다 — 빈 문자열은
    grounding 대조에서 "아무거나 통과"로 작동한다(`has_evidence` 가 빈 source 를
    False 로 막지만, 다른 원문과 섞여 있으면 경계가 흐려진다).

    한 건도 없으면 NO_DETAIL_TEXT 를 돌려준다. 그러면 `run()` 이 개선안 생성을 아예
    건너뛰고 None 을 돌려준다 — **근거가 없는데 있는 척하지 않는 게 의도된 동작이다.**
    """
    quotable = quotable_inquiries(inquiries)
    if not quotable:
        return NO_DETAIL_TEXT
    return "\n".join(f"- {inquiry.raw_text.strip()}" for inquiry in quotable)


def _summarize_cs_evidence(alert: DetectionAlert) -> str:
    """root_cause 통계 한 줄 요약 — **근거가 아니라 맥락이다**.

    프롬프트에 "몇 건 중 몇 건" 이라는 규모를 보여주는 용도이고, 컬렉션2 적재
    (`record_hitl_outcome`)의 검색용 텍스트에도 쓴다.

    **grounding 대조 대상으로 쓰지 말 것.** 이 문자열은 우리가 만든 문장이라
    LLM 이 그대로 되풀이하면 무조건 통과한다. 대조는 `_collect_cs_quotes()` 가
    만든 실제 원문(cs_quotes)하고만 한다.
    """
    if alert.root_cause is None:
        return NO_DETAIL_TEXT
    return f"CS {alert.root_cause.total}건 중 {alert.root_cause.count}건이 '{alert.root_cause.label}' 관련 언급"


@traceable
async def route_proposal_type(alert: DetectionAlert, context: dict) -> ProposalType:
    """개선안 도구 라우팅 — LLM이 tool 호출로 직접 판단.

    copy_draft/image_guide 두 tool과 실제 근거(상세페이지 원문 + CS 요약)를 모델에게
    보여주고, tool_choice="required"로 반드시 둘 중 하나를 호출하게 한다. 판단
    로직을 규칙으로 코드에 박지 않는 게 핵심 — 규칙 기반으로 되돌리면 workflow로
    후퇴한다.
    """
    root_cause_label = alert.root_cause.label if alert.root_cause else "미상"
    prompt = load_prompt(ROUTING_PROMPT_PATH).format(
        aspect=alert.main_aspect.value,
        root_cause_label=root_cause_label,
        detail_text=context["detail_text"],
        cs_quotes=context["cs_quotes"],
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

    # 라우팅은 Agent3 의 유일한 자율 판단 지점이라, 왜 그 도구를 골랐는지가 남아야
    # 나중에 이상한 선택을 추적할 수 있다. 트레이싱이 꺼져 있어도 남는 유일한 단서다.
    logger.info(
        "라우팅 alert=%s 원인=%s → %s (사유: %s)",
        alert.alert_id,
        root_cause_label,
        tool_name,
        (result.get("arguments") or {}).get("reason", "-"),
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
    evaluate가 실제 근거(context)와 문자 그대로 대조해 사후 검증한다. type
    (route_proposal_type 결과)·target_field·detailpage_grounded는 alert/context에서
    이미 알고 있어 LLM에 맡기지 않는다.

    previous_failure/temperature는 run()의 재시도 전용이다 — temperature=0.0 에 같은
    프롬프트를 그대로 재요청하면 거의 같은 답이 돌아와 재시도가 무의미해진다. 실패
    이유를 프롬프트에 실제로 알려줘야 LLM이 진짜 다른 시도를 할 수 있다.
    """
    root_cause_label = alert.root_cause.label if alert.root_cause else "미상"
    anomaly = f"{alert.channel.value} · {alert.main_aspect.value} 이상 (원인: {root_cause_label})"
    rejection_reasons = context.get("similar_case") or "없음"

    evidence_text = evidence_for(proposal_type, context)

    if proposal_type == ProposalType.COPY_DRAFT:
        prompt = load_prompt(COPY_DRAFT_PROMPT_PATH).format(
            anomaly=anomaly, detail_pages=evidence_text, rejection_reasons=rejection_reasons
        )
    else:
        # 근거는 cs_quotes(실제 원문) 하나뿐이다. cs_summary 는 규모를 알려주는 맥락으로
        # 같이 넘기지만 grounding 대조 대상이 아니다(retrieve_context docstring 참고).
        prompt = load_prompt(IMAGE_GUIDE_PROMPT_PATH).format(
            anomaly=anomaly,
            cs_quotes=evidence_text,
            cs_summary=context["cs_summary"],
            rejection_reasons=rejection_reasons,
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
    """근거없음 경로 — grounding이 MAX_RETRY번 실패했을 때만 호출된다.

    특정 인용 없이 일반 가이드 문구로 대체한다. current_text는 NO_DETAIL_TEXT로
    고정 — "근거를 특정하지 못했다"를 그대로 반영한다. evaluate()를 다시 태우지
    않는다(run() 참고) — 애초에 근거가 없다고 선언하는 경로라 재검증할 인용 자체가
    없다.
    """
    root_cause_label = alert.root_cause.label if alert.root_cause else "미상"
    anomaly = f"{alert.channel.value} · {alert.main_aspect.value} 이상 (원인: {root_cause_label})"

    prompt = load_prompt(FALLBACK_GUIDE_PROMPT_PATH).format(anomaly=anomaly)
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
    """스코프 한계 원인(SCOPE_LIMIT_LABELS) 전용 — LLM 호출 없이 고정 문구.

    텍스트·이미지 어느 쪽으로도 해결 안 되는 원인이라고 표에 이미 정해져 있어서,
    LLM한테 뭘 만들라고 시켜봐야 근거 없이 지어내는 것밖에 안 된다. type은 copy_draft로
    고정(표기 그대로), current_text는 인용할 근거가 없으므로 NO_DETAIL_TEXT.
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
    """rationale이 실제 진단된 원인 라벨을 근거로 삼고 있는지(자기일관성).

    grounding은 통과해도(current_text는 진짜 인용) rationale이 엉뚱한 사유를 댈 수
    있다 — 이 경우를 잡는다. root_cause가 없으면(원칙적으로 게이트에서 걸러지지만
    방어적으로) 검사 대상이 없으니 통과 처리.

    라벨을 "_"로 쪼갠 조각 단위로 확인한다(예: "사진_색감_오차" → 사진/색감/오차).
    통째로 대조하면 항상 False다 — LLM은 "사진 색감이 다르게 촬영되어"처럼 풀어쓰지
    언더스코어째 베끼지 않아서, 정규화를 거쳐도 라벨 원문과 안 겹친다. 조각 절반
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
    """proposed_text가 실행 가능한 안내문 형태인지(actionability).

    ACTIONABLE_TEXT_MARKERS 키워드 휴리스틱 — "느낌"이나 "확신도" 같은 관찰만 있고
    행동 지시가 없는 문장을 거른다.
    """
    return any(marker in proposed_text for marker in ACTIONABLE_TEXT_MARKERS)


def evaluate(proposal: Proposal, alert: DetectionAlert, context: dict, attempt: int = 1) -> Evaluator:
    """Evaluator 검증 — 3기준 실제 판정.

    - grounding: proposal.current_text(LLM이 "이게 근거다"라고 주장한 인용)가 실제
      근거(context["detail_text"] 또는 context["cs_quotes"], 도구별 분리)에
      있는지 grounding.py로 대조. 없는 내용을 인용했다고 우기면 실패한다.
      image_guide 쪽 대조 대상은 **cs_quotes(고객 원문)이지 cs_summary가 아니다.**
      cs_summary는 우리가 만든 문장이라 그걸 대조하면 LLM이 되풀이만 해도 통과한다.
    - consistency: rationale이 실제 원인 라벨(alert.root_cause.label)을 근거로
      삼고 있는지 — grounding은 통과했는데 사유가 엉뚱한 경우를 잡는다.
    - actionability: proposed_text가 행동 지시 형태인지 키워드로 확인.

    attempt는 run()의 재시도 루프가 몇 번째 시도인지 넘겨준다(1부터 시작).
    """
    evidence_text = evidence_for(proposal.type, context)

    failure_reasons: list[str] = []

    if evidence_text == NO_DETAIL_TEXT:
        # 근거가 없으면 무조건 실패다. 대조할 원문이 없는데 통과시키면 "검증했다"는
        # 거짓 기록이 남는다. 프롬프트가 근거 없을 때 current_text 에 "정보 없음" 을
        # 쓰라고 지시하면 has_evidence 가 "정보 없음" 끼리 대조해 **통과해버린다** —
        # 고객 문의 0건인데 확신도 높음이 나간다. run() 이 이 상황을 미리 걸러 여기까지
        # 오지 않는 게 정상이지만, evaluate() 를 직접 부르는 호출부까지 막는 방어선이다.
        grounding_ok = False
        failure_reasons.append("근거 원문 자체가 없음(NO_DETAIL_TEXT) — 대조 대상 없음")
    else:
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
    """개선안 확신도 산정 + 캡핑 2단계.

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
    이 두 규칙은 결과 확신도만 후처리로 깎는 안전장치다(도구선택표):
    1. 원인 라벨이 "기타"면 상한을 중간으로 캡핑.
    2. SCOPE_LIMIT_LABELS(실물_염색_편차·실제_원단_문제)는 위 계산을 다 건너뛰고
       무조건 낮음으로 확정 — 텍스트·이미지 어느 쪽으로도 해결 안 되는 케이스라
       확신도를 매길 근거 자체가 없다.

    마지막으로 alert.detection_confidence로 한 번 더 캡핑한다 — 탐지
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


def _build_citations(
    proposal: Proposal,
    evaluator: Evaluator,
    inquiries: Sequence[LinkedCSInquiry],
) -> list[Citation]:
    """`current_text` 가 실제로 인용한 CS 문의를 역추적한다.

    `evidence.inquiry_ids` 를 통째로 싣지 않는다 — `citations` 의 정의가 "근거가 된
    문의" 가 아니라 **"실제로 인용한 문의"** 라서다. 전부 실으면 인용하지 않은 문의까지
    인용된 것처럼 보인다.

    판정은 grounding 과 **같은 함수**(`has_evidence`)로 한다. 기준이 갈리면 "grounding
    은 통과인데 citations 는 비어 있다" 가 생긴다. copy_draft 는 상세페이지를 인용하므로
    빈 리스트가 정직한 값이고, fallback_guide·scope_limit 는 `grounding=False` 라 같이
    걸러진다.

    같은 문구가 여러 문의에 있으면 **quote 가 동일한 Citation 이 N 개** 나온다. 집계에서
    "고객 N 명이 그렇게 말했다" 로 읽으면 안 된다 — 문구는 하나고, 그 문구가 등장한
    문의가 N 건이라는 뜻이다.
    """
    if proposal.type != ProposalType.IMAGE_GUIDE or not evaluator.checks.grounding:
        return []

    return [
        Citation(inquiry_id=inquiry.item_id, quote=proposal.current_text)
        for inquiry in quotable_inquiries(inquiries)
        if has_evidence(proposal.current_text, inquiry.raw_text)
    ]


def assemble(
    alert: DetectionAlert,
    proposal: Proposal,
    evaluator: Evaluator,
    context: dict,
    inquiries: Sequence[LinkedCSInquiry] = (),
) -> Recommendation:
    """단계 결과 조립 → Recommendation. hitl은 항상 대기 상태로 시작."""
    confidence, confidence_reason, capped = score_confidence(proposal, context, alert, evaluator)

    return Recommendation(
        # alert_id 에서 파생 — 같은 알림을 재처리하면 같은 ID 다(guideline_id 와 같은 규칙).
        # 예전 uuid4 는 재실행마다 달라져서 payload 를 글자 단위로 대조할 수 없었다.
        recommendation_id=build_recommendation_id(alert.alert_id),
        alert_id=alert.alert_id,
        created_at=datetime.now(timezone.utc),
        proposal=proposal,
        # evidence.inquiry_ids 중 current_text가 실제로 인용한 문의만 담긴다.
        # copy_draft·fallback·scope_limit 경로는 빈 리스트가 정직한 값이다(_build_citations).
        citations=_build_citations(proposal, evaluator, inquiries),
        evaluator=evaluator,
        similar_case=context.get("similar_case"),
        recommendation_confidence=confidence,
        confidence_reason=confidence_reason,
        capped_by_detection=capped,
        hitl_status=HitlStatus.PENDING,
        hitl_feedback=None,
    )


class SkipReason(str, Enum):
    """개선안이 안 나온 사유. **호출부가 데이터 갭과 고장을 가르는 근거다.**

    `run()`·`generate_for_alert()` 는 아래 사유 전부에 똑같이 `None` 을 돌려주는데
    성격이 전혀 다르다 — `NO_EVIDENCE` 는 "입력에 근거가 없다"는 사실이고 파이프라인은
    정상이며, `ERROR` 는 고장이다. 배치가 이걸 로그 문자열로 되짚지 않도록 값으로
    돌려준다(`RecommendationOutcome`).
    """

    GATE_CLOSED = "게이트_미충족"
    """`recommended_action` 이 '개선안 생성' 이 아님 — 조치 6종의 정상값."""

    NO_EVIDENCE = "근거_0건"
    """상세페이지 미등록 + CS 원문 0건. **데이터 갭이라 배치 실패가 아니다** —
    이것까지 실패로 세면 배치가 상시 종료코드 1로 끝나 진짜 장애 신호가 무뎌진다."""

    ROUTED_WITHOUT_EVIDENCE = "라우팅_근거없음"
    """근거가 한쪽엔 있었는데 모델이 **빈 쪽** 도구를 골랐다. 쓸 수 있던 근거를 버린
    것이라 `NO_EVIDENCE` 와 구분하지만, **배치 실패로는 세지 않는다** —
    `RecommendationOutcome.is_routing_miss` 참고. 라우팅 프롬프트를 손볼 근거가
    여기서 나오므로 요약에는 따로 센다."""

    ERROR = "생성_예외"
    """생성 중 예외. 사유는 `detail` 에 `repr(exc)` 로 담긴다."""


@dataclass(frozen=True)
class RecommendationOutcome:
    """개선안 + **없을 때의 사유.** `recommendation` 이 None 이면 `reason` 이 항상 있다.

    `run()`·`generate_for_alert()` 의 `Recommendation | None` 계약은 그대로 두고,
    사유가 필요한 배치만 이 객체를 받는다.
    """

    recommendation: Recommendation | None = None
    reason: SkipReason | None = None
    detail: str | None = None
    """사람이 읽을 한 줄. 배치 요약의 실패 사유로 그대로 나간다."""

    @property
    def is_evidence_gap(self) -> bool:
        """근거가 아예 없어서 안 만든 것인가. **배치가 실패로 세지 않는다.**

        판정을 여기 두는 이유: "어떤 사유가 실패인가"가 호출부마다 갈리면, 사유를
        하나 추가할 때 배치·테스트·모니터링이 따로 놀게 된다.
        """
        return self.reason is SkipReason.NO_EVIDENCE

    @property
    def is_routing_miss(self) -> bool:
        """모델이 빈 쪽 도구를 골라서 못 만든 것인가. **배치가 실패로 세지 않는다.**

        `is_evidence_gap` 과 사유는 다르지만 **종료코드에서는 똑같이 뺀다** — 근본 원인이
        상세페이지 미등록으로 같고(mock 504행 중 489행이 "정보 없음"), 모델의 선택을
        코드로 강제하지 않는 것도 의도다. 안 고치기로 한 것을 매일 실패로 세면 배치가
        상시 종료코드 1 로 끝나 진짜 장애가 묻힌다.

        대신 요약에는 따로 센다(`daily.py` 의 `routing_miss`) — 여기가 조용해지면 라우팅
        프롬프트를 손볼 근거가 사라진다.
        """
        return self.reason is SkipReason.ROUTED_WITHOUT_EVIDENCE

    @property
    def counts_as_failure(self) -> bool:
        """배치가 **실패로 세는가.** 종료코드가 이 값으로 갈린다.

        개선안이 나온 경우와 위 두 사유는 실패가 아니다. 남는 것은 `ERROR`(생성 중
        예외)와 `GATE_CLOSED`(배치가 미리 걸러 여기까지 안 온다) 뿐이다.
        """
        if self.recommendation is not None:
            return False
        return not (self.is_evidence_gap or self.is_routing_miss)


@traceable
async def run_with_outcome(
    alert: DetectionAlert,
    inquiries: Sequence[LinkedCSInquiry] = (),
) -> RecommendationOutcome:
    """오케스트레이터: 트리거 게이트 → 근거조회 → 라우팅(LLM) → (생성→검증) 최대 3회
    → (그래도 실패하면) 근거없음 경로 → 조립.

    `run()` 과 같은 일을 하고 **결과에 사유가 붙는다.** 개선안이 필요 없거나 만들 수
    없을 때 그게 데이터 갭인지 고장인지를 배치가 구분해야 하기 때문이다(`SkipReason`).

    inquiries 는 `alert.evidence.inquiry_ids` 에 해당하는 CS 원문이다(배치가
    `app/core/inquiries.py` 로 만들어 넘긴다). image_guide 의 근거이자 citations 의
    출처다.

    **근거가 없으면 개선안을 만들지 않는다.** 근거 0건은 입력만 보고 결정론적으로
    아는 사실이고 모델이 만들어낼 수 있는 게 아니라서, 생성을 태워봐야 일반론밖에 안
    나온다 — SCOPE_LIMIT 을 LLM 없이 처리하는 것과 같은 이유다. 셀러에겐 알림만 나가고
    개선안 카드가 안 붙는다(`recommendation: null` 은 조치 6종에서 이미 정상값이다).
    그때 `reason=NO_EVIDENCE` 가 붙고, **배치는 이걸 실패로 세지 않는다** — 상세페이지
    미등록은 흔한 데이터 갭이라 실패로 세면 배치가 상시 종료코드 1로 끝나 진짜 장애
    신호가 무뎌진다. 대신 요약에 건수로 남는다.

    **fallback_guide 는 남는다. 성격이 다르다:**
      - 근거가 아예 없음        → recommendation=None + NO_EVIDENCE (여기)
      - 근거는 있는데 LLM 이 MAX_ATTEMPTS 번 다 인용에 실패 → fallback_guide
    후자는 셀러가 제대로 된 개선안을 받을 수 **있었는데** 우리가 못 만든 경우라,
    일반 가이드로라도 떨어뜨리는 게 맞다.

    route_proposal_type()은 alert 하나당 1회만 — 재시도는 "같은 도구로 다시 생성"이지
    "도구를 바꿔서 다시 판단"이 아니다. MAX_ATTEMPTS를 다 써도 grounding이 안 되면
    generate_fallback_proposal로 넘어간다 — 억지로 근거 있는 척 넘기지
    않는다. LLM 호출 있는 함수는 async로 통일한다(협업 규칙 5).

    SCOPE_LIMIT_LABELS(스코프 한계)면 라우팅·생성 둘 다 건너뛰고 고정 문구로
    바로 조립한다 — LLM한테 물어봐도 답이 안 바뀌는 케이스라 호출 자체를 안 한다.
    """
    if not should_generate(alert):
        return RecommendationOutcome(reason=SkipReason.GATE_CLOSED)

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
        return RecommendationOutcome(
            assemble(alert, proposal, evaluator, {"similar_case": None})
        )

    context = retrieve_context(alert, inquiries)

    # 두 근거가 다 없으면 어느 도구를 골라도 인용할 원문이 없다 — 라우팅조차 낭비다.
    if context["detail_text"] == NO_DETAIL_TEXT and context["cs_quotes"] == NO_DETAIL_TEXT:
        logger.warning(
            "개선안 생략 alert=%s — 상세페이지·CS 원문이 둘 다 없어 근거가 0건입니다"
            " (상세페이지 미등록이거나 evidence.inquiry_ids 조회가 실패했을 수 있습니다)",
            alert.alert_id,
        )
        return RecommendationOutcome(
            reason=SkipReason.NO_EVIDENCE,
            detail="상세페이지·CS 원문이 둘 다 없어 근거 0건 — 라우팅 전에 생략",
        )

    proposal_type = await route_proposal_type(alert, context)

    if evidence_for(proposal_type, context) == NO_DETAIL_TEXT:
        logger.warning(
            "개선안 생략 alert=%s — %s 로 라우팅됐으나 그쪽 근거가 없습니다",
            alert.alert_id,
            proposal_type.value,
        )
        # 여기는 NO_EVIDENCE 가 아니다 — 반대쪽엔 근거가 있었는데 모델이 빈 쪽을
        #    골라 버린 것이라, 데이터 갭이 아니라 우리가 놓친 건이다(배치 실패로 남는다).
        return RecommendationOutcome(
            reason=SkipReason.ROUTED_WITHOUT_EVIDENCE,
            detail=f"{proposal_type.value} 로 라우팅됐으나 그쪽 근거가 없음",
        )

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
                # 이 둘은 True 로 하드코딩하지 않는다. 순수 함수라 LLM 호출 없이
                # 계산되므로, 검증하지 않은 값을 검증한 것처럼 남길 이유가 없다.
                consistency=_is_consistent_with_root_cause(proposal.rationale, alert),
                actionability=_is_actionable(proposal.proposed_text),
            ),
            failure_reason=f"근거를 찾지 못해 일반 가이드로 대체(MAX_RETRY={MAX_RETRY} 소진)",
        )

    return RecommendationOutcome(assemble(alert, proposal, evaluator, context, inquiries))


async def run(
    alert: DetectionAlert,
    inquiries: Sequence[LinkedCSInquiry] = (),
) -> Recommendation | None:
    """`run_with_outcome()` 에서 개선안만 꺼낸다 — 사유가 필요 없는 호출부용.

    REST(`service.py`)·eval 하네스처럼 "만들어졌나 아닌가"만 보면 되는 쪽이 쓴다.
    **배치는 `generate_outcome_for_alert()` 를 쓴다** — 근거 0건(데이터 갭)과 고장을
    구분해야 하는데 `None` 하나로는 못 가른다.
    """
    return (await run_with_outcome(alert, inquiries)).recommendation


async def generate_outcome_for_alert(
    alert: DetectionAlert,
    inquiries: list[LinkedCSInquiry],
) -> RecommendationOutcome:
    """배치 진입점 — 알림 1건에 개선안 1건. `run_with_outcome()` 을 감싸고 실패를 흡수한다.

    `generate_for_alert()` 와 같은 일을 하고 **개선안이 없을 때 사유가 붙는다.**
    배치가 데이터 갭(근거 0건)을 실패로 세지 않으려면 그 구분이 필요하다.

    Returns:
        `recommendation` 이 None 이면 `reason` 이 넷 중 하나다:
          1. `GATE_CLOSED` — 조치 6종. 정상이고 LLM 을 안 부른다.
          2. `NO_EVIDENCE` — 근거 0건. **데이터 갭이라 배치 실패가 아니다.**
          3. `ROUTED_WITHOUT_EVIDENCE` — 모델이 빈 쪽 도구를 골랐다. 실패로 센다.
          4. `ERROR` — 예외. `detail` 에 `repr(exc)`.
    """
    if not should_generate(alert):
        return RecommendationOutcome(reason=SkipReason.GATE_CLOSED)

    try:
        return await run_with_outcome(alert, inquiries)
    except Exception as exc:  # noqa: BLE001 - 배치 격리가 목적
        # 배치 요약에는 한 줄로만 남으므로 스택은 여기서 남겨야 추적된다.
        logger.warning(
            "개선안 생성 실패 alert=%s — recommendation 없이 발행합니다: %r",
            alert.alert_id,
            exc,
        )
        return RecommendationOutcome(reason=SkipReason.ERROR, detail=repr(exc))


async def generate_for_alert(
    alert: DetectionAlert,
    inquiries: list[LinkedCSInquiry],
) -> Recommendation | None:
    """배치 진입점 — `generate_outcome_for_alert()` 에서 개선안만 꺼낸다.

    **예외를 던지지 않는다.** 배치가 알림 여러 건을 도는 중이라, 개선안 1건의 실패가
    밖으로 나가면 그 알림의 **발행까지 막힌다** — 셀러가 이상 알림 자체를 못 받는다.
    실패도 게이트 미충족도 None 이고, payload 의 `recommendation` 은 null 이다
    (조치 6종에서 이미 정상인 값). 단 `asyncio.CancelledError` 는 BaseException 이라
    그대로 나간다 — 배치 중단이 "개선안 실패" 로 둔갑하면 안 된다.

    **None 의 사유는 여기서 구분되지 않는다.** 데이터 갭과 고장을 갈라야 하면
    `generate_outcome_for_alert()` 를 쓸 것(`daily.py` 가 그렇게 한다).

    Args:
        alert: 탐지 알림.
        inquiries: `alert.evidence.inquiry_ids` 의 CS 원문(`app/core/inquiries.py`).
            image_guide 의 근거이자 `citations` 의 출처라, **비면 image_guide 가
            근거 0건으로 떨어진다.**
    """
    return (await generate_outcome_for_alert(alert, inquiries)).recommendation


def record_hitl_outcome(alert: DetectionAlert, recommendation: Recommendation) -> None:
    """승인/반려 결과를 컬렉션2(과거·반려 사례)에 적재.

    Agent3는 hitl_status를 판단하지도, Recommendation을 저장하지도 않는다 — 그
    소유자는 Spring Boot다. 이 함수는 그 결정이 끝난 뒤 호출되는 사이드이펙트
    하나뿐이다: 다음 유사 케이스 생성 때 "이런 개선안은 반려/승인됐었다"를 참고할
    수 있도록 결과를 인덱싱한다.

    승인·반려 둘 다 적재 대상이다(: "승인 또는 반려된 개선안 1건 = 문서 1건").
    id는 recommendation_id로 결정적 — 같은 건에 대해 재호출해도 upsert라 중복 없음.

    적재 본문은 **수정후승인·승인**이면 `hitl_feedback.edited_text`(비었으면
    `proposal.proposed_text`), **반려**면 항상 `proposal.proposed_text`다 — 정예시엔
    셀러가 실제로 승인한 문장을, 부예시엔 거절당한 우리 제안문을 남긴다.

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
    # 수정후승인이면 셀러가 승인한 건 우리 제안문이 아니라 셀러가 고쳐 쓴 문장이다 —
    # 그쪽을 적재해야 다음 유사 케이스가 실제 승인본을 참고한다.
    # 반려는 제외한다: 부예시의 뜻이 "이런 제안이 거절당했다"라 본문은 우리 제안문이어야
    # 한다. 스키마상 반려 시 edited_text는 null이지만(recommendation_schema.md "대기·
    # 승인·반려 시 null"), 실려 오더라도 셀러 문장이 반려 사례로 새지 않게 막는다.
    # strip 하는 이유: 빈 문자열·공백만 있는 값(셀러가 입력칸을 비우고 저장)을 그대로
    # 쓰면 문서에서 개선안 본문이 통째로 빠진다 — 폴백 대상으로 정규화한다.
    edited_text = (
        (recommendation.hitl_feedback.edited_text or "").strip()
        if recommendation.hitl_feedback
        else ""
    )
    approved_text = (
        edited_text
        if edited_text and recommendation.hitl_status != HitlStatus.REJECTED
        else proposed_text
    )
    # 검색용 본문은 스펙대로 "원인 라벨 + CS 요약 + 개선안 본문" 이다. aspect 로
    # 대신하면 같은 aspect 의 사례가 전부 뭉뚱그려져 유사 검색이 무의미해진다.
    cs_summary = _summarize_cs_evidence(alert)
    document = f"{root_cause_label} {cs_summary} {approved_text}"

    # **한 번만 읽어 ID·metadata 에 같이 쓴다** — 두 번 읽으면 어긋날 수 있고,
    #    조회 필터(retrieve_context)까지 셋이 같은 값이어야 격리가 성립한다.
    tenant = current_tenant()
    outcome = "반려" if recommendation.hitl_status == HitlStatus.REJECTED else "승인"
    decided_at = (
        recommendation.hitl_feedback.processed_at
        if recommendation.hitl_feedback
        else datetime.now(timezone.utc)
    )

    metadata: dict[str, Any] = {
        # 조회 필터가 이 키로 회사를 좁힌다(retrieve_context). 쓰기와 읽기가 짝이므로
        # 한쪽만 지우면 조용히 다른 회사 사례가 섞인다.
        TENANT_METADATA_KEY: tenant,
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

    upsert_documents(
        get_rejection_reasons(),
        # 회사 축을 붙인다. `recommendation_id` 는 `alert_id` 파생이라 **회사 안에서만
        #    유일**해서, 그대로 쓰면 회사가 다른 같은 논리 알림이 서로를 덮는다.
        ids=[scoped_document_id(tenant, recommendation.recommendation_id)],
        documents=[document],
        metadatas=[metadata],
    )
