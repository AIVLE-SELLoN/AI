"""실험⑤ Agent3 정량 실험 — Retrieval hit rate / Grounding precision / RAG 유무 비교 / 라우팅 정확도.

각 실험이 무엇을 재는지·비용·실측 결과는 eval/README.md 표 참고. 설계 이유가
비자명한 부분(왜 재시도를 안 태우는지 등)은 해당 함수 docstring에 있음.

실행:
    python eval/run_recommendation_eval.py                # Retrieval hit rate만, $0
    python eval/run_recommendation_eval.py --grounding    # + Grounding precision, 실비용
    python eval/run_recommendation_eval.py --rag-baseline # + RAG 유무 비교, 실비용
    python eval/run_recommendation_eval.py --routing      # + 라우팅 정확도(golden 15건), 실비용
    python eval/run_recommendation_eval.py --routing-real # + 라우팅 정확도(실제 CS 201건), 실비용

전제: scripts/seed_vectordb.py로 Chroma에 실데이터가 이미 시딩돼 있어야 함.

⚠️ 2026-08-09부터 golden 15건 실험도 CS 코퍼스(input_cs_inquiries.csv·golden_cs_labels.csv)를
읽는다. image_guide 의 근거가 통계 요약에서 **실제 CS 원문**으로 바뀌었기 때문에, 원문을
안 넘기면 cs_quotes 가 "정보 없음"이 되어 라우팅이 copy_draft 로 쏠린다(운영과 다른 경로를
재게 됨). 그래서 mock 재생성 시 상세페이지 기반 실험과 달리 **재측정이 필요하다.**
"""

from __future__ import annotations

import asyncio
import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.console import force_utf8_output

# 라우팅이 왜 그 도구를 골랐는지(pipeline.route_proposal_type 의 INFO 로그)를 보려면
# 로거를 켜야 한다 — MISS 케이스 진단의 유일한 단서다.
logging.basicConfig(level=logging.INFO, format="    · %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)

from app.core.llm_client import get_llm_client
from app.core.schemas import (
    DetectionAlert,
    DetectionConfidence,
    DetectionStats,
    Evidence,
    LinkedCSInquiry,
    ProposalType,
    Recommendation,
    RecommendedAction,
    RootCause,
    SourceSignals,
    Verdict,
)
from app.core.vectordb import (
    TENANT_METADATA_KEY,
    current_tenant,
    get_detail_pages,
    get_documents,
    get_rejection_reasons,
)
from app.recommendation import pipeline
from scripts.generate_detail_fields import FIFTEEN_COMBOS

GOLDEN_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "golden" / "golden_detail_fields.csv"
)
INPUT_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "input" / "input_detail_fields.csv"
)

CS_LABELS_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "golden" / "golden_cs_labels.csv"
)
CS_INQUIRIES_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "input" / "input_cs_inquiries.csv"
)
CHANNEL_PRODUCTS_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "input"
    / "input_channel_products.csv"
)
GOLDEN_MAPPING_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "golden" / "golden_mapping.csv"
)

NO_RAG_PROMPT = """당신은 이커머스 상세페이지 개선안을 작성하는 어시스턴트입니다.
아래 "이상징후 + 원인"만 보고 개선안을 작성하세요. 실제 상세페이지 원문은
제공되지 않습니다 — current_text에는 이 상품 상세페이지에 있을 법한 문구를
자연스럽게 작성하세요.

이상징후 + 원인: {anomaly}

JSON만 반환:
{{"current_text": "...", "proposed_text": "...", "rationale": "..."}}
"""
"""§5-3 (A) 원인 라벨만으로 생성 조건 전용 — copy_draft_v1.md와 달리 "정보 없음이면
정보 없음이라 쓰라"는 지시가 없음. 있으면 NO_DETAIL_TEXT 입력 시 LLM이 정직하게
"정보 없음"이라 답해서 다른 실험이 됨."""


EXPECTED_TOOL_BY_ROOT_CAUSE = {
    "표기_오타": ProposalType.COPY_DRAFT,
    "실측_표기_편차": ProposalType.COPY_DRAFT,
    "채널_사이즈_표준차이": ProposalType.COPY_DRAFT,
    "소재_정보_누락": ProposalType.COPY_DRAFT,
    "사진_색감_오차": ProposalType.IMAGE_GUIDE,
    "조명_보정_차이": ProposalType.IMAGE_GUIDE,
    "이미지_질감표현_부족": ProposalType.IMAGE_GUIDE,
}
"""§4-3 도구선택표(팀 Notion, 결정론적 매핑) — copy_draft형/image_guide형 원인 분류는
scripts/prompts/generate_detail_field_text_v1.md의 설계 원칙("원인 유형이 있음/없음의
방향을 결정한다")에서 가져옴. SCOPE_LIMIT_LABELS(실물_염색_편차·실제_원단_문제)와
원인 미지정("") 케이스는 정답이 없어 이 표에서 제외 — 라우팅 정확도 실험에서도 제외."""


def load_expected_texts() -> dict[tuple[str, str, str], str]:
    """input_detail_fields.csv → {(product_group_id, channel, aspect): detail_text}."""
    with INPUT_PATH.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    return {
        (r["product_group_id"], r["channel"], r["aspect"]): r["detail_text"]
        for r in rows
    }


def check_collection1_hit_rate() -> None:
    with GOLDEN_PATH.open(encoding="utf-8-sig", newline="") as f:
        golden_rows = list(csv.DictReader(f))

    expected = load_expected_texts()
    detail_pages = get_detail_pages()
    # 운영(`pipeline._get_detail_page_text`)과 **같은 조건**으로 조회한다 — 회사 축을
    # 빼면 이 실험만 필터가 느슨해져 운영보다 잘 맞는 수치가 나온다.
    tenant = current_tenant()

    hits = 0
    misses = []

    for row in golden_rows:
        key = (row["golden_group_id"], row["channel"], row["aspect"])
        expected_text = expected.get(key)

        retrieved = get_documents(
            detail_pages,
            where={
                "$and": [
                    {TENANT_METADATA_KEY: tenant},
                    {"product_group_id": key[0]},
                    {"channel": key[1]},
                    {"aspect": key[2]},
                ]
            },
        )
        retrieved_text = retrieved[0]["document"] if retrieved else None

        if retrieved_text is not None and retrieved_text == expected_text:
            hits += 1
        else:
            misses.append(
                {
                    "key": key,
                    "ground_truth_evidence": row["ground_truth_evidence"],
                    "expected": expected_text,
                    "retrieved": retrieved_text,
                }
            )

    total = len(golden_rows)
    print(f"컬렉션1 get 일치율: {hits}/{total} ({hits / total:.0%})")
    if misses:
        print("\n불일치 케이스:")
        for m in misses:
            print(f"  {m['key']} (ground_truth_evidence={m['ground_truth_evidence']})")
            print(f"    기대: {m['expected']!r}")
            print(f"    실제: {m['retrieved']!r}")


def _load_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_negative_cs_by_product() -> dict[tuple[str, str, str], list[dict]]:
    """실제 부정 CS 를 {(golden_group_id, channel, aspect): [문의…]} 로 모은다.

    조인은 `build_alerts_from_real_cs_data()` 가 쓰는 체인을 **거꾸로** 탄 것이다:
    channel_product_id → variant_row_id → golden_group_id.

    쓰는 곳은 `attach_cs_inquiries()` — synthetic alert 에 그 상품의 진짜 CS 원문을
    달아주기 위해서다. 상품과 무관하게 aspect 만 맞는 남의 CS 를 쓰면 운영과 다른
    걸 재게 된다(운영은 그 상품의 CS 를 넣는다).

    ⚠️ 이 함수 때문에 golden 15건 실험이 CS 코퍼스에 묶인다 — mock 재생성하면 뽑히는
    문장이 바뀔 수 있다. 상세페이지 기반인 다른 실험과 달리 **재측정이 필요하다.**
    """
    labels = {r["inquiry_id"]: r for r in _load_csv(CS_LABELS_PATH)}
    golden_mapping = {
        r["variant_row_id"]: r["golden_group_id"] for r in _load_csv(GOLDEN_MAPPING_PATH)
    }
    channel_product_to_group = {}
    for row in _load_csv(CHANNEL_PRODUCTS_PATH):
        group_id = golden_mapping.get(row["variant_row_id"])
        if group_id:
            channel_product_to_group[(row["channel"], row["channel_product_id"])] = group_id

    buckets: dict[tuple[str, str, str], list[dict]] = {}
    for row in _load_csv(CS_INQUIRIES_PATH):
        group_id = channel_product_to_group.get((row["channel"], row["channel_product_id"]))
        label = labels.get(row["inquiry_id"])
        if not group_id or not label or label["true_sentiment"] != "-1":
            continue
        key = (group_id, row["channel"], label["true_aspect"])
        buckets.setdefault(key, []).append(
            {
                "inquiry_id": row["inquiry_id"],
                "content": row["content"],
                "inquired_at": row["inquired_at"],
                "cause": label["true_cause"].strip(),
            }
        )
    return buckets


def attach_cs_inquiries(
    alerts: list[DetectionAlert],
    buckets: dict[tuple[str, str, str], list[dict]],
) -> list[tuple[DetectionAlert, list[LinkedCSInquiry]]]:
    """alert 마다 그 상품의 실제 CS 원문을 붙이고, evidence.inquiry_ids 도 맞춰 준다.

    **evidence 를 같이 고치는 게 중요하다.** 안 그러면 citations 가 evidence 밖의
    문의를 인용한 꼴이 되어 `validate_citations_grounded()` 가 잡는다 — 운영에서는
    애초에 evidence 로부터 원문을 조회하므로 어긋날 수 없는 관계다.

    우선순위는 원인 라벨 일치 → 같은 aspect 부정 CS. 원인까지 맞는 문의가 있으면
    그걸 쓰는 게 운영에 가깝다(탐지가 원인별로 evidence 를 모아 준다).
    """
    attached = []
    for alert in alerts:
        key = (alert.product_group_id, alert.channel.value, alert.main_aspect.value)
        rows = buckets.get(key, [])
        label = alert.root_cause.label if alert.root_cause else None
        preferred = [r for r in rows if label and r["cause"] == label] or rows
        picked = preferred[: pipeline.CS_QUOTE_TOP_N]

        inquiries = [
            LinkedCSInquiry(
                item_id=r["inquiry_id"],
                raw_text=r["content"],
                created_at=r["inquired_at"],
            )
            for r in picked
        ]
        if inquiries:
            alert = alert.model_copy(
                update={
                    "evidence": Evidence(
                        inquiry_ids=[q.item_id for q in inquiries],
                        linked_change_id=alert.evidence.linked_change_id,
                    )
                }
            )
        attached.append((alert, inquiries))
    return attached


def check_collection2_status() -> None:
    count = get_rejection_reasons().count()
    if count == 0:
        print(
            "\n컬렉션2(rejection_reasons) query 상위3 포함율: N/A "
            "(0건 — HITL 반려 실적이 쌓여야 측정 가능, 정상 상태)"
        )
    else:
        print(
            f"\n컬렉션2(rejection_reasons): {count}건 존재 — 상위3 포함율 실측은 별도 구현 필요"
        )


def build_synthetic_alerts() -> list[DetectionAlert]:
    """FIFTEEN_COMBOS(golden 15건) → synthetic DetectionAlert 15개.

    root_cause=""(SC-032 2건)는 "원인 미특정" 케이스로 None 처리 — 실제로 있을 수
    있는 정상 케이스라 억지로 값을 채우지 않는다. 나머지 필드(stats·source_signals
    등)는 Agent3 경로에 영향 없는 항목이라 tests/conftest.py의 biased_alert
    fixture와 동일한 템플릿 값을 쓴다.
    """
    alerts = []
    for i, combo in enumerate(FIFTEEN_COMBOS):
        root_cause = (
            RootCause(label=combo["root_cause"], count=14, total=20, consistent=True)
            if combo["root_cause"]
            else None
        )
        alerts.append(
            DetectionAlert(
                alert_id=f"ALT-EVAL-{combo['case_id']}-{i}",
                detected_at="2026-07-28T00:00:00",
                product_group_id=combo["golden_group_id"],
                channel=combo["channel"],
                window_start="2026-07-21",
                window_end="2026-07-28",
                verdict=Verdict.BIASED,
                significant_channels=[combo["channel"]],
                main_aspect=combo["aspect"],
                stats=DetectionStats(
                    source="cs",
                    cur_rate=0.13,
                    past_rate=0.05,
                    delta=0.08,
                    p_value=0.00013,
                    bh_significant=True,
                    cur_total=200,
                ),
                source_signals=SourceSignals(
                    cs=True, review=False, interpretation="eval용 synthetic alert"
                ),
                root_cause=root_cause,
                detection_confidence=DetectionConfidence.HIGH,
                scope_in=True,
                recommended_action=RecommendedAction.GENERATE_RECOMMENDATION,
                evidence=Evidence(inquiry_ids=["INQ-EVAL-0001"]),
            )
        )
    return alerts


async def check_grounding_precision() -> list[tuple[DetectionAlert, Recommendation]]:
    alerts = attach_cs_inquiries(build_synthetic_alerts(), load_negative_cs_by_product())

    grounded_cases = []
    skipped: list[str] = []
    for alert, inquiries in alerts:
        recommendation = await pipeline.run(alert, inquiries)
        if recommendation is None:
            # 미생성을 조용히 넘기면 **분모만 줄어 점수가 좋아 보인다.**
            # run() 이 None 을 돌려주는 경로는 둘 다 근거 문제다(근거 0건 / 라우팅된
            # 쪽 근거 없음) — 성능이 아니라 커버리지 결손이므로 따로 세서 드러낸다.
            skipped.append(alert.alert_id)
            continue
        grounded_cases.append((alert, recommendation))

    if skipped:
        print(
            f"\n⚠️ 개선안 미생성 {len(skipped)}/{len(alerts)}건 — 근거 부족(사유는"
            f" pipeline 경고 로그). 아래 지표의 분모에서 빠진다: {skipped[:5]}"
        )

    denom = [(a, r) for a, r in grounded_cases if r.proposal.detailpage_grounded]

    print(
        f"\nGrounding precision 대상(detailpage_grounded=true): {len(denom)}/{len(grounded_cases)}건"
    )

    if not denom:
        print("Grounding precision: N/A (detailpage_grounded=true 케이스 없음)")
    else:
        expected = load_expected_texts()
        hits = 0
        for alert, rec in denom:
            key = (alert.product_group_id, alert.channel.value, alert.main_aspect.value)
            original_text = expected.get(key, "")
            is_hit = rec.proposal.current_text in original_text
            hits += is_hit
            marker = "OK" if is_hit else "MISS"
            print(f"  [{marker}] {key} current_text={rec.proposal.current_text!r}")

        print(f"\nGrounding precision: {hits}/{len(denom)} ({hits / len(denom):.0%})")

    report_evaluator_quality(grounded_cases)
    return grounded_cases


def report_evaluator_quality(
    cases: list[tuple[DetectionAlert, Recommendation]],
) -> None:
    """Evaluator 3기준 중 grounding 이외 2개(consistency·actionability) + 재시도(attempts) 분포.

    consistency·actionability는 프롬프트가 이미 지시하는 항목이라 순환적 — 100%여도
    판단력 증거로는 약함. attempts(재시도 여부)는 지시 안 한 결과값이라 더 신뢰 가능.
    """
    total = len(cases)
    consistency_pass = sum(1 for _, r in cases if r.evaluator.checks.consistency)
    actionability_pass = sum(1 for _, r in cases if r.evaluator.checks.actionability)
    attempts_hist: dict[int, int] = {}
    for _, r in cases:
        attempts_hist[r.evaluator.attempts] = (
            attempts_hist.get(r.evaluator.attempts, 0) + 1
        )

    print(
        f"\nConsistency 통과율: {consistency_pass}/{total} ({consistency_pass / total:.0%})"
    )
    print(
        f"Actionability 통과율: {actionability_pass}/{total} ({actionability_pass / total:.0%})"
    )
    print(
        "재시도(attempts) 분포:", {k: attempts_hist[k] for k in sorted(attempts_hist)}
    )
    report_citation_coverage(cases)


def report_citation_coverage(
    cases: list[tuple[DetectionAlert, Recommendation]],
) -> None:
    """image_guide 가 실제로 CS 문의를 인용했는가(§4-3 citations).

    분모를 image_guide 로 한정하는 이유: copy_draft 는 상세페이지를 인용하므로 CS
    인용이 0건인 게 정상이다. 여기에 섞으면 커버리지가 이유 없이 낮아 보인다.

    같이 찍는 두 값이 해석의 핵심이다:
      - fallback 건수(attempts 소진 + grounding=False) — 근거를 못 찾아 일반 가이드로
        떨어진 것. citations 가 비는 게 당연한 케이스라 분모에서 갈라 봐야 한다.
      - evidence 이탈 — citations 가 evidence.inquiry_ids 밖을 가리키면 계약 위반이다
        (`validate_citations_grounded`). 0 이 아니면 즉시 버그다.
    """
    image_guide = [
        (a, r) for a, r in cases if r.proposal and r.proposal.type == ProposalType.IMAGE_GUIDE
    ]
    if not image_guide:
        print("\nCitation 커버리지: N/A (image_guide 라우팅 0건)")
        return

    grounded = [(a, r) for a, r in image_guide if r.evaluator.checks.grounding]
    with_citations = [(a, r) for a, r in grounded if r.citations]
    quotes = sum(len(r.citations) for _, r in grounded)

    escaped = 0
    for alert, rec in image_guide:
        allowed = set(alert.evidence.inquiry_ids)
        escaped += sum(1 for c in rec.citations if c.inquiry_id not in allowed)

    print(f"\nCitation 커버리지(image_guide {len(image_guide)}건 기준)")
    print(f"  grounding 통과      : {len(grounded)}/{len(image_guide)}")
    print(
        f"  인용 1건 이상 확보  : {len(with_citations)}/{len(grounded)}"
        f" (총 {quotes}건 인용)"
    )
    print(f"  evidence 이탈       : {escaped}건 (0이어야 정상)")

    for alert, rec in image_guide:
        if rec.evaluator.checks.grounding and not rec.citations:
            print(
                f"  ⚠️ grounding 통과인데 인용 0건 — {alert.alert_id}"
                f" current_text={rec.proposal.current_text[:40]!r}"
            )


async def check_rag_baseline_comparison(
    cases: list[tuple[DetectionAlert, Recommendation]],
) -> None:
    """§5-3 베이스라인 비교 — (A) RAG 없음 vs (B) RAG 있음(2단계에서 이미 측정한 100%).

    cases는 check_grounding_precision()이 계산한 결과를 재사용 — route_proposal_type()
    중복 호출 방지. SCOPE_LIMIT은 proposal.type이 copy_draft로 고정돼 있어도 실제
    라우팅 판단이 아니므로 별도 제외.
    """
    expected = load_expected_texts()
    client = get_llm_client()

    copy_draft_cases = [
        alert
        for alert, rec in cases
        if not (
            alert.root_cause and alert.root_cause.label in pipeline.SCOPE_LIMIT_LABELS
        )
        and rec.proposal.type == ProposalType.COPY_DRAFT
    ]

    print(f"\n(A) RAG 없음 — copy_draft 라우팅 대상: {len(copy_draft_cases)}건")

    if not copy_draft_cases:
        print("RAG 없음 Grounding precision: N/A (copy_draft 라우팅 케이스 없음)")
        return

    hits = 0
    for alert in copy_draft_cases:
        root_cause_label = alert.root_cause.label if alert.root_cause else "미상"
        anomaly = f"{alert.channel.value} · {alert.main_aspect.value} 이상 (원인: {root_cause_label})"
        prompt = NO_RAG_PROMPT.format(anomaly=anomaly)

        response = await client.complete_json(
            prompt, trace_key=f"rag-baseline:alert_id={alert.alert_id}", temperature=0.0
        )
        current_text = response["current_text"]

        key = (alert.product_group_id, alert.channel.value, alert.main_aspect.value)
        original_text = expected.get(key, "")
        is_hit = current_text in original_text
        hits += is_hit
        marker = "OK" if is_hit else "MISS"
        print(f"  [{marker}] {key} current_text={current_text!r}")

    print(
        f"\nGrounding precision — (A) RAG 없음: {hits}/{len(copy_draft_cases)} ({hits / len(copy_draft_cases):.0%})"
    )
    print("Grounding precision — (B) RAG 있음(2단계 결과): 4/4 (100%)")


async def check_routing_accuracy(
    alerts: list[tuple[DetectionAlert, list[LinkedCSInquiry]]], *, label: str
) -> None:
    """§4-3 도구선택표 대조 — LLM의 choose_tool() 판단이 팀 정답표와 얼마나 일치하는가.

    SCOPE_LIMIT·원인 미지정 등 EXPECTED_TOOL_BY_ROOT_CAUSE에 없는 원인은 정답 자체가
    없어 제외. golden 15건 기반(synthetic)과 실제 CS 데이터 기반 둘 다 이 함수를
    공유한다 — label로 어느 쪽 결과인지만 구분.

    🔴 **CS 원문을 반드시 같이 넘긴다.** 안 넘기면 cs_quotes 가 "정보 없음"이 되고,
    라우팅 프롬프트의 "CS 원문이 없으면 copy_draft 쪽에 무게" 지시가 발동해 결과가
    copy_draft 로 쏠린다. 그건 모델 성능이 아니라 **하네스가 근거를 안 준 것**이라
    숫자를 그대로 읽으면 안 된다(2026-08-09).
    """
    targets = [
        (a, q)
        for a, q in alerts
        if a.root_cause and a.root_cause.label in EXPECTED_TOOL_BY_ROOT_CAUSE
    ]
    print(f"\n라우팅 정확도 대상({label}): {len(targets)}건")

    missing_quotes = [a.alert_id for a, q in targets if not q]
    if missing_quotes:
        print(
            f"  ⚠️ CS 원문을 못 붙인 alert {len(missing_quotes)}건 — 이 건들은 근거 없이"
            f" 라우팅되므로 결과 해석에서 빼야 한다: {missing_quotes[:3]}…"
        )

    hits = 0
    for alert, inquiries in targets:
        context = pipeline.retrieve_context(alert, inquiries)
        actual = await pipeline.route_proposal_type(alert, context)
        expected = EXPECTED_TOOL_BY_ROOT_CAUSE[alert.root_cause.label]
        is_hit = actual == expected
        hits += is_hit
        marker = "OK" if is_hit else "MISS"
        print(
            f"  [{marker}] {alert.product_group_id} {alert.channel.value} "
            f"{alert.main_aspect.value} 원인={alert.root_cause.label} "
            f"기대={expected.value} 실제={actual.value}"
        )

    print(
        f"\n라우팅 정확도({label}): {hits}/{len(targets)} ({hits / len(targets):.0%})"
    )


def build_alerts_from_real_cs_data() -> list[
    tuple[DetectionAlert, list[LinkedCSInquiry]]
]:
    """실제 CS 문의(golden_cs_labels.csv, true_cause 있는 건)를 DetectionAlert로 변환.

    조인 경로: inquiry_id(golden_cs_labels) → channel_product_id(input_cs_inquiries)
    → variant_row_id(input_channel_products) → golden_group_id(golden_mapping).

    alert 1건 = 문의 1건이라, 그 문의의 원문을 `LinkedCSInquiry` 로 같이 돌려준다 —
    운영에서 `app/core/inquiries.py` 가 `evidence.inquiry_ids` 로 채우는 그 자리다.
    """
    cs_labels = _load_csv(CS_LABELS_PATH)
    inquiry_rows = {r["inquiry_id"]: r for r in _load_csv(CS_INQUIRIES_PATH)}
    cpid_to_vrid = {
        (r["channel"], r["channel_product_id"]): r["variant_row_id"]
        for r in _load_csv(CHANNEL_PRODUCTS_PATH)
    }
    vrid_to_ggid = {
        r["variant_row_id"]: r["golden_group_id"] for r in _load_csv(GOLDEN_MAPPING_PATH)
    }

    alerts = []
    for r in cs_labels:
        cause = r["true_cause"].strip()
        if cause not in EXPECTED_TOOL_BY_ROOT_CAUSE:
            continue
        inquiry_row = inquiry_rows.get(r["inquiry_id"])
        ch_cpid = (
            (inquiry_row["channel"], inquiry_row["channel_product_id"])
            if inquiry_row
            else None
        )
        vrid = cpid_to_vrid.get(ch_cpid) if ch_cpid else None
        ggid = vrid_to_ggid.get(vrid) if vrid else None
        if not ggid:
            continue

        inquiries = [
            LinkedCSInquiry(
                item_id=r["inquiry_id"],
                raw_text=inquiry_row["content"],
                created_at=inquiry_row["inquired_at"],
            )
        ]
        alert = DetectionAlert(
            alert_id=f"ALT-REAL-{r['inquiry_id']}",
            detected_at="2026-07-28T00:00:00",
            product_group_id=ggid,
            channel=ch_cpid[0],
            window_start="2026-07-21",
            window_end="2026-07-28",
            verdict=Verdict.BIASED,
            significant_channels=[ch_cpid[0]],
            main_aspect=r["true_aspect"],
            stats=DetectionStats(
                source="cs",
                cur_rate=0.13,
                past_rate=0.05,
                delta=0.08,
                p_value=0.00013,
                bh_significant=True,
                cur_total=200,
            ),
            source_signals=SourceSignals(
                cs=True, review=False, interpretation="실제 CS 데이터 기반 eval"
            ),
            root_cause=RootCause(label=cause, count=14, total=20, consistent=True),
            detection_confidence=DetectionConfidence.HIGH,
            scope_in=True,
            recommended_action=RecommendedAction.GENERATE_RECOMMENDATION,
            evidence=Evidence(inquiry_ids=[r["inquiry_id"]]),
        )
        alerts.append((alert, inquiries))
    return alerts


def main() -> None:
    # 🔴 첫 문장이어야 한다. stdout 만 바꾸면 안 된다 — 로깅은 **stderr** 로 나가서
    #    cp949 로 깨지고, 라우팅 사유(한글)가 그 로그로 나오므로 진단이 통째로 못 읽는
    #    글자가 된다. 예전엔 모듈 최상단에서 불렀는데 **import 만 해도** 남의 스트림을 바꿨다.
    # ⚠️ 위 `logging.basicConfig()` 가 먼저 핸들러를 만들어도 무해하다 — `reconfigure` 는
    #    스트림 객체를 교체하지 않고 제자리에서 바꾼다(`app/core/console.py`).
    force_utf8_output()

    run_grounding = "--grounding" in sys.argv
    run_rag_baseline = "--rag-baseline" in sys.argv
    run_routing = "--routing" in sys.argv
    run_routing_real = "--routing-real" in sys.argv

    check_collection1_hit_rate()
    check_collection2_status()

    grounded_cases: list[tuple[DetectionAlert, Recommendation]] = []
    if run_grounding or run_rag_baseline:
        grounded_cases = asyncio.run(check_grounding_precision())
    else:
        print("\n(Grounding precision은 --grounding 플래그로 별도 실행 — 실비용 발생)")

    if run_rag_baseline:
        asyncio.run(check_rag_baseline_comparison(grounded_cases))
    else:
        print(
            "(RAG 유무 베이스라인 비교는 --rag-baseline 플래그로 별도 실행 — 실비용 발생)"
        )

    if run_routing:
        asyncio.run(
            check_routing_accuracy(
                attach_cs_inquiries(
                    build_synthetic_alerts(), load_negative_cs_by_product()
                ),
                label="golden 15건",
            )
        )
    else:
        print("(라우팅 정확도는 --routing 플래그로 별도 실행 — 실비용 발생)")

    if run_routing_real:
        asyncio.run(
            check_routing_accuracy(
                build_alerts_from_real_cs_data(), label="실제 CS 데이터"
            )
        )
    else:
        print(
            "(실제 CS 데이터 기반 라우팅 정확도는 --routing-real 플래그로 별도 실행 — 실비용 발생)"
        )


if __name__ == "__main__":
    main()
