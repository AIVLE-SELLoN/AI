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
"""

from __future__ import annotations

import asyncio
import csv
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.llm_client import get_llm_client
from app.core.schemas import (
    DetectionAlert,
    DetectionConfidence,
    DetectionStats,
    Evidence,
    ProposalType,
    Recommendation,
    RecommendedAction,
    RootCause,
    SourceSignals,
    Verdict,
)
from app.core.vectordb import get_detail_pages, get_documents, get_rejection_reasons
from app.recommendation import pipeline
from scripts.generate_detail_fields import FIFTEEN_COMBOS

GOLDEN_PATH = Path(__file__).resolve().parents[1] / "data" / "golden" / "golden_detail_fields.csv"
INPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "input" / "input_detail_fields.csv"

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
    return {(r["product_group_id"], r["channel"], r["aspect"]): r["detail_text"] for r in rows}


def check_collection1_hit_rate() -> None:
    with GOLDEN_PATH.open(encoding="utf-8-sig", newline="") as f:
        golden_rows = list(csv.DictReader(f))

    expected = load_expected_texts()
    detail_pages = get_detail_pages()

    hits = 0
    misses = []

    for row in golden_rows:
        key = (row["golden_group_id"], row["channel"], row["aspect"])
        expected_text = expected.get(key)

        retrieved = get_documents(
            detail_pages,
            where={
                "$and": [
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


def check_collection2_status() -> None:
    count = get_rejection_reasons().count()
    if count == 0:
        print(
            "\n컬렉션2(rejection_reasons) query 상위3 포함율: N/A "
            "(0건 — HITL 반려 실적이 쌓여야 측정 가능, 정상 상태)"
        )
    else:
        print(f"\n컬렉션2(rejection_reasons): {count}건 존재 — 상위3 포함율 실측은 별도 구현 필요")


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
                    source="cs", cur_rate=0.13, past_rate=0.05, delta=0.08,
                    p_value=0.00013, bh_significant=True, cur_total=200,
                ),
                source_signals=SourceSignals(cs=True, review=False, interpretation="eval용 synthetic alert"),
                root_cause=root_cause,
                detection_confidence=DetectionConfidence.HIGH,
                scope_in=True,
                recommended_action=RecommendedAction.GENERATE_RECOMMENDATION,
                evidence=Evidence(inquiry_ids=["INQ-EVAL-0001"]),
            )
        )
    return alerts


async def check_grounding_precision() -> list[tuple[DetectionAlert, Recommendation]]:
    alerts = build_synthetic_alerts()

    grounded_cases = []
    for alert in alerts:
        recommendation = await pipeline.run(alert)
        if recommendation is None:
            continue
        grounded_cases.append((alert, recommendation))

    denom = [
        (a, r) for a, r in grounded_cases if r.proposal.detailpage_grounded
    ]

    print(f"\nGrounding precision 대상(detailpage_grounded=true): {len(denom)}/{len(grounded_cases)}건")

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


def report_evaluator_quality(cases: list[tuple[DetectionAlert, Recommendation]]) -> None:
    """Evaluator 3기준 중 grounding 이외 2개(consistency·actionability) + 재시도(attempts) 분포.

    consistency·actionability는 프롬프트가 이미 지시하는 항목이라 순환적 — 100%여도
    판단력 증거로는 약함. attempts(재시도 여부)는 지시 안 한 결과값이라 더 신뢰 가능.
    """
    total = len(cases)
    consistency_pass = sum(1 for _, r in cases if r.evaluator.checks.consistency)
    actionability_pass = sum(1 for _, r in cases if r.evaluator.checks.actionability)
    attempts_hist: dict[int, int] = {}
    for _, r in cases:
        attempts_hist[r.evaluator.attempts] = attempts_hist.get(r.evaluator.attempts, 0) + 1

    print(f"\nConsistency 통과율: {consistency_pass}/{total} ({consistency_pass / total:.0%})")
    print(f"Actionability 통과율: {actionability_pass}/{total} ({actionability_pass / total:.0%})")
    print("재시도(attempts) 분포:", {k: attempts_hist[k] for k in sorted(attempts_hist)})


async def check_rag_baseline_comparison(cases: list[tuple[DetectionAlert, Recommendation]]) -> None:
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
        if not (alert.root_cause and alert.root_cause.label in pipeline.SCOPE_LIMIT_LABELS)
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

    print(f"\nGrounding precision — (A) RAG 없음: {hits}/{len(copy_draft_cases)} ({hits / len(copy_draft_cases):.0%})")
    print("Grounding precision — (B) RAG 있음(2단계 결과): 4/4 (100%)")


async def check_routing_accuracy(alerts: list[DetectionAlert], *, label: str) -> None:
    """§4-3 도구선택표 대조 — LLM의 choose_tool() 판단이 팀 정답표와 얼마나 일치하는가.

    SCOPE_LIMIT·원인 미지정 등 EXPECTED_TOOL_BY_ROOT_CAUSE에 없는 원인은 정답 자체가
    없어 제외. golden 15건 기반(synthetic)과 실제 CS 데이터 기반 둘 다 이 함수를
    공유한다 — label로 어느 쪽 결과인지만 구분.
    """
    targets = [
        a for a in alerts
        if a.root_cause and a.root_cause.label in EXPECTED_TOOL_BY_ROOT_CAUSE
    ]
    print(f"\n라우팅 정확도 대상({label}): {len(targets)}건")

    hits = 0
    for alert in targets:
        context = pipeline.retrieve_context(alert)
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

    print(f"\n라우팅 정확도({label}): {hits}/{len(targets)} ({hits / len(targets):.0%})")


CS_LABELS_PATH = Path(__file__).resolve().parents[1] / "data" / "golden" / "golden_cs_labels.csv"
CS_INQUIRIES_PATH = Path(__file__).resolve().parents[1] / "data" / "input" / "input_cs_inquiries.csv"
CHANNEL_PRODUCTS_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "input" / "mapping_42" / "input_channel_products.csv"
)
GOLDEN_MAPPING_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "golden" / "mapping_42" / "golden_mapping.csv"
)


def build_alerts_from_real_cs_data() -> list[DetectionAlert]:
    """실제 CS 문의(golden_cs_labels.csv, true_cause 있는 건)를 DetectionAlert로 변환.

    조인 경로: inquiry_id(golden_cs_labels) → channel_product_id(input_cs_inquiries)
    → variant_row_id(input_channel_products) → golden_group_id(golden_mapping).
    """
    def load(path: Path) -> list[dict]:
        with path.open(encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))

    cs_labels = load(CS_LABELS_PATH)
    inq_to_cpid = {r["inquiry_id"]: (r["channel"], r["channel_product_id"]) for r in load(CS_INQUIRIES_PATH)}
    cpid_to_vrid = {
        (r["channel"], r["channel_product_id"]): r["variant_row_id"] for r in load(CHANNEL_PRODUCTS_PATH)
    }
    vrid_to_ggid = {r["variant_row_id"]: r["golden_group_id"] for r in load(GOLDEN_MAPPING_PATH)}

    alerts = []
    for r in cs_labels:
        cause = r["true_cause"].strip()
        if cause not in EXPECTED_TOOL_BY_ROOT_CAUSE:
            continue
        ch_cpid = inq_to_cpid.get(r["inquiry_id"])
        vrid = cpid_to_vrid.get(ch_cpid) if ch_cpid else None
        ggid = vrid_to_ggid.get(vrid) if vrid else None
        if not ggid:
            continue

        alerts.append(
            DetectionAlert(
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
                    source="cs", cur_rate=0.13, past_rate=0.05, delta=0.08,
                    p_value=0.00013, bh_significant=True, cur_total=200,
                ),
                source_signals=SourceSignals(cs=True, review=False, interpretation="실제 CS 데이터 기반 eval"),
                root_cause=RootCause(label=cause, count=14, total=20, consistent=True),
                detection_confidence=DetectionConfidence.HIGH,
                scope_in=True,
                recommended_action=RecommendedAction.GENERATE_RECOMMENDATION,
                evidence=Evidence(inquiry_ids=[r["inquiry_id"]]),
            )
        )
    return alerts


def main() -> None:
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
        print("(RAG 유무 베이스라인 비교는 --rag-baseline 플래그로 별도 실행 — 실비용 발생)")

    if run_routing:
        asyncio.run(check_routing_accuracy(build_synthetic_alerts(), label="golden 15건"))
    else:
        print("(라우팅 정확도는 --routing 플래그로 별도 실행 — 실비용 발생)")

    if run_routing_real:
        asyncio.run(check_routing_accuracy(build_alerts_from_real_cs_data(), label="실제 CS 데이터"))
    else:
        print("(실제 CS 데이터 기반 라우팅 정확도는 --routing-real 플래그로 별도 실행 — 실비용 발생)")


if __name__ == "__main__":
    main()
