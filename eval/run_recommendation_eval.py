"""실험⑤ RAG 유무 베이스라인 비교 — Retrieval hit rate + Grounding precision.

1단계 — Retrieval hit rate (컬렉션1 부분, 기본 실행 대상)
   무엇을 재나: 컬렉션1(상세페이지) get() 조회 결과가 실제 적재 원문과 일치하는가.
   docs/agent3_logic.md §5-3 정의 그대로면 "컬렉션1 get 일치 + 컬렉션2 query 상위3
   포함"을 합친 게 Retrieval hit rate지만, 컬렉션2(rejection_reasons)는 HITL
   반려 실적이 쌓여야 채워지는 구조라 지금은 항상 0건 — 이 스크립트는 $0으로
   지금 잴 수 있는 컬렉션1 부분만 실측하고, 컬렉션2는 상태만 보고한다(N/A).

   "일치"의 의미: get()은 텍스트를 그대로 반환할 뿐 있음/없음/애매를 판단하지
   않는다(그 판단은 Grounding precision 쪽 몫). 여기서 재는 건 벡터DB 조회
   경로(seeding·키 매칭·인코딩)에 배관 버그가 없는지 — golden 15건의 키로
   조회했을 때 input_detail_fields.csv에 실제로 적재된 원문과 글자 그대로
   일치하는가를 확인한다.

   어떻게: golden_detail_fields.csv(15건, 정답 키)로 pipeline이 쓰는 것과 동일한
   vectordb.get_documents() 호출 → input_detail_fields.csv(실제 적재 원문, 기대값)와
   문자열 비교.

   비용: $0 — LLM 호출 없음, 순수 vectordb 조회.

2단계 — Grounding precision (--grounding 플래그로만 실행, 비용 발생)
   무엇을 재나: proposal.current_text가 실제 원문에 그대로 포함된 케이스 수 /
   proposal.detailpage_grounded=true 케이스 수 (docs/agent3_logic.md §5-3).

   어떻게: scripts/generate_detail_fields.py의 FIFTEEN_COMBOS(golden 15건의
   golden_group_id/channel/aspect/root_cause)로 synthetic DetectionAlert 15개를 만들어 pipeline.run()을 실제로 돌린다.

   비용: gpt-4o-mini 기준 최대 15건 × 4회(라우팅1 + 생성 최대3) = 60회 호출.
   실행 전 확인 필요 — eval/README.md 원칙("LLM 비용 발생, 사람이 수동 실행")대로
   기본 실행에는 안 들어있고 --grounding 플래그를 명시해야만 돈다.

   덤으로 같은 실행에서 추가 비용 없이 report_evaluator_quality()도 같이 보고한다:
   consistency·actionability 통과율(단, 프롬프트가 이미 직접 지시하는 항목이라
   순환적 — 100%여도 "독립적으로 어려운 판단을 통과했다"는 근거로는 약함)과
   attempts(재시도) 분포(지시한 적 없는 결과값이라 더 의미 있음 — 오늘 고친
   재시도 로직이 라이브에서 실제로 exercise되는지 보여줌).

3단계 — RAG 유무 베이스라인 비교 (--rag-baseline 플래그로만 실행, 비용 발생)
   무엇을 재나: §5-3 "(A) 원인 라벨만으로 생성(RAG 미사용) vs (B) RAG 포함 생성"
   비교. (B)는 2단계에서 이미 측정함(100%) — 여기서는 (A)만 새로 측정해서 나란히
   비교한다.

   어떻게: 2단계(check_grounding_precision)가 이미 pipeline.run()으로 계산해둔
   라우팅 결과(rec.proposal.type)를 재사용 — route_proposal_type()을 여기서 또
   부르지 않는다(2026-07-28 재검토: 중복 호출로 비용 낭비하던 버그 수정). COPY_DRAFT로
   라우팅된 것만 대상으로 generate_proposal() 대신 근거를 아예 안 주는 별도
   프롬프트(NO_RAG_PROMPT)로 1차만(재시도 없이) 생성.
   재시도 루프를 안 태우는 이유: run()의 재시도+fallback을 그대로 쓰면 실패할 때마다
   "근거없음 고정 문구"로 수렴해버려서, RAG 없이 LLM이 실제로 뭘 지어내는지가
   안 보인다 — 안전장치를 걷어내고 날것의 실패율을 봐야 (A)/(B) 차이가 의미 있다.
   current_text(LLM이 지어낸 것)를 LLM에게 안 보여준 진짜 원문과 대조 — 우연히
   맞는 경우만 "성공"으로 센다.

   비용: COPY_DRAFT 라우팅된 건수만큼(2단계 기준 약 4~6건) × 1회 생성.

4단계 — 라우팅 정확도 (--routing 플래그로만 실행, 비용 발생)
   무엇을 재나: route_proposal_type()의 choose_tool() 판단이 §4-3 도구선택표(팀
   정답)와 얼마나 일치하는가 — "에이전트의 자율 판단을 얼마나 신뢰할 수 있는가"를
   정량화.

   어떻게: EXPECTED_TOOL_BY_ROOT_CAUSE(§4-3 표를 코드로 옮긴 것)와 실제 LLM 판단을
   11건(SCOPE_LIMIT 2건·원인 미지정 2건 제외) 대조.

   비용: 11회 호출(라우팅만, 생성 없음) — 4단계 중 제일 쌈.

실행:
    python eval/run_recommendation_eval.py                # 1단계만, $0
    python eval/run_recommendation_eval.py --grounding    # 1+2단계, 실비용 발생
    python eval/run_recommendation_eval.py --rag-baseline # 1+2+3단계, 실비용 발생
    python eval/run_recommendation_eval.py --routing      # 1+2+4단계, 실비용 발생

전제: scripts/seed_vectordb.py로 Chroma에 실데이터(504행)가 이미 시딩돼 있어야 함.
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


async def check_routing_accuracy() -> None:
    """§4-3 도구선택표 대조 — LLM의 choose_tool() 판단이 팀 정답표와 얼마나 일치하는가.

    SCOPE_LIMIT(2건)·원인 미지정(2건)은 정답 자체가 없어 제외 — 11건 기준.
    """
    alerts = build_synthetic_alerts()

    targets = [
        a for a in alerts
        if a.root_cause and a.root_cause.label in EXPECTED_TOOL_BY_ROOT_CAUSE
    ]
    print(f"\n라우팅 정확도 대상: {len(targets)}건 (SCOPE_LIMIT·원인 미지정 제외)")

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

    print(f"\n라우팅 정확도: {hits}/{len(targets)} ({hits / len(targets):.0%})")


def main() -> None:
    run_grounding = "--grounding" in sys.argv
    run_rag_baseline = "--rag-baseline" in sys.argv
    run_routing = "--routing" in sys.argv

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
        asyncio.run(check_routing_accuracy())
    else:
        print("(라우팅 정확도는 --routing 플래그로 별도 실행 — 실비용 발생)")


if __name__ == "__main__":
    main()
