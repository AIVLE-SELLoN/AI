"""Agent2 → Agent3 실연동 확인 (check_agent1_to_agent2.py 의 다음 구간).

무엇을 보나
-----------
1. 계약: Agent2 가 낸 DetectionAlert 를 Agent3 가 그대로 먹는가
2. 게이트: recommended_action == "개선안 생성" 인 alert 가 실제로 나오는가
   (안 나오면 run() 이 None 만 반환해 크로스체크가 성립하지 않는다)
3. 근거: retrieve_context 가 Chroma 에서 진짜 상세페이지를 찾는가
   — NO_DETAIL_TEXT 면 grounding 이 fallback 으로만 흘러 검증 의미가 반감된다
4. 산출: Recommendation 이 스키마 유효하고 evaluator·확신도가 채워지는가

Agent1(분류)은 태우지 않는다 — golden 라벨을 oracle 로 쓴다.
이 확인의 관심사는 '분류 정확도'가 아니라 '**Agent2 산출물이 Agent3 에 그대로 들어가는가**'다.
분류 오차 전파는 check_agent1_to_agent2.py 가 이미 본다.

비용
   ① Agent2 [6] 원인분류 — detect_anomaly() 가 편중형·스코프 내 후보마다 배치 1회
   ② Agent3 — alert 1건당 LLM 2회(라우팅+생성). grounding 실패 시 재시도로 최대 4회
   --max-alerts 로 ②의 상한을 건다.

   **--dry-run 은 LLM 을 한 번도 안 부르면서 판정 로직은 전부 돌린다.** 스텁
   클라이언트가 호출을 가로채 횟수를 세므로, 실제로 몇 번 부를지가 추정이 아니라
   실측으로 나온다. 돈 쓰기 전에 항상 이걸 먼저 돌릴 것.

실행:
    python scripts/crosscheck_agent2_to_agent3.py --dry-run     # 비용 0, 호출 횟수 실측
    python scripts/crosscheck_agent2_to_agent3.py               # 기본: alert 1건만 Agent3 에
    python scripts/crosscheck_agent2_to_agent3.py --max-alerts 3
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

# 앞 구간 스크립트에서 재사용 — Day 기준일과 CSV 리더가 갈라지면 두 확인의 결과가
# 서로 안 맞게 된다. 같은 폴더의 형제 모듈이라 sys.path 로 잡는다.
from check_agent1_to_agent2 import DAY1, read

# dry-run 스텁은 배치(app/batch/daily.py)와 **같은 것을 쓴다.** 복제하면 한쪽만
# 고쳤을 때 두 도구의 실측 호출 수가 갈린다.
from app.batch.daily import STUB_CAUSE, CountingClient
from app.core.console import force_utf8_output
from app.core.inquiries import build_linked_inquiries
from app.core.schemas import (
    AspectSentiment,
    ClassifiedItem,
    DetectionAlert,
    RecommendedAction,
)
from app.detection.loader import check_coverage, unreliable_slots
from app.detection.service import detect_anomaly
from app.recommendation.pipeline import (
    NO_DETAIL_TEXT,
    retrieve_context,
    run,
    should_generate,
)


def build_items(
    only_products: set[str] | None,
) -> tuple[list[ClassifiedItem], list[dict], int]:
    """golden 라벨로 ClassifiedItem 과 **원본 문서 목록**을 만든다. LLM 을 부르지 않는다.

    (channel, channel_product_id) → golden_group_id 매핑을 태워서 상품을 붙인다.
    golden 라벨이 없는 문의는 aspects=[] 로 들어간다 — 분모에는 들고 분자에는 안 드는
    정상 케이스라 버리지 않는다.

    documents 를 함께 내는 이유 — **운영과 같은 분모 경로를 타기 위해서다.**
    `detect_anomaly()` 는 documents 를 받으면 `loader.build_rows()` 로 원본에서 분모를 세고,
    안 받으면 `normalize(items)` 로 items 에서 센다. 재현 도구가 운영과 다른 경로를 타면
    결과가 이상할 때 **로직 문제인지 경로 차이인지 구분이 안 된다.**

    이 스크립트는 golden oracle 이라 CS 전량에 라벨이 있어 두 경로의 분모가 지금은
    같지만, 그건 **우연이지 보장이 아니다** — golden 에 빈 라벨이 생기면 갈린다.

    Returns:
        (ClassifiedItem 목록, 원본 문서 목록, 상품 매핑이 없어 제외한 문의 수)
    """
    variant_group = {
        r["variant_row_id"]: r["golden_group_id"] for r in read("data/golden/golden_mapping.csv")
    }
    product_of: dict[tuple[str, str], str] = {}
    for r in read("data/input/input_channel_products.csv"):
        group = variant_group.get(r["variant_row_id"])
        if group:
            product_of.setdefault((r["channel"], r["channel_product_id"]), group)

    labels = {r["inquiry_id"]: r for r in read("data/golden/golden_cs_labels.csv")}

    items: list[ClassifiedItem] = []
    documents: list[dict] = []
    skipped = 0
    for r in read("data/input/input_cs_inquiries.csv"):
        product = product_of.get((r["channel"], r["channel_product_id"]))
        if product is None:
            skipped += 1
            continue
        if only_products is not None and product not in only_products:
            continue

        label = labels.get(r["inquiry_id"], {})
        aspects = []
        if label.get("true_aspect"):
            aspects.append(
                AspectSentiment(
                    aspect=label["true_aspect"], sentiment=int(label["true_sentiment"])
                )
            )

        items.append(
            ClassifiedItem(
                item_id=r["inquiry_id"],
                source="cs",
                channel=r["channel"],
                product_group_id=product,
                raw_text=r["content"],
                aspects=aspects,
                created_at=r["inquired_at"],
            )
        )
        # 분모의 출처. loader.build_rows() 가 요구하는 키만 담는다.
        documents.append(
            {
                "id": r["inquiry_id"],
                "product": product,
                "channel": r["channel"],
                "source": "cs",
                "created_at": r["inquired_at"],
                "text": r["content"],
            }
        )

    return items, documents, skipped


def load_case(case_id: str) -> dict:
    """config_anomaly 에서 기대 케이스 1건을 고른다.

    한 case_id 가 채널별로 여러 행을 갖는다(예: SC-001 은 COUPANG/NAVER/ZIGZAG 3행).
    그중 intended_answer=TRUE 인 행이 '발화해야 하는' 조합이다.
    """
    rows = [r for r in read("data/config/config_anomaly.csv") if r["case_id"] == case_id]
    if not rows:
        raise SystemExit(f"config_anomaly 에 {case_id} 없음")

    intended = [r for r in rows if r["intended_answer"] == "TRUE"]
    if len(intended) > 1:
        picked = ", ".join(f"{r['channel']}·{r['aspect']}" for r in intended)
        print(f"⚠️  {case_id} 의 intended_answer=TRUE 가 {len(intended)}건입니다 ({picked}).")
        print("    첫 번째를 기대 케이스로 씁니다 — 다른 걸 보려면 --case 를 바꾸세요.")
    if not intended:
        print(f"⚠️  {case_id} 에 intended_answer=TRUE 인 행이 없습니다(위양성 함정 케이스).")
        print("    발화가 '안 되는' 게 정상이라 크로스체크가 성립하지 않을 수 있습니다.")

    target = intended[0] if intended else rows[0]
    return {
        "product": target["golden_group_id"],
        "channel": target["channel"],
        "aspect": target["aspect"],
        "window_end": date.fromordinal(DAY1.toordinal() + int(target["window_end_day"]) - 1),
        "past_rate": float(target["past_rate"]),
        "cur_rate": float(target["cur_rate"]),
    }


def _matches_case(alert: DetectionAlert, case: dict) -> bool:
    """이 alert 이 config_anomaly 의 기대 케이스인가.

    main_aspect 는 enum 이고 case["aspect"] 는 CSV 문자열이라 .value 로 비교한다
    (enum 과 str 을 그냥 == 하면 항상 False 여서 정렬이 조용히 무력화된다).
    """
    return (
        alert.product_group_id == case["product"]
        and alert.main_aspect.value == case["aspect"]
        and alert.channel.value == case["channel"]
    )


def rank_key(alert: DetectionAlert, case: dict) -> tuple:
    """기대 케이스에 해당하는 alert 를 먼저 검사하도록 정렬한다."""
    return (0 if _matches_case(alert, case) else 1, alert.alert_id)


def report_alert(alert: DetectionAlert, case: dict) -> None:
    # enum 은 .value 로 찍는다 — Python 3.11+ 에서 str,Enum 을 그냥 포매팅하면
    # 한글 값이 아니라 상수명(Verdict.BIASED)이 나와서 실제 payload 와 달라 보인다.
    mark = " ← 기대 케이스" if _matches_case(alert, case) else ""
    print(
        f"    [{alert.alert_id}] {alert.product_group_id} {alert.channel.value} "
        f"{alert.verdict.value} {alert.main_aspect.value} "
        f"{alert.stats.past_rate:.1%}→{alert.stats.cur_rate:.1%} "
        f"확신도={alert.detection_confidence.value} 조치={alert.recommended_action.value}{mark}"
    )
    if alert.root_cause:
        print(
            f"        원인={alert.root_cause.label} "
            f"({alert.root_cause.count}/{alert.root_cause.total}, 일관={alert.root_cause.consistent})"
        )


def _enum_value(v: object) -> str:
    """enum 이면 값, None 이면 '없음', 그 외엔 그대로."""
    if v is None:
        return "없음"
    return getattr(v, "value", v)


async def crosscheck_one(alert: DetectionAlert, documents: list[dict]) -> bool:
    """alert 1건을 Agent3 에 태우고 계약·근거·산출을 검사한다. 통과하면 True.

    **documents 를 받는 이유는 운영과 같은 경로를 타기 위해서다** — 배치는
    `build_linked_inquiries()` 로 CS 원문을 만들어 Agent3 에 넘긴다. 안 넘기면
    image_guide 근거가 0건이라 개선안이 아예 안 나오고, 그러면 이 스크립트가 재현
    도구로서 가치가 없다(`build_items` docstring 의 분모 경로 얘기와 같은 이유).
    """
    print(f"\n  ── Agent3: {alert.alert_id} ──")

    inquiries = build_linked_inquiries(alert, documents)

    # ③ 근거: LLM 없이 Chroma 조회만 — 상세페이지를 진짜로 찾는지 먼저 본다
    context = retrieve_context(alert, inquiries)
    detail = context.get("detail_text", "")
    found = bool(detail) and detail != NO_DETAIL_TEXT
    print(f"    근거 detail_text : {'✅ 조회됨' if found else '⚠️  NO_DETAIL_TEXT'}")
    if found:
        print(f'        "{detail[:60]}{"…" if len(detail) > 60 else ""}"')
    quotes = context.get("cs_quotes", "")
    has_quotes = quotes != NO_DETAIL_TEXT
    print(
        f"    근거 cs_quotes   : "
        f"{f'✅ {len(inquiries)}건' if has_quotes else '⚠️  NO_DETAIL_TEXT (원문 조회 실패)'}"
    )
    if has_quotes:
        print(f'        "{quotes.splitlines()[0][:60]}…"')
    print(f"    맥락 cs_summary  : {context.get('cs_summary', '')[:60]}")
    print(
        f"    유사사례         : "
        f"{'있음' if context.get('similar_case') else '없음(컬렉션2 비어있음 — 정상)'}"
    )

    # ①④ 계약·산출: 실제 LLM 호출.
    # 예외를 잡는 이유: 1건이 터져도 나머지 alert 검사는 계속돼야 하고, 무엇보다
    # 여기까지 쓴 LLM 비용을 스택트레이스로 날리면 안 된다.
    try:
        rec = await run(alert, inquiries)
    except Exception as exc:  # noqa: BLE001 — 어떤 실패든 결과에 남겨야 한다
        print(f"    ❌ run() 예외: {type(exc).__name__}: {exc}")
        return False

    if rec is None:
        # None 사유가 둘이라 구분해서 찍는다 — 게이트는 정상, 근거 0건은 데이터 문제다.
        if not should_generate(alert):
            print("    ❌ run() 이 None — 게이트에서 걸렸다(recommended_action 불일치)")
        else:
            print("    ❌ run() 이 None — 근거 0건(상세페이지·CS 원문 둘 다 없음)")
        return False

    # 계약 검사를 성공 출력보다 먼저 한다 — 어긋났는데 ✅ 가 먼저 찍히면 안 된다.
    if rec.alert_id != alert.alert_id:
        print(f"    ❌ alert_id 불일치: {alert.alert_id} → {rec.alert_id}")
        return False

    ev = rec.evaluator
    print(f"    ✅ Recommendation 생성 — {rec.recommendation_id}")
    if rec.proposal is None:
        # 스키마상 Proposal 은 None 을 허용한다(schemas.py:245). 정상 경로에서는
        # assemble() 이 항상 채우므로, None 이면 그 자체가 이상 신호다.
        print("    ⚠️  proposal 이 None 입니다 — 스키마상 허용이지만 정상 경로에선 안 나옵니다")
    else:
        print(f"        타입      : {_enum_value(rec.proposal.type)}")
        print(f"        본문      : {rec.proposal.proposed_text[:70]}…")
    cap = " (탐지 확신도로 강등됨)" if rec.capped_by_detection else ""
    print(f"        확신도    : {_enum_value(rec.recommendation_confidence)}{cap}")
    print(f"        산출근거  : {rec.confidence_reason}")
    print(
        f"        evaluator : passed={ev.passed} attempts={ev.attempts} "
        f"grounding={ev.checks.grounding} consistency={ev.checks.consistency} "
        f"actionability={ev.checks.actionability}"
    )
    print(f"        citations : {len(rec.citations)}건")
    return True


def print_cost_estimate(stub: CountingClient, alerts: list, max_alerts: int) -> None:
    gated = [a for a in alerts if should_generate(a)]
    n = min(len(gated), max_alerts)
    print(f"\n{'=' * 60}")
    print("[dry-run] 실제로 돌리면 드는 비용")
    print(f"  Agent2 [6] 원인분류 : {stub.calls}회  ← 실측(스텁이 가로채 셈)")
    print(f"  게이트 통과 alert   : {len(gated)}건 / 발행 {len(alerts)}건")
    print(f"  Agent3 (--max-alerts={max_alerts}) : {n}건 × 2~4회 = {n * 2}~{n * 4}회")
    print("  ─────────────────────────────────────")
    print(f"  합계 LLM 호출        : {stub.calls + n * 2} ~ {stub.calls + n * 4}회")

    if stub.empty_extractions:
        print(
            f"\n  ❌ 스텁이 프롬프트에서 cs_id 를 못 찾은 호출 {stub.empty_extractions}건 "
            f"— 위 숫자를 믿지 마세요."
        )
        print("     프롬프트 포맷이 바뀌었을 수 있습니다(CountingClient 의 정규식 확인).")
    else:
        print(f"\n  ⚠️ root_cause 라벨은 스텁 고정값({STUB_CAUSE})입니다.")
        print("     호출 횟수·발행 건수·게이트 통과 수는 실제 판정 로직이 낸 값입니다.")


async def main(args: argparse.Namespace) -> None:
    case = load_case(args.case)
    only = None if args.products == "all" else {case["product"]}

    print("입력 데이터 로딩 중... (CSV 4종, 문의 약 10만 행)")
    items, documents, skipped = build_items(only)
    if not items:
        raise SystemExit("입력 ClassifiedItem 이 0건 — 매핑/필터를 확인하세요")

    # golden oracle 이라 커버리지는 100% 여야 정상이다. 아니면 golden 에 빈 라벨이
    # 생긴 것이고, 그 슬롯은 검정 전에 BH family 에서 통째로 빠진다(로더 §unreliable).
    # 조용히 빠지면 "왜 이 케이스가 안 뜨지?"의 원인을 못 찾으므로 여기서 알린다.
    gaps = check_coverage(documents, items)
    if gaps:
        slots = unreliable_slots(gaps)
        print(f"\n  ⚠️ 분류 커버리지 미달 {len(gaps)}일자 / {len(slots)}슬롯 — 검정에서 제외됩니다.")
        for slot in sorted(slots)[:5]:
            print(f"     {slot}")

    products = {i.product_group_id for i in items}
    print(f"\n기대 케이스 {args.case} | 상품 {case['product']} · {case['channel']} · {case['aspect']}")
    print(f"  의도: {case['past_rate']:.1%} → {case['cur_rate']:.1%}")
    print(f"  현재 윈도우 마지막 날: {case['window_end']}")
    print(f"  입력 ClassifiedItem {len(items)}건 / 상품 {len(products)}개 (golden 라벨 oracle)")
    print(f"  매핑 없어 제외된 문의 {skipped}건")

    stub = CountingClient() if args.dry_run else None
    how = "스텁 클라이언트 — LLM 호출 0" if args.dry_run else "실제 LLM"
    print(f"\nAgent2 탐지 중... ({how})")
    # documents 를 함께 넘겨 **운영과 같은 분모 경로**를 탄다 (build_items docstring 참고).
    alerts, suppressed = await detect_anomaly(
        items, documents=documents, window_end=case["window_end"], client=stub
    )
    print(f"  발행 {len(alerts)}건 / 억제 {len(suppressed)}건")
    for a in alerts:
        report_alert(a, case)

    if not alerts:
        print("\n⚠️  알림 0건 — Agent3 에 넘길 게 없습니다.")
        if args.products == "case":
            print("    --products all 로 BH family 를 키워보세요(단일 상품이면 컷오프를 못 넘길 수 있습니다).")
        else:
            print("    다른 --case 를 시도하거나, config_anomaly 의 의도값과 대조해보세요.")
        return

    if stub is not None:
        print_cost_estimate(stub, alerts, args.max_alerts)
        return

    # 기대 케이스가 어떻게 됐는지 먼저 명시한다. 이걸 안 찍으면 게이트에서 막힌 케이스일 때
    # 엉뚱한 alert 을 대신 검사하고 "통과"로 읽게 된다.
    expected = [a for a in alerts if _matches_case(a, case)]
    print(f"\n기대 케이스({args.case}) alert:")
    if not expected:
        print("    ⚠️  발화하지 않았습니다 — 아래 검사는 다른 alert 으로 진행됩니다.")
        print("       위양성 함정 케이스면 발화 안 하는 게 정상입니다(config 의 intended_answer 확인).")
    elif should_generate(expected[0]):
        print("    ✅ 발화 + 게이트 통과 — Agent3 검사 대상입니다.")
    else:
        ea = expected[0]
        print(f"    ⛔ 발화했으나 게이트에서 막힘 (조치={ea.recommended_action.value})")
        # run() 은 게이트에서 바로 None 을 내므로 LLM 을 안 부른다 — 공짜 검증이다.
        # inquiries 를 안 넘기는 게 맞다: 게이트가 근거 조회보다 먼저라 쓸 일이 없다.
        blocked = await run(ea)
        ok = "✅ None (정상 차단)" if blocked is None else f"❌ None 이 아님: {blocked}"
        print(f"       run() 반환: {ok}")

    targets = [a for a in alerts if should_generate(a)]
    print(f"\n게이트 통과(개선안 생성) {len(targets)}건 / 전체 {len(alerts)}건")
    if not targets:
        # 개선안 생성이 하나도 없는 배치는 '검사 불가'가 아니라 **게이트 검증 기회**다.
        # run() 은 should_generate 에서 바로 None 을 내므로 LLM 을 안 부른다(비용 0).
        print(
            f"⚠️  '{RecommendedAction.GENERATE_RECOMMENDATION.value}' 인 alert 가 없습니다 — "
            f"대신 게이트가 제대로 막는지 확인합니다 (LLM 0회)."
        )
        blocked = 0
        for a in alerts:
            rec = await run(a)
            mark = "✅ None" if rec is None else f"❌ 생성됨 {rec.recommendation_id}"
            print(f"    [{a.alert_id}] 조치={a.recommended_action.value} → run() {mark}")
            if rec is None:
                blocked += 1
        print(f"\n{'=' * 60}")
        print(f"결과: 게이트 차단 {blocked}/{len(alerts)} 건 정상")
        if blocked == len(alerts):
            print("✅ 스코프 밖·비생성 조치는 Agent3 가 정확히 걸러냅니다.")
        else:
            print("❌ 막혔어야 할 alert 에서 개선안이 생성됐습니다.")
        return

    targets.sort(key=lambda a: rank_key(a, case))
    targets = targets[: args.max_alerts]
    print(f"이 중 {len(targets)}건을 Agent3 에 태웁니다 (--max-alerts={args.max_alerts})")

    passed = 0
    for alert in targets:
        if await crosscheck_one(alert, documents):
            passed += 1

    print(f"\n{'=' * 60}")
    print(f"결과: {passed}/{len(targets)} 건 통과")
    if passed == len(targets):
        print("✅ Agent2 → Agent3 계약 통과 — 결선 코드를 이 전제 위에 짜도 됩니다.")
    else:
        print("❌ 실패 건이 있습니다 — 결선 전에 원인을 먼저 잡으세요.")


if __name__ == "__main__":
    # 첫 문장이어야 한다. 이 파일은 `--help` 자체는 통과한다 — `description` 이 리터럴이라
    # docstring 의 `—`·`⚠️` 가 도움말에 안 실리고, `→`(U+2192)는 cp949 에 **있다**. 대신 아래
    # 대조 결과 출력이 그 문자를 써서 결과가 통째로 사라진다.
    force_utf8_output()

    ap = argparse.ArgumentParser(description="Agent2 → Agent3 실연동 확인")
    ap.add_argument("--case", default="SC-001", help="config_anomaly 의 case_id (기대 케이스)")
    ap.add_argument(
        "--products",
        choices=["all", "case"],
        default="all",
        help="all=전 상품(BH family 현실적) / case=해당 케이스 상품만(알림이 안 뜰 수 있음)",
    )
    ap.add_argument(
        "--max-alerts", type=int, default=1, help="Agent3 에 태울 alert 수 상한 (비용 통제)"
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="LLM 을 부르지 않고 판정 로직만 돌려 호출 횟수를 실측한다(비용 0)",
    )
    asyncio.run(main(ap.parse_args()))
