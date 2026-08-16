"""실험⑥ 프롬프트3 평가 — 원인 분류 정확도 ([6] 원인 진단).

번호 근거: ①~④(mock 정의서 §579) + ⑤ RAG 베이스라인 비교(개선안 로직 §182, 지인).
   ⑤가 이미 RAG 실험이라 원인분류는 ⑥. 지인 승인 완료(2026-07-23).
   → mock 정의서 "4종"·기술 정리 "①~④"는 노션에서 6종으로 갱신 필요(타 담당 문서).

무엇을 재나: 발화 건의 원인 라벨(예: 사진_색감_오차)이 정답과 맞나 + '원인 특정'
            판정(최다 ≥50% AND ≥5건)이 골든과 일치하나
어떻게:     골든 라벨이 붙은 문의 텍스트 → 프롬프트3 배치 분류 → 골든과 대조
비용:       배치 호출(기본 20건/콜). --limit 200 이면 배치 10여 회 수준.

⚠️ 이 실험은 detection_confidence(신뢰도 출력)와 다르다.
   - 여기: 원인 라벨이 '맞았나'를 채점 = 성능 평가
   - decide_confidence(): 원인이 '일관됐나'를 재료로 신뢰도를 정함 = 런타임 출력
   섞지 말 것.

⚠️ 스코프: 색상/사이즈/소재만 채점한다. **파손·오배송은 제외**(이상탐지 로직에서
   원인 후보 미정의·개선안 없음). 골든에 그 aspect 로 원인 라벨이 붙어 있어도 버린다.

⚠️ 시험지 유출 금지. 프롬프트3의 few-shot 예시는 평가셋 밖에서 사람이 작성한다.
   생성 프롬프트(작가)와 원인분류 프롬프트(응시자)를 분리 관리할 것.

실행:
    python eval/run_cause_eval.py --dry-run       # 비용 0, 표본 구성만 확인
    python eval/run_cause_eval.py --limit 200     # 미니 (기본)
    python eval/run_cause_eval.py --limit 0       # 전량
    python eval/run_cause_eval.py --golden data/golden/golden_cs_labels.csv

재현성: --seed 로 표본이 고정된다. 결과 JSON 에 프롬프트 버전·모델·시드·일시를 남긴다.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import get_settings
from app.core.console import force_utf8_output
from app.core.constants import CONSISTENT_COUNT, CONSISTENT_RATIO, KST
from app.detection.cause import classify_cause, judge_cause
from app.detection.scope import SCOPE_ASPECTS

GOLDEN_LABELS = Path("data/golden/golden_cs_labels.csv")
GOLDEN_MAPPING = ROOT / "data" / "golden" / "golden_mapping.csv"
INPUT_INQUIRIES = ROOT / "data" / "input" / "input_cs_inquiries.csv"
INPUT_CHANNEL_PRODUCTS = ROOT / "data" / "input" / "input_channel_products.csv"
RESULTS_DIR = ROOT / "eval" / "results"

# confidence 캘리브레이션 확인용 구간 (service.py 미결사항: 0.5~0.8 이 비면 사실상 이진 플래그).
CONFIDENCE_BUCKETS = [(0.0, 0.5), (0.5, 0.8), (0.8, 0.9), (0.9, 1.01)]


def _read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_dataset(golden_path: Path) -> list[dict]:
    """골든 원인 라벨이 붙은 스코프 내(색상·사이즈·소재) 문의를 텍스트와 조인한다.

    Returns:
        [{"cs_id", "aspect", "raw_text", "channel", "product", "true_cause"}, ...]
    """
    inquiries = {r["inquiry_id"]: r for r in _read_csv(INPUT_INQUIRIES)}

    # channel_product_id → 상품 그룹(golden_group_id). '원인 특정' 판정 단위용.
    group_of_variant = {
        r["variant_row_id"]: r["golden_group_id"] for r in _read_csv(GOLDEN_MAPPING)
    }
    product_of: dict[tuple[str, str], str] = {}
    for r in _read_csv(INPUT_CHANNEL_PRODUCTS):
        group = group_of_variant.get(r["variant_row_id"])
        if group:
            product_of.setdefault((r["channel"], r["channel_product_id"]), group)

    rows: list[dict] = []
    missing_text = 0
    for label in _read_csv(golden_path):
        cause = label["true_cause"].strip()
        aspect = label["true_aspect"].strip()
        # 스코프 밖(파손·오배송 등)은 원인 후보 자체가 없으므로 채점 대상이 아니다.
        if not cause or aspect not in SCOPE_ASPECTS:
            continue
        inquiry = inquiries.get(label["inquiry_id"])
        if inquiry is None:
            missing_text += 1
            continue
        channel = inquiry["channel"]
        rows.append(
            {
                "cs_id": label["inquiry_id"],
                "aspect": aspect,
                "raw_text": inquiry["content"],
                "channel": channel,
                "product": product_of.get((channel, inquiry["channel_product_id"]), "?"),
                "true_cause": cause,
            }
        )

    if missing_text:
        print(f"⚠️  텍스트를 못 찾은 골든 행 {missing_text}건 — 채점에서 제외")
    return rows


def sample_rows(rows: list[dict], limit: int, seed: int) -> list[dict]:
    """aspect 비율을 유지한 층화 표본. limit<=0 이면 전량.

    미니 실험이라도 aspect 가 한쪽으로 쏠리면 정확도가 그 aspect 성능이 돼버리므로
    원 분포를 유지한다. seed 고정 → 재현 가능.
    """
    if limit <= 0 or limit >= len(rows):
        return rows

    rng = random.Random(seed)
    by_aspect: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_aspect[r["aspect"]].append(r)

    picked: list[dict] = []
    for aspect, group in sorted(by_aspect.items()):
        quota = round(limit * len(group) / len(rows))
        picked.extend(rng.sample(group, min(quota, len(group))))

    # 반올림 오차 보정 — 할당량 합이 limit 에 못 미치면 남은 것에서 채운다.
    chosen_ids = {r["cs_id"] for r in picked}
    leftover = [r for r in rows if r["cs_id"] not in chosen_ids]
    rng.shuffle(leftover)
    picked.extend(leftover[: max(0, limit - len(picked))])
    return picked[:limit]


async def run_batches(
    rows: list[dict], batch_size: int, concurrency: int
) -> tuple[dict[str, dict], list[dict]]:
    """aspect 별 배치를 호출하고 성공 응답과 격리된 실패를 함께 돌려준다."""
    by_aspect: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_aspect[r["aspect"]].append(r)

    batches: list[tuple[str, list[dict]]] = []
    for aspect, group in sorted(by_aspect.items()):
        for i in range(0, len(group), batch_size):
            batches.append((aspect, group[i : i + batch_size]))

    semaphore = asyncio.Semaphore(concurrency)
    print(f"→ LLM 배치 {len(batches)}회 호출 (배치당 최대 {batch_size}건, 동시 {concurrency})")

    async def one(index: int, aspect: str, chunk: list[dict]) -> tuple[list[dict], dict | None]:
        items = [{"cs_id": r["cs_id"], "raw_text": r["raw_text"]} for r in chunk]
        try:
            async with semaphore:
                results = await classify_cause(
                    aspect, items, trace_key=f"cause_eval batch={index} aspect={aspect}"
                )
        except Exception as exc:  # noqa: BLE001 - 유료 평가의 배치별 실패 격리
            failure = {
                "batch": index,
                "aspect": aspect,
                "n_items": len(chunk),
                "cs_ids": [item["cs_id"] for item in items],
                "error": f"{type(exc).__name__}: {exc}",
            }
            print(
                f"   [{index + 1}/{len(batches)}] {aspect} {len(chunk)}건 → 실패: "
                f"{failure['error']}"
            )
            return [], failure
        print(f"   [{index + 1}/{len(batches)}] {aspect} {len(chunk)}건 → {len(results)}건 응답")
        return results, None

    done = await asyncio.gather(
        *(one(i, aspect, chunk) for i, (aspect, chunk) in enumerate(batches))
    )
    prediction_map = {
        row["cs_id"]: row
        for results, _failure in done
        for row in results
        if row.get("cs_id")
    }
    failures = [failure for _results, failure in done if failure is not None]
    return prediction_map, failures


def score(rows: list[dict], predictions: dict[str, dict]) -> dict:
    """라벨 정확도 · aspect_match · confidence 캘리브레이션 · '원인 특정' 일치."""
    scored = [r for r in rows if r["cs_id"] in predictions]
    unanswered = len(rows) - len(scored)

    hit = 0
    per_aspect: dict[str, list[int]] = defaultdict(lambda: [0, 0])  # [맞음, 전체]
    confusion: Counter = Counter()
    aspect_match_false = 0
    buckets: dict[str, list[int]] = {f"{lo}~{hi}": [0, 0] for lo, hi in CONFIDENCE_BUCKETS}

    for r in scored:
        pred = predictions[r["cs_id"]]
        predicted = pred.get("cause", "")
        correct = predicted == r["true_cause"]

        hit += correct
        per_aspect[r["aspect"]][0] += correct
        per_aspect[r["aspect"]][1] += 1
        confusion[(r["true_cause"], predicted)] += 1
        if not pred.get("aspect_match", True):
            aspect_match_false += 1

        confidence = pred.get("confidence")
        if isinstance(confidence, (int, float)):
            for lo, hi in CONFIDENCE_BUCKETS:
                if lo <= confidence < hi:
                    key = f"{lo}~{hi}"
                    buckets[key][0] += correct
                    buckets[key][1] += 1
                    break

    # '원인 특정' 판정 일치 — (상품, aspect, 채널) 그룹별 judge_cause 결과 비교.
    # 예측 쪽은 런타임과 동일하게 aspect_match=false 를 먼저 걷어낸다 (cause.diagnose_cause).
    group_rows: dict[tuple, list[dict]] = defaultdict(list)
    for r in scored:
        group_rows[(r["product"], r["aspect"], r["channel"])].append(r)

    verdict_match = 0
    label_match = 0
    groups = []
    for key, members in sorted(group_rows.items(), key=lambda kv: str(kv[0])):
        gold_label, gold_consistent, _ = judge_cause([m["true_cause"] for m in members])
        pred_label, pred_consistent, _ = judge_cause(
            [
                predictions[m["cs_id"]].get("cause", "")
                for m in members
                if predictions[m["cs_id"]].get("aspect_match", True)
            ]
        )
        verdict_match += gold_consistent == pred_consistent
        label_match += gold_label == pred_label
        groups.append(
            {
                "group": list(key),
                "n": len(members),
                "golden": {"label": gold_label, "consistent": gold_consistent},
                "predicted": {"label": pred_label, "consistent": pred_consistent},
            }
        )

    return {
        "n_sampled": len(rows),
        "n_scored": len(scored),
        "n_unanswered": unanswered,
        "label_accuracy": round(hit / len(scored), 4) if scored else 0.0,
        "per_aspect_accuracy": {
            a: {"accuracy": round(c / t, 4), "n": t} for a, (c, t) in sorted(per_aspect.items())
        },
        "aspect_match_false_rate": round(aspect_match_false / len(scored), 4) if scored else 0.0,
        "confidence_buckets": {
            k: {"accuracy": round(c / t, 4) if t else None, "n": t} for k, (c, t) in buckets.items()
        },
        "cause_identification": {
            "n_groups": len(groups),
            "consistent_verdict_match": round(verdict_match / len(groups), 4) if groups else 0.0,
            "label_match": round(label_match / len(groups), 4) if groups else 0.0,
            "groups": groups,
        },
        "confusion": [
            {"true": t, "pred": p, "n": n} for (t, p), n in confusion.most_common() if t != p
        ],
    }


def report(result: dict) -> None:
    meta = result["meta"]
    s = result["scores"]
    print("\n" + "=" * 62)
    print(f"실험⑥ 원인분류 — {meta['prompt_version']} / {meta['model']} / seed={meta['seed']}")
    print("=" * 62)
    print(f"채점 {s['n_scored']}건 (표본 {s['n_sampled']}, 무응답 {s['n_unanswered']})")
    print(f"\n■ 라벨 정확도  {s['label_accuracy']:.1%}")
    for aspect, v in s["per_aspect_accuracy"].items():
        print(f"    {aspect:5s} {v['accuracy']:.1%}  (n={v['n']})")
    print(
        f"\n■ aspect_match=false 비율  {s['aspect_match_false_rate']:.1%}"
        "  (>20% 면 상류 aspect 분류 재점검)"
    )
    print("\n■ confidence 구간별 정확도 (단조 증가 + 중간구간 분포 확인)")
    for bucket, v in s["confidence_buckets"].items():
        acc = f"{v['accuracy']:.1%}" if v["accuracy"] is not None else "  -  "
        print(f"    {bucket:9s} {acc}  (n={v['n']})")
    ci = s["cause_identification"]
    print(f"\n■ '원인 특정' 판정 (최다 ≥{CONSISTENT_RATIO:.0%} AND ≥{CONSISTENT_COUNT}건)")
    print(
        f"    그룹 {ci['n_groups']}개 | 일관여부 일치 {ci['consistent_verdict_match']:.1%}"
        f" | 주원인 라벨 일치 {ci['label_match']:.1%}"
    )
    if s["confusion"]:
        print("\n■ 혼동 상위 (정답 → 예측)")
        for c in s["confusion"][:8]:
            print(f"    {c['true']} → {c['pred']}  {c['n']}건")


async def main_async(args: argparse.Namespace) -> int:
    golden_path = Path(args.golden)
    if not golden_path.is_absolute():
        golden_path = ROOT / golden_path

    rows = load_dataset(golden_path)
    sampled = sample_rows(rows, args.limit, args.seed)

    print(f"골든: {golden_path}")
    print(f"스코프 내 원인 라벨 {len(rows)}건 → 표본 {len(sampled)}건")
    print(f"  aspect 구성: {dict(Counter(r['aspect'] for r in sampled))}")
    print(f"  정답 분포:   {dict(Counter(r['true_cause'] for r in sampled).most_common())}")

    if args.dry_run:
        n_batches = sum(
            -(-len([r for r in sampled if r["aspect"] == a]) // args.batch_size)
            for a in {r["aspect"] for r in sampled}
        )
        print(f"\n[dry-run] LLM 호출 안 함. 실제 실행 시 배치 약 {n_batches}회.")
        return 0

    predictions, batch_failures = await run_batches(
        sampled, args.batch_size, args.concurrency
    )

    result = {
        "meta": {
            "experiment": "⑥ 원인분류 정확도",
            "run_at": datetime.now(KST).isoformat(timespec="seconds"),
            "golden": golden_path.name,
            "prompt_version": args.prompt_version,
            "model": get_settings().llm_model,
            "seed": args.seed,
            "limit": args.limit,
            "batch_size": args.batch_size,
            "batch_failures": len(batch_failures),
            "batch_failure_details": batch_failures,
        },
        "scores": score(sampled, predictions),
    }
    report(result)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    out = RESULTS_DIR / f"cause_eval_{stamp}.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n결과 저장: eval/results/{out.name}")
    if batch_failures:
        print(
            f"⚠️ 배치 실패 {len(batch_failures)}건은 무응답으로 채점했습니다. "
            "결과는 저장했지만 실행은 실패 상태로 종료합니다."
        )
    return len(batch_failures)


def main() -> None:
    # 🔴 첫 문장이어야 한다. ⚠️ 이 파일은 `--help` 가 원래 통과한다 — `description` 이
    #    리터럴이라 docstring 의 `—`·`⚠️` 가 도움말에 안 실린다(`⑥` 은 cp949 에 있다).
    #    대신 아래 채점 결과 출력이 그 문자를 써서 결과가 통째로 사라진다.
    force_utf8_output()

    parser = argparse.ArgumentParser(description="실험⑥ 프롬프트3 원인분류 정확도")
    parser.add_argument("--golden", default=str(GOLDEN_LABELS), help="골든 라벨 CSV 경로")
    parser.add_argument("--limit", type=int, default=200, help="표본 수 (0=전량)")
    parser.add_argument("--seed", type=int, default=42, help="표본 추출 시드 (재현용)")
    parser.add_argument("--batch-size", type=int, default=20, help="LLM 배치당 문의 수")
    parser.add_argument("--concurrency", type=int, default=4, help="동시 배치 호출 수")
    parser.add_argument(
        "--prompt-version", default="classify_cause_v1", help="결과 기록용 프롬프트 버전 라벨"
    )
    parser.add_argument("--dry-run", action="store_true", help="LLM 호출 없이 표본 구성만 출력")
    args = parser.parse_args()

    batch_failures = asyncio.run(main_async(args))
    if batch_failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
