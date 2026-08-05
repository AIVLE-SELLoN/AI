"""실험③ 프롬프트1 평가 — aspect 분류기 정확도 (실제 golden_cs_labels.csv 기반).

무엇을 재나: aspect F1(다중 예측 vs 단일 정답), 감성 정확도(aspect 일치 건 중),
            완전일치율
            
어떻게:     golden_cs_labels.csv에서 층화표본 추출 → input_cs_inquiries.csv와 조인
            → app.classification.service.classify_aspect() 실제 호출(진짜 서비스 코드
            재사용, app/는 golden을 안 읽으므로 컨닝 아님) → golden과 대조
비용:       배치(청크) 단위 동시호출. --limit 300이면 청크 15회 수준(청크당 20건).

⚠️ 이 실험은 브라우저 Artifact로 했던 42/48건 파일럿 평가와 다르다 — 그건 손으로
   만든 소량 케이스 검증(정성적 오류분석용), 이건 우리가 실제로 생성한 96,514건
   규모 데이터로 돌리는 정량 실험.

⚠️ 프롬프트 버전은 기본적으로 service.py의 PROMPT_ASPECT_VERSION을 그대로 쓴다.
   다른 버전과 비교하고 싶으면 --prompt-version으로 override(모듈 상수를 일시적으로
   바꿔서 호출 — service.py 자체는 안 건드림).

실행:
    python eval/run_classify_eval.py --dry-run        # 비용 0, 표본 구성만 확인
    python eval/run_classify_eval.py --limit 300       # 미니 (기본)
    python eval/run_classify_eval.py --limit 0         # 전량(96,514건 — 매우 비쌈, 권장 안 함)
    python eval/run_classify_eval.py --prompt-version classify_aspect_v3   # 버전 비교

재현성: --seed로 표본이 고정된다. 결과 JSON에 프롬프트 버전·모델·시드·일시를 남긴다.
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

import app.classification.service as classification_service
from app.classification.service import ClassifyRequestItem
from app.core.exceptions import AiServiceError
from app.core.llm_client import get_llm_client
from app.core.schemas import Channel, Source

GOLDEN_LABELS = ROOT / "data" / "golden" / "golden_cs_labels.csv"
INPUT_INQUIRIES = ROOT / "data" / "input" / "input_cs_inquiries.csv"
RESULTS_DIR = ROOT / "eval" / "results"

VALID_ASPECTS = {"색상", "사이즈", "소재", "파손", "오배송", "기타"}


def _read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_dataset(golden_path: Path) -> list[dict]:
    """golden_cs_labels.csv를 input_cs_inquiries.csv와 조인.

    Returns:
        [{"inquiry_id", "raw_text", "channel", "true_aspect", "true_sentiment"}, ...]
    """
    inquiries = {r["inquiry_id"]: r for r in _read_csv(INPUT_INQUIRIES)}

    rows: list[dict] = []
    missing_text = 0
    for label in _read_csv(golden_path):
        inquiry = inquiries.get(label["inquiry_id"])
        if inquiry is None:
            missing_text += 1
            continue
        rows.append(
            {
                "inquiry_id": label["inquiry_id"],
                "raw_text": inquiry["content"],
                "channel": inquiry["channel"],
                "true_aspect": label["true_aspect"].strip(),
                "true_sentiment": int(label["true_sentiment"]),
            }
        )

    if missing_text:
        print(f"⚠️  텍스트를 못 찾은 골든 행 {missing_text}건 — 채점에서 제외")
    return rows


def sample_rows(rows: list[dict], limit: int, seed: int, only_negative: bool = False) -> list[dict]:
    """true_aspect 비율을 유지한 층화 표본. limit<=0이면 전량.

    only_negative=True면 sentiment=-1인 것만 대상으로 표본을 뽑는다(지인님 A안
    ①②, 2026-08-04) — 탐지가 실제로 소비하는 값은 부정(전체의 7.3%)뿐인데,
    aspect 기준으로만 층화하면 --limit 300에도 부정이 약 22건밖에 안 뽑혀서
    "부정 한정 aspect 정확도" 같은 지표가 통계적으로 무의미해진다.
    """
    if only_negative:
        rows = [r for r in rows if r["true_sentiment"] == -1]

    if limit <= 0 or limit >= len(rows):
        return rows

    rng = random.Random(seed)
    by_aspect: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_aspect[r["true_aspect"]].append(r)

    picked: list[dict] = []
    for aspect, group in sorted(by_aspect.items()):
        quota = round(limit * len(group) / len(rows))
        picked.extend(rng.sample(group, min(quota, len(group))))

    chosen_ids = {r["inquiry_id"] for r in picked}
    leftover = [r for r in rows if r["inquiry_id"] not in chosen_ids]
    rng.shuffle(leftover)
    picked.extend(leftover[: max(0, limit - len(picked))])
    # aspect 순서대로 쌓여 있어 그냥 자르면 뒤쪽 aspect가 덜 뽑힌다 — 자르기 전에 섞는다.
    rng.shuffle(picked)
    return picked[:limit]


async def run_chunks(
    rows: list[dict], chunk_size: int, concurrency: int
) -> tuple[dict[str, list[dict]], list[str]]:
    """청크로 나눠 app.classification.service.classify_aspect()를 실제 호출.

    classify_aspect() 내부는 asyncio.gather()로 청크 안 항목을 동시 호출하는데,
    청크 하나라도 예외 나면 그 청크 전체 결과가 날아간다(gather가 return_exceptions
    없이 실패하면 통째로 raise) — 청크 단위로 감싸서, 실패한 청크는 "무응답"으로
    기록하고 나머지는 살린다.

    ⚠️ 이름은 "청크"지만 실제로는 item마다 별도 LLM 호출(동시 실행일 뿐, "진짜 배치"
    아님) — service.py의 classify_aspect()가 asyncio.gather()로 item당 개별 호출.
    프롬프트(시스템+예시, 현재 약 6,000+ 토큰)가 매 항목마다 통째로 반복 전송된다.

    Returns:
        (predictions: {inquiry_id: [{"aspect":.., "sentiment":..}, ...]}, failed_ids)
    """
    chunks = [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]
    semaphore = asyncio.Semaphore(concurrency)
    predictions: dict[str, list[dict]] = {}
    failed_ids: list[str] = []

    async def one(index: int, chunk: list[dict]) -> None:
        items = [
            ClassifyRequestItem(
                item_id=r["inquiry_id"],
                source=Source.CS,
                channel=Channel(r["channel"]),
                product_group_id="EVAL",  # 평가용 — product_group_id는 채점에 안 씀
                raw_text=r["raw_text"],
                created_at=datetime.now(),
            )
            for r in chunk
        ]
        async with semaphore:
            try:
                results = await classification_service.classify_aspect(items)
            except AiServiceError as exc:
                # classify_aspect()가 return_exceptions=True로 바뀌면서(2026-08-04
                # 계약) 개별 실패는 더 이상 여기로 안 옴 — 이 except는 gather 시작
                # 전 셋업 단계 등 정말 예외적인 전체 실패만 잡는 안전망으로 남긴다.
                print(f"   [{index + 1}/{len(chunks)}] ⚠️ 청크 전체 실패({len(chunk)}건 무응답): {exc}")
                failed_ids.extend(r["inquiry_id"] for r in chunk)
                return

        n_item_failed = 0
        for item, result in zip(items, results):
            if isinstance(result, Exception):
                # 이제 청크 전체가 아니라 실패한 이 건만 무응답 처리(계약 반영 —
                # 이전엔 1건 실패로 청크 20건 전체가 날아갔음).
                n_item_failed += 1
                failed_ids.append(item.item_id)
                continue
            predictions[item.item_id] = [
                {"aspect": a.aspect.value, "sentiment": a.sentiment.value} for a in result.aspects
            ]
        if n_item_failed:
            print(f"   [{index + 1}/{len(chunks)}] ⚠️ {n_item_failed}건 개별 실패(그 건만 무응답)")
        print(f"   [{index + 1}/{len(chunks)}] {len(chunk)}건 → {len(chunk) - n_item_failed}건 응답")

    await asyncio.gather(*(one(i, c) for i, c in enumerate(chunks)))
    return predictions, failed_ids


def _build_batch_prompt(chunk: list[dict]) -> str:
    """프롬프트 앞부분(시스템+예시)은 그대로 재사용하고, 뒷부분(분류 대상)만
    N건을 item_id와 함께 한 번에 넣는 '진짜 배치' 프롬프트를 만든다.

    service.py의 classify_aspect_v5 파일 구조: "## 분류 대상 CS 문의\n\n$cs_text"
    로 끝나는데, 배치판은 이 마지막 섹션만 N건짜리로 바꿔치기한다(예시 목록·규칙은
    1건 처리 때와 완전히 동일 — 우리가 검증한 지시문을 안 건드림).
    """
    full = classification_service.load_llm_prompt("classification", classification_service.PROMPT_ASPECT_VERSION)
    head = full.split("## 분류 대상 CS 문의")[0].strip()

    items_json = json.dumps(
        [{"item_id": r["inquiry_id"], "text": r["raw_text"]} for r in chunk],
        ensure_ascii=False,
    )
    tail = f"""

## 분류 대상 CS 문의 (총 {len(chunk)}건 — 각각 독립적인 별개 고객의 문의입니다)

아래는 서로 다른 고객의 독립적인 문의 목록입니다. **한 문의의 내용이 다른 문의의 판단에
영향을 주면 안 됩니다** — 각 건을 완전히 개별적으로 판단하세요.

{items_json}

**출력 형식** (JSON, 다른 텍스트 없이 이 형식만 출력. item_id는 입력과 정확히 동일하게,
누락·순서변경 없이 전부 포함할 것)
```json
{{"results": [{{"item_id": "...", "aspects": [{{"aspect": "...", "sentiment": ...}}]}}, ...]}}
```"""
    return head + tail


async def run_batch_chunks(
    rows: list[dict], chunk_size: int, concurrency: int
) -> tuple[dict[str, list[dict]], list[str]]:
    """'진짜 배치' — 청크(N건)를 한 프롬프트에 다 넣어 LLM 호출 1번으로 처리.

    서영님 제안(②실험 전 검증) — 프롬프트 반복 전송을 없애 비용을 크게 줄이되,
    item_id 매칭 오류·교차오염(문항끼리 서로 영향)으로 정확도가 떨어지지 않는지
    이 함수로 확인한다.
    """
    chunks = [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]
    semaphore = asyncio.Semaphore(concurrency)
    predictions: dict[str, list[dict]] = {}
    failed_ids: list[str] = []
    client = get_llm_client()

    async def one(index: int, chunk: list[dict]) -> None:
        prompt = _build_batch_prompt(chunk)
        trace_key = f"batch_chunk={index}_n={len(chunk)}"
        async with semaphore:
            try:
                data = await client.complete_json(prompt, trace_key=trace_key)
            except Exception as exc:  # noqa: BLE001 — 평가 스크립트는 실패해도 계속 진행
                print(f"   [{index + 1}/{len(chunks)}] ⚠️ 배치 호출 실패({len(chunk)}건 무응답): {exc}")
                failed_ids.extend(r["inquiry_id"] for r in chunk)
                return

        results = data.get("results", [])
        expected_ids = {r["inquiry_id"] for r in chunk}
        got_ids = set()
        hallucinated = []
        for r in results:
            iid = r.get("item_id")
            aspects = r.get("aspects", [])
            if not iid or not isinstance(aspects, list):
                continue
            if iid not in expected_ids:
                # 이 청크에 없는 item_id — LLM 환각. 채점엔 안 쓰이지만 이상 징후라 로그로 남긴다.
                hallucinated.append(iid)
                continue
            valid_aspects = [
                a for a in aspects
                if isinstance(a, dict) and a.get("aspect") in VALID_ASPECTS and a.get("sentiment") in (-1, 0, 1)
            ]
            predictions[iid] = valid_aspects
            got_ids.add(iid)

        if hallucinated:
            print(f"   [{index + 1}/{len(chunks)}] ⚠️ 요청에 없는 item_id {len(hallucinated)}건 응답(환각) → 무시")
        missing = expected_ids - got_ids
        if missing:
            print(f"   [{index + 1}/{len(chunks)}] ⚠️ 응답에서 누락된 item_id {len(missing)}건 → 무응답 처리")
            failed_ids.extend(missing)
        print(f"   [{index + 1}/{len(chunks)}] {len(chunk)}건 → {len(got_ids)}건 응답(배치 1회 호출)")

    await asyncio.gather(*(one(i, c) for i, c in enumerate(chunks)))
    return predictions, failed_ids


def score(rows: list[dict], predictions: dict[str, list[dict]]) -> dict:
    """aspect F1(다중예측 vs 단일정답 set 비교) + 감성정확도 + 완전일치
    + 지인님 A안 지표 3종(2026-08-04, 실험③ 지표 재설계).

    ⚠️ 두 지표군의 용도가 다르다(합의사항 — 결과 JSON에 병기):
    - aspect_f1 등 기존 지표: 대시보드·채널비교분석·개선리포트가 소비(중립 포함
      전체 영역). 대표 지표 자리에서는 내려왔지만 폐기 아님.
    - negative_detection / negative_scoped_aspect: 이상탐지가 실제로 소비하는
      값(sentiment=-1만). 이제 대표 지표.
    """
    scored = [r for r in rows if r["inquiry_id"] in predictions]
    unanswered = len(rows) - len(scored)

    tp = fp = fn = 0
    sent_correct = sent_total = 0
    exact_match = 0
    per_aspect: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])  # [tp, fp, fn]
    mismatches: list[dict] = []  # 🆕 오답 상세(수동 오류분석용)

    # 🆕 ① 부정 판별 정확도용 2x2
    neg_tp = neg_fp = neg_fn = neg_tn = 0
    # 🆕 ② 부정 한정 aspect 정확도용(골든이 부정인 문항만)
    neg_aspect_tp = neg_aspect_fp = neg_aspect_fn = 0
    # 🆕 ③ 예측 측 다중 출력 건수(진단용, explode 계약 근거)
    multi_output_count = 0

    for r in scored:
        pred_aspects = predictions[r["inquiry_id"]]
        pred_aspect_set = {p["aspect"] for p in pred_aspects}
        true_aspect = r["true_aspect"]
        true_sentiment = r["true_sentiment"]

        if len(pred_aspect_set) >= 2:
            multi_output_count += 1

        if true_aspect in pred_aspect_set:
            tp += 1
            per_aspect[true_aspect][0] += 1
        else:
            fn += 1
            per_aspect[true_aspect][2] += 1

        extra = pred_aspect_set - {true_aspect}
        fp += len(extra)
        for a in extra:
            per_aspect[a][1] += 1

        matched_pred = next((p for p in pred_aspects if p["aspect"] == true_aspect), None)
        item_exact = len(pred_aspect_set) == 1 and true_aspect in pred_aspect_set
        if matched_pred is not None:
            sent_total += 1
            sent_ok = matched_pred["sentiment"] == true_sentiment
            sent_correct += sent_ok
            item_exact = item_exact and sent_ok
        else:
            item_exact = False
        exact_match += item_exact

        # 🆕 ① 문의 단위 부정/비부정 2x2 — 탐지의 분자를 결정하는 값
        true_neg = true_sentiment == -1
        pred_neg = any(p["sentiment"] == -1 for p in pred_aspects)
        if true_neg and pred_neg:
            neg_tp += 1
        elif true_neg and not pred_neg:
            neg_fn += 1
        elif not true_neg and pred_neg:
            neg_fp += 1
        else:
            neg_tn += 1

        # 🆕 ② 골든이 부정인 문항만 — aspect가 맞아야 올바른 분자(상품×aspect)에 잡힘
        if true_neg:
            if true_aspect in pred_aspect_set:
                neg_aspect_tp += 1
            else:
                neg_aspect_fn += 1
            neg_aspect_fp += len(pred_aspect_set - {true_aspect})

        if not item_exact:  # 🆕 틀린 문항만 상세 기록
            mismatches.append({
                "inquiry_id": r["inquiry_id"],
                "raw_text": r["raw_text"],
                "true_aspect": true_aspect,
                "true_sentiment": true_sentiment,
                "predicted": pred_aspects,
            })

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    def _prf1(tp_, fp_, fn_):
        p = tp_ / (tp_ + fp_) if (tp_ + fp_) else 0.0
        r = tp_ / (tp_ + fn_) if (tp_ + fn_) else 0.0
        f = 2 * p * r / (p + r) if (p + r) else 0.0
        return round(p, 4), round(r, 4), round(f, 4)

    neg_precision, neg_recall, neg_f1 = _prf1(neg_tp, neg_fp, neg_fn)
    neg_asp_precision, neg_asp_recall, neg_asp_f1 = _prf1(neg_aspect_tp, neg_aspect_fp, neg_aspect_fn)
    n_true_negative = neg_tp + neg_fn  # 골든상 부정인 문항 수(표본 크기 확인용)

    return {
        "n_sampled": len(rows),
        "n_scored": len(scored),
        "n_unanswered": unanswered,
        # ── 대시보드·리포트용(중립 포함 전체 영역) — 대표 지표 아님, 용도별 유지 ──
        "aspect_f1": round(f1, 4),
        "aspect_f1_note": "대시보드·채널비교분석·개선리포트 소비 지표(중립 포함) — 탐지는 안 씀",
        "aspect_precision": round(precision, 4),
        "aspect_recall": round(recall, 4),
        "sentiment_accuracy": round(sent_correct / sent_total, 4) if sent_total else 0.0,
        "exact_match_rate": round(exact_match / len(scored), 4) if scored else 0.0,
        "per_aspect": {
            a: {
                "precision": round(v[0] / (v[0] + v[1]), 4) if (v[0] + v[1]) else 0.0,
                "recall": round(v[0] / (v[0] + v[2]), 4) if (v[0] + v[2]) else 0.0,
                "n_true": v[0] + v[2],
            }
            for a, v in sorted(per_aspect.items())
        },
        # ── 이상탐지 소비 지표(부정만) — 지인님 A안, 2026-08-04부터 대표 지표 ──
        "negative_detection": {
            "note": "탐지 분자를 결정하는 값 — 문의 단위 부정/비부정 2분류",
            "precision": neg_precision, "recall": neg_recall, "f1": neg_f1,
            "tp": neg_tp, "fp": neg_fp, "fn": neg_fn, "tn": neg_tn,
        },
        "negative_scoped_aspect": {
            "note": "골든이 부정인 문항만 대상 — aspect까지 맞아야 올바른 분자에 잡힘",
            "precision": neg_asp_precision, "recall": neg_asp_recall, "f1": neg_asp_f1,
            "n_true_negative": n_true_negative,
            "n_true_negative_warning": (
                f"부정 표본이 {n_true_negative}건뿐 — 신뢰구간이 넓을 수 있음. "
                f"--only-negative 권장" if n_true_negative < 100 else None
            ),
        },
        "multi_output_diagnostic": {
            "note": "예측 측 다중 aspect 출력 건수 — explode 계약 근거 진단(정답 없어 recall 계산 불가)",
            "rate": round(multi_output_count / len(scored), 4) if scored else 0.0,
            "count": multi_output_count,
        },
        "mismatches": mismatches,  # 🆕
    }


def report(result: dict) -> None:
    meta = result["meta"]
    s = result["scores"]
    print("\n" + "=" * 62)
    print(f"실험③ 프롬프트1 aspect 분류 — {meta['prompt_version']} / {meta['model']} / seed={meta['seed']} / mode={meta.get('mode', 'per_item')}")
    print("=" * 62)
    print(f"채점 {s['n_scored']}건 (표본 {s['n_sampled']}, 무응답 {s['n_unanswered']})")

    nd = s["negative_detection"]
    na = s["negative_scoped_aspect"]
    print(f"\n★★★ [대표지표] ① 부정 판별 정확도(탐지 분자 결정)  P={nd['precision']:.1%} R={nd['recall']:.1%} F1={nd['f1']:.1%}")
    print(f"    tp={nd['tp']} fp={nd['fp']} fn={nd['fn']} tn={nd['tn']}")
    print(f"★★★ [대표지표] ② 부정 한정 aspect 정확도(탐지 분자 위치 결정)  P={na['precision']:.1%} R={na['recall']:.1%} F1={na['f1']:.1%}")
    print(f"    골든 부정 표본 n={na['n_true_negative']}" + (f"  ⚠️ {na['n_true_negative_warning']}" if na["n_true_negative_warning"] else ""))

    md = s["multi_output_diagnostic"]
    print(f"\n③ 다중 aspect 출력률(진단) = {md['rate']:.1%} ({md['count']}건) — explode 계약 근거 현황")

    print(f"\n■ [대시보드·리포트용, 중립 포함] Aspect F1  {s['aspect_f1']:.1%}  (Precision {s['aspect_precision']:.1%} / Recall {s['aspect_recall']:.1%})")
    print(f"■ 감성 정확도(aspect 일치 건 중)  {s['sentiment_accuracy']:.1%}")
    print(f"■ 완전일치(aspect+감성 100%)  {s['exact_match_rate']:.1%}")
    print("\n■ aspect별 정밀도/재현율")
    for aspect, v in s["per_aspect"].items():
        print(f"    {aspect:6s} P={v['precision']:.1%}  R={v['recall']:.1%}  (n={v['n_true']})")


async def main_async(args: argparse.Namespace) -> None:
    golden_path = Path(args.golden)
    if not golden_path.is_absolute():
        golden_path = ROOT / golden_path

    if args.prompt_version:
        classification_service.PROMPT_ASPECT_VERSION = args.prompt_version

    rows = load_dataset(golden_path)
    sampled = sample_rows(rows, args.limit, args.seed, only_negative=args.only_negative)

    print(f"골든: {golden_path}")
    print(f"전체 {len(rows)}건 → 표본 {len(sampled)}건")
    print(f"  aspect 구성: {dict(Counter(r['true_aspect'] for r in sampled))}")
    print(f"  프롬프트 버전: {classification_service.PROMPT_ASPECT_VERSION}")

    if args.dry_run:
        n_chunks = -(-len(sampled) // args.chunk_size)
        print(f"\n[dry-run] LLM 호출 안 함. 실제 실행 시 청크 약 {n_chunks}회(청크당 {args.chunk_size}건).")
        print(f"  모드: {args.mode} ({'item당 개별호출×동시실행' if args.mode == 'per_item' else '청크당 호출 1회(진짜 배치)'})")
        return

    if args.mode == "batch":
        predictions, failed_ids = await run_batch_chunks(sampled, args.chunk_size, args.concurrency)
    else:
        predictions, failed_ids = await run_chunks(sampled, args.chunk_size, args.concurrency)
    if failed_ids:
        print(f"\n⚠️ 청크 실패로 무응답 처리된 건: {len(failed_ids)}건")

    from app.config import get_settings

    result = {
        "meta": {
            "experiment": "③ 프롬프트1 aspect 분류 정확도",
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "golden": golden_path.name,
            "prompt_version": classification_service.PROMPT_ASPECT_VERSION,
            "model": get_settings().llm_model,
            "seed": args.seed,
            "limit": args.limit,
            "chunk_size": args.chunk_size,
            "mode": args.mode,  # 🆕 per_item(기존, item당 개별호출) vs batch(청크당 호출 1회)
        },
        "scores": score(sampled, predictions),
    }
    report(result)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = RESULTS_DIR / f"classify_eval_{stamp}.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n결과 저장: eval/results/{out.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="실험③ 프롬프트1 aspect 분류 정확도")
    parser.add_argument("--golden", default=str(GOLDEN_LABELS), help="골든 라벨 CSV 경로")
    parser.add_argument("--limit", type=int, default=300, help="표본 수 (0=전량)")
    parser.add_argument("--seed", type=int, default=42, help="표본 추출 시드 (재현용)")
    parser.add_argument(
        "--only-negative", action="store_true",
        help="sentiment=-1인 문항만 표본으로 뽑는다(지인님 A안 — 탐지가 실제로 소비하는 부분만 통계적으로 의미 있게 검증)",
    )
    parser.add_argument("--chunk-size", type=int, default=20, help="청크당 문의 수")
    parser.add_argument("--concurrency", type=int, default=4, help="동시 청크 호출 수")
    parser.add_argument(
        "--prompt-version", default=None, help="service.py 기본값 대신 이 버전으로 override(예: classify_aspect_v3)"
    )
    parser.add_argument(
        "--mode",
        choices=["per_item", "batch"],
        default="per_item",
        help="per_item(기존, item당 개별 LLM호출×동시실행) / batch(청크당 LLM호출 1회 — 서영님 제안 검증용)",
    )
    parser.add_argument("--dry-run", action="store_true", help="LLM 호출 없이 표본 구성만 출력")
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()