"""relabel_300 vs llm_generated_700 — 정확도 역검증 + few-shot 유출 재확인.

배경(GENERATE_700_기준.md §"같은 모델 사용에 대한 보완" 2·3번)
------------------------------------------------------------
분류기·생성기가 같은 모델(gpt-4o-mini)이라, "그 모델이 잘 만드는 패턴 = 그 모델이
잘 맞히는 패턴"이 우연히 겹칠 위험이 있다. relabel_300(진짜 사람 리뷰)을 기준선 삼아,
llm_generated_700이 부자연스럽게 더 잘 맞으면(10%p 이상 차이) 의심 신호로 본다.

동시에 §6 B안 도구(compute_leak_map)를 재사용해 llm_generated_700이 few-shot과
안 겹치는지도 확인한다 — 생성 시점에 few-shot을 안 봤다고 했지만, 우연히 비슷한
문장이 나왔을 가능성은 별도로 검증해야 한다.

평가셋 파일이 둘로 나뉘어 있는 이유 (2026-08-09)
--------------------------------------------------
AI Hub 71603 이용정책상 원문을 저장소에 두지 않는다. 그래서 relabel_300 은 두 파일이다.

    eval/eval_sets/relabel_300_labels.csv   사람이 매긴 라벨 (커밋됨, 원문 없음)
    eval/eval_sets/relabel_300.csv          AI Hub 원문 (gitignore, 로컬에만)

채점할 때 `id`(RELABEL-####)로 조인한다. 원문 파일이 없으면 어떻게 준비하는지
안내하고 종료한다(load_raw_texts 참고). 라벨은 사람 작업물이라 재생성이 안 되므로
반드시 저장소에 남는다 — select_relabel_300.py 는 relabel_* 컬럼을 빈 값으로 낸다.

사용법
------
    # 로컬에 eval/eval_sets/relabel_300.csv 가 있으면 그대로
    python scripts/verify_hybrid_eval_sets.py
    # 원문이 다른 경로에 있으면
    python scripts/verify_hybrid_eval_sets.py --relabel-text ~/aihub/relabel_300.csv
    python scripts/verify_hybrid_eval_sets.py --dry-run   # 비용 0, 표본 구성만 확인
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from eval.run_classify_eval import (  # noqa: E402
    compute_leak_map,
    parse_few_shot_examples,
    run_batch_chunks,
    score,
    tag_leaked_rows,
)
import app.classification.service as classification_service  # noqa: E402


def load_raw_texts(text_path: Path) -> dict[str, str]:
    """AI Hub 원문을 `id → raw_text` 로 읽는다 (2026-08-09 분리).

    `relabel_300_labels.csv` 에는 **사람이 매긴 라벨만** 있고 원문이 없다. AI Hub 71603
    이용정책상 원문을 저장소에 두지 않기 때문이다(.gitignore 참고). 채점하려면 원문이
    필요하므로 로컬 파일에서 `id` 로 조인한다.

    로컬 파일은 `id`(RELABEL-####)와 `raw_text` 컬럼만 있으면 된다 — 예전
    `relabel_300.csv` 를 그대로 쓰면 되고, 없으면 아래 안내대로 다시 만들면 된다.
    """
    if not text_path.exists():
        raise SystemExit(
            f"\n🔴 원문 파일이 없습니다: {text_path}\n"
            "   relabel_300_labels.csv 에는 라벨만 있고 원문은 저장소에 두지 않습니다"
            "(AI Hub 이용정책).\n"
            "   채점하려면 로컬에 원문이 필요합니다. 둘 중 하나로 준비하세요.\n\n"
            "   ① 예전에 받아둔 relabel_300.csv 가 있으면 그 경로를 넘기세요\n"
            "      python scripts/verify_hybrid_eval_sets.py --relabel-text <경로>\n\n"
            "   ② 없으면 AI Hub 71603 을 받아 같은 선정 규칙으로 다시 만드세요\n"
            "      python scripts/select_relabel_300.py --data-dir ./aihub71603 \\\n"
            "          --seed 11 --outfile eval/eval_sets/relabel_300.csv\n"
            "      (선정 로직·seed 가 같으므로 id 가 동일하게 붙습니다. 이 파일의\n"
            "       relabel_* 컬럼은 비어 있지만, 라벨은 labels 파일에서 오므로 상관없습니다)\n"
        )
    with text_path.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    if not rows or "raw_text" not in rows[0] or "id" not in rows[0]:
        raise SystemExit(f"🔴 {text_path} 에 id·raw_text 컬럼이 필요합니다.")
    return {r["id"]: r["raw_text"] for r in rows}


def load_relabel_300(path: Path, text_path: Path) -> tuple[list[dict], list[dict]]:
    """relabel_300_labels.csv(라벨) + 로컬 원문을 id 로 조인해 로드.

    relabel_aspect가 콤마 포함(다중aspect)이면 별도 리스트로 분리.

    Returns: (single_aspect_rows, multi_aspect_rows)
    둘 다 {"inquiry_id", "raw_text", "true_aspect", "true_sentiment"} 형태로 통일
    (multi는 true_aspect/true_sentiment가 리스트).
    """
    with path.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    # 라벨 파일에 원문이 없으면(정상) 로컬 원문과 조인한다. 옛 relabel_300.csv 처럼
    # raw_text 가 이미 들어 있으면 그대로 쓴다 — 두 형식을 모두 받는다.
    if rows and "raw_text" in rows[0]:
        texts = {r["id"]: r["raw_text"] for r in rows}
    else:
        texts = load_raw_texts(text_path)
        missing = [r["id"] for r in rows if r["id"] not in texts]
        if missing:
            raise SystemExit(
                f"🔴 원문에 없는 id {len(missing)}건: {missing[:5]}\n"
                f"   {path.name} 과 {text_path.name} 의 선정 결과가 다릅니다"
                " — seed·선정 규칙이 같은지 확인하세요."
            )
        print(f"원문 조인: {text_path} ({len(texts)}건)")

    single, multi = [], []
    for r in rows:
        aspect_raw = r["relabel_aspect"].strip()
        sent_raw = r["relabel_sentiment"].strip()
        if not aspect_raw:
            continue  # 아직 미완료 행(방어적)
        if "," in aspect_raw:
            aspects = aspect_raw.split(",")
            sentiments = [int(float(s)) for s in sent_raw.split(",")]
            if len(aspects) != len(sentiments):
                print(f"⚠️ {r['id']}: aspect·sentiment 개수 불일치({aspect_raw} vs {sent_raw}) — 스킵")
                continue
            multi.append({
                "inquiry_id": r["id"], "raw_text": texts[r["id"]],
                "true_aspect": aspects, "true_sentiment": sentiments,
            })
        else:
            single.append({
                "inquiry_id": r["id"], "raw_text": texts[r["id"]],
                "true_aspect": aspect_raw, "true_sentiment": int(float(sent_raw)),
            })
    return single, multi


def load_generated_700(path: Path) -> tuple[list[dict], list[dict]]:
    """llm_generated_700.csv 로드. 구조는 relabel_300과 컬럼명만 다름(aspect/sentiment)."""
    with path.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    single, multi = [], []
    for r in rows:
        aspect_raw = r["aspect"].strip()
        sent_raw = r["sentiment"].strip()
        if "," in aspect_raw:
            aspects = aspect_raw.split(",")
            sentiments = [int(float(s)) for s in sent_raw.split(",")]
            if len(aspects) != len(sentiments):
                print(f"⚠️ {r['id']}: aspect·sentiment 개수 불일치 — 스킵")
                continue
            multi.append({
                "inquiry_id": r["id"], "raw_text": r["raw_text"],
                "true_aspect": aspects, "true_sentiment": sentiments,
            })
        else:
            single.append({
                "inquiry_id": r["id"], "raw_text": r["raw_text"],
                "true_aspect": aspect_raw, "true_sentiment": int(float(sent_raw)),
            })
    return single, multi


def score_multi_aspect(rows: list[dict], predictions: dict[str, list[dict]]) -> dict:
    """다중aspect 행 채점 — 모든 aspect가 다 맞아야 완전정답, aspect별 부분점수도 집계.

    score()는 true_aspect가 단일 문자열이라고 가정하므로 여기서는 재사용 안 하고
    별도 로직으로 채점한다(explode 계약 검증 목적 그 자체 — §4 결정사항②).
    """
    if not rows:
        return {"n": 0, "note": "다중aspect 표본 없음"}

    full_correct = 0
    aspect_level_tp = aspect_level_total = 0
    unanswered = 0

    for r in rows:
        pred = predictions.get(r["inquiry_id"])
        if pred is None:
            unanswered += 1
            continue
        pred_map = {p["aspect"]: p["sentiment"] for p in pred}
        row_all_correct = True
        for true_a, true_s in zip(r["true_aspect"], r["true_sentiment"]):
            aspect_level_total += 1
            if pred_map.get(true_a) == true_s:
                aspect_level_tp += 1
            else:
                row_all_correct = False
        if row_all_correct:
            full_correct += 1

    n_scored = len(rows) - unanswered
    return {
        "n": len(rows),
        "n_scored": n_scored,
        "n_unanswered": unanswered,
        "full_row_accuracy": round(full_correct / n_scored, 4) if n_scored else 0.0,
        "aspect_level_accuracy": round(aspect_level_tp / aspect_level_total, 4) if aspect_level_total else 0.0,
        "note": "full_row_accuracy=모든 aspect 다 맞은 행 비율(explode 계약 핵심 지표), "
                "aspect_level_accuracy=개별 aspect+감성 조합 단위 정확도",
    }


async def retry_failed_rows(
    all_rows: list[dict],
    predictions: dict[str, list[dict]],
    failed_ids: list[str],
    retry_chunk_size: int,
) -> list[str]:
    """Retry rows omitted from an otherwise valid batch response.

    Some batch responses are valid JSON but omit item_id values. Treating those as
    final failures makes the metric depend on response formatting luck, so retry
    the missing IDs in smaller chunks and then as singletons.
    """
    remaining = list(dict.fromkeys(failed_ids))
    row_by_id = {r["inquiry_id"]: r for r in all_rows}

    for attempt, size in enumerate((retry_chunk_size, 1), start=1):
        retry_rows = [row_by_id[iid] for iid in remaining if iid in row_by_id]
        if not retry_rows:
            break

        print(f"  retry {attempt}: {len(retry_rows)} missing rows, chunk_size={size}, concurrency=1")
        retry_predictions, _ = await run_batch_chunks(retry_rows, size, 1)
        predictions.update(retry_predictions)

        answered = set(retry_predictions)
        remaining = [iid for iid in remaining if iid not in answered]
        if not remaining:
            break

    return remaining


async def evaluate_set(
    name: str,
    single_rows: list[dict],
    multi_rows: list[dict],
    chunk_size: int,
    concurrency: int,
    retry_chunk_size: int,
) -> dict:
    all_rows = single_rows + multi_rows
    print(f"\n=== {name} 채점 중 (단일{len(single_rows)}건 + 다중{len(multi_rows)}건) ===")
    predictions, failed_ids = await run_batch_chunks(all_rows, chunk_size, concurrency)
    if failed_ids:
        print(f"  ⚠️ 무응답 {len(failed_ids)}건")
        failed_ids = await retry_failed_rows(all_rows, predictions, failed_ids, retry_chunk_size)
        if failed_ids:
            print(f"  ⚠️ 재시도 후에도 무응답 {len(failed_ids)}건")
        else:
            print("  ✅ 재시도로 무응답 전부 복구")

    single_result = score(single_rows, predictions) if single_rows else None
    multi_result = score_multi_aspect(multi_rows, predictions)

    return {
        "single": single_result,
        "multi": multi_result,
        "n_total": len(all_rows),
        "n_failed": len(failed_ids),
    }


def print_comparison(relabel_result: dict, generated_result: dict) -> None:
    print("\n" + "=" * 62)
    print("① 정확도 역검증 — relabel_300(사람) vs llm_generated_700(모델)")
    print("=" * 62)

    r_f1 = relabel_result["single"]["aspect_f1"]
    g_f1 = generated_result["single"]["aspect_f1"]
    gap = g_f1 - r_f1
    warn = "🔴 10%p 이상 차이 — 같은 모델끼리 잘 맞는 것 의심 필요" if abs(gap) >= 0.10 else "✅ 정상 범위"

    print(f"relabel_300(단일)   aspect_f1: {r_f1:.4f}  (n={relabel_result['single']['n_scored']})")
    print(f"generated_700(단일) aspect_f1: {g_f1:.4f}  (n={generated_result['single']['n_scored']})")
    print(f"차이: {gap:+.4f}  →  {warn}")

    r_sent = relabel_result["single"]["sentiment_accuracy"]
    g_sent = generated_result["single"]["sentiment_accuracy"]
    sent_gap = g_sent - r_sent
    sent_warn = ""
    if (gap >= 0.10) != (sent_gap >= 0.10) and (gap >= 0.10 or sent_gap >= 0.10):
        sent_warn = "  ⚠️ aspect_f1과 방향이 반대 — 단순 '같은모델이라 유리함'으로는 설명 안 됨, 원인 추가 조사 필요"
    print(f"\nsentiment_accuracy — relabel_300: {r_sent:.4f} / generated_700: {g_sent:.4f}  (차이 {sent_gap:+.4f}){sent_warn}")

    # 🆕 2026-08-06 — F1은 extra aspect(fp)에 영향받지만, recall은 안 받는다(tp/(tp+fn)이라
    # fp가 아예 안 들어감). relabel_300처럼 단일라벨 데이터는 recall로 다시 봐야 공정하다.
    r_recall, r_prec = relabel_result["single"]["aspect_recall"], relabel_result["single"]["aspect_precision"]
    g_recall, g_prec = generated_result["single"]["aspect_recall"], generated_result["single"]["aspect_precision"]
    recall_gap = g_recall - r_recall
    print(f"\naspect_recall(extra aspect 무관, 더 공정) — relabel_300: {r_recall:.4f} / generated_700: {g_recall:.4f}"
          f"  (차이 {recall_gap:+.4f})")
    print(f"aspect_precision(extra aspect 있으면 낮아짐) — relabel_300: {r_prec:.4f} / generated_700: {g_prec:.4f}")
    print("  → precision이 낮은데 recall은 상대적으로 덜 낮다면, relabel_300 오답 상당수가"
          " '정답은 맞혔는데 extra를 더 잡은' 케이스라는 뜻(F1만 보면 과대평가된 격차로 보임)")

    print("\n--- 다중aspect(explode 계약 검증) ---")
    for label, result in [("relabel_300", relabel_result["multi"]), ("generated_700", generated_result["multi"])]:
        if result.get("n", 0) == 0:
            print(f"{label}: {result.get('note', '표본 없음')}")
            continue
        print(f"{label}: full_row_accuracy={result['full_row_accuracy']:.4f} "
              f"aspect_level_accuracy={result['aspect_level_accuracy']:.4f} (n={result['n_scored']})")

    # 🆕 오답 상세 — 원인 진단용(도메인 불일치 가설 확인)
    print("\n--- relabel_300 오답 샘플(최대 15건, 원인 진단용) ---")
    mismatches = relabel_result["single"].get("mismatches", [])
    for m in mismatches[:15]:
        pred_str = ", ".join(f"{p['aspect']}:{p['sentiment']}" for p in m["predicted"]) or "(빈 응답)"
        print(f"  정답={m['true_aspect']}:{m['true_sentiment']} | 예측={pred_str} | {m['raw_text'][:55]}")
    if len(mismatches) > 15:
        print(f"  ... 외 {len(mismatches) - 15}건 더(전체는 결과 JSON 참고)")


def print_leak_check(generated_single: list[dict], generated_multi: list[dict], threshold: float) -> None:
    print("\n" + "=" * 62)
    print("③ few-shot 유출 재확인 — llm_generated_700")
    print("=" * 62)

    few_shot = parse_few_shot_examples("classify_aspect_v5")
    print(f"few-shot 예시 {len(few_shot)}개 대비 유사도 검사(임계 {threshold})")

    all_rows = generated_single + generated_multi
    leak_map = compute_leak_map(all_rows, few_shot, threshold)
    tag_leaked_rows(all_rows, leak_map)

    leaked = [r for r in all_rows if r["is_leaked"]]
    print(f"\n유출 판정: {len(leaked)}/{len(all_rows)}건 ({len(leaked)/len(all_rows)*100:.1f}%)")
    if leaked:
        print("\n유출된 문장 목록:")
        for r in sorted(leaked, key=lambda x: -x["leak_similarity"])[:20]:
            print(f"  유사도{r['leak_similarity']:.2f} | {r['inquiry_id']} | {r['raw_text'][:50]}")
    else:
        print("✅ 유출 없음 — 생성 문장이 few-shot과 안 겹침 확인됨")


async def main_async(args: argparse.Namespace) -> None:
    relabel_single, relabel_multi = load_relabel_300(Path(args.relabel), Path(args.relabel_text))
    generated_single, generated_multi = load_generated_700(Path(args.generated))

    print(f"relabel_300: 단일{len(relabel_single)}건 + 다중{len(relabel_multi)}건")
    print(f"generated_700: 단일{len(generated_single)}건 + 다중{len(generated_multi)}건")

    if args.dry_run:
        print("\n[dry-run] LLM 호출 안 함.")
        return

    # ③ 유출 확인 먼저(LLM 호출 없음, 무료 — 순서 상관없지만 저비용부터)
    print_leak_check(generated_single, generated_multi, args.leak_threshold)

    # ① 정확도 역검증(LLM 호출 발생)
    relabel_result = await evaluate_set(
        "relabel_300",
        relabel_single,
        relabel_multi,
        args.chunk_size,
        args.concurrency,
        args.retry_chunk_size,
    )
    generated_result = await evaluate_set(
        "llm_generated_700",
        generated_single,
        generated_multi,
        args.chunk_size,
        args.concurrency,
        args.retry_chunk_size,
    )
    print_comparison(relabel_result, generated_result)

    # 🆕 결과 저장 — mismatches 포함, 나중에 원인 진단용으로 다시 열어볼 수 있게
    import json
    from datetime import datetime

    # 🔴 relabel_300 은 AI Hub 71603 리뷰다. 원문(raw_text)을 결과 JSON에 남기면 그 파일이
    # 곧 원문 사본이 되고, 커밋 시 AI Hub 이용정책 위반 소지가 생긴다(.gitignore 의
    # review_eval_*.json 주석과 같은 사유 — 2026-08-09 발견해 여기서도 막는다).
    # inquiry_id 는 남기므로 원문이 필요하면 relabel_300.csv 를 조회하면 된다.
    # llm_generated_700 은 LLM 생성물이라 해당 없음 — 그대로 둔다.
    for m in relabel_result["single"].get("mismatches", []):
        m.pop("raw_text", None)
    relabel_result["single"]["mismatches_note"] = (
        "AI Hub 71603 리뷰 원문(raw_text)은 이용정책상 저장하지 않는다. "
        "원문이 필요하면 inquiry_id 로 eval/eval_sets/relabel_300.csv 를 조회할 것."
    )

    out_dir = ROOT / "eval" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"hybrid_verify_{stamp}.json"
    out_path.write_text(
        json.dumps({"relabel_300": relabel_result, "llm_generated_700": generated_result}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n결과 저장: eval/results/{out_path.name}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--relabel", default="eval/eval_sets/relabel_300_labels.csv",
                    help="사람 재라벨링 결과(원문 없음). 저장소에 커밋되는 파일")
    ap.add_argument("--relabel-text", default="eval/eval_sets/relabel_300.csv",
                    help="AI Hub 원문(id·raw_text). 저장소에 없으니 로컬에 두고 경로를 넘길 것")
    ap.add_argument("--generated", default="eval/eval_sets/llm_generated_700.csv")
    ap.add_argument("--chunk-size", type=int, default=20)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--retry-chunk-size", type=int, default=5)
    ap.add_argument("--leak-threshold", type=float, default=0.75)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
