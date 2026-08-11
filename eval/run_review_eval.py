"""실험④ 프롬프트2 평가 — 리뷰 aspect별 긍부정 정확도 (AI Hub 71630 원본 라벨 기준).

무엇을 재나: aspect(색상/사이즈/소재) F1 + 감성 정확도 + mixed_signal 정확도 + 완전일치
어떻게:     AI Hub 71630 원본 zip(Source=쇼핑몰, 여성/남성의류, Training)에서 표본 추출
            → app.classification.service.classify_aspect() 실제 호출(source=REVIEW라
            자동으로 프롬프트2 분기) → 71630 원본 라벨과 대조

⚠️ 계획B 채택(현진↔팀 논의 결과) — 71630 원본 sentiment 라벨을 **재라벨링 없이 그대로**
   신뢰도 있는 외부 정답지로 사용. 계획A(사람이 300건 재검증)는 보류.
   한계: 71630 원본이 우리 프롬프트2의 세부 판정 규칙(착용감 배제, 부정우선 통합 등)과
   완전히 같은 기준으로 라벨링됐다는 보장은 없음 — 오답으로 나온 건 중 일부는 모델이
   아니라 원본 라벨과의 기준 차이일 수 있음(결과 해석 시 감안할 것).

🔴 **알려진 구체적 충돌** (PR 리뷰에서 지적됨, 일반론이 아니라 실측치): 이 스크립트의
   `build_dataset()`은 71630 낱개 라벨을 **무조건 "부정 우선"으로 통합**해서 골든을
   만드는데, 같은 PR에서 classify_sentiment_v4.md에 추가한 예시11은 정반대를 가르친다 —
   "화자가 '~것만 빼면'으로 스스로 사소한 흠이라 규정하면 부정 우선 예외를 적용해
   `sentiment: 1` + `mixed_signal: true`로 판단". 즉 v4가 의도대로 동작할수록 이 골든
   기준으로는 오답 처리된다. seed=42/n=300/batch 실행(review_eval_20260731_143326.scrubbed.json
   — 원본은 raw_text 에 71630 원문이 들어가 public 저장소에 못 올린다, .gitignore:46-57)
   기준 실측: 전체 오답 166건 중 "것만 빼면/제외하고/빼고는" 류 패턴 **3건(1.8%)** —
   방향은 확인됐으나 감성정확도(79.7%) 하락분의 대부분을 설명하진 못함(나머지는 71630
   라벨링 관습과 소재 스코프 정의 차이 등 다른 원인). golden 쪽에도 같은 예외처리 규칙을
   반영할지는 별도 논의 필요(미해결).

   🔻 2026-08-11 전수 점검 — **이 충돌은 실측상 0.13%(2,314건 중 3건)이고, 그중 실제
   프레이밍은 1건이다.** 71630 낱개 라벨에는 "화자가 사소하다고 프레이밍했는가"가 없어
   자동 반영도 불가능하다. 같은 점검에서 **7배 큰 결함**이 나왔다 — 아래 TARGET_MAP 참고.
   분해 결과·수정 방향 목록은 `eval/README.md` §④ 참고.

🔴 **TARGET_MAP 이 프롬프트2보다 좁다 (2026-08-11, 미수정)**: 71630은 aspect를 18종으로
   쪼개는데 여기선 4종만 매핑한다. 그런데 프롬프트2는 `신축성`을 소재에 포함하라 명시
   (v4 28행)하고 예시12는 `마감`을, 예시8은 `길이`(기장)를 각각 소재·사이즈로 분류한다 —
   골든이 이들을 별도 aspect로 떼어두는 바람에 정답이 오답으로 잡힌다. 소재 FP 40건 중
   21건(52%)·사이즈 FP 11건 중 5건이 이 원인이고, 실제 모델 오류는 소재 FP의 22.5%뿐이다.

⚠️ "핏"은 TARGET_MAP으로 "사이즈"에 통합(REVIEW_ALLOWED_ASPECTS와 스코프 일치).
⚠️ 같은 aspect가 리뷰 한 건 안에서 여러 번(감성 다르게) 나오면, 우리 프롬프트와 동일한
   "부정 우선 통합" 규칙으로 골든을 만든다(71630 원본은 이 통합 개념이 없이 낱개로만 존재).

실행:
    python eval/run_review_eval.py --data-dir <71630 압축 푼 폴더> --dry-run --limit 300
    python eval/run_review_eval.py --data-dir <71630 압축 푼 폴더> --mode per_item --limit 300 --seed 99
    python eval/run_review_eval.py --data-dir <71630 압축 푼 폴더> --mode batch    --limit 300 --seed 99
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import json
import random
import sys
import unicodedata
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

RESULTS_DIR = ROOT / "eval" / "results"
TARGET_MAP = {"색상": "색상", "소재": "소재", "사이즈": "사이즈", "핏": "사이즈"}
VALID_ASPECTS = {"색상", "사이즈", "소재"}

# 프롬프트1·2 few-shot에 이미 쓴 원문 — 시험지 유출 방지(select_relabel_300.py와 동일 목록)
USED_PREFIXES = [
    "베이지색상은 화면과 달리", "가성비 좋은 렉스조끼", "네이비로 구매했는데 셔츠와 바지가",
    "편하고 간편하게 입을 수 있어서", "큰사이즈로 구매하라는", "조금 커서 한사이즈 작게",
    "고무밴드가 딱딱해서", "기모는 아닌데 모직바지느낌", "다른 상품 보다 사이즈가 타이트해서",
    "아들에게 선물했는데 불편하고", "생각보다 원단이 좋네요", "구김이 심하고 상의는 66사이즈",
    "재질은 까칠하고 바지는 핏이", "핏은 좀 맘에 들지않지만", "특히 자켓이 넘작고",
    "완전 맘에들어요 한치수", "허벅지도 끼고 입으니", "소재,디자인도 좋아요 사이즈가",
    "텐션감이 좋아 착용감은", "뱃살이 좀 있는편이라",
]


def _already_used(text: str) -> bool:
    return any(text.startswith(p) or p in text for p in USED_PREFIXES)


def _dataset_split(path: str | Path) -> str:
    """71630 파일 경로에서 데이터 분할을 OS와 무관하게 판별한다."""
    normalized = unicodedata.normalize("NFC", str(path)).replace("\\", "/")
    parts = normalized.split("/")
    if "Training" in parts:
        return "Training"
    if "Validation" in parts:
        return "Validation"
    return "Unknown"


def load_71630(data_dir: str) -> list[dict]:
    """71630 압축 해제 폴더 로딩.

    macOS의 NFD 파일명과 Windows의 역슬래시 경로를 모두 처리한다.
    """
    files = [f for f in glob.glob(f"{data_dir}/**/*.json", recursive=True) if "__MACOSX" not in f]
    all_data = []
    for fp in files:
        split = _dataset_split(fp)
        with open(fp, encoding="utf-8") as f:
            recs = json.load(f)
        for i, r in enumerate(recs):
            r["_split"] = split
            r["_uid"] = f"{Path(fp).stem}-{i}"  # 71630엔 안정적 고유ID가 없어 파일명+인덱스로 생성
        all_data.extend(recs)
    return all_data


def build_dataset(data_dir: str, max_len: int = 300) -> list[dict]:
    """71630 원본 → (원문 + 통합된 골든 aspect/sentiment/mixed_signal) 리스트.

    필터: Source=쇼핑몰, MainCategory=여성/남성의류, Split=Training, 대상aspect 1개 이상,
          few-shot 유출 원문 제외, 길이 제한.
    """
    all_data = load_71630(data_dir)
    rows = []
    for r in all_data:
        if not (
            r.get("Source") == "쇼핑몰"
            and r.get("MainCategory") in ("여성의류", "남성의류")
            and r["_split"] == "Training"
        ):
            continue
        text = r.get("RawText", "")
        if _already_used(text) or len(text) > max_len:
            continue

        by_aspect: dict[str, list[str]] = defaultdict(list)
        for a in r.get("Aspects", []):
            mapped = TARGET_MAP.get(a["Aspect"])
            if mapped:
                by_aspect[mapped].append(a["SentimentPolarity"])
        if not by_aspect:
            continue

        # 우리 프롬프트와 동일한 "부정 우선 통합" 규칙으로 골든 생성
        gold = {}
        for asp, pols in by_aspect.items():
            pols_int = [int(p) for p in pols]
            if -1 in pols_int:
                sentiment = -1
            elif 1 in pols_int:
                sentiment = 1
            else:
                sentiment = 0
            gold[asp] = {"sentiment": sentiment, "mixed_signal": len(set(pols_int)) > 1}

        rows.append({"review_id": r["_uid"], "raw_text": text, "gold": gold})
    return rows


def sample_rows(rows: list[dict], limit: int, seed: int) -> list[dict]:
    """대표(첫 번째) aspect 기준 층화표본. limit<=0이면 전량."""
    if limit <= 0 or limit >= len(rows):
        return rows
    rng = random.Random(seed)
    by_primary: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        primary = sorted(r["gold"].keys())[0]
        by_primary[primary].append(r)

    picked: list[dict] = []
    for asp, group in sorted(by_primary.items()):
        quota = round(limit * len(group) / len(rows))
        picked.extend(rng.sample(group, min(quota, len(group))))

    chosen_ids = {r["review_id"] for r in picked}
    leftover = [r for r in rows if r["review_id"] not in chosen_ids]
    rng.shuffle(leftover)
    picked.extend(leftover[: max(0, limit - len(picked))])
    # aspect 순서대로 쌓여 있어 그냥 자르면 뒤쪽 aspect가 덜 뽑힌다 — 자르기 전에 섞는다.
    rng.shuffle(picked)
    return picked[:limit]


async def run_chunks(rows: list[dict], chunk_size: int, concurrency: int) -> tuple[dict[str, list[dict]], list[str]]:
    """item당 개별 호출(동시 실행) — 기존 방식."""
    chunks = [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]
    semaphore = asyncio.Semaphore(concurrency)
    predictions: dict[str, list[dict]] = {}
    failed_ids: list[str] = []

    async def one(index: int, chunk: list[dict]) -> None:
        items = [
            ClassifyRequestItem(
                item_id=r["review_id"], source=Source.REVIEW, channel=Channel.ALL,
                product_group_id="EVAL", raw_text=r["raw_text"], created_at=datetime.now(),
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
                failed_ids.extend(r["review_id"] for r in chunk)
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
                {"aspect": a.aspect.value, "sentiment": a.sentiment.value, "mixed_signal": getattr(a, "mixed_signal", False)}
                for a in result.aspects
            ]
        if n_item_failed:
            print(f"   [{index + 1}/{len(chunks)}] ⚠️ {n_item_failed}건 개별 실패(그 건만 무응답)")
        print(f"   [{index + 1}/{len(chunks)}] {len(chunk)}건 → {len(chunk) - n_item_failed}건 응답")

    await asyncio.gather(*(one(i, c) for i, c in enumerate(chunks)))
    return predictions, failed_ids


def _build_batch_prompt(chunk: list[dict]) -> str:
    full = classification_service.load_llm_prompt("classification", classification_service.PROMPT_SENTIMENT_VERSION)
    marker = "## 분류 대상 리뷰"
    head = full.split(marker)[0].strip() if marker in full else full.strip()

    items_json = json.dumps(
        [{"item_id": r["review_id"], "text": r["raw_text"]} for r in chunk], ensure_ascii=False
    )
    tail = f"""

{marker} (총 {len(chunk)}건 — 각각 독립적인 별개 리뷰입니다)

아래는 서로 다른 리뷰 목록입니다. **한 리뷰의 내용이 다른 리뷰의 판단에 영향을 주면
안 됩니다** — 각 건을 완전히 개별적으로 판단하세요.

{items_json}

**출력 형식** (JSON, 다른 텍스트 없이 이 형식만 출력. item_id는 입력과 정확히 동일하게,
누락·순서변경 없이 전부 포함할 것)
```json
{{"results": [{{"item_id": "...", "aspects": [{{"aspect": "...", "sentiment": ..., "mixed_signal": true|false}}]}}, ...]}}
```"""
    return head + tail


async def run_batch_chunks(rows: list[dict], chunk_size: int, concurrency: int) -> tuple[dict[str, list[dict]], list[str]]:
    """청크당 LLM 호출 1회(진짜 배치)."""
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
            except Exception as exc:  # noqa: BLE001
                print(f"   [{index + 1}/{len(chunks)}] ⚠️ 배치 호출 실패({len(chunk)}건 무응답): {exc}")
                failed_ids.extend(r["review_id"] for r in chunk)
                return

        results = data.get("results", [])
        expected_ids = {r["review_id"] for r in chunk}
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
            valid = [
                a for a in aspects
                if isinstance(a, dict) and a.get("aspect") in VALID_ASPECTS and a.get("sentiment") in (-1, 0, 1)
            ]
            predictions[iid] = valid
            got_ids.add(iid)

        if hallucinated:
            print(f"   [{index + 1}/{len(chunks)}] ⚠️ 요청에 없는 item_id {len(hallucinated)}건 응답(환각) → 무시")
        missing = expected_ids - got_ids
        if missing:
            print(f"   [{index + 1}/{len(chunks)}] ⚠️ 누락된 item_id {len(missing)}건 → 무응답 처리")
            failed_ids.extend(missing)
        print(f"   [{index + 1}/{len(chunks)}] {len(chunk)}건 → {len(got_ids)}건 응답(배치 1회 호출)")

    await asyncio.gather(*(one(i, c) for i, c in enumerate(chunks)))
    return predictions, failed_ids


def score(rows: list[dict], predictions: dict[str, list[dict]]) -> dict:
    """다중 aspect(색상/사이즈/소재) F1 + 감성정확도 + mixed_signal정확도 + 완전일치."""
    scored = [r for r in rows if r["review_id"] in predictions]
    unanswered = len(rows) - len(scored)

    tp = fp = fn = 0
    sent_correct = sent_total = 0
    mix_correct = mix_total = 0
    exact_match = 0
    per_aspect: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    mismatches: list[dict] = []

    for r in scored:
        pred_list = predictions[r["review_id"]]
        pred_by_aspect = {p["aspect"]: p for p in pred_list}
        gold = r["gold"]

        item_all_correct = True
        for asp, gv in gold.items():
            if asp in pred_by_aspect:
                tp += 1
                per_aspect[asp][0] += 1
                sent_total += 1
                sok = pred_by_aspect[asp]["sentiment"] == gv["sentiment"]
                sent_correct += sok
                mix_total += 1
                mixok = bool(pred_by_aspect[asp].get("mixed_signal", False)) == gv["mixed_signal"]
                mix_correct += mixok
                if not (sok and mixok):
                    item_all_correct = False
            else:
                fn += 1
                per_aspect[asp][2] += 1
                item_all_correct = False

        extra = set(pred_by_aspect) - set(gold)
        fp += len(extra)
        for a in extra:
            per_aspect[a][1] += 1
        if extra:
            item_all_correct = False

        exact_match += item_all_correct
        if not item_all_correct:
            mismatches.append({
                "review_id": r["review_id"], "raw_text": r["raw_text"],
                "gold": gold, "predicted": pred_list,
            })

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return {
        "n_sampled": len(rows), "n_scored": len(scored), "n_unanswered": unanswered,
        "aspect_f1": round(f1, 4), "aspect_precision": round(precision, 4), "aspect_recall": round(recall, 4),
        "sentiment_accuracy": round(sent_correct / sent_total, 4) if sent_total else 0.0,
        "mixed_signal_accuracy": round(mix_correct / mix_total, 4) if mix_total else 0.0,
        "exact_match_rate": round(exact_match / len(scored), 4) if scored else 0.0,
        "per_aspect": {
            a: {
                "precision": round(v[0] / (v[0] + v[1]), 4) if (v[0] + v[1]) else 0.0,
                "recall": round(v[0] / (v[0] + v[2]), 4) if (v[0] + v[2]) else 0.0,
                "n_true": v[0] + v[2],
            }
            for a, v in sorted(per_aspect.items())
        },
        "mismatches": mismatches,
    }


def report(result: dict) -> None:
    meta = result["meta"]
    s = result["scores"]
    print("\n" + "=" * 62)
    print(f"실험④ 프롬프트2 리뷰 감성 — {meta['prompt_version']} / {meta['model']} / seed={meta['seed']} / mode={meta['mode']}")
    print("=" * 62)
    print(f"채점 {s['n_scored']}건 (표본 {s['n_sampled']}, 무응답 {s['n_unanswered']})")
    print(f"\n■ Aspect F1  {s['aspect_f1']:.1%}  (Precision {s['aspect_precision']:.1%} / Recall {s['aspect_recall']:.1%})")
    print(f"■ 감성 정확도  {s['sentiment_accuracy']:.1%}")
    print(f"■ mixed_signal 정확도  {s['mixed_signal_accuracy']:.1%}")
    print(f"■ 완전일치  {s['exact_match_rate']:.1%}")
    print("\n■ aspect별 정밀도/재현율")
    for aspect, v in s["per_aspect"].items():
        print(f"    {aspect:6s} P={v['precision']:.1%}  R={v['recall']:.1%}  (n={v['n_true']})")


async def main_async(args: argparse.Namespace) -> None:
    if args.prompt_version:
        classification_service.PROMPT_SENTIMENT_VERSION = args.prompt_version

    print("71630 로딩 중...")
    rows = build_dataset(args.data_dir)
    sampled = sample_rows(rows, args.limit, args.seed)

    print(f"필터+클린 후 전체 {len(rows)}건 → 표본 {len(sampled)}건")
    primary_dist = Counter(sorted(r["gold"].keys())[0] for r in sampled)
    print(f"  대표aspect 구성: {dict(primary_dist)}")
    print(f"  프롬프트 버전: {classification_service.PROMPT_SENTIMENT_VERSION}")

    if args.dry_run:
        n_chunks = -(-len(sampled) // args.chunk_size)
        print(f"\n[dry-run] LLM 호출 안 함. 청크 약 {n_chunks}회(청크당 {args.chunk_size}건). 모드: {args.mode}")
        return

    if args.mode == "batch":
        predictions, failed_ids = await run_batch_chunks(sampled, args.chunk_size, args.concurrency)
    else:
        predictions, failed_ids = await run_chunks(sampled, args.chunk_size, args.concurrency)
    if failed_ids:
        print(f"\n⚠️ 무응답 처리된 건: {len(failed_ids)}건")

    from app.config import get_settings

    result = {
        "meta": {
            "experiment": "④ 프롬프트2 리뷰 aspect별 감성 정확도",
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "data_source": "AI Hub 71630 (원본 라벨, 재라벨링 없음 — 계획B)",
            "prompt_version": classification_service.PROMPT_SENTIMENT_VERSION,
            "model": get_settings().llm_model,
            "seed": args.seed, "limit": args.limit, "chunk_size": args.chunk_size, "mode": args.mode,
        },
        "scores": score(sampled, predictions),
    }
    report(result)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = RESULTS_DIR / f"review_eval_{stamp}.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n결과 저장: eval/results/{out.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", required=True, help="71630 zip을 풀어놓은 폴더")
    parser.add_argument("--limit", type=int, default=300, help="표본 수 (0=전량)")
    parser.add_argument("--seed", type=int, default=42, help="표본 추출 시드")
    parser.add_argument("--chunk-size", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--prompt-version", default=None)
    parser.add_argument("--mode", choices=["per_item", "batch"], default="per_item")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
