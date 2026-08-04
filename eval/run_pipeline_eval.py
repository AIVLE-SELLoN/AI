"""실험② 분류 오류 전파 — 실제 LLM 분류가 탐지 성능을 얼마나 깎는가.

무엇을 재나: 실험① 대비 하락폭 (①−②)
어떻게:     ①과 **딱 한 가지만** 다르게 돌린다 — 케이스 슬롯의 현재 윈도우
            카운트를 config 값(=golden 라벨) 대신 **Agent1 이 실제로 분류한
            결과**로 채운다. 배치 구성·과거 기준·채점 로직은 ①과 완전히 동일하다.
            그래야 차이가 오직 분류 오차에서만 온다.

왜 케이스만 실제 분류하나
-------------------------
배경 슬롯은 ① 에서도 BASELINE_RATE 공식으로 만든 합성값이라 대응하는 원문이 없다.
거기까지 바꾸면 비교 조건이 둘이 되어 (①−②)가 '분류 오차'를 뜻하지 않게 된다.
비용도 96,514건으로 자릿수가 뛴다.

과거 윈도우는 config 값(oracle)을 쓴다. ②가 묻는 것은 "현재 윈도우의 분류 오차가
판정을 흔드는가"이고, 과거는 28일 누적이라 오차가 상쇄되는 안정된 기준선이다.

분모는 원본에서 센다
--------------------
`loader.build_rows()` 경유. 분류 결과에서 분모를 세면 aspect 가 0개로 나온 문서가
통째로 빠져 부정률이 부풀려진다(탐지 분모 산출 방식 §1). CS 는 '기타' 가 있어
실무상 안전하지만, 운영과 같은 경로를 쓰는 편이 회귀에 강하다.

비용 — `--mode` 로 호출 방식을 고른다
------------------------------------
실측 토큰(tiktoken o200k_base, gpt-4o-mini 기준):

    classify_aspect_v5 시스템 프롬프트   6,189 토큰
    CS 문의 본문                          평균 14 토큰 (최대 34)
    → 건당 호출은 1회의 99.8% 가 프롬프트 재전송

    per_item   현재 윈도우 11,990건 × 3회 ≈ $34.3   운영(워커)과 같은 호출 방식
    batch      같은 규모, 청크 20건        ≈ $2.6   기본값

**기본값이 batch 인 이유**: 같은 숫자를 13분의 1 비용으로 얻는다. 분류 품질이
동등하다는 근거는 실험③ 비교(aspect_f1 0.9899→0.9933, exact_match 0.9767→0.9766)다.
운영이 건당인 동안에는 이게 **측정이 아니라 가정**이므로 리포트에 명시한다.
운영과 완전히 같은 조건으로 재현하려면 `--mode per_item`.

배치 경로는 실험③의 `run_classify_eval.run_batch_chunks()` 를 그대로 쓴다. item_id
매칭·응답 누락 감지·환각 id 무시가 이미 들어 있어서 여기서 다시 만들지 않는다.

**분류 결과는 회차별로 캐싱한다.** 한 번 태운 문의는 다시 부르지 않으므로 재실행·
채점 로직 수정에는 과금이 없다. 회차를 나누는 이유는 LLM 이 temperature=0 에서도
실행마다 흔들리기 때문이다(실험⑥에서 같은 입력에 89.0% / 84.0%).

실행:
    python eval/run_pipeline_eval.py --dry-run          # 비용 0, 대상·호출수만 확인
    python eval/run_pipeline_eval.py --limit 300 --runs 1   # 파일럿
    python eval/run_pipeline_eval.py --runs 3           # 본실행 (3회 평균, batch)
    python eval/run_pipeline_eval.py --runs 3 --mode per_item   # 운영과 같은 조건
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import statistics
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

# ⚠️ 이게 없으면 Windows(cp949) 에서 **LLM 을 다 태운 뒤 리포트 출력에서** 죽는다.
#    분류 캐시는 청크마다 저장되니 돈이 날아가진 않지만, 숫자를 못 본다.
sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "eval"))

from run_classify_eval import run_batch_chunks  # 실험③이 검증한 '진짜 배치' 경로
from run_detection_eval import (  # ①과 배치·채점을 공유해야 비교가 성립한다
    CONFIG_ANOMALY,
    CONFIG_PRODUCTS,
    GOLDEN_ANOMALY,
    build_combinations,
    cause_consistency,
    change_log,
    read,
    score,
)

from app.classification.service import (
    PROMPT_ASPECT_VERSION,
    ClassifyRequestItem,
    _cs_empty_fallback,
    classify_aspect,
    load_llm_prompt,
)
from app.core.constants import CURRENT_WINDOW_DAYS
from app.core.schemas import AspectSentiment, ClassifiedItem
from app.detection.aggregate import count_window
from app.detection.loader import build_rows, check_coverage, unreliable_slots
from app.detection.service import _build_candidates
from app.detection.statistics import run_detection
from app.detection.verdict import run_verdict

INPUT_INQUIRIES = ROOT / "data" / "input" / "input_cs_inquiries.csv"
INPUT_CHANNEL_PRODUCTS = ROOT / "data" / "input" / "input_channel_products.csv"
GOLDEN_MAPPING = ROOT / "data" / "golden" / "golden_mapping.csv"
GOLDEN_CS_LABELS = ROOT / "data" / "golden" / "golden_cs_labels.csv"
CACHE_DIR = ROOT / "data" / "eval_cache"

DAY1 = date(2026, 6, 30)  # Day 1 = 문의 데이터 첫날
SOURCE_CS = "cs"
SOURCES = ("cs", "review")

# per_item: 넘긴 항목 수만큼 동시 호출이 뜬다(classify_aspect 내부 asyncio.gather).
# batch:    청크 하나가 LLM 호출 1회. 청크 크기가 곧 실패 반경이다.
CHUNK_SIZE = 20
CONCURRENCY = 4

MODE_BATCH = "batch"
MODE_PER_ITEM = "per_item"


def prompt_fingerprint() -> str:
    """캐시 키에 넣을 프롬프트 지문 — 이름(버전) + **내용 해시**.

    버전 문자열만으로는 부족하다. Agent1 이 예시를 classify_aspect_v5.md 에 **그대로
    추가**하면 파일명이 안 바뀌어서, 캐시가 안 갈리고 옛 결과를 조용히 재사용한다.
    "고쳤는데 숫자가 안 변한다"가 되고, 캐시 탓인지 수정이 무효한 탓인지 못 가린다.
    내용 해시는 제자리 수정까지 잡는다. 이름을 같이 남기는 건 사람이 읽기 위해서다.
    """
    body = load_llm_prompt("classification", PROMPT_ASPECT_VERSION)
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()[:8]
    return f"{PROMPT_ASPECT_VERSION}-{digest}"


# ── 대상 문의 수집 ───────────────────────────────────────────────


def _product_of() -> dict[tuple[str, str], str]:
    """(channel, channel_product_id) → 상품 그룹 ID. 매핑 2단 조인."""
    group_of = {r["variant_row_id"]: r["golden_group_id"] for r in read(GOLDEN_MAPPING)}
    out: dict[tuple[str, str], str] = {}
    for r in read(INPUT_CHANNEL_PRODUCTS):
        group = group_of.get(r["variant_row_id"])
        if group:
            out.setdefault((r["channel"], r["channel_product_id"]), group)
    return out


def collect_documents(config_rows: list[dict]) -> tuple[list[dict], dict[str, tuple]]:
    """케이스 상품의 **현재 윈도우** CS 문의를 모은다 (분모의 출처).

    Returns:
        (documents, windows)  — windows 는 product → (cur_start, cur_end) 실제 날짜
    """
    windows: dict[str, tuple] = {}
    for r in config_rows:
        if r["source"] != SOURCE_CS:
            continue
        end = DAY1 + timedelta(days=int(r["window_end_day"]) - 1)
        windows[r["golden_group_id"]] = (
            end - timedelta(days=CURRENT_WINDOW_DAYS - 1),
            end,
        )

    product_of = _product_of()
    documents: list[dict] = []
    for r in read(INPUT_INQUIRIES):
        product = product_of.get((r["channel"], r["channel_product_id"]))
        if product not in windows:
            continue
        created = datetime.fromisoformat(r["inquired_at"])
        cur_start, cur_end = windows[product]
        if not (cur_start <= created.date() <= cur_end):
            continue
        documents.append(
            {
                "id": r["inquiry_id"],
                "product": product,
                "channel": r["channel"],
                "source": SOURCE_CS,
                "created_at": created,
                "text": r["content"],
            }
        )
    return documents, windows


def take_whole_products(documents: list[dict], limit: int) -> list[dict]:
    """대략 limit 건까지 자르되 **상품 경계에서 끊는다.**

    문서를 그냥 documents[:limit] 로 자르면 한 상품이 중간에 잘려 그 슬롯의
    **분모가 실제보다 작아진다.** measure() 가 총문의를 실제 행에서 세기 때문이다.
    분모가 깎이면 부정률이 부풀려져 판정이 왜곡되고, 그 상품은 ①과 비교 자체가
    성립하지 않는다. 파일럿이라도 슬롯은 통째로 넣어야 한다.
    """
    by_product: dict[str, list[dict]] = {}
    for doc in documents:
        by_product.setdefault(doc["product"], []).append(doc)

    taken: list[dict] = []
    for docs in by_product.values():
        if taken and len(taken) + len(docs) > limit:
            break
        taken.extend(docs)
    return taken


# ── 분류 ─────────────────────────────────────────────────────────


def _to_items(
    documents: list[dict], aspects_of: dict[str, list]
) -> list[ClassifiedItem]:
    return [
        ClassifiedItem(
            item_id=d["id"],
            source=d["source"],
            channel=d["channel"],
            product_group_id=d["product"],
            raw_text=d["text"],
            aspects=[
                AspectSentiment(aspect=a["aspect"], sentiment=a["sentiment"])
                for a in aspects_of.get(d["id"], [])
            ],
            created_at=d["created_at"],
        )
        for d in documents
    ]


def _cs_fallback_aspects(item_id: str) -> list[dict]:
    """CS 빈 배열 → 기타/중립. **운영과 같은 함수**(service._cs_empty_fallback)를 쓴다.

    배치 경로는 client.complete_json 을 직접 불러 _parse_llm_response 를 우회하므로,
    거기 들어있는 이 폴백이 안 걸린다. 그대로 두면 빈 배열이 살아남아 커버리지 검사가
    그 슬롯을 통째로 검정에서 빼버린다(실측: P019 의 CS 슬롯 2개가 다 빠졌다).
    폴백 규칙을 여기 다시 적지 않고 불러 쓰는 이유는, 두 벌이 되면 갈라지기 때문이다.
    """
    return [
        {"aspect": a.aspect.value, "sentiment": int(a.sentiment)}
        for a in _cs_empty_fallback(f"eval item_id={item_id}")
    ]


class _FallbackCounter(logging.Handler):
    """CS 빈 배열 폴백이 몇 번 걸렸는지 센다.

    **결과 모양으로는 못 센다.** LLM 이 진짜로 '기타/중립'을 낸 것과 폴백이 채운 것이
    똑같이 생겼기 때문이다(파일럿에서 진짜 기타/0 이 27건 있었다). 폴백은 호출당
    warning 을 정확히 1건 남기므로 그걸 세는 게 유일하게 정확한 방법이고, 건당·배치
    **양쪽 경로에 똑같이** 통한다.

    이 숫자가 프롬프트 개선(빈 배열을 애초에 안 내게 하기)의 효과 측정치다.
    """

    MARKER = "cs_empty_aspects"

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.count = 0

    def emit(self, record: logging.LogRecord) -> None:
        if record.getMessage().startswith(self.MARKER):
            self.count += 1


async def _run_batch(todo: list[dict], cache: dict, save) -> tuple[int, int]:
    """청크 1개 = LLM 호출 1회. 실험③이 검증한 run_batch_chunks 를 그대로 쓴다.

    그 함수가 이미 막아둔 것들이라 여기서 다시 하지 않는다 — item_id 매칭(위치가 아님),
    응답 누락 감지, 요청에 없는 id(환각) 무시, aspect·sentiment 값 검증, 청크 단위
    실패 격리.

    다만 결과를 전부 모아 마지막에 한 번에 돌려주므로 중간에 죽으면 다 날아간다.
    그래서 CONCURRENCY 청크씩만 넘기고 **그 묶음마다 캐시를 저장**한다.
    """
    done = failed = 0
    group = CHUNK_SIZE * CONCURRENCY
    for start in range(0, len(todo), group):
        part = todo[start : start + group]
        rows = [{"inquiry_id": d["id"], "raw_text": d["text"]} for d in part]
        predictions, failed_ids = await run_batch_chunks(rows, CHUNK_SIZE, CONCURRENCY)

        for item_id, aspects in predictions.items():
            cache[item_id] = aspects or _cs_fallback_aspects(item_id)
        save()

        done += len(predictions)
        # 무응답 건은 **캐시에 넣지 않는다** — 다음 회차 실행이 그것만 다시 부르고,
        # 그때까지는 커버리지 검사가 잡아서 그 슬롯을 검정에서 뺀다(조용한 왜곡 방지).
        failed += len(failed_ids)
        print(f"    누적 {done:,}/{len(todo):,}건 (무응답 {failed:,})")
    return done, failed


async def _run_per_item(todo: list[dict], cache: dict, save) -> tuple[int, int]:
    """문의 1건 = LLM 호출 1회. 운영(워커)과 같은 호출 방식.

    ⚠️ 전량을 classify_aspect() 에 한 번에 넘기지 말 것. 내부가 asyncio.gather() 라
       넘긴 만큼 동시 호출이 뜬다(11,990건이면 11,990개). 속도 제한에 걸리고, gather 가
       통째로 raise 하면 그때까지의 결과가 다 날아간다.
    """
    semaphore = asyncio.Semaphore(CONCURRENCY)
    chunks = [todo[i : i + CHUNK_SIZE] for i in range(0, len(todo), CHUNK_SIZE)]
    done = failed = 0

    for index, chunk in enumerate(chunks, start=1):
        items = [
            ClassifyRequestItem(
                item_id=d["id"],
                source=d["source"],
                channel=d["channel"],
                product_group_id=d["product"],
                raw_text=d["text"],
                created_at=d["created_at"],
            )
            for d in chunk
        ]
        async with semaphore:
            try:
                results = await classify_aspect(items)
            except Exception as exc:  # noqa: BLE001 — 계약 밖 예외(예: 설정 오류)
                # classify_aspect 는 item 실패를 raise 하지 않는다(계약 4번). 여기까지
                # 올라온 건 get_llm_client 실패 같은 **프로세스 전역 문제**이므로,
                # 청크 하나가 아니라 전체가 같은 이유로 죽을 가능성이 높다.
                print(f"    [{index}/{len(chunks)}] ⚠️ 청크 전체 실패 {len(chunk)}건 — {exc}")
                failed += len(chunk)
                continue

        # 계약 1·2번: 길이·순서가 입력과 같고, 실패는 그 자리에 예외 객체로 온다.
        for source_item, result in zip(items, results, strict=True):
            if isinstance(result, Exception):
                # 캐시에 넣지 않는다 — 다음 회차가 이것만 다시 부르고, 그때까지는
                # 커버리지 검사가 잡아서 그 슬롯을 검정에서 뺀다(조용한 왜곡 방지).
                failed += 1
                continue
            # 건당 경로는 _parse_llm_response 를 타므로 CS 폴백이 이미 적용돼 온다.
            cache[source_item.item_id] = [
                {"aspect": a.aspect.value, "sentiment": int(a.sentiment)}
                for a in result.aspects
            ]
            done += 1
        save()
        if index % 10 == 0 or index == len(chunks):
            print(f"    [{index}/{len(chunks)}] 누적 {done:,}/{len(todo):,}건")
    return done, failed


async def classify_cached(
    documents: list[dict], run: int, tag: str = "full", mode: str = MODE_BATCH
) -> list[ClassifiedItem]:
    """Agent1 분류. 회차별 캐시를 먼저 보고 없는 것만 태운다.

    캐시 키에 넣는 것과 그 이유:
        tag      파일럿(--limit)과 본실행이 섞이지 않게. 분모가 다르다.
        mode     건당 ↔ 배치. 한 회차 안에 두 호출 방식의 결과가 섞이면 안 된다.
        지문     프롬프트 이름 + **내용 해시** (prompt_fingerprint 참고).
        run      회차. temperature=0 에서도 실행마다 흔들려 평균을 내야 한다.

    이 넷 중 하나라도 다르면 다른 파일이 된다. 캐시가 조용히 재사용돼 "고쳤는데 숫자가
    안 변한다"가 생기는 걸 막는 게 목적이다.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # 대상이 CS 전용이라 프롬프트1 지문만 본다 (collect_documents 가 source=cs 만 모음).
    cache_path = (
        CACHE_DIR / f"pipeline_{tag}_{mode}_{prompt_fingerprint()}_run{run}.json"
    )
    cache: dict = (
        json.loads(cache_path.read_text(encoding="utf-8"))
        if cache_path.exists()
        else {}
    )

    def save() -> None:
        """청크마다 저장 — 중단돼도 이어서 돌 수 있게. 재실행에 과금이 없다."""
        cache_path.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    todo = [d for d in documents if d["id"] not in cache]
    print(
        f"  회차 {run}[{mode}]: 캐시 {len(documents) - len(todo):,}건"
        f" / 신규 호출 {len(todo):,}건"
    )

    if todo:
        counter = _FallbackCounter()
        logging.getLogger("app.classification.service").addHandler(counter)
        try:
            runner = _run_batch if mode == MODE_BATCH else _run_per_item
            _done, failed = await runner(todo, cache, save)
        finally:
            logging.getLogger("app.classification.service").removeHandler(counter)

        if failed:
            print(f"  ⚠️ 무응답 {failed:,}건 — 다음 회차 실행 시 이것만 다시 호출된다")
        if counter.count:
            share = counter.count / len(todo)
            print(
                f"  ⚠️ CS 빈 배열 {counter.count:,}건 ({share:.1%}) → 기타/중립 으로 대체"
                " — 프롬프트1 개선 대상"
            )

    return _to_items(documents, cache)


def oracle_classified(documents: list[dict]) -> list[ClassifiedItem]:
    """golden 라벨로 만든 ClassifiedItem — ①과 같은 입력. LLM 0회."""
    labels = {r["inquiry_id"]: r for r in read(GOLDEN_CS_LABELS)}
    aspects_of: dict[str, list] = {}
    for d in documents:
        label = labels.get(d["id"], {})
        if label.get("true_aspect"):
            aspects_of[d["id"]] = [
                {
                    "aspect": label["true_aspect"],
                    "sentiment": int(label["true_sentiment"]),
                }
            ]
    return _to_items(documents, aspects_of)


# ── 예측 (①의 배치에 실측 카운트만 덮어쓴다) ─────────────────────


def measure(rows: list[dict], config_rows: list[dict]) -> dict:
    """정규화 행 → {(product, aspect, channel, source): (cur_neg, cur_total)}.

    케이스가 정의된 슬롯만 낸다 — 배경을 건드리면 (①−②)가 분류 오차만 뜻하지 않게 된다.

    ⚠️ 분모는 (상품, 채널, source) 에서 가져온다 — aspect 무관(aggregate §129). 케이스
       슬롯만 실측으로 교체하므로, config 의 cur_total 과 실제 문서 수가 다르면 같은
       (상품, 채널) 안에서 aspect 마다 분모가 갈린다. 지금은 전 슬롯이 일치하지만
       (지인 리뷰 2026-08-04 확인: 117/117), 목 데이터를 재생성하면 조용히 깨지는
       종류라 아래에서 대조한다.
    """
    slots = {
        (r["golden_group_id"], r["aspect"], r["channel"], r["source"])
        for r in config_rows
        if r["source"] == SOURCE_CS
    }
    expected_total = {
        (r["golden_group_id"], r["channel"], r["source"]): int(r["cur_total"])
        for r in config_rows
        if r["source"] == SOURCE_CS
    }
    days = sorted({r["day"] for r in rows})
    if not days:
        return {}
    totals, negs = count_window(rows, days[0], days[-1])

    out: dict[tuple, tuple[int, int]] = {}
    mismatched: list[str] = []
    for slot in sorted(slots):
        product, _aspect, channel, source = slot
        total = totals.get((product, channel, source), 0)
        if not total:
            continue
        want = expected_total.get((product, channel, source))
        if want is not None and want != total:
            mismatched.append(f"{product}/{channel} config {want} vs 실측 {total}")
        out[slot] = (negs.get(slot, 0), total)

    if mismatched:
        # 실패시키지 않는다 — --limit 파일럿은 원래 분모가 작다. 다만 전량 실행에서
        # 뜨면 config 와 목 데이터가 어긋났다는 뜻이라 결과 해석 전에 확인해야 한다.
        print(f"  ⚠️ 분모 불일치 {len(mismatched)}건 — {'; '.join(mismatched[:3])}")
    return out


def predict_with_counts(
    config_rows: list[dict],
    products: list[str],
    measured: dict,
    unreliable: set | None = None,
) -> dict:
    """①의 배치 구성을 그대로 쓰되, 케이스 슬롯의 현재 윈도우 카운트만 교체한다.

    unreliable: 분류 커버리지 미달로 분모를 믿을 수 없는 (상품, 채널, source).
        **검정 전에** family 에서 빠진다 — 부풀려진 p값 하나가 BH step-up 에서
        기각 개수를 늘려 다른 검정의 임계까지 완화시키기 때문이다.
    """
    combos = []
    for product, aspect, channel, source, counts in build_combinations(
        config_rows, products
    ):
        cur_neg, cur_total, past_neg, past_total = counts
        hit = measured.get((product, aspect, channel, source))
        if hit is not None:
            cur_neg, cur_total = hit
        combos.append(
            (
                product,
                aspect,
                channel,
                source,
                (cur_neg, cur_total, past_neg, past_total),
            )
        )

    batch, held = run_detection(combos, unreliable_denominators=unreliable)
    verdicts = run_verdict(batch, held)
    tests = {t["key"]: t for t in batch}
    counts_map = {(p, a, c, s): cnt for p, a, c, s, cnt in combos}

    candidates: dict[tuple, dict] = {}
    for source in SOURCES:
        for key, cand in _build_candidates(verdicts, source, tests, counts_map).items():
            candidates[(*key, source)] = cand

    return {
        "tests": tests,
        "candidates": candidates,
        "consistent": cause_consistency(config_rows),
        "changes": change_log(config_rows),
        "n_batch": len(batch),
        "n_held": len(held),
    }


# ── 진단 (LLM 0회 — 캐시만 읽는다) ───────────────────────────────


def _golden_negatives() -> dict[str, tuple[str, str]]:
    """{inquiry_id: (true_aspect, true_sentiment)} — 부정으로 라벨된 CS 문의만."""
    return {
        r["inquiry_id"]: (r["true_aspect"], r["true_sentiment"])
        for r in read(GOLDEN_CS_LABELS)
        if r.get("true_aspect") and r.get("true_sentiment") == "-1"
    }


def classify_errors(documents: list[dict], cache: dict) -> tuple[Counter, Counter]:
    """골든 부정 문의가 실제 분류에서 어떻게 됐는지 분해한다.

    ②의 하락폭이 '어떤 종류의 분류 오차'에서 오는지 가르는 게 목적이다. aspect 를
    틀린 것과 aspect 는 맞고 감성만 뒤집힌 것은 고칠 곳이 다르다 — 전자는 aspect
    정의, 후자는 감성 판단 규칙이다.

    Returns:
        (분해 카운터, 감성만 뒤집힌 문장 빈도)
    """
    gold = _golden_negatives()
    breakdown: Counter = Counter()
    flipped: Counter = Counter()

    for doc in documents:
        label = gold.get(doc["id"])
        if label is None:
            continue
        aspect, _ = label
        predicted = cache.get(doc["id"], [])
        same_aspect = [p for p in predicted if p["aspect"] == aspect]

        if same_aspect and same_aspect[0]["sentiment"] == -1:
            breakdown["정답 (부정 유지)"] += 1
        elif same_aspect:
            breakdown["같은 aspect · 감성만 뒤집힘"] += 1
            flipped[(aspect, doc["text"])] += 1
        elif predicted:
            breakdown["다른 aspect 로"] += 1
        else:
            breakdown["분류 결과 없음"] += 1
    return breakdown, flipped


def restore_sentiment(documents: list[dict], cache: dict) -> tuple[dict, int]:
    """감성만 골든으로 되돌린 캐시 사본을 만든다. **민감도 분석 전용.**

    ⚠️ 성능 주장이 아니다. 골든을 예측에 주입하므로 이 숫자는 '달성 가능한 성능'이
       아니라 **"손실이 이 오차 하나에 얼마나 귀속되는가"** 를 재는 상한이다.
       aspect 오류(다른 aspect 로 간 건)는 손대지 않는다 — 감성 축만 분리해서 본다.

    후속 작업(프롬프트1 감성 개선)의 **대조군**이라 코드로 재현 가능해야 한다.
    """
    gold = _golden_negatives()
    restored = {k: [dict(a) for a in v] for k, v in cache.items()}
    n = 0
    for doc in documents:
        label = gold.get(doc["id"])
        if label is None:
            continue
        aspect, _ = label
        for entry in restored.get(doc["id"], []):
            if entry["aspect"] == aspect and entry["sentiment"] == 0:
                entry["sentiment"] = -1
                n += 1
    return restored, n


def _load_caches(tag: str, mode: str, fingerprint: str, runs: int) -> list[tuple]:
    """해당 지문의 회차 캐시를 모은다. 없는 회차는 건너뛴다."""
    out = []
    for run in range(1, runs + 1):
        path = CACHE_DIR / f"pipeline_{tag}_{mode}_{fingerprint}_run{run}.json"
        if path.exists():
            out.append((run, json.loads(path.read_text(encoding="utf-8"))))
    return out


def diagnose(documents, config_rows, products, golden, tag, mode, runs) -> None:
    """캐시된 분류 결과로 ②의 하락 원인을 분해한다. LLM 호출 0회."""
    fingerprint = prompt_fingerprint()
    caches = _load_caches(tag, mode, fingerprint, runs)

    if not caches:
        # 지금 프롬프트로 돌린 캐시가 없다. 과거 실행을 진단하는 건 정당한 용도지만
        # (개선 전 대조군을 다시 뽑는 등), **어느 프롬프트 결과인지 반드시 밝힌다** —
        # 조용히 옛 캐시를 쓰면 캐시 키에 지문을 넣은 의미가 없어진다.
        stale = sorted(CACHE_DIR.glob(f"pipeline_{tag}_{mode}_*_run1.json"))
        if not stale:
            raise SystemExit(
                f"캐시 없음 — {CACHE_DIR} 에 pipeline_{tag}_{mode}_*_run*.json 가"
                " 있어야 한다. 먼저 실험을 돌릴 것."
            )
        fingerprint = stale[-1].stem.split(f"pipeline_{tag}_{mode}_")[1].rsplit("_run", 1)[0]
        caches = _load_caches(tag, mode, fingerprint, runs)
        print(
            f"\n⚠️ 현재 프롬프트({prompt_fingerprint()})로 돌린 캐시가 없어"
            f" **과거 실행({fingerprint})** 을 진단한다. 지금 코드의 성능이 아니다."
        )

    print(f"\n{'=' * 72}")
    print(f"실험② 오차 분해 — {mode} · {fingerprint} · 캐시 {len(caches)}회차")
    print(f"{'=' * 72}")

    total_breakdown: Counter = Counter()
    total_flipped: Counter = Counter()
    for _run, cache in caches:
        breakdown, flipped = classify_errors(documents, cache)
        total_breakdown.update(breakdown)
        total_flipped.update(flipped)

    grand = sum(total_breakdown.values())
    print(f"\n■ 골든 부정 CS 문의가 실제 분류에서 어떻게 됐나 ({len(caches)}회차 합산)")
    for label, n in total_breakdown.most_common():
        print(f"    {n:7,d} ({n / grand:5.1%})  {label}")

    print("\n■ 감성이 부정→중립으로 뒤집힌 문장 (상위 10)")
    for (aspect, text), n in total_flipped.most_common(10):
        print(f"    {n:5,d}회 [{aspect}] {text[:58]}")

    print("\n■ 민감도 — 감성만 골든으로 되돌리면 (성능 주장 아님, 상한)")
    print(f"    {'회차':6s} {'현재':>8s} {'감성 복원':>10s}  (복원 건수)")
    for run, cache in caches:
        restored, n = restore_sentiment(documents, cache)
        rates = []
        for source in (cache, restored):
            rows = build_rows(documents, _to_items(documents, source))
            pred = predict_with_counts(config_rows, products, measure(rows, config_rows))
            rates.append(_rate(score(golden, pred)["recall"]))
        print(f"    {run:<6d} {rates[0]:>8.1%} {rates[1]:>10.1%}  ({n:,}건)")


# ── 리포트 ───────────────────────────────────────────────────────


def _rate(metric) -> float:
    ok, n = metric if isinstance(metric, tuple) else (metric.ok, metric.n)
    return ok / n if n else 0.0


METRICS = [
    ("탐지율(recall)", "recall"),
    ("오탐률(FPR)", "fpr"),
    ("verdict 정확도", "verdict"),
    ("is_biased 정확도", "biased"),
    ("main_aspect 정확도", "aspect"),
    ("편중 채널 정확도", "channel"),
]


def report(runs: list[dict], oracle: dict, mode: str = MODE_BATCH) -> None:
    print(f"\n{'=' * 72}")
    print("실험② 분류 오류 전파 — ①(oracle) vs ②(실제 분류)")
    print(f"{'=' * 72}")
    print(f"분류 호출 방식: {mode} · 프롬프트 {prompt_fingerprint()} · {len(runs)}회 평균")
    if mode == MODE_BATCH:
        # 운영(워커)은 건당이라 조건이 다르다. 근거를 숫자와 함께 남겨야, 나중에
        # "왜 운영은 건당인데 실험은 배치냐"는 질문에 답이 있다.
        print("  ↳ 운영은 건당 호출. 동등성 근거는 실험③(aspect_f1 0.9899→0.9933,")
        print("     exact_match 0.9767→0.9766) — 측정이 아니라 **가정**이다.")
    print("-" * 72)
    print(f"{'지표':24s} {'① oracle':>10s} {'② 실제분류':>13s} {'하락폭':>9s}")
    print("-" * 72)
    for label, key in METRICS:
        a = _rate(oracle[key])
        values = [_rate(r[key]) for r in runs]
        b = statistics.mean(values)
        spread = f" ±{(max(values) - min(values)) / 2:.1%}" if len(values) > 1 else ""
        print(f"{label:24s} {a:>10.1%} {b:>8.1%}{spread:<5s} {a - b:>+9.1%}")

    per_run = " / ".join(f"{_rate(r['recall']):.1%}" for r in runs)
    print(f"\n회차별 탐지율: {per_run}")

    only_in_pipeline = sorted({m for r in runs for m in r["misses"]})
    if only_in_pipeline:
        print(f"  ②에서 놓친 케이스: {', '.join(only_in_pipeline)}")


# ── 진입점 ───────────────────────────────────────────────────────


async def main_async(args) -> None:
    config_rows = read(CONFIG_ANOMALY)
    products = [r["golden_group_id"] for r in read(CONFIG_PRODUCTS)]
    golden = read(GOLDEN_ANOMALY)

    documents, windows = collect_documents(config_rows)
    if args.limit > 0:
        documents = take_whole_products(documents, args.limit)

    print(f"케이스 상품 {len(windows)}개 · 현재 윈도우 CS 문의 {len(documents):,}건")
    print("과거 윈도우·배경 슬롯은 ①과 동일(oracle) — 차이는 현재 윈도우 분류뿐")
    calls = (
        -(-len(documents) // CHUNK_SIZE) if args.mode == MODE_BATCH else len(documents)
    )
    print(
        f"→ 분류 {len(documents):,}건 × {args.runs}회"
        f" = LLM 호출 {calls * args.runs:,}회 [{args.mode}] (캐시 적중분 제외)"
    )

    if args.dry_run:
        print("\n[dry-run] LLM 호출 안 함.")
        return

    tag = "full" if args.limit <= 0 else f"limit{args.limit}"

    if args.diagnose:
        diagnose(documents, config_rows, products, golden, tag, args.mode, args.runs)
        return

    # ① 기준선 — 같은 코드에 oracle 입력을 태운다. 채점 버그면 여기서 먼저 드러난다.
    oracle_rows = build_rows(documents, oracle_classified(documents))
    oracle_pred = predict_with_counts(
        config_rows, products, measure(oracle_rows, config_rows)
    )
    oracle_score = score(golden, oracle_pred)

    runs = []
    for run in range(1, args.runs + 1):
        classified = await classify_cached(documents, run, tag, args.mode)

        gaps = check_coverage(documents, classified)
        unreliable = unreliable_slots(gaps)
        if unreliable:
            print(
                f"  ⚠️ 분류 커버리지 미달 — {len(unreliable)}슬롯을 검정 전에 제외"
                f" (누락 {sum(g['documents'] - g['classified'] for g in gaps)}건)"
            )

        rows = build_rows(documents, classified)
        pred = predict_with_counts(
            config_rows, products, measure(rows, config_rows), unreliable
        )
        runs.append(score(golden, pred))

    report(runs, oracle_score, args.mode)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--runs", type=int, default=3, help="LLM 실행 횟수 (평균용)")
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="문의 수 **근사** 상한 (0=전량). 상품 경계에서 끊으므로 정확한 상한이"
        " 아니다 — 첫 상품은 이 값을 넘어도 통째로 들어간다(상품 평균 ~315건)."
        " 실제 건수는 --dry-run 으로 먼저 확인할 것.",
    )
    ap.add_argument("--dry-run", action="store_true", help="LLM 호출 없이 대상만 확인")
    ap.add_argument(
        "--diagnose",
        action="store_true",
        help="캐시된 분류 결과로 하락 원인을 분해 (LLM 0회). 오차 분해 + 감성 뒤집힌"
        " 문장 빈도 + 감성만 복원했을 때의 민감도. 프롬프트 개선의 대조군 산출용.",
    )
    ap.add_argument(
        "--mode",
        choices=[MODE_BATCH, MODE_PER_ITEM],
        default=MODE_BATCH,
        help="batch(청크당 호출 1회, 기본) / per_item(문의당 호출 1회 — 운영과 동일)",
    )
    args = ap.parse_args()

    if not GOLDEN_ANOMALY.exists():
        raise SystemExit(
            f"{GOLDEN_ANOMALY} 없음 — scripts/build_golden_anomaly.py 를 먼저 실행할 것"
        )
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
