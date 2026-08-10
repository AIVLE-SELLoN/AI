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
    PROMPT_SENTIMENT_VERSION,
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
INPUT_REVIEWS = ROOT / "data" / "input" / "input_reviews.csv"
INPUT_CHANNEL_PRODUCTS = ROOT / "data" / "input" / "input_channel_products.csv"
GOLDEN_MAPPING = ROOT / "data" / "golden" / "golden_mapping.csv"
GOLDEN_CS_LABELS = ROOT / "data" / "golden" / "golden_cs_labels.csv"
GOLDEN_REVIEW_LABELS = ROOT / "data" / "golden" / "golden_review_labels.csv"
CACHE_DIR = ROOT / "data" / "eval_cache"

DAY1 = date(2026, 6, 30)  # Day 1 = 문의 데이터 첫날
SOURCE_CS = "cs"
SOURCE_REVIEW = "review"
SOURCES = ("cs", "review")

# source 별 원본·골든·날짜 컬럼. collect_documents 가 이 표만 보고 돈다.
SOURCE_SPEC = {
    SOURCE_CS: {
        "input": INPUT_INQUIRIES,
        "golden": GOLDEN_CS_LABELS,
        "id_col": "inquiry_id",
        "date_col": "inquired_at",
    },
    SOURCE_REVIEW: {
        "input": INPUT_REVIEWS,
        "golden": GOLDEN_REVIEW_LABELS,
        "id_col": "review_id",
        "date_col": "created_at",
    },
}

# per_item: 넘긴 항목 수만큼 동시 호출이 뜬다(classify_aspect 내부 asyncio.gather).
# batch:    청크 하나가 LLM 호출 1회. 청크 크기가 곧 실패 반경이다.
CHUNK_SIZE = 20
CONCURRENCY = 4

MODE_BATCH = "batch"
MODE_PER_ITEM = "per_item"


def prompt_fingerprint() -> str:
    """캐시 키에 넣을 프롬프트 지문 — 프롬프트1·2 각각 이름(버전) + **내용 해시**.

    ⚠️ 리뷰가 대상에 들어오면서 프롬프트2 도 결과를 좌우한다(2026-08-09). 프롬프트1 만
       해싱하면 프롬프트2 를 고쳐도 캐시가 안 갈려 옛 리뷰 라벨을 조용히 재사용한다.

    버전 문자열만으로는 부족하다. Agent1 이 예시를 classify_aspect_v5.md 에 **그대로
    추가**하면 파일명이 안 바뀌어서, 캐시가 안 갈리고 옛 결과를 조용히 재사용한다.
    "고쳤는데 숫자가 안 변한다"가 되고, 캐시 탓인지 수정이 무효한 탓인지 못 가린다.
    내용 해시는 제자리 수정까지 잡는다. 이름을 같이 남기는 건 사람이 읽기 위해서다.
    """
    parts = []
    for version in (PROMPT_ASPECT_VERSION, PROMPT_SENTIMENT_VERSION):
        body = load_llm_prompt("classification", version)
        parts.append(f"{version}-{hashlib.sha256(body.encode('utf-8')).hexdigest()[:8]}")
    return "_".join(parts)


def data_fingerprint(documents: list[dict]) -> str:
    """캐시 키에 넣을 **데이터 지문** — 분류 대상 문서의 (id, 본문) 해시.

    위 prompt_fingerprint 와 정확히 같은 논리를 데이터로 확장한 것이다. 캐시는
    `d["id"]` 로만 조회하는데(:todo 계산), mock 을 재생성하면 **같은 id 에 다른
    텍스트**가 들어간다. 파일명이 안 갈리면 "신규 호출 0건" 으로 조용히 통과하면서
    **옛 라벨로 새 문서를 채점한다.** 비용이 안 드는 것처럼 보이면서 결과만 틀리는,
    제일 나쁜 형태의 실패다.

    ⚠️ 수동 규칙("재생성하면 캐시 지우기")으로 막지 않는 이유: `data/` 가 gitignore 라
       팀원마다 캐시가 따로 놀고, 한 명만 잊으면 틀린 숫자가 나온다. 기계가 막아야 한다.
       (지인님 리뷰 조건 1, 2026-08-09 — 배경 baseline 재생성 직전에 지적됨)
    """
    h = hashlib.sha256()
    for d in documents:  # 생성이 결정론이라 순서가 안정적이다
        h.update(str(d["id"]).encode("utf-8"))
        h.update(b"\x00")
        h.update(str(d["text"]).encode("utf-8"))
        h.update(b"\x1e")
    return h.hexdigest()[:8]


def cache_fingerprint(documents: list[dict]) -> str:
    """캐시 파일명에 들어가는 지문 = 프롬프트 + 데이터. 둘 중 하나만 바뀌어도 갈린다."""
    return f"{prompt_fingerprint()}-d{data_fingerprint(documents)}"


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


def collect_documents(config_rows: list[dict]) -> tuple[list[dict], dict[tuple, tuple]]:
    """케이스 상품의 **현재 윈도우** 문서를 모은다 (분모의 출처). CS·리뷰 둘 다.

    Returns:
        (documents, windows)  — windows 는 (product, source) → (cur_start, cur_end)

    ⚠️ **리뷰를 빼면 안 된다** (2026-08-09). 채점 단위 33건 중 리뷰가 2건이고
       (SC-034/review FALSE, SC-035/review TRUE), 리뷰를 안 태우면 그 2건이 oracle
       인 채로 점수에 들어가 (①−②)가 'CS 분류 오차'만 뜻하게 된다. 설계도 데모도
       두 소스를 다 쓰는데(로직 §[8] combine_sources) 실험②만 CS 전용이던 것은
       근거가 문서 어디에도 없었고, 실험③(프롬프트1·CS 배치) 경로를 재사용하면서
       따라온 공백으로 보인다.

    ⚠️ 윈도우 키가 (product, source) 다. 같은 상품이 CS·리뷰에서 다른 창을 가질 수
       있으므로 product 단독 키로 두면 한쪽이 다른 쪽 창을 덮어쓴다. 현재 config 는
       P034·P035 가 양쪽 같은 창이라 결과가 같지만, 키를 좁혀두면 config 가 바뀔 때
       조용히 어긋나는 걸 막는다.
    """
    windows: dict[tuple, tuple] = {}
    for r in config_rows:
        end = DAY1 + timedelta(days=int(r["window_end_day"]) - 1)
        windows[(r["golden_group_id"], r["source"])] = (
            end - timedelta(days=CURRENT_WINDOW_DAYS - 1),
            end,
        )

    product_of = _product_of()
    documents: list[dict] = []
    for source, spec in SOURCE_SPEC.items():
        for r in read(spec["input"]):
            product = product_of.get((r["channel"], r["channel_product_id"]))
            span = windows.get((product, source))
            if span is None:
                continue
            created = datetime.fromisoformat(r[spec["date_col"]])
            if not (span[0] <= created.date() <= span[1]):
                continue
            documents.append(
                {
                    "id": r[spec["id_col"]],
                    "product": product,
                    "channel": r["channel"],
                    "source": source,
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


async def _run_batch(
    todo: list[dict], cache: dict, save, concurrency: int = CONCURRENCY
) -> tuple[int, int]:
    """청크 1개 = LLM 호출 1회. 실험③이 검증한 run_batch_chunks 를 그대로 쓴다.

    그 함수가 이미 막아둔 것들이라 여기서 다시 하지 않는다 — item_id 매칭(위치가 아님),
    응답 누락 감지, 요청에 없는 id(환각) 무시, aspect·sentiment 값 검증, 청크 단위
    실패 격리.

    다만 결과를 전부 모아 마지막에 한 번에 돌려주므로 중간에 죽으면 다 날아간다.
    그래서 CONCURRENCY 청크씩만 넘기고 **그 묶음마다 캐시를 저장**한다.
    """
    done = failed = 0
    group = CHUNK_SIZE * concurrency
    for start in range(0, len(todo), group):
        part = todo[start : start + group]
        rows = [{"inquiry_id": d["id"], "raw_text": d["text"]} for d in part]
        predictions, failed_ids = await run_batch_chunks(rows, CHUNK_SIZE, concurrency)

        for item_id, aspects in predictions.items():
            cache[item_id] = aspects or _cs_fallback_aspects(item_id)
        save()

        done += len(predictions)
        # 무응답 건은 **캐시에 넣지 않는다** — 다음 회차 실행이 그것만 다시 부르고,
        # 그때까지는 커버리지 검사가 잡아서 그 슬롯을 검정에서 뺀다(조용한 왜곡 방지).
        failed += len(failed_ids)
        print(f"    누적 {done:,}/{len(todo):,}건 (무응답 {failed:,})")
    return done, failed


async def _run_per_item(
    todo: list[dict], cache: dict, save, concurrency: int = CONCURRENCY
) -> tuple[int, int]:
    """문의 1건 = LLM 호출 1회. 운영(워커)과 같은 호출 방식.

    ⚠️ 전량을 classify_aspect() 에 한 번에 넘기지 말 것. 내부가 asyncio.gather() 라
       넘긴 만큼 동시 호출이 뜬다(11,990건이면 11,990개). 속도 제한에 걸리고, gather 가
       통째로 raise 하면 그때까지의 결과가 다 날아간다.
    """
    semaphore = asyncio.Semaphore(concurrency)
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
    documents: list[dict],
    run: int,
    tag: str = "full",
    mode: str = MODE_BATCH,
    concurrency: int = CONCURRENCY,
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
    # 지문 = 프롬프트1+2 이름·내용 해시 + 데이터 (id, 본문) 해시. 리뷰가 들어오면서
    # 프롬프트2 도 결과에 영향을 주므로 둘 다 본다 (prompt_fingerprint 참고).
    cache_path = (
        CACHE_DIR / f"pipeline_{tag}_{mode}_{cache_fingerprint(documents)}_run{run}.json"
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
            # ⚠️ 리뷰는 항상 per_item 이다. 배치 프롬프트 조립기
            #    (run_classify_eval._build_batch_prompt)가 프롬프트1 전용이라
            #    "## 분류 대상 CS 문의" 로 잘라 쓰는데, 프롬프트2 에는 그 구분자가
            #    없어서 넣으면 프롬프트가 통째로 깨진다. per_item 은 source 를
            #    ClassifyRequestItem 에 실어 보내 classify_item 이 알아서 분기한다.
            cs_todo = [d for d in todo if d["source"] == SOURCE_CS]
            rv_todo = [d for d in todo if d["source"] != SOURCE_CS]
            failed = 0
            if cs_todo:
                runner = _run_batch if mode == MODE_BATCH else _run_per_item
                _done, f = await runner(cs_todo, cache, save, concurrency)
                failed += f
            if rv_todo:
                print(f"  리뷰 {len(rv_todo):,}건 — per_item (프롬프트2)")
                _done, f = await _run_per_item(rv_todo, cache, save, concurrency)
                failed += f
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
    """golden 라벨로 만든 ClassifiedItem — ①과 같은 입력. LLM 0회.

    CS·리뷰 골든을 한 표로 합친다. id 체계가 INQ-/RVW- 로 갈려 충돌하지 않는다.
    """
    labels = {r["inquiry_id"]: r for r in read(GOLDEN_CS_LABELS)}
    labels.update({r["review_id"]: r for r in read(GOLDEN_REVIEW_LABELS)})
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
    }
    expected_total = {
        (r["golden_group_id"], r["channel"], r["source"]): int(r["cur_total"])
        for r in config_rows
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


def _golden_labels(negatives_only: bool = True) -> dict[str, tuple[str, str]]:
    """{문서 id: (true_aspect, true_sentiment)} — **CS·리뷰 둘 다.**

    ⚠️ 리뷰가 빠져 있었다(2026-08-09). 오차 분해가 CS 만 돌아서, 리뷰 config 슬롯
       35건이 분해에 안 들어갔다. 역방향 오판이 CS 의 50배인 곳이 정확히 거기다.
       (현진님 리뷰 §3)
    """
    out: dict[str, tuple[str, str]] = {}
    for spec in SOURCE_SPEC.values():
        for r in read(spec["golden"]):
            rid = r.get(spec["id_col"])
            aspect, sent = r.get("true_aspect"), r.get("true_sentiment")
            if not rid or not aspect:
                continue
            if negatives_only and sent != "-1":
                continue
            out[rid] = (aspect, sent)
    return out


def _golden_negatives() -> dict[str, tuple[str, str]]:
    """부정 라벨만. 하위호환 별칭."""
    return _golden_labels(negatives_only=True)


def config_slots(config_rows: list[dict]) -> set:
    """config 에 정의된 (상품, aspect, 채널, source) 슬롯.

    **채점에 닿는 유일한 집합이다.** measure() 가 이 슬롯만 내고, 나머지는
    build_combinations 의 합성값을 그대로 쓴다. 즉 슬롯 밖 문서의 분류 결과는
    분자에도 분모에도 안 들어간다(분모는 build_rows 경유라 분류와 무관).

    ⚠️ 이 구분이 없으면 --diagnose 숫자를 못 읽는다. 2026-08-09 배경 baseline
       재생성으로 케이스 윈도우의 골든 부정이 1,618 → 3,367 로 늘었는데 config
       슬롯 몫은 1,278 로 그대로다. 슬롯 내 비율이 79% → 38% 로 떨어져서, 필터
       없이 세면 분모의 62% 가 채점과 무관한 문서다. (현진님 리뷰 §1)
    """
    return {
        (r["golden_group_id"], r["aspect"], r["channel"], r["source"])
        for r in config_rows
    }


def _in_slot(doc: dict, aspect: str, slots: set) -> bool:
    return (doc["product"], aspect, doc["channel"], doc["source"]) in slots


def classify_errors(
    documents: list[dict], cache: dict, slots: set | None = None
) -> tuple[Counter, Counter]:
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
        if slots is not None and not _in_slot(doc, aspect, slots):
            continue
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


def restore_sentiment(
    documents: list[dict], cache: dict, slots: set | None = None
) -> tuple[dict, int]:
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
        if slots is not None and not _in_slot(doc, aspect, slots):
            continue
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


def diagnose(documents, config_rows, products, golden, tag, mode, runs,
             sources: str = "all") -> None:
    """캐시된 분류 결과로 ②의 하락 원인을 분해한다. LLM 호출 0회.

    ⚠️ `sources` 로 좁혀도 **캐시 조회는 전체 문서 집합의 지문으로 한다.** 캐시가
       그 집합으로 만들어졌기 때문이다. 문서를 먼저 거르면 data_fingerprint 가 바뀌어
       "캐시 없음 → 과거 실행 폴백" 으로 떨어진다(실제로 밟았다). 좁히기는 채점
       단계에서만 한다 — 범위 몫과 프롬프트 몫을 분리해 보려는 것이 목적이므로,
       분류 결과 자체는 같은 것을 써야 비교가 성립한다.
    """
    fingerprint = cache_fingerprint(documents)
    caches = _load_caches(tag, mode, fingerprint, runs)

    if not caches:
        # 지금 프롬프트·데이터로 돌린 캐시가 없다. 과거 실행을 진단하는 건 정당한 용도지만
        # (개선 전 대조군을 다시 뽑는 등), **어느 실행 결과인지 반드시 밝힌다** —
        # 조용히 옛 캐시를 쓰면 캐시 키에 지문을 넣은 의미가 없어진다.
        # ⚠️ mock 재생성 후에는 데이터 지문이 갈리므로 여기로 떨어진다. 그때 나오는
        #    숫자는 **옛 데이터로 낸 것**이라 새 데이터의 성능이 아니다.
        stale = sorted(CACHE_DIR.glob(f"pipeline_{tag}_{mode}_*_run1.json"))
        if not stale:
            raise SystemExit(
                f"캐시 없음 — {CACHE_DIR} 에 pipeline_{tag}_{mode}_*_run*.json 가"
                " 있어야 한다. 먼저 실험을 돌릴 것."
            )
        fingerprint = stale[-1].stem.split(f"pipeline_{tag}_{mode}_")[1].rsplit("_run", 1)[0]
        caches = _load_caches(tag, mode, fingerprint, runs)
        print(
            f"\n⚠️ 현재 프롬프트·데이터({cache_fingerprint(documents)})로 돌린 캐시가 없어"
            f" **과거 실행({fingerprint})** 을 진단한다."
            f"\n   지금 코드·데이터의 성능이 아니다. 지문의 -d 뒤가 다르면 다른 mock 이다."
        )

    print(f"\n{'=' * 72}")
    print(f"실험② 오차 분해 — {mode} · {fingerprint} · 캐시 {len(caches)}회차")
    print(f"{'=' * 72}")

    if sources != "all":
        # ⚠️ config_rows 를 거르면 안 된다. build_combinations 는 격자를 항상 두 source
        #    로 도는데, config 에서 빠진 슬롯은 BASELINE_RATE x BG_VOLUME 합성값이 되고
        #    cur rate == past rate 라 **구조적으로 100% 미탐**이 된다. 실제로 그렇게 짰다가
        #    SC-035/review(TRUE, 14/70)가 통째로 미탐 처리돼 4.0%p 가 그 한 건이었다.
        #    (현진님 리뷰 §1, 2026-08-09. m 은 1,464 로 불변이고 바뀌는 건 BH 의 기각 수 k 다)
        #    measured 키만 걸러야 그 슬롯이 config(oracle) 값으로 떨어져,
        #    de6600c 이전(=리뷰 oracle) 동작을 정확히 재현한다.
        n0 = len(documents)
        documents = [d for d in documents if d["source"] == sources]
        print(f"\n⚠️ 채점 범위 {sources} 로 좁힘 — 문서 {n0:,} → {len(documents):,}"
              f"\n   (다른 source 슬롯은 config oracle 값으로 떨어진다. 캐시는 전체 집합 것)")

    slots = config_slots(config_rows)
    both: dict[str, Counter] = {"전체": Counter(), "config 슬롯 내": Counter()}
    total_flipped: Counter = Counter()
    for _run, cache in caches:
        for name, flt in (("전체", None), ("config 슬롯 내", slots)):
            breakdown, flipped = classify_errors(documents, cache, flt)
            both[name].update(breakdown)
            if flt is not None:
                total_flipped.update(flipped)

    print(f"\n■ 골든 부정이 실제 분류에서 어떻게 됐나 ({len(caches)}회차 합산)")
    print("    'config 슬롯 내' 만 채점에 닿는다 — measure() 가 그 슬롯만 내고,")
    print("    밖은 build_combinations 합성값을 쓴다. 분자·분모 어느 쪽도 안 움직인다.")
    labels = list(both["전체"].keys())
    print(f"\n    {'':28s} {'전체':>16s} {'config 슬롯 내':>18s}")
    for label in labels:
        a, b = both["전체"][label], both["config 슬롯 내"][label]
        ga, gb = sum(both["전체"].values()), sum(both["config 슬롯 내"].values())
        print(f"    {label:28s} {a:7,d} ({a/ga:5.1%}) {b:9,d} ({b/gb:5.1%})")
    ga, gb = sum(both["전체"].values()), sum(both["config 슬롯 내"].values())
    print(f"    {'합계':28s} {ga:7,d}          {gb:9,d}   ({gb/ga:.0%})")

    print("\n■ 감성이 부정→중립으로 뒤집힌 문장 — config 슬롯 내만 (상위 10)")
    for (aspect, text), n in total_flipped.most_common(10):
        print(f"    {n:5,d}회 [{aspect}] {text[:58]}")

    print("\n■ 민감도 — 감성만 골든으로 되돌리면 (성능 주장 아님, 상한)")
    print(f"    {'회차':6s} {'현재':>8s} {'감성 복원':>10s}  (복원 건수)")
    print("    복원 대상은 config 슬롯 내로 한정한다 — 밖은 복원해도 점수가 안 움직인다")
    for run, cache in caches:
        restored, n = restore_sentiment(documents, cache, slots)
        rates = []
        for source in (cache, restored):
            rows = build_rows(documents, _to_items(documents, source))
            m = measure(rows, config_rows)
            if sources != "all":
                m = {k: v for k, v in m.items() if k[3] == sources}
            pred = predict_with_counts(config_rows, products, m)
            rates.append(_rate(score(golden, pred)["recall"]))
        print(f"    {run:<6d} {rates[0]:>8.1%} {rates[1]:>10.1%}  ({n:,}건)")

    reverse_flips(documents, caches)
    missed_slot_table(documents, config_rows, products, golden, caches)


def reverse_flips(documents: list[dict], caches: list) -> None:
    """골든 **비부정**이 부정으로 뒤집힌 건수 — 오탐을 만들 수 있는 유일한 방향.

    classify_errors 는 골든 부정만 순회하므로 이 방향을 아예 안 센다. 그 상태로
    "분류 오차는 탐지율만 깎고 오탐은 안 만든다"고 말하면, **측정하지 않은 것을
    없다고 주장**하는 게 된다. (현진님 리뷰 §4, 2026-08-09)

    ⚠️ 이 숫자가 0 이 아니어도 ②의 FPR 은 0 일 수 있다. ②는 현재 윈도우만 실제
       분류로 갈고 과거 윈도우·배경은 oracle 이라(docstring 11~16행), 기준선이
       깨끗한 채 분자만 움직인다. 즉 ②의 FPR 0% 는 설계의 산물이지 분류 오차의
       성질이 아니다. 운영은 과거 윈도우도 LLM 분류라 양쪽이 같이 움직인다.
    """
    gold_all = _golden_labels(negatives_only=False)

    print("\n■ 역방향 — 골든 비부정(0/1)이 부정(-1)으로 뒤집힌 건수")
    print("    오탐을 만들 수 있는 유일한 방향이다. ②의 FPR 0% 는 현재 윈도우만 실제")
    print("    분류로 갈고 과거·배경은 oracle 인 설계의 산물이라 이걸 못 잡는다.")
    for run, cache in caches:
        flips: Counter = Counter()
        base: Counter = Counter()
        for doc in documents:
            label = gold_all.get(doc["id"])
            if label is None or label[1] == "-1":
                continue
            base[doc["source"]] += 1
            if any(p["sentiment"] == -1 for p in cache.get(doc["id"], [])):
                flips[doc["source"]] += 1
        parts = [
            f"{src} {flips[src]:,}/{base[src]:,} ({flips[src] / base[src]:.2%})"
            for src in SOURCE_SPEC
            if base[src]
        ]
        print(f"    회차 {run}: " + "  ·  ".join(parts))
    print("    ⚠️ 리뷰 분모는 골든 비부정만 센 것이다(부정 79건 제외). 리뷰 오판률이")
    print("       CS 의 수십 배면, 지금 FPR 0/8 은 안전성이 아니라 표본 크기(n=70)의")
    print("       결과일 수 있다 — SC-034/review 가 그 정상 8슬롯 중 하나다.")


def missed_slot_table(documents, config_rows, products, golden, caches) -> None:
    """미탐 슬롯이 **왜** 안 울렸는지 — oracle 대비 실측 부정 수·delta·p 값.

    "오차가 케이스 슬롯에 몰린다"와 "오차는 균일한데 케이스 창이 작아 몇 건만 빠져도
    Fisher 가 못 낸다"는 처방이 다르다(문장 유형 고치기 vs 구조적 민감도). 캐시만으로
    갈린다 — 미탐 슬롯의 cur_neg 가 얼마나 깎였는지 보면 된다. (현진님 리뷰 §5)
    """
    _run, cache = caches[0]
    oracle_rows = build_rows(documents, oracle_classified(documents))
    real_rows = build_rows(documents, _to_items(documents, cache))
    oracle_m = measure(oracle_rows, config_rows)
    real_m = measure(real_rows, config_rows)

    for want, title in (("TRUE", "TRUE 슬롯"), ("FALSE", "정상(FALSE) 슬롯")):
        _slot_table(config_rows, oracle_m, real_m, want, title, _run)


def _slot_table(config_rows, oracle_m, real_m, want, title, run) -> None:
    """oracle 대비 실측 cur_neg. TRUE 는 미탐 원인, FALSE 는 오탐 여지를 본다.

    FALSE 쪽을 같이 찍는 이유: ②의 FPR 0% 가 "분류 오차가 오탐을 안 만든다" 인지
    "정상 슬롯의 n 이 작아 아직 안 터졌다" 인지 가른다. 역방향 오판률이 높은 소스가
    이 표에 있으면 후자 쪽이다. (현진님 리뷰 §2)
    """
    truth = {
        (r["golden_group_id"], r["aspect"], r["channel"], r["source"])
        for r in config_rows
        if r.get("intended_answer", "").strip().upper() == want
    }
    print(f"\n■ {title}의 현재 윈도우 부정 수 — oracle vs 실측 (회차 {run})"
)
    print(f"    {'슬롯':38s} {'oracle':>12s} {'실측':>12s} {'차':>6s}")
    shrunk = 0
    for key in sorted(truth):
        o, r = oracle_m.get(key), real_m.get(key)
        if not o or not r:
            continue
        if o[0] != r[0]:
            shrunk += 1
        name = f"{key[0]}/{key[1]}/{key[2]}/{key[3]}"
        print(
            f"    {name:38s} {o[0]:>5d}/{o[1]:<6d} {r[0]:>5d}/{r[1]:<6d}"
            f" {r[0] - o[0]:>+6d}"
        )
    verb = "깎인" if want == "TRUE" else "달라진"
    print(f"    → 부정 수가 {verb} 슬롯 {shrunk}/{len(truth)}개")


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
    print(
        f"분류 호출 방식: CS {mode} · 리뷰 per_item"
        f" · 프롬프트 {prompt_fingerprint()} · {len(runs)}회 평균"
    )
    print(
        "  ↳ ⚠️ 리뷰는 항상 per_item 이다(프롬프트2 에 배치 조립기가 없다). 아래 배치↔건당"
        "\n     동등성 근거(실험③)는 프롬프트1·CS 에서 잰 것이라 리뷰엔 안 걸린다."
    )
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

    n_by_source = Counter(d["source"] for d in documents)
    print(
        f"케이스 상품 {len({p for p, _s in windows})}개 · 현재 윈도우 문서"
        f" {len(documents):,}건"
        f" (CS {n_by_source[SOURCE_CS]:,} · 리뷰 {n_by_source[SOURCE_REVIEW]:,})"
    )
    print("과거 윈도우·배경 슬롯은 ①과 동일(oracle) — 차이는 현재 윈도우 분류뿐")
    # 리뷰는 배치 프롬프트가 없어 항상 per_item 이다(_build_batch_prompt 가 프롬프트1
    # 전용). 그래서 호출 수도 source 별로 따로 센다.
    n_cs, n_rv = n_by_source[SOURCE_CS], n_by_source[SOURCE_REVIEW]
    calls = (-(-n_cs // CHUNK_SIZE) if args.mode == MODE_BATCH else n_cs) + n_rv
    print(
        f"→ 분류 {len(documents):,}건 × {args.runs}회"
        f" = LLM 호출 {calls * args.runs:,}회"
        f" [CS {args.mode} · 리뷰 per_item] (캐시 적중분 제외)"
    )

    if args.dry_run:
        print("\n[dry-run] LLM 호출 안 함.")
        return

    tag = "full" if args.limit <= 0 else f"limit{args.limit}"

    if args.diagnose:
        diagnose(documents, config_rows, products, golden, tag, args.mode,
                 args.runs, args.sources)
        return

    # ① 기준선 — 같은 코드에 oracle 입력을 태운다. 채점 버그면 여기서 먼저 드러난다.
    oracle_rows = build_rows(documents, oracle_classified(documents))
    oracle_pred = predict_with_counts(
        config_rows, products, measure(oracle_rows, config_rows)
    )
    oracle_score = score(golden, oracle_pred)

    runs = []
    for run in range(1, args.runs + 1):
        classified = await classify_cached(
            documents, run, tag, args.mode, args.concurrency
        )

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
        "--sources",
        default="all",
        choices=["all", SOURCE_CS, SOURCE_REVIEW],
        help="채점 범위. all=CS+리뷰(기본). 범위 몫과 프롬프트 몫을 분리해 볼 때 cs 로"
        " 좁힌다 — 캐시를 그대로 쓰므로 LLM 호출은 없다.",
    )
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
    ap.add_argument(
        "--concurrency",
        type=int,
        default=CONCURRENCY,
        help=f"동시 LLM 호출 수 (기본 {CONCURRENCY}). 전량 실행이 오래 걸릴 때 올린다."
        " ⚠️ batch 는 이 값만큼 청크가 동시에 뜨지만, per_item 은 청크마다 내부에서"
        f" CHUNK_SIZE({CHUNK_SIZE})개를 또 gather 하므로 실제 동시 호출은 이 값의"
        f" {CHUNK_SIZE}배다. 속도 제한(TPM)에 걸리면 무응답이 늘고, 그 건들은 캐시에"
        " 안 들어가 다음 실행이 다시 부른다(과금은 되고 결과는 못 씀).",
    )
    args = ap.parse_args()

    if not GOLDEN_ANOMALY.exists():
        raise SystemExit(
            f"{GOLDEN_ANOMALY} 없음 — scripts/build_golden_anomaly.py 를 먼저 실행할 것"
        )
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
