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
    python eval/run_classify_eval.py --only-negative --limit 300 --mode batch  # 🆕 대표지표①②
        # 용(부정N+비부정N 균형표본 — PR 리뷰 반영 후 FPR도 같이 나옴)

재현성: --seed로 표본이 고정된다. 결과 JSON에 프롬프트 버전·해시·모델·시드·모드·
       only_negative 여부·일시를 남긴다(PR 리뷰 반영 — 같은 prompt_version 문자열이라도
       파일을 제자리수정하면 내용이 달라질 수 있어 해시로 구분).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import random
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import app.classification.service as classification_service
from app.classification.service import ClassifyRequestItem
from app.core.console import force_utf8_output
from app.core.constants import KST
from app.core.exceptions import AiServiceError
from app.core.llm_client import get_llm_client
from app.core.schemas import Channel, Source

GOLDEN_LABELS = ROOT / "data" / "golden" / "golden_cs_labels.csv"
INPUT_INQUIRIES = ROOT / "data" / "input" / "input_cs_inquiries.csv"
RESULTS_DIR = ROOT / "eval" / "results"
PROMPT_DIR = ROOT / "app" / "classification" / "prompts"
CACHE_DIR = ROOT / "data" / "eval_cache"

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


def operational_negative_rate(all_rows: list[dict]) -> float | None:
    """운영 부정비율 p — **표본이 아니라 골든 전량**에서 센다.

    ⚠️ 반드시 `sample_rows()` **이전**의 전체 행을 넘길 것. 표본은 aspect 층화(또는
       `--only-negative`)라 부정비율이 설계상 왜곡돼 있고, 그 값으로 환산하면
       precision_operational 이 표본 precision 과 같아져 지표 자체가 무의미해진다.

    ⚠️ 하드코딩 금지 (2026-08-10). 원래 `P_OPERATIONAL = 0.074` 상수였는데,
       golden_cs_labels.csv 가 재생성되면(배경 baseline 분모 수정 등) 골든 부정비율이
       움직이는데 상수는 안 움직여서 환산값이 조용히 틀린다. 실제로 7,117/96,531
       (7.4%) 기준으로 박아둔 값이 재생성 후 3배 넘게 어긋났다. 골든에서 직접 세면
       어떤 골든을 쓰든 자동으로 맞는다.
    """
    if not all_rows:
        return None
    return sum(1 for r in all_rows if r["true_sentiment"] == -1) / len(all_rows)


# ── 분류 캐시 ────────────────────────────────────────────────────
#
# 왜 필요한가 (2026-08-11): 전량 실행(96,524건 · 4,827청크 · 약 1시간)이 중간에
# 죽으면 **그때까지 쓴 돈이 통째로 날아간다.** 실제로 API 크레딧이 3,248청크째에
# 소진돼 70%만 채점된 결과가 나왔고, 재개할 방법이 없어 전부 다시 사야 했다.
# 실험②(run_pipeline_eval)는 같은 이유로 이미 캐시를 쓰고 있다 — 그 설계를 따른다.


def prompt_fingerprint(prompt_version: str) -> str:
    """캐시 키에 넣을 프롬프트 지문 — 이름(버전) + **내용 해시**.

    버전 문자열만으로는 부족하다. 예시를 파일에 **그대로 추가**하면 파일명이 안 바뀌어서
    캐시가 안 갈리고 옛 결과를 조용히 재사용한다. "고쳤는데 숫자가 안 변한다"가 되고,
    캐시 탓인지 수정이 무효한 탓인지 못 가린다. (실험②의 같은 이름 함수와 동일 논리)

    load_llm_prompt 를 쓰므로 "## System Prompt" 위의 변경이력은 해시에 안 들어간다 —
    LLM 에 안 나가는 부분이라 결과를 못 바꾸고, 주석 한 줄에 캐시가 날아가면 안 된다.
    """
    body = classification_service.load_llm_prompt("classification", prompt_version)
    return f"{prompt_version}-{hashlib.sha256(body.encode('utf-8')).hexdigest()[:8]}"


def data_fingerprint(rows: list[dict]) -> str:
    """채점 대상 행의 (id, 본문) 해시.

    캐시는 `inquiry_id` 로만 조회하는데, 목 데이터를 재생성하면 **같은 id 에 다른
    텍스트**가 들어간다. 지문이 안 갈리면 "신규 호출 0건"으로 조용히 통과하면서
    **옛 라벨로 새 문서를 채점한다.** 비용이 안 드는 것처럼 보이면서 결과만 틀리는,
    제일 나쁜 실패다. 표본 구성(--limit/--seed/--only-negative)도 행 집합이 달라지므로
    이 지문 하나에 같이 잡힌다.
    """
    h = hashlib.sha256()
    for r in rows:  # sample_rows 가 결정론이라 순서가 안정적이다
        h.update(str(r["inquiry_id"]).encode("utf-8"))
        h.update(b"\x00")
        h.update(str(r["raw_text"]).encode("utf-8"))
        h.update(b"\x1e")
    return h.hexdigest()[:8]


def _corpus_fingerprint() -> str | None:
    """실험②가 정본으로 쓰는 케이스 윈도우 지문 — 두 실험의 코퍼스 대조용.

    ③의 `data_fingerprint` 는 **이 실행이 채점한 행**의 지문이라 `--limit`/`--seed` 에
    따라 달라진다. 그것만으로는 "②와 같은 코퍼스인가"를 못 본다. 같은 `data/golden` 을
    읽으면서 모집단이 다르기 때문이다. 그래서 ②의 값을 같이 남긴다.

    run_pipeline_eval 은 `eval/` 안에 있고 패키지가 아니라, 임포트 실패는 정상 경로다
    (경로 설정 없이 이 파일만 임포트한 경우). 그때는 None — 기록을 못 남길 뿐 채점은 돈다.
    """
    try:
        sys.path.insert(0, str(ROOT / "eval"))
        from run_pipeline_eval import CONFIG_ANOMALY, collect_documents
        from run_pipeline_eval import data_fingerprint as pipeline_fingerprint
        from run_pipeline_eval import read as read_config

        documents, _ = collect_documents(read_config(CONFIG_ANOMALY))
        return pipeline_fingerprint(documents)
    except Exception:  # noqa: BLE001 — 기록용 부가 정보라 채점을 막으면 안 된다
        return None


def cache_path_for(rows: list[dict], prompt_version: str, mode: str, limit: int) -> Path:
    tag = "full" if limit <= 0 else f"limit{limit}"
    fp = f"{prompt_fingerprint(prompt_version)}_{data_fingerprint(rows)}"
    return CACHE_DIR / f"classify_{tag}_{mode}_{fp}.json"


async def run_with_cache(
    rows: list[dict], chunk_size: int, concurrency: int, mode: str, path: Path
) -> tuple[dict[str, list[dict]], list[str]]:
    """캐시에 없는 행만 태우고, **묶음마다 저장한다.**

    run_chunks / run_batch_chunks 자체는 안 건드린다 — run_pipeline_eval 이
    run_batch_chunks 를 import 해서 쓰므로 시그니처가 바뀌면 실험②가 깨진다.
    여기서는 그 함수들을 묶음 단위로 여러 번 부르고 사이사이 저장만 한다.

    ⚠️ **무응답은 캐시에 안 넣는다.** 다음 실행이 그것만 다시 부르게 하려는 것이다.
       넣어버리면 실패가 영구히 굳는다.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache: dict[str, list[dict]] = (
        json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    )
    todo = [r for r in rows if r["inquiry_id"] not in cache]
    print(f"  캐시 {len(rows) - len(todo):,}건 적중 / 신규 호출 {len(todo):,}건 → {path.name}")
    if not todo:
        return {r["inquiry_id"]: cache[r["inquiry_id"]] for r in rows}, []

    runner = run_batch_chunks if mode == "batch" else run_chunks
    failed: list[str] = []
    group = chunk_size * concurrency  # 이 묶음이 끝날 때마다 디스크에 쓴다
    for start in range(0, len(todo), group):
        part = todo[start : start + group]
        preds, part_failed = await runner(part, chunk_size, concurrency)
        cache.update(preds)
        path.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
        failed.extend(part_failed)
        done = min(start + group, len(todo))
        print(f"    누적 {done:,}/{len(todo):,}건 저장 (무응답 {len(failed):,})")

    return {r["inquiry_id"]: cache[r["inquiry_id"]] for r in rows if r["inquiry_id"] in cache}, failed


def parse_few_shot_examples(prompt_version: str) -> list[str]:
    """프롬프트 파일에서 few-shot '입력:' 문장을 전부 파싱한다(§6 B안 1번, 지인님 리뷰
    2026-08-06). 하드코딩 목록 대신 파일을 직접 읽어서, 예시가 늘어나도 자동 반영된다
    (`USED_PREFIXES` 방식의 반대 — 그쪽은 71630 재라벨링 스크립트 전용이고, 이건 우리
    자체 코퍼스 채점용이라 목적이 다름).
    """
    path = PROMPT_DIR / f"{prompt_version}.md"
    if not path.exists():
        print(f"⚠️  프롬프트 파일 없음: {path} — few-shot 유출 검사 건너뜀")
        return []
    content = path.read_text(encoding="utf-8")
    return re.findall(r'입력: "(.+?)"', content)


def compute_leak_map(rows: list[dict], few_shot_texts: list[str], threshold: float) -> dict[str, dict]:
    """골든의 고유 raw_text마다, few-shot 예시들과의 최대 유사도를 계산해 유출 여부를
    판정한다(§6 B안 — "완전일치 + 유사도 임계", 실험④의 prefix 부분일치와 다름 — 실험③은
    문장 전체가 템플릿이라 전체 유사도가 더 맞음). SequenceMatcher는 difflib.SequenceMatcher.
    ratio()로, 지인님이 노션에 기록한 예시20(1.00)·25(0.88)·20-2(0.79)를 그대로 재현하는
    걸 확인한 알고리즘이다(2026-08-06 검증).

    Returns: {raw_text: {"is_leaked": bool, "max_similarity": float, "matched_example": str|None}}
    """
    if not few_shot_texts:
        return {}
    unique_texts = {r["raw_text"] for r in rows}
    leak_map: dict[str, dict] = {}
    for text in unique_texts:
        best_sim = 0.0
        best_match: str | None = None
        for fs in few_shot_texts:
            sim = SequenceMatcher(None, text, fs).ratio()
            if sim > best_sim:
                best_sim, best_match = sim, fs
        is_leaked = best_sim >= threshold
        leak_map[text] = {
            "is_leaked": is_leaked,
            "max_similarity": round(best_sim, 4),
            "matched_example": best_match if is_leaked else None,
        }
    return leak_map


def tag_leaked_rows(rows: list[dict], leak_map: dict[str, dict]) -> None:
    """rows 각 행에 is_leaked·leak_similarity를 in-place로 붙인다(load_dataset() 직후
    단계 — §6 B안 2번). leak_map이 비어있으면(few-shot 파싱 실패 등) 전부 False로 채워
    이후 로직이 안전하게 동작하게 한다.
    """
    for r in rows:
        info = leak_map.get(r["raw_text"], {"is_leaked": False, "max_similarity": 0.0})
        r["is_leaked"] = info["is_leaked"]
        r["leak_similarity"] = info["max_similarity"]


def _stratified_by_aspect(rows: list[dict], limit: int, rng: random.Random) -> list[dict]:
    """true_aspect 비율을 유지한 층화 표본(내부 헬퍼). limit<=0이면 전량."""
    if limit <= 0 or limit >= len(rows):
        return list(rows)

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
    rng.shuffle(picked)
    return picked[:limit]


def sample_rows(rows: list[dict], limit: int, seed: int, only_negative: bool = False) -> list[dict]:
    """true_aspect 비율을 유지한 층화 표본. limit<=0이면 전량.

    ⚠️ only_negative 동작 변경(PR 리뷰 반영, 2026-08-05) — 기존엔 sentiment=-1인
    것만 걸러서 뽑았는데, 그러면 골든에 비부정(0/1) 표본이 아예 없어져서
    score()의 neg_fp·neg_tn이 구조적으로 항상 0이 된다(오탐을 원리적으로 측정
    불가 — "모든 문의를 부정으로 뭉개는" 최악의 모델도 precision 100%가 나옴).
    이제 only_negative=True면 **부정 N건 + 비부정 N건을 절반씩 균형 표본**으로
    뽑는다 — 부정 쪽에서 tp/fn(재현율), 비부정 쪽에서 fp/tn(오탐률)을 같이
    측정할 수 있게. limit이 홀수면 부정 쪽에 1건 더 준다.
    """
    rng = random.Random(seed)

    if not only_negative:
        return _stratified_by_aspect(rows, limit, rng)

    neg_rows = [r for r in rows if r["true_sentiment"] == -1]
    nonneg_rows = [r for r in rows if r["true_sentiment"] != -1]

    if limit <= 0:
        # 전량이면 부정 전체 + 비부정 전체(양쪽 다 무제한)
        return _stratified_by_aspect(neg_rows, 0, rng) + _stratified_by_aspect(nonneg_rows, 0, rng)

    neg_limit = (limit + 1) // 2  # 홀수면 부정 쪽에 1건 더
    nonneg_limit = limit // 2
    picked = _stratified_by_aspect(neg_rows, neg_limit, rng) + _stratified_by_aspect(nonneg_rows, nonneg_limit, rng)
    rng.shuffle(picked)
    return picked


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
                created_at=datetime.now(KST),
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


def _basic_metrics(rows_subset: list[dict], predictions: dict[str, list[dict]]) -> dict:
    """aspect_f1·sentiment_accuracy·exact_match_rate만 계산하는 경량 헬퍼.

    score()의 메인 루프와 로직은 동일하되(같은 tp/fp/fn·정답 판정 규칙), 임의의 행
    부분집합에 적용 가능하게 분리했다 — leak_filter의 "제외 후" 값을 실제로 재계산하는
    용도(§6 B안 4번, 지인님 리뷰 2026-08-06). LLM 재호출 없음 — predictions는 이미 있는
    걸 그대로 재사용.
    """
    tp = fp = fn = 0
    sent_correct = sent_total = 0
    exact_match = 0
    for r in rows_subset:
        pred_aspects = predictions[r["inquiry_id"]]
        pred_aspect_set = {p["aspect"] for p in pred_aspects}
        true_aspect = r["true_aspect"]
        true_sentiment = r["true_sentiment"]

        if true_aspect in pred_aspect_set:
            tp += 1
        else:
            fn += 1
        fp += len(pred_aspect_set - {true_aspect})

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

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    n = len(rows_subset)
    return {
        "aspect_f1": round(f1, 4),
        "aspect_precision": round(precision, 4),
        "aspect_recall": round(recall, 4),
        "sentiment_accuracy": round(sent_correct / sent_total, 4) if sent_total else 0.0,
        "exact_match_rate": round(exact_match / n, 4) if n else 0.0,
        "n_scored": n,
    }


def score(
    rows: list[dict],
    predictions: dict[str, list[dict]],
    leak_threshold: float | None = None,
    operational_rate: float | None = None,
) -> dict:
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
    # ⚠️ PR 리뷰 반영(2026-08-05) — fp+tn==0(비부정 표본이 0건)이면 precision·F1은
    # "측정 불가"이지 100%가 아니다. only_negative 표본이 부정만 있던 예전 버전에선
    # 이 조건이 항상 참이라서, "전부 부정으로 뭉개는" 최악의 모델도 precision 100%가
    # 나오는 함정이 있었다. sample_rows()가 이제 비부정도 같이 뽑아오므로 정상적으로는
    # fp+tn>0이어야 하지만, 혹시 비부정 표본이 우연히 0건인 극단적 케이스를 대비해 null 처리.
    has_nonneg_sample = (neg_fp + neg_tn) > 0
    neg_fpr = round(neg_fp / (neg_fp + neg_tn), 4) if has_nonneg_sample else None
    neg_precision_reported = neg_precision if has_nonneg_sample else None

    # ⚠️ **반대 방향도 같이 막는다** (서영님 리뷰 2026-08-10). 골든 부정 표본이 0건이면
    #    recall 은 0/0 이라 **못 재는 것**이지 0% 가 아니다. _prf1 이 0.0 을 폴백으로 내는
    #    바람에 recall·f1·환산 precision 이 전부 "0%" 로 찍혔다 — 비부정 쪽은
    #    has_nonneg_sample 로 막아뒀는데 이쪽만 빠져 있었다. 이 파일이 없애려던
    #    "못 재는 걸 그럴듯한 숫자로 채우는" 실패 그대로다.
    #    precision 은 예외다 — tp/(tp+fp) 는 부정 표본이 없어도 "낸 예측이 다 틀렸다"로
    #    실제 측정된 값이라 0.0 이 맞다.
    has_neg_sample = (neg_tp + neg_fn) > 0
    neg_recall_reported = neg_recall if has_neg_sample else None
    neg_f1_reported = neg_f1 if (has_nonneg_sample and has_neg_sample) else None

    # 🆕 운영 비율 환산 precision (Notion A안, 지인님 PR리뷰 2026-08-06 재요청)
    # 균형표본(50:50)의 precision은 실제 운영 트래픽 비율(부정 7.4%)의 값이 아니다.
    # recall·FPR은 각자 자기 클래스 안에서만 계산돼 표본비율과 무관하지만, precision은
    # 두 클래스의 상대적 비중에 직접 좌우된다 — 베이즈 정리로 표본비율 의존성을 제거해
    # "실제 운영 트래픽에 이 모델을 붙이면 나올 precision"으로 환산한다.
    #   P(true=부정|pred=부정) = P(pred=부정|true=부정)*P(true=부정) / P(pred=부정)
    #                          = recall*p / (recall*p + FPR*(1-p))
    # p는 호출자가 골든 전량에서 세어 넘긴다(operational_negative_rate). 하드코딩했다가
    # 골든 재생성에 안 따라가서 조용히 틀리는 사고가 있었다 — 그 함수의 주석 참고.
    # 안 넘어오면 **추정하지 않고 None**을 낸다. 못 재는 걸 그럴듯한 숫자로 채우면
    # 아무도 안 본다(위 neg_precision_reported 와 같은 원칙).
    # ⚠️ **반올림 전 값으로 계산한다** (서영님 리뷰 2026-08-10). neg_recall·neg_fpr 은
    #    보고용으로 4자리 반올림돼 있는데, FPR 이 작을수록 그 반올림이 환산값을 크게
    #    흔든다 — FPR 0.00004 가 0.0 으로 접히면 환산 precision 이 100%로 튄다.
    p_operational = operational_rate
    raw_recall = neg_tp / (neg_tp + neg_fn) if (neg_tp + neg_fn) else 0.0
    raw_fpr = neg_fp / (neg_fp + neg_tn) if has_nonneg_sample else None

    # ⚠️ **분모가 0이면 None** (서영님 리뷰 2026-08-10). recall 과 FPR 이 둘 다 0이면
    #    — 모델이 부정을 하나도 안 낸 상태 — 분모가 0이 돼 ZeroDivisionError 로 죽었다.
    #    이 계산이 JSON 쓰기 **전**에 있어서, 터지면 그 회차 LLM 비용이 통째로 날아간다.
    #    가드가 `is not None` 뿐이라 값이 0 인 건 안 걸렸다.
    neg_precision_operational = None
    if raw_fpr is not None and p_operational is not None and has_neg_sample:
        denom = p_operational * raw_recall + (1 - p_operational) * raw_fpr
        if denom > 0:
            neg_precision_operational = round(p_operational * raw_recall / denom, 4)

    # fp==0 이면 환산값이 무조건 100%로 나온다. 그건 "오탐이 없다"가 아니라 **이 표본에서
    # 오탐을 못 봤다**는 뜻이고, 환산식은 FPR 0 근처에서 극도로 민감하다. 표본이 30건이든
    # 3,000건이든 똑같이 100%로 찍히면 읽는 사람이 구분할 방법이 없다.
    # 그래서 rule of three(0 관측 시 95% 상한 ≈ 3/n)로 FPR 상한을 잡아 **precision 하한**을
    # 같이 낸다. 100% 를 지우지 않고 "표본이 이만큼일 때 최소 이 값"을 옆에 붙이는 방식이다.
    neg_precision_operational_lower = None
    if (
        neg_precision_operational is not None
        and neg_fp == 0
        and (neg_fp + neg_tn) > 0
        and raw_recall > 0
    ):
        fpr_upper = 3 / (neg_fp + neg_tn)
        neg_precision_operational_lower = round(
            (p_operational * raw_recall)
            / (p_operational * raw_recall + (1 - p_operational) * fpr_upper),
            4,
        )

    neg_asp_precision, _, _ = _prf1(neg_aspect_tp, neg_aspect_fp, neg_aspect_fn)
    n_true_negative = neg_tp + neg_fn  # 골든상 부정인 문항 수(표본 크기 확인용)
    n_true_nonnegative = neg_fp + neg_tn  # 골든상 비부정인 문항 수(FPR 측정 가능 여부 확인용)

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
            "note": "탐지 분자를 결정하는 값 — 문의 단위 부정/비부정 2분류. "
                    "못 재는 값은 0/100이 아니라 null이다 — precision은 비부정 표본(fp+tn)이 "
                    "0건일 때, recall은 부정 표본(tp+fn)이 0건일 때, f1은 둘 중 하나라도 0건일 때. "
                    "precision_operational도 recall에 기대므로 같이 null이 된다",
            "precision": neg_precision_reported, "recall": neg_recall_reported, "f1": neg_f1_reported,
            "fpr": neg_fpr,
            "precision_operational": neg_precision_operational,
            # 어느 p로 환산했는지 JSON에 남긴다 — 골든이 바뀌면 p도 바뀌므로, 이게 없으면
            # 옛 결과 JSON과 새 결과 JSON을 나란히 놓고 비교할 수 없다.
            "precision_operational_p": round(p_operational, 4) if p_operational is not None else None,
            "precision_operational_lower": neg_precision_operational_lower,
            "precision_operational_note": (
                "⚠️ 환산값입니다 — 직접 측정한 게 아니라, 균형표본(50:50)의 recall·FPR을 "
                f"베이즈 정리로 실제 운영 부정비율(p={p_operational})에 맞춰 재계산한 것. "
                "p는 골든 전량에서 실측한 값이라 골든이 바뀌면 같이 움직인다. "
                "위 'precision'(표본기준)과 절대 혼동하지 말 것 — FPR이 0%에 가까울 땐 둘이 "
                "비슷해 보이지만, FPR이 조금만 올라도 크게 벌어짐(예: FPR5%→표본95% vs 운영60%). "
                "fp==0이면 환산값이 무조건 100%로 나오는데 그건 '오탐이 없다'가 아니라 "
                "'이 표본에서 못 봤다'는 뜻이라, precision_operational_lower(rule of three, "
                "FPR 95% 상한 3/n 기준 하한)를 같이 본다. null이면 fp>0이라 하한이 불필요한 것."
            ),
            "tp": neg_tp, "fp": neg_fp, "fn": neg_fn, "tn": neg_tn,
            "n_true_negative": n_true_negative, "n_true_nonnegative": n_true_nonnegative,
        },
        "negative_scoped_aspect": {
            "note": (
                "골든이 부정인 문항만 대상 — aspect까지 맞아야 올바른 분자에 잡힘. "
                "PR 리뷰 반영: 단일 aspect 출력에서는 fp==fn이 구조적으로 성립해 "
                "precision=recall=F1이 항상 같은 값이라 accuracy 하나로 통합함 "
                "(multi_output_diagnostic.rate가 0%보다 커지면 이 등식이 깨지므로 그때 재분리)"
            ),
            "accuracy": neg_asp_precision,  # == recall == f1 (위 note 참고)
            "tp": neg_aspect_tp, "fp": neg_aspect_fp, "fn": neg_aspect_fn,
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
        "leak_filter": _compute_leak_filter(scored, predictions, leak_threshold),  # 🆕
    }


def _compute_leak_filter(
    scored: list[dict],
    predictions: dict[str, list[dict]],
    leak_threshold: float | None = None,
) -> dict:
    """§6 B안(2026-08-06, 지인님 리뷰) — few-shot 유출 제외 전/후 병기.

    영향받는 지표는 aspect_f1·sentiment_accuracy·exact_match_rate뿐이다(대시보드·
    리포트 소비 영역, §6 본문 명시). negative_detection/negative_scoped_aspect(탐지
    소비, 부정만)는 그대로 둔다 — 오탐 자체는 실제로 일어난 사건이라 부정판별 지표에서
    빼면 오히려 왜곡된다.

    "전" 값은 score()의 최상위 aspect_f1 등(이미 계산됨, scored 전체 기준)을 그대로
    쓰면 되고, 여기서는 "후"(clean_rows만) 값만 _basic_metrics()로 추가 계산한다.
    LLM 재호출 없음 — 이미 있는 predictions 재사용(§6 요구사항: "검증에 LLM 재실행 불필요").

    rows에 is_leaked 태그가 없으면(tag_leaked_rows() 미호출) 유출 없음으로 간주 —
    이 필드를 몰라도 기존 호출부는 그대로 동작한다(하위호환).
    """
    tagged = [r for r in scored if "is_leaked" in r]
    if not tagged:
        return {"note": "유출 태깅 안 됨(tag_leaked_rows 미호출) — leak_filter 정보 없음", "applied": False}

    leaked_rows = [r for r in scored if r.get("is_leaked")]
    clean_rows = [r for r in scored if not r.get("is_leaked")]

    if not leaked_rows:
        return {
            "note": "이번 표본에 유출 few-shot과 겹치는 문항이 0건",
            "applied": True, "n_excluded_rows_in_sample": 0,
        }

    after = _basic_metrics(clean_rows, predictions)

    return {
        "note": "제외 후(leak_excluded_*)는 clean_rows만으로 재집계한 값 — LLM 재호출 없음",
        "applied": True,
        "threshold_used": leak_threshold,
        "n_excluded_unique_templates": len({r["raw_text"] for r in leaked_rows}),
        "n_excluded_rows_in_sample": len(leaked_rows),
        "excluded_pct_of_sample": round(len(leaked_rows) / len(scored), 4) if scored else 0.0,
        "excluded_examples": sorted(
            {(r["raw_text"], r["leak_similarity"]) for r in leaked_rows}, key=lambda x: -x[1]
        )[:10],
        "leak_excluded_aspect_f1": after["aspect_f1"],
        "leak_excluded_sentiment_accuracy": after["sentiment_accuracy"],
        "leak_excluded_exact_match_rate": after["exact_match_rate"],
        "leak_excluded_n_scored": after["n_scored"],
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
    p_str = f"{nd['precision']:.1%}" if nd["precision"] is not None else "측정불가(비부정 표본 0건)"
    f1_str = f"{nd['f1']:.1%}" if nd["f1"] is not None else "측정불가"
    fpr_str = f"{nd['fpr']:.1%}" if nd["fpr"] is not None else "측정불가(비부정 표본 0건)"
    p_op_str = f"{nd['precision_operational']:.1%}" if nd["precision_operational"] is not None else "측정불가"
    r_str = f"{nd['recall']:.1%}" if nd["recall"] is not None else "측정불가(부정 표본 0건)"
    print(f"\n★★★ [대표지표] ① 부정 판별 정확도(탐지 분자 결정)  P(표본기준)={p_str} R={r_str} F1={f1_str}")
    print(f"    🆕 FPR(오탐률) = {fpr_str}  — eval/README.md가 경고한 '부정 강화하면 FPR 상승' 여부를 실제로 보는 값")
    p_op = nd.get("precision_operational_p")
    p_label = f"p={p_op:.1%}" if p_op is not None else "p 미지정"
    lower = nd.get("precision_operational_lower")
    # fp==0 이면 점추정이 무조건 100% 라, 하한을 같이 안 찍으면 표본 크기가 안 보인다.
    lower_str = f"  (fp=0 — 표본 n={nd['n_true_nonnegative']} 기준 하한 {lower:.1%})" if lower is not None else ""
    print(f"    🆕 P(운영환산, {p_label}) = {p_op_str}{lower_str}  — ⚠️ 위 P(표본기준)와 다른 값, 실제 트래픽에 붙였을 때 기대되는 precision")
    print(f"    tp={nd['tp']} fp={nd['fp']} fn={nd['fn']} tn={nd['tn']}  (골든 부정 n={nd['n_true_negative']}, 비부정 n={nd['n_true_nonnegative']})")
    print(f"★★★ [대표지표] ② 부정 한정 aspect 정확도(탐지 분자 위치 결정)  accuracy={na['accuracy']:.1%}  (tp={na['tp']} fp={na['fp']} fn={na['fn']})")
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
    # 표본을 뽑기 **전에** 센다 — 표본은 층화라 부정비율이 왜곡돼 있다.
    p_operational = operational_negative_rate(rows)
    sampled = sample_rows(rows, args.limit, args.seed, only_negative=args.only_negative)

    # 🆕 §6 B안 — few-shot 유출 태깅(2026-08-06, 지인님 리뷰)
    # ⚠️ leak_threshold<=0이면 검사 자체를 끈다(few_shot_texts를 안 채워서 compute_leak_map이
    # 빈 결과 반환 → 전부 is_leaked=False). threshold=0.0을 compute_leak_map에 그대로 넘기면
    # SequenceMatcher.ratio()>=0.0이 사실상 항상 참이라 오히려 전부 유출로 잡히므로 여기서 분기.
    few_shot_texts = parse_few_shot_examples(classification_service.PROMPT_ASPECT_VERSION) if args.leak_threshold > 0 else []
    leak_map = compute_leak_map(sampled, few_shot_texts, args.leak_threshold)
    tag_leaked_rows(sampled, leak_map)
    n_leaked_in_sample = sum(1 for r in sampled if r["is_leaked"])

    print(f"골든: {golden_path}")
    print(f"전체 {len(rows)}건 → 표본 {len(sampled)}건")
    if p_operational is not None:
        print(f"  운영 부정비율 p = {p_operational:.1%} (골든 전량 실측 — 환산 precision 에 쓰임)")
    print(f"  aspect 구성: {dict(Counter(r['true_aspect'] for r in sampled))}")
    print(f"  프롬프트 버전: {classification_service.PROMPT_ASPECT_VERSION}")
    if few_shot_texts:
        print(f"  few-shot 유출 검사: {len(few_shot_texts)}개 예시 대비, 표본 중 {n_leaked_in_sample}건 유출(임계 {args.leak_threshold})")

    cache_file = cache_path_for(
        sampled, classification_service.PROMPT_ASPECT_VERSION, args.mode, args.limit
    )
    if args.dry_run:
        n_chunks = -(-len(sampled) // args.chunk_size)
        print(f"\n[dry-run] LLM 호출 안 함. 실제 실행 시 청크 약 {n_chunks}회(청크당 {args.chunk_size}건).")
        print(f"  모드: {args.mode} ({'item당 개별호출×동시실행' if args.mode == 'per_item' else '청크당 호출 1회(진짜 배치)'})")
        n_cached = len(json.loads(cache_file.read_text(encoding="utf-8"))) if cache_file.exists() else 0
        print(f"  캐시: {cache_file.name} — {'적중 ' + format(n_cached, ',') + '건' if n_cached else '없음(전량 신규)'}")
        return

    if args.no_cache:
        # 캐시를 아예 안 타는 경로. 측정을 처음부터 다시 하고 싶을 때만 쓴다 —
        # 중단되면 그때까지의 비용이 전부 날아간다.
        print("  ⚠️ --no-cache: 중단 시 이번 실행 비용이 전부 날아갑니다")
        runner = run_batch_chunks if args.mode == "batch" else run_chunks
        predictions, failed_ids = await runner(sampled, args.chunk_size, args.concurrency)
    else:
        predictions, failed_ids = await run_with_cache(
            sampled, args.chunk_size, args.concurrency, args.mode, cache_file
        )
    if failed_ids:
        print(f"\n⚠️ 청크 실패로 무응답 처리된 건: {len(failed_ids)}건")

    import hashlib

    from app.config import get_settings

    prompt_path = ROOT / "app" / "classification" / "prompts" / f"{classification_service.PROMPT_ASPECT_VERSION}.md"
    prompt_hash = hashlib.md5(prompt_path.read_bytes()).hexdigest()[:12] if prompt_path.exists() else None

    result = {
        "meta": {
            "experiment": "③ 프롬프트1 aspect 분류 정확도",
            "run_at": datetime.now(KST).isoformat(timespec="seconds"),
            "golden": golden_path.name,
            "prompt_version": classification_service.PROMPT_ASPECT_VERSION,
            "prompt_hash": prompt_hash,  # 🆕 PR 리뷰 반영 — 파일명은 같아도 제자리수정으로 내용이
                                          # 달라질 수 있어, 어느 JSON이 어느 실제 프롬프트 내용으로
                                          # 나온 건지 이 해시로 구분(같은 이름=다른 프롬프트 문제 해결)
            "model": get_settings().llm_model,
            "seed": args.seed,
            "limit": args.limit,
            "chunk_size": args.chunk_size,
            "mode": args.mode,  # 🆕 per_item(기존, item당 개별호출) vs batch(청크당 호출 1회)
            "only_negative": args.only_negative,  # 🆕 PR 리뷰 반영 — 이 플래그 없인 8개 JSON이
                                                    # run_at 빼고 구분 불가능했음
            "leak_threshold": args.leak_threshold,  # 🆕 §6 B안 — few-shot 유출 판정 유사도 임계
            "few_shot_examples_checked": len(few_shot_texts),  # 🆕 파싱된 few-shot 개수(0이면 검사 자체가 스킵됨)
            # 🆕 어느 코퍼스에서 잰 값인가 (2026-08-11). ②는 이 장치를 갖고 있는데 ③에는
            # 없어서, 재생성 후 "같은 id 에 다른 텍스트"가 들어가도 조용히 통과했다.
            # 08-11 실행분은 사람이 사후 대조해서 07276bc5 임을 확인했지만 다음엔 못 잡는다.
            #   sample  이 실행이 실제로 채점한 행의 (id, 본문) 해시 — 캐시 키와 같은 값
            #   corpus  실험②가 정본으로 쓰는 케이스 윈도우 지문. 두 실험을 나란히 놓고
            #           같은 코퍼스인지 대조하려면 이게 필요하다(전량이어도 모집단이 다름)
            "data_fingerprint": data_fingerprint(sampled),
            "corpus_fingerprint": _corpus_fingerprint(),
        },
        "scores": score(
            sampled,
            predictions,
            leak_threshold=args.leak_threshold,
            operational_rate=p_operational,
        ),
    }
    report(result)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    out = RESULTS_DIR / f"classify_eval_{stamp}.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n결과 저장: eval/results/{out.name}")


def main() -> None:
    # 🔴 첫 문장이어야 한다 — 아래 `parse_args()` 가 `--help` 를 먼저 찍고, 그 도움말
    #    (`--only-negative` · `--leak-threshold` · `--no-cache` · `--mode`)에 `—`·`⚠️` 가 있다.
    #    `app/core/console.py`.
    force_utf8_output()

    parser = argparse.ArgumentParser(description="실험③ 프롬프트1 aspect 분류 정확도")
    parser.add_argument("--golden", default=str(GOLDEN_LABELS), help="골든 라벨 CSV 경로")
    parser.add_argument("--limit", type=int, default=300, help="표본 수 (0=전량)")
    parser.add_argument("--seed", type=int, default=42, help="표본 추출 시드 (재현용)")
    parser.add_argument(
        "--only-negative", action="store_true",
        help="sentiment=-1인 문항만 표본으로 뽑는다(지인님 A안 — 탐지가 실제로 소비하는 부분만 통계적으로 의미 있게 검증)",
    )
    parser.add_argument(
        "--leak-threshold", type=float, default=0.75,
        help="few-shot과 코퍼스 문장의 유사도가 이 값 이상이면 유출로 판정(§6 B안, 2026-08-06)."
             " difflib.SequenceMatcher 기준 — 예시20(1.00완전일치)·25(0.88)·20-2(0.79) 3건을"
             " 전부 잡으려면 0.75 이하 필요(예시20-2가 기준선). 0으로 주면 검사 자체를 끔.",
    )
    parser.add_argument(
        "--no-cache", action="store_true",
        help="분류 캐시를 안 쓴다(처음부터 재측정). ⚠️ 중단되면 그때까지의 LLM 비용이"
             " 전부 날아간다 — 전량 실행에서는 쓰지 말 것",
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
