"""llm_generated_700 sentiment 재라벨링 — 2026-08-09 확정 정책 반영.

배경
----
`app/현진/현진님_전달_검증결과_20260806.md` 후속 조사에서 이 평가셋의 sentiment 라벨이
두 갈래로 망가져 있음이 확인됐다(mismatch 229건 분석 기준).

  ① 미탐 축 — 가정형·예방형·불안 표현 문의에 -1이 붙음 (FN 60건 중 36건)
     예) GEN-0071 "상품이 파손되면 어떻게 하나요?" → gold -1
     같은 뜻의 GEN-0148 "상품 파손되면 어떻게 해?" 는 gold 0. 같은 패턴이 -1/0/1로 3분할.
  ② 오탐 축 — 실제로 문제가 발생한 문장에 0/1이 붙음 (FP 112건 거의 전부)
     예) GEN-0124 "상품이 파손돼서 왔어요!" → gold 0
         GEN-0158 "…전혀 다른 제품이 배송되어서 매우 당황스럽습니다" → gold 1

원인은 `scripts/generate_hybrid_700.py`가 감성을 **먼저 뽑아 생성 지시로 쓰고, 생성 결과를
검수하지 않은** 것. 즉 라벨이 "관측"이 아니라 "지시"였다. 생성 쪽은 같은 날 수정됐고
(감성 조작적 정의 + 검수 게이트 + 페르소나 배제), 이 스크립트는 **이미 생성된 700건의
텍스트는 그대로 두고 라벨만** 정책에 맞게 다시 매긴다.

🔴 순환논리 경고 — 반드시 읽을 것
----------------------------------
재라벨링을 분류기와 같은 모델로 하면, 그 라벨로 그 모델을 채점하는 건 순환논리다.
sentiment accuracy가 자동으로 높게 나오고 그 수치는 아무 의미가 없다.

따라서 이 스크립트는 **라벨을 자동으로 덮어쓰지 않는다.** 기본 동작은
"현재 gold ≠ 정책 판단"인 행만 뽑아 **사람 검토용 큐(CSV)** 를 만드는 것이다.
`--apply`는 그 큐를 사람이 확인한 뒤에만 쓰라고 만든 옵션이다.

이 재라벨 결과를 반영한 뒤에도 `llm_generated_700`의 sentiment 지표는
**최종 성능 지표가 아니라 회귀 감시용**으로만 쓴다. 실성능 기준선은 `relabel_300`이다.

사용법
------
    # 1) 검토 큐만 생성 (LLM 호출 있음, 라벨 안 건드림)
    python scripts/relabel_generated_sentiment.py

    # 2) 비용 0 — 규칙 기반 예상 변경분만 미리 보기
    python scripts/relabel_generated_sentiment.py --dry-run

    # 3) 사람이 큐 검토 후 실제 반영 (백업 자동 생성)
    python scripts/relabel_generated_sentiment.py --apply

전체 재현 절차 (2026-08-09 반영분을 그대로 다시 만들려면)
--------------------------------------------------------
    # ① 재현성 확인된 축만 3회 실행 다수결로 반영
    python scripts/relabel_generated_sentiment.py --apply-from \
        eval/eval_sets/relabel_runs/run1.csv run2.csv run3.csv
    # ② 손검토 확정분(기타·다중 153건)
    python scripts/relabel_generated_sentiment.py \
        --apply-manual eval/eval_sets/relabel_manual_review.csv
    # ③ 정책 전수 스캔분(큐가 구조적으로 못 잡은 12건)
    python scripts/relabel_generated_sentiment.py --apply-sweep
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import shutil
import sys
from collections import Counter
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.core.llm_client import get_llm_client
from scripts.generate_hybrid_700 import SENTIMENT_DEFINITIONS

DEFAULT_INFILE = "eval/eval_sets/llm_generated_700.csv"
DEFAULT_QUEUE = "eval/eval_sets/relabel_queue_generated_sentiment.csv"
DEFAULT_MANUAL = "eval/eval_sets/relabel_manual_review.csv"
# 정책 확정 후 전수 스캔으로 잡은 행 — 손검토 큐가 구조적으로 못 잡는 몫이다.
# 큐는 "3회 중 하나라도 gold와 다른 행"만 담으므로, 3회가 모두 틀린 gold에 동의하면
# 검토 대상에서 아예 빠진다. 그 행들을 정책 확정 뒤 규칙으로 훑어 여기에 기록한다.
DEFAULT_SWEEP = "eval/eval_sets/relabel_policy_sweep.csv"

# 3회 실행(2026-08-09)에서 실행 간 재현성이 확인된 축 — 이것만 자동 반영한다.
#   색상·사이즈·소재 100% / 오배송 98.7% / 파손 96.7%  ←  자동
#   기타 80.7% / 다중aspect 82.0%                    ←  손검토(build_manual_queue)
STABLE_ASPECTS_DEFAULT = "파손,오배송,색상,사이즈,소재"


# ── 정책 프롬프트 (배치) ──────────────────────────────────────────────────
# few-shot 예시는 인용하지 않는다(생성 스크립트와 같은 원칙). 정의 + 판단 순서만.
#
# 배치인 이유: 1행 1콜로 하면 감성 정의 블록(~500토큰)이 700번 반복 전송된다.
# 청크 20건이면 정의 블록은 35번만 나가고 나머지는 본문 토큰뿐 — 대략 5배 절약.
# eval/run_classify_eval.py의 run_batch_chunks와 같은 발상이되, 여기선 aspect가
# 이미 정해져 있고(예측 대상 아님) few-shot을 안 쓰므로 프롬프트를 따로 만든다.
def build_batch_relabel_prompt(chunk: list[dict]) -> str:
    guide = "\n".join(f"- {s}: {d}" for s, d in sorted(SENTIMENT_DEFINITIONS.items()))
    items = "\n".join(
        f'{i}. item_id={r["id"]} | 대상 속성=[{r["aspect"]}] | 문의="{r["raw_text"]}"'
        for i, r in enumerate(chunk, 1)
    )
    return f"""아래는 온라인 쇼핑몰 CS 문의 {len(chunk)}건입니다.
각 문의마다, 지정된 **대상 속성 각각에** 대해 감성을 매기세요.

🔴 감성 정의 — 반드시 이 정의대로만 판단하세요:
{guide}

판단 순서(위에서부터 차례로. 앞 단계에서 걸리면 **거기서 확정**하고 뒤는 보지 마세요):

1. 🔴 **포장·박스 상태가 나빴다는 서술이 있는가?**
   ("포장이 찢어져 있었다 / 뜯어진 상태 / 허술하다 / 헐겁다 / 엉망이다 / 손상되어 있었다",
    "박스가 찌그러졌다 / 눌렸다", "완충재가 없었다" 등)
   → 있으면 **-1로 확정**. 아래 조건에 해당해도 뒤집지 마세요:
      · 내용물은 멀쩡하다고 했어도 -1
      · 뒤에 "파손되면 어떻게 하나요?" 같은 가정형 질문이 붙어 있어도 -1
      · "다행히 괜찮았다"로 끝나도 -1
   이유: 포장 불량은 그 자체가 셀러가 조치할 결함(포장재·물류사)이라 탐지 대상입니다.
   ⚠️ 이 단계에서 가장 많이 틀립니다. "아직 파손은 안 일어났으니 중립"으로 새지 마세요.

2. 화자가 **이미 겪은 다른 문제**가 있는가?
   (파손된 상품을 받음, 다른 상품이 옴, 입어보니 안 맞음 등)
   → 있으면 **-1**. 말투가 담백해도 -1입니다.

2-1. 🔴 **배송 문의는 "안 왔다·늦다고 말했는가" 하나로만 가르세요.** (2026-08-09 손검토 확정)
   → **-1**: 미도착·지연을 **주장**함
      · "아직 안 왔어요", "아직 배송되지 않았습니다", "아직 도착하지 않았는데"
      · "배송이 지연되고 있어요", "너무 늦어요", "며칠째 안 오는데"
      ⚠️ 말투가 정중해도, "확인 부탁드립니다"로 끝나도 **-1**입니다.
      ⚠️ 기간·예정일 같은 **구체적 근거가 없어도 -1**입니다.
         일정을 안내해줘도 "안 왔다"는 근본 문제는 그대로 남기 때문입니다.
   → **0**: 지연 주장 없이 **일정만** 물음
      · "주문한 상품이 언제 배송되나요?", "배송이 언제쯤 될까요?"
      · "배송 상태가 궁금한데요, 언제 도착하나요?"
   갈림길은 **"안 왔다/늦다"는 주장의 유무** 하나뿐입니다.

3. **칭찬·만족이 명시적으로 표현**됐는가? ("마음에 들어요", "만족해요", "기뻐요")
   → 있으면 **1**. "무사히 도착했다" 같은 사실 확인은 칭찬이 아니라 중립(0)입니다.

4. 위에 다 해당 없음 → **0**
   (가정형 "파손되면 어떻게 하나요?", 예방형 "안전한가요?",
    아직 안 벌어진 일에 대한 "걱정된다", 전언 "작다고 하던데", 문제 없음 확인)

⚠️ 문의끼리 서로 영향을 주면 안 됩니다. 각 문의는 **독립적으로** 판단하세요.
⚠️ {len(chunk)}건 **전부** 출력하세요. 하나도 빠뜨리면 안 됩니다.
⚠️ 각 문의의 sentiments 배열 길이는 그 문의의 '대상 속성' 개수·순서와 정확히 같아야 합니다.

문의 목록:
{items}

출력 형식(JSON만, 다른 텍스트 없이):
{{"results": [
  {{"item_id": "GEN-0001", "sentiments": [{{"aspect": "속성명", "sentiment": -1, "reason": "근거 한 줄"}}]}}
]}}
"""


# ── 규칙 기반 사전 스캔 (dry-run 전용, 비용 0) ────────────────────────────
# LLM 없이 "바뀔 가능성이 큰 행"을 대략 세어보는 용도. 판정 자체는 LLM이 한다.
OCCURRED_HINTS = (
    "왔어요", "왔습니다", "배송됐", "배달됐", "도착했는데", "받았는데", "깨져", "깨진",
    "찢어", "흠집", "긁힘", "얼룩", "찌그러", "허술", "엉망", "지연", "늦어",
    "아직도", "안 왔", "너무 작아", "너무 커", "실망", "화가",
)
HYPO_HINTS = ("되면", "될 경우", "된 경우", "만약", "될까", "안전한가", "안전할까", "가능성")


def scan_by_rules(rows: list[dict]) -> dict:
    """LLM 없이 대략적인 변경 후보 수를 센다(정확한 판정 아님 — 감만 잡는 용도)."""
    hypo_neg = occurred_nonneg = 0
    for r in rows:
        t = r["raw_text"]
        sents = [s.strip() for s in r["sentiment"].split(",")]
        occurred = any(h in t for h in OCCURRED_HINTS)
        hypo = any(h in t for h in HYPO_HINTS)
        if hypo and not occurred and "-1" in sents:
            hypo_neg += 1
        if occurred and all(s != "-1" for s in sents):
            occurred_nonneg += 1
    return {"가정형인데 -1": hypo_neg, "문제 발생인데 비-1": occurred_nonneg}


def make_backup(infile: Path) -> Path:
    """덮어쓰지 않는 백업 (2026-08-09 추가).

    이전엔 항상 `.csv.bak` 하나에 복사해서, 반영을 두 번 하면(자동 반영 → 손검토 반영)
    두 번째가 **첫 백업(=원본)을 덮어써** 원본이 사라졌다. 비어 있는 번호를 찾아 쓴다.
    """
    candidate = infile.with_suffix(".csv.bak")
    n = 1
    while candidate.exists():
        candidate = infile.with_suffix(f".csv.bak{n}")
        n += 1
    shutil.copy2(infile, candidate)
    print(f"백업 생성 → {candidate}")
    return candidate


def parse_final(value: str, n_aspects: int) -> list[str] | None:
    """손검토 `final` 값 검증. 형식이 어긋나면 None(=건너뜀)."""
    parts = [p.strip() for p in value.split(",") if p.strip() != ""]
    if len(parts) != n_aspects or any(p not in ("-1", "0", "1") for p in parts):
        return None
    return parts


def _row_result(row: dict, new: list[str], reasons: str) -> dict:
    old = [s.strip() for s in row["sentiment"].split(",")]
    return {
        "id": row["id"],
        "aspect": row["aspect"],
        "old_sentiment": ",".join(old),
        "new_sentiment": ",".join(new),
        "changed": "Y" if old != new else "",
        "reason": reasons,
        "raw_text": row["raw_text"],
    }


async def relabel_chunks(
    client, rows: list[dict], chunk_size: int, concurrency: int, progress: dict
) -> tuple[dict[str, dict], list[str]]:
    """청크 단위 재라벨. (id→결과, 무응답 id 목록) 반환."""
    chunks = [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]
    sem = asyncio.Semaphore(concurrency)
    out: dict[str, dict] = {}
    missing: list[str] = []

    async def one(index: int, chunk: list[dict]) -> None:
        async with sem:
            try:
                data = await client.complete_json(
                    build_batch_relabel_prompt(chunk),
                    trace_key=f"relabel_chunk={index}_n={len(chunk)}",
                    temperature=0.0,
                )
            except Exception as e:  # noqa: BLE001 — 청크 실패는 무응답으로 넘겨 재시도한다
                print(f"  ⚠️ 청크 {index + 1} 실패({len(chunk)}건 무응답): {e}", flush=True)
                missing.extend(r["id"] for r in chunk)
                return

        row_by_id = {r["id"]: r for r in chunk}
        answered = set()
        for res in data.get("results", []):
            iid = res.get("item_id")
            row = row_by_id.get(iid)
            if row is None:
                continue  # 이 청크에 없는 id — 환각. 무시하고 아래에서 무응답 처리
            aspects = [a.strip() for a in row["aspect"].split(",")]
            got = {
                d["aspect"]: (int(d["sentiment"]), d.get("reason", ""))
                for d in res.get("sentiments", [])
                if isinstance(d, dict) and d.get("sentiment") in (-1, 0, 1)
            }
            if not all(a in got for a in aspects):
                continue  # aspect 누락 — 무응답으로 넘겨 재시도
            out[iid] = _row_result(
                row, [str(got[a][0]) for a in aspects], " | ".join(got[a][1] for a in aspects)
            )
            answered.add(iid)

        missing.extend(iid for iid in row_by_id if iid not in answered)
        progress["done"] += len(chunk)
        print(f"  진행 {progress['done']}/{progress['total']}건 "
              f"(청크 {index + 1}/{len(chunks)})", flush=True)

    await asyncio.gather(*(one(i, c) for i, c in enumerate(chunks)))
    return out, missing


async def run(
    infile: Path,
    queue_file: Path,
    chunk_size: int,
    concurrency: int,
    retry_chunk_size: int,
    apply: bool,
    only_aspects: set[str] | None = None,
) -> None:
    with infile.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    n_chunks = (len(rows) + chunk_size - 1) // chunk_size
    print(f"입력 {len(rows)}건 — {infile}")
    print(f"배치 재라벨: 청크 {chunk_size}건 × {n_chunks}콜, 동시 {concurrency}", flush=True)

    client = get_llm_client()
    progress = {"done": 0, "total": len(rows)}
    out, missing = await relabel_chunks(client, rows, chunk_size, concurrency, progress)

    # 배치 응답이 JSON은 멀쩡한데 item_id를 통째로 빠뜨리는 일이 있다(2026-08-06 검증에서
    # relabel_300 15건이 그랬음). 무응답을 최종 실패로 두면 결과가 응답 포맷 운에 좌우되므로
    # 작은 청크 → 1건씩으로 재시도한다.
    row_by_id = {r["id"]: r for r in rows}
    remaining = list(dict.fromkeys(missing))
    for attempt, size in enumerate((retry_chunk_size, 1), start=1):
        retry_rows = [row_by_id[i] for i in remaining if i in row_by_id]
        if not retry_rows:
            break
        print(f"  retry {attempt}: 무응답 {len(retry_rows)}건, chunk_size={size}", flush=True)
        retry_out, _ = await relabel_chunks(
            client, retry_rows, size, 1, {"done": 0, "total": len(retry_rows)}
        )
        out.update(retry_out)
        remaining = [i for i in remaining if i not in retry_out]

    ok = list(out.values())
    failed = len(rows) - len(ok)
    if failed:
        print(f"  ⚠️ 재시도 후에도 무응답 {failed}건: {remaining[:10]}")
    changed = [r for r in ok if r["changed"]]

    print(f"재라벨 완료 {len(ok)}건 (실패 {failed}건)")
    print(f"현재 gold와 다른 판정: {len(changed)}건 ({len(changed) / max(len(ok), 1):.1%})")

    # 변경 방향 요약
    moves: dict[str, int] = {}
    for r in changed:
        key = f"{r['old_sentiment']} → {r['new_sentiment']}"
        moves[key] = moves.get(key, 0) + 1
    print("\n주요 변경 방향(상위 10):")
    for k, v in sorted(moves.items(), key=lambda x: -x[1])[:10]:
        print(f"  {k}: {v}건")

    # 검토 큐 저장 — 바뀐 것만
    queue_file.parent.mkdir(parents=True, exist_ok=True)
    fields = ["id", "aspect", "old_sentiment", "new_sentiment", "changed", "reason", "raw_text"]
    with queue_file.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(changed)
    print(f"\n검토 큐 저장 → {queue_file} ({len(changed)}건)")

    if not apply:
        print("\n라벨은 건드리지 않았습니다. 큐를 사람이 검토한 뒤 --apply 로 반영하세요.")
        print("⚠️ 같은 모델이 매긴 라벨로 같은 모델을 채점하면 순환논리입니다 — 모듈 docstring 참고.")
        return

    make_backup(infile)

    new_by_id = {r["id"]: r["new_sentiment"] for r in ok}  # 무응답 행은 원본 라벨 유지
    applied = skipped = 0
    for row in rows:
        if row["id"] not in new_by_id or new_by_id[row["id"]] == row["sentiment"]:
            continue
        # --only-aspects: 재현성이 확인된 축만 자동 반영한다(2026-08-09 3회 실행 검증).
        # 기타(80.7%)·다중aspect(82.0%)는 실행마다 판정이 뒤집혀 손검토로 돌린다.
        if only_aspects is not None and row["aspect"] not in only_aspects:
            skipped += 1
            continue
        row["sentiment"] = new_by_id[row["id"]]
        applied += 1
    with infile.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "aspect", "sentiment", "raw_text", "source"])
        w.writeheader()
        w.writerows({k: row[k] for k in w.fieldnames} for row in rows)
    print(f"반영 완료 → {infile} ({applied}건 변경)")
    if skipped:
        print(f"  --only-aspects 로 건너뜀: {skipped}건 (손검토 대상 — --build-manual-queue 참고)")


def build_manual_queue(
    infile: Path,
    run_files: list[Path],
    out_file: Path,
    only_aspects: set[str] | None,
    accept_agree: bool = False,
) -> None:
    """여러 번 실행한 큐를 합쳐 **손검토 대상**을 뽑는다 (2026-08-09 추가).

    `--only-aspects`에서 제외된 축(기타·다중aspect)은 실행마다 판정이 뒤집혀서
    자동 반영을 못 한다. 대신 N회 실행 결과를 모아 이렇게 가른다:

      · 전 실행 만장일치 + gold와 같음  → 손댈 것 없음(출력 안 함)
      · 전 실행 만장일치 + gold와 다름  → `agree` = 판단이 안정적이니 그대로 받아도 됨
      · 실행 간 판정이 갈림             → `split` = 사람이 직접 읽고 정해야 함

    3회 다수결을 `majority`에 같이 실어서, 검토자가 처음부터 다시 읽지 않아도 되게 한다.
    """
    with infile.open(encoding="utf-8-sig", newline="") as f:
        rows = {r["id"]: r for r in csv.DictReader(f)}

    runs: list[dict[str, str]] = []
    for p in run_files:
        with p.open(encoding="utf-8-sig", newline="") as f:
            runs.append({r["id"]: r["new_sentiment"] for r in csv.DictReader(f)})
    print(f"실행 {len(runs)}개 병합: {', '.join(p.name for p in run_files)}")

    out: list[dict] = []
    for iid, row in rows.items():
        if only_aspects is not None and row["aspect"] in only_aspects:
            continue  # 자동 반영된 축은 손검토 대상이 아니다
        # 큐에 없는 실행 = 그 실행에선 gold 그대로라는 뜻
        labels = [r.get(iid, row["sentiment"]) for r in runs]
        counts = Counter(labels)
        majority, n_top = counts.most_common(1)[0]
        if len(counts) == 1 and majority == row["sentiment"]:
            continue  # 전원 일치 + gold와 같음 → 볼 필요 없음
        verdict = "agree" if len(counts) == 1 else "split"
        prefilled = accept_agree and verdict == "agree"
        out.append({
            "id": iid,
            "aspect": row["aspect"],
            "gold": row["sentiment"],
            "majority": majority,
            # 사람이 채우는 칸. 비워두면 그 행은 반영 안 됨(= gold 유지).
            "final": majority if prefilled else "",
            # 🔴 이 칸이 순환논리 가드다. --accept-agree 로 미리 채운 행은 "모델이 정한 값"이지
            # 사람이 검토한 값이 아니다. 둘을 파일에서 구분 못 하면 "모델 출력으로 모델
            # 평가셋을 만드는" 순환이 그대로 뚫린다(2026-08-09 PR 리뷰 지적).
            # 검토자는 확인한 행의 이 값을 'human' 으로 바꿔야 한다.
            "decided_by": "model_majority" if prefilled else "",
            "verdict": verdict,
            "votes": " / ".join(labels),
            "confidence": f"{n_top}/{len(labels)}",
            "raw_text": row["raw_text"],
        })

    out.sort(key=lambda r: (r["verdict"] != "split", r["aspect"], r["id"]))
    fields = ["id", "aspect", "gold", "majority", "final", "decided_by", "verdict",
              "votes", "confidence", "raw_text"]
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(out)

    n_split = sum(1 for r in out if r["verdict"] == "split")
    print(f"\n손검토 큐 저장 → {out_file}")
    print(f"  총 {len(out)}건 — split(사람이 판단) {n_split}건 / agree(만장일치, 받아도 됨) {len(out) - n_split}건")
    print(f"  aspect별: {dict(Counter(r['aspect'] for r in out).most_common(8))}")
    n_filled = sum(1 for r in out if r["final"])
    print("\n  'final' 칸에 최종 라벨을 적으세요. 비워두면 그 행은 반영 안 됩니다(gold 유지).")
    if accept_agree:
        print(f"  --accept-agree 로 agree {n_filled}건은 majority 로 미리 채웠고 "
              "decided_by='model_majority' 로 표시했습니다.")
        print("  ⚠️ 확인하신 행은 decided_by 를 'human' 으로 바꿔주세요 — 안 바꾸면 "
              "'모델이 정한 라벨'로 기록에 남습니다(순환논리 가드).")
    try:  # 저장소 기준 상대경로로 안내 — 베어 파일명은 ROOT 기준으로 해석돼 죽는다
        shown = out_file.relative_to(ROOT)
    except ValueError:
        shown = out_file
    print(f"  반영: python scripts/relabel_generated_sentiment.py --apply-manual {shown}")


def check_text_drift(rows: list[dict], recorded: dict[str, str], label: str) -> list[str]:
    """저장된 라벨이 **다른 문장**에 붙는 걸 막는다 (2026-08-09 PR 리뷰 지적).

    `GEN-####` 는 생성 시점의 **생존 행 순번**이라 내용 기반이 아니다. 검수 게이트가
    실패 행을 버리므로 재생성하면 같은 ID에 다른 문장이 들어간다. 그 상태로 예전
    라벨 파일을 반영하면 **엉뚱한 문장에 라벨이 붙는다.**

    provenance 파일들이 raw_text 를 같이 들고 있으므로, 반영 전에 현재 CSV와 대조해
    어긋난 ID를 돌려준다. 호출부는 이걸 발견하면 **반영을 중단**해야 한다.
    """
    now = {r["id"]: r["raw_text"] for r in rows}
    drift = [iid for iid, txt in recorded.items() if iid in now and now[iid] != txt]
    if drift:
        print(f"\n🔴 {label}: 저장된 문장과 현재 CSV의 문장이 다른 행 {len(drift)}건")
        for iid in drift[:5]:
            print(f"    {iid}")
            print(f"      기록: {recorded[iid][:60]}")
            print(f"      현재: {now[iid][:60]}")
        print("  → ID가 재부여됐을 가능성이 큽니다(생성 순번 기반). 반영을 중단합니다.")
    return drift


def apply_from_runs(
    infile: Path, run_files: list[Path], only_aspects: set[str] | None
) -> None:
    """이미 끝난 실행들의 **다수결**을 CSV에 반영한다 (LLM 호출 없음, 2026-08-09 추가).

    `--apply`는 재라벨을 처음부터 다시 돌리므로 매번 다른 결과가 나온다(실행 간 판정이
    갈리는 축이 있기 때문). 이미 N회 돌려놓고 그 결과를 검토했다면, **검토한 그 결과**를
    그대로 반영해야 재현 가능하다. 그래서 오프라인 경로를 따로 둔다.

    `only_aspects`에 없는 축(기타·다중aspect)은 건드리지 않는다 — 손검토 대상이다.
    """
    with infile.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    runs: list[dict[str, str]] = []
    recorded_text: dict[str, str] = {}
    for p in run_files:
        with p.open(encoding="utf-8-sig", newline="") as f:
            rs = list(csv.DictReader(f))
        runs.append({r["id"]: r["new_sentiment"] for r in rs})
        recorded_text.update({r["id"]: r["raw_text"] for r in rs if r.get("raw_text")})
    print(f"실행 {len(runs)}개 다수결 반영: {', '.join(p.name for p in run_files)}")

    if check_text_drift(rows, recorded_text, "run 파일"):
        return

    make_backup(infile)

    changed = skipped_aspect = tied = 0
    for row in rows:
        if only_aspects is not None and row["aspect"] not in only_aspects:
            skipped_aspect += 1
            continue
        labels = [r.get(row["id"], row["sentiment"]) for r in runs]
        counts = Counter(labels).most_common()
        # 동률이면 손대지 않는다 — 판단이 안 선 행을 임의로 확정하면 안 된다
        if len(counts) > 1 and counts[0][1] == counts[1][1]:
            tied += 1
            continue
        if counts[0][0] != row["sentiment"]:
            row["sentiment"] = counts[0][0]
            changed += 1

    with infile.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "aspect", "sentiment", "raw_text", "source"])
        w.writeheader()
        w.writerows({k: r[k] for k in w.fieldnames} for r in rows)

    print(f"\n반영 완료 → {infile}")
    print(f"  라벨 변경 {changed}건")
    print(f"  손검토로 넘김(--only-aspects 제외 축) {skipped_aspect}건")
    print(f"  다수결 동률이라 보류 {tied}건")


def apply_sweep(infile: Path, sweep_file: Path) -> None:
    """정책 전수 스캔 결과를 반영한다 (LLM 호출 없음, 2026-08-09 PR 리뷰 반영).

    `relabel_policy_sweep.csv` 는 `id, old_sentiment, new_sentiment, decided_by, reason,
    raw_text` 를 들고 있어, **어떤 행이 왜 바뀌었는지 재현 가능**하다. 이 경로가 없으면
    전수 스캔분이 provenance 없이 CSV에만 반영돼 "왜 이 라벨인가"에 답할 수 없다.
    """
    with infile.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    with sweep_file.open(encoding="utf-8-sig", newline="") as f:
        sweep = list(csv.DictReader(f))

    if check_text_drift(rows, {r["id"]: r["raw_text"] for r in sweep if r.get("raw_text")},
                        sweep_file.name):
        return

    by_id = {r["id"]: r for r in rows}
    changed = skipped = 0
    for s_row in sweep:
        row = by_id.get(s_row["id"])
        if row is None:
            print(f"  ⚠️ {s_row['id']}: CSV에 없는 id")
            continue
        parsed = parse_final(s_row["new_sentiment"], len(row["aspect"].split(",")))
        if parsed is None:
            print(f"  ⚠️ {s_row['id']}: new_sentiment 형식 오류 '{s_row['new_sentiment']}'")
            continue
        new = ",".join(parsed)
        if row["sentiment"] == new:
            skipped += 1
        else:
            row["sentiment"] = new
            changed += 1

    print(f"전수 스캔 {len(sweep)}행 — 반영 {changed}건 / 이미 반영됨 {skipped}건")
    if not changed:
        print("반영할 변경이 없어 파일을 쓰지 않았습니다.")
        return
    make_backup(infile)
    with infile.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "aspect", "sentiment", "raw_text", "source"])
        w.writeheader()
        w.writerows({k: r[k] for k in w.fieldnames} for r in rows)
    print(f"반영 완료 → {infile} ({changed}건 변경)")


def apply_manual(infile: Path, review_file: Path) -> None:
    """손검토 CSV의 `final` 칸을 읽어 반영한다 (LLM 호출 없음, 2026-08-09 추가).

    검토자는 `final` 에 최종 라벨만 적으면 된다. 다중 aspect 행은 aspect 개수·순서에
    맞춰 쉼표로 (예: aspect 가 `기타,파손` 이면 `-1,0`).

      · 비어 있으면      → 그 행은 건드리지 않는다(gold 유지). "아직 안 봤음"과 같다.
      · 형식이 어긋나면  → 반영하지 않고 경고를 찍는다. 조용히 넘기면 오라벨이 남는다.
    """
    with infile.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    row_by_id = {r["id"]: r for r in rows}

    with review_file.open(encoding="utf-8-sig", newline="") as f:
        reviewed = list(csv.DictReader(f))
    if "final" not in (reviewed[0].keys() if reviewed else {}):
        print(f"⚠️ {review_file.name} 에 'final' 컬럼이 없습니다. --build-manual-queue 로 다시 만드세요.")
        return

    recorded_text = {r["id"]: r["raw_text"] for r in reviewed if r.get("raw_text")}
    if check_text_drift(rows, recorded_text, review_file.name):
        return

    changed = blank = invalid = unchanged = 0
    problems: list[str] = []
    for rev in reviewed:
        row = row_by_id.get(rev["id"])
        if row is None:
            problems.append(f"{rev['id']}: llm_generated_700.csv 에 없는 id")
            invalid += 1
            continue
        raw = (rev.get("final") or "").strip()
        if not raw:
            blank += 1
            continue
        parsed = parse_final(raw, len(row["aspect"].split(",")))
        if parsed is None:
            problems.append(
                f"{rev['id']}: final='{raw}' 가 aspect '{row['aspect']}' 와 안 맞음"
                f" (-1/0/1 을 {len(row['aspect'].split(','))}개, 쉼표 구분)"
            )
            invalid += 1
            continue
        new = ",".join(parsed)
        if new == row["sentiment"]:
            unchanged += 1
        else:
            row["sentiment"] = new
            changed += 1

    print(f"손검토 파일 {len(reviewed)}행 읽음 — {review_file}")
    print(f"  반영 {changed}건 / final 이 gold 와 같아 변화 없음 {unchanged}건")

    # 🔴 순환논리 가드를 반영 시점에도 보이게 한다 (2026-08-09 PR 리뷰 반영).
    # decided_by 가 비어 있으면 "사람이 봤는지 모르는 행"이다. 기록만 해두고 조용히
    # 넘어가면 컬럼을 둔 의미가 없다.
    by = Counter((r.get("decided_by") or "(빈칸)").strip() or "(빈칸)" for r in reviewed)
    print(f"  라벨 출처(decided_by): {dict(by)}")
    if by.get("(빈칸)"):
        print(f"  ⚠️ decided_by 가 비어 있는 행 {by['(빈칸)']}건 — 사람이 확인했는지 알 수 없습니다.")
        print("     확인한 행은 'human', 모델 프리필을 그대로 둔 행은 'model_majority' 로 채워주세요.")
    print(f"  비어 있어 건너뜀 {blank}건 / 형식 오류 {invalid}건")
    for msg in problems[:10]:
        print(f"    ⚠️ {msg}")
    if len(problems) > 10:
        print(f"    ... 외 {len(problems) - 10}건")

    if not changed:
        print("\n반영할 변경이 없어 파일을 쓰지 않았습니다.")
        return

    make_backup(infile)
    with infile.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "aspect", "sentiment", "raw_text", "source"])
        w.writeheader()
        w.writerows({k: r[k] for k in w.fieldnames} for r in rows)
    print(f"반영 완료 → {infile} ({changed}건 변경)")


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--infile", default=DEFAULT_INFILE)
    ap.add_argument("--queue", default=DEFAULT_QUEUE)
    # 기본값은 repo 관례에 맞춘다 — run_classify_eval / run_review_eval / run_cause_eval /
    # run_pipeline_eval / verify_hybrid_eval_sets 전부 청크 20 · 동시 4 · 재시도 청크 5.
    ap.add_argument("--chunk-size", type=int, default=20, help="한 콜에 넣을 문의 수(토큰 절약)")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--retry-chunk-size", type=int, default=5, help="무응답 재시도 시 청크 크기")
    ap.add_argument(
        "--only-aspects",
        default=STABLE_ASPECTS_DEFAULT,
        help="이 aspect만 --apply 로 반영(쉼표 구분). 3회 실행에서 재현성이 확인된 축이 기본값. "
             "'all' 을 주면 전부 반영(권장 안 함 — 기타·다중은 실행마다 판정이 뒤집힘)",
    )
    ap.add_argument(
        "--build-manual-queue",
        nargs="+",
        metavar="RUN_CSV",
        help="여러 번 실행한 큐 CSV를 합쳐 손검토 대상을 뽑는다(LLM 호출 없음). "
             "예: --build-manual-queue eval/eval_sets/relabel_runs/run*.csv",
    )
    ap.add_argument("--manual-out", default=DEFAULT_MANUAL, help="손검토 큐 출력 경로")
    ap.add_argument(
        "--accept-agree",
        action="store_true",
        help="--build-manual-queue 시 verdict=agree 행의 final 을 majority 로 미리 채움(split 만 보면 됨)",
    )
    ap.add_argument(
        "--apply-manual",
        metavar="REVIEW_CSV",
        help="손검토 CSV의 final 칸을 읽어 반영(LLM 호출 없음)",
    )
    ap.add_argument(
        "--apply-sweep",
        nargs="?",
        const=DEFAULT_SWEEP,
        metavar="SWEEP_CSV",
        help=f"정책 전수 스캔 CSV를 반영(LLM 호출 없음). 기본 {DEFAULT_SWEEP}",
    )
    ap.add_argument(
        "--apply-from",
        nargs="+",
        metavar="RUN_CSV",
        help="이미 끝난 실행들의 다수결을 CSV에 반영(LLM 호출 없음). --only-aspects 축만 반영",
    )
    ap.add_argument("--dry-run", action="store_true", help="비용 0 — 규칙 기반 예상 변경분만")
    ap.add_argument(
        "--apply",
        action="store_true",
        help="검토 큐 확인 후 실제 CSV에 반영(백업 자동 생성). 사람 검토 전엔 쓰지 말 것",
    )
    args = ap.parse_args()

    infile = ROOT / args.infile if not Path(args.infile).is_absolute() else Path(args.infile)
    queue_file = ROOT / args.queue if not Path(args.queue).is_absolute() else Path(args.queue)

    if args.dry_run:
        with infile.open(encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        print(f"[dry-run] {len(rows)}건 — LLM 호출 안 함. 규칙 기반 개략 스캔:")
        for k, v in scan_by_rules(rows).items():
            print(f"  {k}: 약 {v}건")
        print("\n※ 실제 판정은 LLM이 합니다. 위 숫자는 규모 감을 잡기 위한 근사치입니다.")
        return

    only = None if args.only_aspects.strip().lower() == "all" else {
        a.strip() for a in args.only_aspects.split(",") if a.strip()
    }

    if args.apply_sweep:
        sp = Path(args.apply_sweep)
        apply_sweep(infile, sp if sp.is_absolute() else ROOT / args.apply_sweep)
        return

    if args.apply_manual:
        rp = Path(args.apply_manual)
        review = rp if rp.is_absolute() else (rp if rp.exists() else ROOT / args.apply_manual)
        apply_manual(infile, review)
        return

    if args.apply_from:
        runs = [Path(p) if Path(p).is_absolute() else ROOT / p for p in args.apply_from]
        apply_from_runs(infile, runs, only)
        return

    if args.build_manual_queue:
        runs = [Path(p) if Path(p).is_absolute() else ROOT / p for p in args.build_manual_queue]
        out = ROOT / args.manual_out if not Path(args.manual_out).is_absolute() else Path(args.manual_out)
        build_manual_queue(infile, runs, out, only, args.accept_agree)
        return

    asyncio.run(
        run(
            infile, queue_file, args.chunk_size, args.concurrency,
            args.retry_chunk_size, args.apply, only,
        )
    )


if __name__ == "__main__":
    main()
