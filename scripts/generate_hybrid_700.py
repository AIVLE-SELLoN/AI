"""LLM생성 700건 — 하이브리드 1,000건 평가셋의 나머지 절반.

배경
----
`eval/eval_sets/relabel_300.csv`(71603 재라벨링)는 색상·사이즈·소재 3개만 커버한다
(71603이 리뷰 데이터라 파손·오배송·기타가 자연 발생하지 않음 — 리뷰는 "받아서 써본 뒤"
쓰는 거라 애초에 이런 사건이 드묾). 이 700건이 나머지를 채운다.

절대 원칙 — few-shot 절대 참고 금지
-----------------------------------
`app/classification/prompts/classify_aspect_v5.md`의 few-shot 예시(입력:~출력:)는
이 스크립트 어디에서도 읽지 않는다. 오직 "분류 대상 속성" 정의(41~49행)만 인용한다.
예시를 보고 생성하면 그 자체가 "정답을 아는 시험문제"가 된다(2026-08-06, 서영님 지적
— 예시15-3/15-4가 실패사례 패턴에서 나왔던 것과 같은 유형의 실수 재발 방지).

같은 모델 사용에 대한 보완
----------------------------------------
분류기랑 생성기가 같은 모델이라 "그 모델이 잘 만드는 패턴 = 그 모델이 잘 맞히는 패턴"이
우연히 겹칠 위험이 있다. 3가지로 보완한다:
  1. 페르소나·길이·격식을 매 호출마다 무작위로 다르게(고정 패턴 반복 방지)
  2. 생성 후 이 스크립트가 아니라 별도로 relabel_300 대비 정확도 역검증 필요(문서 참고)
  3. 샘플 50~100건은 사람이 직접 읽고 검수 필요(자동화 불가 — 별도 진행)

사용법
------
    python scripts/generate_hybrid_700.py --outfile eval/eval_sets/llm_generated_700.csv
    python scripts/generate_hybrid_700.py --dry-run   # 비용 0, 계획만 출력
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.core.llm_client import get_llm_client  # noqa: E402

PROMPT_PATH = ROOT / "app" / "classification" / "prompts" / "classify_aspect_v5.md"

# ── 배분 계획 (2026-08-06 확정, 조정 시 여기만 수정) ──────────────────────
# (aspect, 목표건수, 부정/중립/긍정 비율)
QUOTA_PLAN = [
    ("파손", 150, (0.70, 0.20, 0.10)),
    ("오배송", 150, (0.70, 0.20, 0.10)),
    ("기타", 150, (0.50, 0.40, 0.10)),
    ("색상", 50, (0.60, 0.25, 0.15)),
    ("사이즈", 50, (0.60, 0.25, 0.15)),
    ("소재", 50, (0.60, 0.25, 0.15)),
]
MULTI_ASPECT_QUOTA = 100  # 2개 aspect 동시 등장(explode 계약 검증용, §4 결정사항②)

# ── 분류 대상 속성 정의(예시 절대 포함 안 함 — classify_aspect_v5.md 41~49행 그대로) ──
ASPECT_DEFINITIONS = {
    "색상": "색상 관련 언급 전체(색상 불만뿐 아니라, 색상 관련 사전질문·재입고문의·긍정확인 포함)",
    "사이즈": "사이즈·핏 관련 언급 전체(불만뿐 아니라 사전질문·긍정확인 포함)",
    "소재": "원단 재질·촉감·신축성·텐션감·스판기 관련 언급 전체(불만뿐 아니라 사전질문·긍정확인 포함)",
    "파손": "상품 손상·불량 관련 언급 전체(실제 손상 불만뿐 아니라, \"파손되면 어떻게 되나요?\" 같은"
            " 사전질문, \"파손 없이 잘 왔어요\" 같은 긍정확인도 포함)",
    "오배송": "주문한 것과 다른 상품이 오는 것 관련 언급 전체(실제 오배송 불만뿐 아니라, \"옵션 변경"
             " 가능한가요?\" 같은 사전질문, \"주문한 대로 정확히 왔어요\" 같은 긍정확인도 포함)",
    "기타": "위 5개 어디에도 속하지 않는 문의(배송 지연, 결제 오류 등 — 위 5개 주제와 조금이라도"
           " 관련 있으면 기타로 보내지 말고 해당 aspect로 분류할 것)",
}

PERSONAS = [
    "급하게 답변을 원하는 고객, 문장이 짧고 다급함",
    "차분하고 정중하게 문의하는 고객, 존댓말 격식 있음",
    "구어체로 편하게 쓰는 고객, 반말 섞이거나 이모티콘 있음",
    "장황하게 상황을 자세히 설명하는 고객",
    "화가 나서 항의하는 고객(단, 폭언·비속어는 없음)",
    "궁금한 점을 담백하게 묻기만 하는 고객",
]
LENGTHS = ["1문장(20자 내외)", "2문장(40~60자)", "3문장 이상(80자 이상, 상황 설명 포함)"]


def load_aspect_definitions_note() -> str:
    """실제로 프롬프트 파일에서 정의 문구가 안 바뀌었는지 대조(있으면 경고만, 하드코딩값 그대로 씀).
    few-shot 예시 자체는 안 읽음 — 정의(41~49행 패턴)와 겹치는지만 확인.
    """
    if not PROMPT_PATH.exists():
        return "⚠️ 프롬프트 파일 없음 — 하드코딩된 정의 그대로 사용"
    content = PROMPT_PATH.read_text(encoding="utf-8")
    for aspect, definition in ASPECT_DEFINITIONS.items():
        # 정의 문구 앞부분(20자)만 대충 대조 — 완전 동일성 요구 안 함(문서가 리팩터링될 수 있음)
        snippet = definition[:15]
        if snippet not in content:
            return f"⚠️ '{aspect}' 정의가 프롬프트 파일과 달라진 것 같음 — 확인 필요"
    return "✅ 정의 문구가 프롬프트 파일과 일치 확인됨"


def build_prompt(aspect: str | list[str], sentiment: int | list[int]) -> str:
    """생성용 프롬프트 조립. few-shot 예시는 절대 안 넣음 — 정의만."""
    is_multi = isinstance(aspect, list)
    persona = random.choice(PERSONAS)
    length = random.choice(LENGTHS)

    if is_multi:
        defs = "\n".join(f'- {a}: {ASPECT_DEFINITIONS[a]}' for a in aspect)
        sent_desc = ", ".join(
            f'{a}는 감성 {s}({"부정" if s == -1 else "중립" if s == 0 else "긍정"})'
            for a, s in zip(aspect, sentiment)
        )
        aspect_instruction = (
            f"아래 {len(aspect)}개 속성이 **한 문장(또는 짧은 문단) 안에 전부 자연스럽게** "
            f"같이 언급되게 CS 문의를 만드세요. 각 속성마다 지정된 감성을 따로 담아야 합니다.\n{defs}\n"
            f"속성별 감성: {sent_desc}"
        )
    else:
        sentiment_label = {-1: "부정", 0: "중립", 1: "긍정"}[sentiment]
        aspect_instruction = (
            f"아래 속성 하나에 대한 CS 문의를 만드세요.\n- {aspect}: {ASPECT_DEFINITIONS[aspect]}\n"
            f"감성: {sentiment_label}"
        )

    return f"""당신은 온라인 쇼핑몰 CS 문의 생성기입니다. 실제 고객이 쓸 법한 자연스러운
한국어 CS 문의 문장을 만드세요.

{aspect_instruction}

문체 조건:
- 페르소나: {persona}
- 길이: {length}
- 실제 고객이 쓸 법한 자연스러운 문장으로(AI가 쓴 것처럼 정형화되거나 과하게 정중한 문어체 금지)
- 이전에 만든 다른 문장과 겹치지 않는 새로운 상황·표현으로

출력 형식(JSON만, 다른 텍스트 없이):
{{"text": "생성된 CS 문의 문장"}}
"""


def sample_sentiment(ratio: tuple[float, float, float], rng: random.Random) -> int:
    """부정/중립/긍정 비율에 따라 감성 하나 뽑기."""
    r = rng.random()
    if r < ratio[0]:
        return -1
    elif r < ratio[0] + ratio[1]:
        return 0
    return 1


async def generate_one(client, aspect, sentiment, seen_texts: set[str], retry: int = 3) -> dict | None:
    """1건 생성 + 중복 방지(seen_texts에 없을 때까지 재시도)."""
    for _ in range(retry):
        prompt = build_prompt(aspect, sentiment)
        try:
            raw = await client.complete(prompt, trace_key=f"gen-{aspect}", temperature=0.9, json_mode=True)
            data = json.loads(raw)
            text = data.get("text", "").strip()
        except Exception as e:
            print(f"  ⚠️ 생성 실패({aspect}): {e}")
            continue
        if text and text not in seen_texts:
            seen_texts.add(text)
            return {"aspect": aspect, "sentiment": sentiment, "text": text}
    print(f"  ⚠️ 중복 회피 {retry}회 실패 — {aspect} 1건 스킵")
    return None


async def run_generation(outfile: Path, seed: int) -> None:
    rng = random.Random(seed)
    client = get_llm_client()
    seen_texts: set[str] = set()
    rows: list[dict] = []

    print("=== 단일 aspect 생성 ===")
    for aspect, quota, ratio in QUOTA_PLAN:
        print(f"{aspect}: {quota}건 목표")
        tasks = []
        for _ in range(quota):
            sentiment = sample_sentiment(ratio, rng)
            tasks.append(generate_one(client, aspect, sentiment, seen_texts))
        results = await asyncio.gather(*tasks)
        got = [r for r in results if r is not None]
        rows.extend(got)
        print(f"  → {len(got)}/{quota}건 생성됨")

    print()
    print(f"=== 다중aspect 생성 ({MULTI_ASPECT_QUOTA}건) ===")
    all_aspects = list(ASPECT_DEFINITIONS.keys())
    tasks = []
    for _ in range(MULTI_ASPECT_QUOTA):
        pair = rng.sample(all_aspects, 2)
        sentiments = [sample_sentiment((0.6, 0.25, 0.15), rng) for _ in pair]
        tasks.append(generate_one(client, pair, sentiments, seen_texts))
    results = await asyncio.gather(*tasks)
    got = [r for r in results if r is not None]
    rows.extend(got)
    print(f"  → {len(got)}/{MULTI_ASPECT_QUOTA}건 생성됨")

    # CSV 저장 — relabel_300.csv와 같은 컬럼 체계로(합치기 쉽게)
    outfile.parent.mkdir(parents=True, exist_ok=True)
    with outfile.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "aspect", "sentiment", "raw_text", "source"])
        w.writeheader()
        for i, r in enumerate(rows, 1):
            aspect_val = ",".join(r["aspect"]) if isinstance(r["aspect"], list) else r["aspect"]
            sent_val = ",".join(str(s) for s in r["sentiment"]) if isinstance(r["sentiment"], list) else str(r["sentiment"])
            w.writerow({
                "id": f"GEN-{i:04d}",
                "aspect": aspect_val,
                "sentiment": sent_val,
                "raw_text": r["text"],
                "source": "llm_generated",
            })

    print()
    print(f"총 {len(rows)}건 → {outfile} 저장 완료")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--outfile", default="eval/eval_sets/llm_generated_700.csv")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--dry-run", action="store_true", help="비용 0, 계획만 출력")
    args = ap.parse_args()

    print(load_aspect_definitions_note())
    print()

    total = sum(q for _, q, _ in QUOTA_PLAN) + MULTI_ASPECT_QUOTA
    print(f"생성 계획: 단일aspect {sum(q for _, q, _ in QUOTA_PLAN)}건 + 다중aspect {MULTI_ASPECT_QUOTA}건 = 총 {total}건")
    for aspect, quota, ratio in QUOTA_PLAN:
        print(f"  {aspect}: {quota}건 (부정{ratio[0]:.0%}/중립{ratio[1]:.0%}/긍정{ratio[2]:.0%})")
    print(f"  다중aspect: {MULTI_ASPECT_QUOTA}건(2개 조합, 무작위)")

    if args.dry_run:
        print("\n[dry-run] LLM 호출 안 함.")
        return

    outfile = ROOT / args.outfile if not Path(args.outfile).is_absolute() else Path(args.outfile)
    asyncio.run(run_generation(outfile, args.seed))


if __name__ == "__main__":
    main()