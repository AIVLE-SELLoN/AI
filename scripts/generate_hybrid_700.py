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
     ⚠️ 아래 라벨 검수 게이트가 생겨도 3번은 여전히 필수다. 게이트를 통과한 문장은
     "검수 모델이 동의한 문장"이라 오히려 평가가 낙관적으로 편향된다.

라벨 검수 게이트 (2026-08-09 추가)
----------------------------------
이전 버전은 `sample_sentiment()`가 뽑은 값을 그대로 라벨로 저장했다. 즉 라벨이 "생성 결과에
대한 관측"이 아니라 "생성 지시"였고, 생성기가 지시를 못 지키면 곧바로 오라벨이 됐다.
실제로 초판 700건에서 가정형 문의에 -1이, 실제 오배송 서술에 0/1이 대량으로 붙었다.

바뀐 점 3가지:
  (a) 감성 -1/0/1의 **조작적 정의**를 프롬프트에 명시(SENTIMENT_DEFINITIONS).
      이전엔 `감성: 부정` 다섯 글자뿐이라 매 호출마다 해석이 달라졌다.
  (b) 생성된 문장을 **다시 읽혀서** 요청 감성과 맞는지 확인하고, 불일치하면 라벨을 고치는 게
      아니라 **문장을 재생성**한다(라벨을 고치면 golden이 모델 판단으로 오염됨).
  (c) `감성=긍정`인데 "화가 나서 항의하는 고객" 페르소나가 뽑히던 조합을 배제(select_personas).

사용법
------
    python scripts/generate_hybrid_700.py --outfile eval/eval_sets/llm_generated_700.csv
    python scripts/generate_hybrid_700.py --dry-run     # 비용 0, 계획만 출력
    python scripts/generate_hybrid_700.py --no-verify   # 검수 게이트 OFF(권장 안 함)

이미 생성된 CSV의 라벨만 정책에 맞게 다시 매기려면 → scripts/relabel_generated_sentiment.py
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.core.llm_client import get_llm_client

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
    "색상": "색상 관련 언급 전체(색상 불만뿐 아니라, 색상 관련 사전질문·재입고문의·문제없음확인 포함)",
    "사이즈": "사이즈·핏 관련 언급 전체(불만뿐 아니라 사전질문·문제없음확인 포함)",
    "소재": "원단 재질·촉감·신축성·텐션감·스판기 관련 언급 전체(불만뿐 아니라 사전질문·문제없음확인 포함)",
    "파손": "상품 손상·불량 관련 언급 전체(실제 손상 불만뿐 아니라, \"파손되면 어떻게 되나요?\" 같은"
            " 사전질문, \"파손 없이 잘 왔어요\" 같은 문제없음확인도 포함)",
    "오배송": "주문한 것과 다른 상품이 오는 것 관련 언급 전체(실제 오배송 불만뿐 아니라, \"옵션 변경"
             " 가능한가요?\" 같은 사전질문, \"주문한 대로 정확히 왔어요\" 같은 문제없음확인도 포함)",
    "기타": "위 5개 어디에도 속하지 않는 문의(배송 지연, 결제 오류 등 — 위 5개 주제와 조금이라도"
           " 관련 있으면 기타로 보내지 말고 해당 aspect로 분류할 것)",
}

# ── 감성 조작적 정의 (2026-08-09 추가) ────────────────────────────────────
# 이전 버전은 프롬프트에 `감성: 부정` 다섯 글자만 넣었다. 정의가 없으니 생성기가 매 호출마다
# "부정"을 다르게 해석했고, 그 해석 차이가 그대로 golden label이 됐다(가정형 문의에 -1이
# 붙고, 실제 오배송 서술에 0/1이 붙는 사고). 정의를 명시해서 해석 폭을 없앤다.
# classify_aspect_v5.md의 감성 정의와 같은 기준 — 단, few-shot 예시는 여전히 인용 금지.
SENTIMENT_DEFINITIONS = {
    -1: (
        "부정 — 화자가 **직접 겪었거나 직접 관측한 문제**가 있는 경우.\n"
        "  (1) 실제로 파손/오배송/지연이 발생했거나, 받아보니 사이즈·색상·소재가 기대와 달랐음\n"
        "  (2) 🔴 **포장·박스 상태가 나쁜 걸 직접 봤음** — '포장이 찢어져 있었다', "
        "'박스가 찌그러졌다', '포장이 허술/헐거웠다', '완충재가 없었다' 등.\n"
        "      **내용물이 멀쩡해도 -1입니다.** 포장 불량은 그 자체가 셀러가 조치할 결함이고,\n"
        "      '아직 파손은 안 일어났으니 중립'으로 빠지면 안 됩니다. 여기서 가장 많이 틀립니다.\n"
        "  (3) 🔴 **배송 미도착·지연을 주장하는 문의** (2026-08-09 손검토 확정)\n"
        "      '아직 안 왔어요' · '아직 배송되지 않았습니다' · '도착하지 않았는데' ·\n"
        "      '지연되고 있어요' · '너무 늦어요' 처럼 **안 왔거나 늦다고 말하면 -1**.\n"
        "      말투가 정중하고 '확인 부탁드립니다'로 끝나도 -1이고, 기간·예정일 같은\n"
        "      구체적 근거가 없어도 -1입니다. 일정을 안내해도 '안 왔다'는 문제는 안 풀립니다.\n"
        "  ⚠️ 화난 말투일 필요 없음 — '주문한 상품이 아닌 다른 게 왔어요' 처럼 담백해도 부정."
    ),
    0: (
        "중립 — 화자가 **관측한 결함이 하나도 없는** 경우.\n"
        "  (1) 사전질문: 아직 안 받아본 상태에서 스펙·절차를 묻는 것\n"
        "  (2) 가정형·예방형: '파손되면 어떻게 하나요?', '안전하게 오나요?' 처럼 "
        "안 일어난 일을 가정하고 묻는 것\n"
        "  (3) 문제 없음 확인: '파손 없이 잘 왔어요', '무사히 도착했습니다' 같은 사실 서술\n"
        "  ⚠️ '걱정된다'는 표현이 있어도, 걱정 대상이 아직 안 벌어진 일이면 중립.\n"
        "  (4) 🔴 **지연 주장 없이 일정만 묻는 배송 문의** (2026-08-09 손검토 확정)\n"
        "      '언제 배송되나요?' · '배송이 언제쯤 될까요?' · '배송 상태가 궁금해요' 처럼\n"
        "      **안 왔다/늦다는 주장 없이 일정만** 물으면 0입니다.\n"
        "      갈림길은 위 -1(3)과 **'안 왔다·늦다'는 주장의 유무** 하나뿐입니다.\n"
        "  🔴 **단, 포장 상태가 나빴다는 서술이 한 줄이라도 있으면 중립이 아니라 -1입니다.**\n"
        "     '포장이 찢어져 있었는데 파손되면 어떻게 하나요?' → 뒤가 가정형이어도 **-1**.\n"
        "     앞의 관측이 이미 -1을 확정하므로, 뒤의 가정형 질문이 그걸 되돌리지 못합니다."
    ),
    1: (
        "긍정 — **칭찬·만족이 명시적으로 표현된** 경우만.\n"
        "  '마음에 들어요', '만족해요', '감동이에요' 처럼 감정 평가가 문장에 드러나야 함.\n"
        "  ⚠️ '문제 없이 잘 왔다'는 사실 확인일 뿐 긍정이 아님 — 그건 중립(0)."
    ),
}

ANGRY_PERSONA = "화가 나서 항의하는 고객(단, 폭언·비속어는 없음)"
PERSONAS = [
    "급하게 답변을 원하는 고객, 문장이 짧고 다급함",
    "차분하고 정중하게 문의하는 고객, 존댓말 격식 있음",
    "구어체로 편하게 쓰는 고객, 반말 섞이거나 이모티콘 있음",
    "장황하게 상황을 자세히 설명하는 고객",
    ANGRY_PERSONA,
    "궁금한 점을 담백하게 묻기만 하는 고객",
]
LENGTHS = ["1문장(20자 내외)", "2문장(40~60자)", "3문장 이상(80자 이상, 상황 설명 포함)"]


def select_personas(sentiment: int | list[int]) -> list[str]:
    """감성과 모순되는 페르소나를 뺀 목록 (2026-08-09 추가).

    이전엔 페르소나를 감성과 **독립적으로** 뽑아서, `감성=긍정 + "화가 나서 항의하는 고객"`
    조합이 나올 수 있었다. 그러면 라벨은 1인데 문장은 항의문이 된다 — 실제로
    GEN-0158("주문한 상품과 전혀 다른 제품이 배송되어서 매우 당황스럽습니다" → gold 1)
    같은 오라벨의 직접 원인이었다.

    부정 aspect가 하나라도 있으면 항의 페르소나를 허용한다(사이즈엔 불만이고 소재는
    중립인 고객이 화나 있는 건 자연스러움). 전부 비부정이면 뺀다.
    """
    sents = sentiment if isinstance(sentiment, list) else [sentiment]
    if any(s == -1 for s in sents):
        return PERSONAS
    return [p for p in PERSONAS if p != ANGRY_PERSONA]


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
    persona = random.choice(select_personas(sentiment))  # 감성과 모순되는 페르소나 배제
    length = random.choice(LENGTHS)

    sents = sentiment if is_multi else [sentiment]
    sentiment_guide = "\n".join(
        f"- 감성 {s}: {SENTIMENT_DEFINITIONS[s]}" for s in sorted(set(sents))
    )

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

🔴 감성 정의 — 반드시 이 정의대로 쓰세요(단어 인상이 아니라 정의로 판단):
{sentiment_guide}

문체 조건:
- 페르소나: {persona}
- 길이: {length}
- 실제 고객이 쓸 법한 자연스러운 문장으로(AI가 쓴 것처럼 정형화되거나 과하게 정중한 문어체 금지)
- 이전에 만든 다른 문장과 겹치지 않는 새로운 상황·표현으로

출력 형식(JSON만, 다른 텍스트 없이):
{{"text": "생성된 CS 문의 문장"}}
"""


def build_verify_prompt(text: str, aspects: list[str]) -> str:
    """생성된 문장을 **다시 읽고** 감성을 매기는 검수용 프롬프트 (2026-08-09 추가).

    few-shot 예시는 여기서도 절대 안 넣는다 — 감성 정의만 준다. 검수기가 예시를 보면
    "정답을 아는 시험문제"가 되는 건 생성기와 똑같다.
    """
    guide = "\n".join(f"- {s}: {d}" for s, d in sorted(SENTIMENT_DEFINITIONS.items()))
    aspect_list = ", ".join(aspects)
    return f"""아래는 온라인 쇼핑몰 CS 문의입니다. 지정된 속성 각각에 대해 감성을 매기세요.

CS 문의: "{text}"

대상 속성: {aspect_list}

🔴 감성 정의 — 반드시 이 정의대로만 판단하세요:
{guide}

판단 순서:
1. 이 속성에 대해 화자가 **직접 겪었거나 직접 관측한 문제**가 있는가? → 있으면 -1
2. 없다면, **칭찬·만족이 명시적으로 표현**됐는가? → 있으면 1
3. 둘 다 아니면 → 0

출력 형식(JSON만, 다른 텍스트 없이). 속성 순서는 위 '대상 속성' 순서 그대로:
{{"sentiments": [{{"aspect": "속성명", "sentiment": -1}}]}}
"""


def sample_sentiment(ratio: tuple[float, float, float], rng: random.Random) -> int:
    """부정/중립/긍정 비율에 따라 감성 하나 뽑기."""
    r = rng.random()
    if r < ratio[0]:
        return -1
    elif r < ratio[0] + ratio[1]:
        return 0
    return 1


async def verify_labels(client, text: str, aspects: list[str]) -> list[int] | None:
    """생성된 문장을 다시 읽고 감성을 매긴다. 실패하면 None."""
    try:
        data = await client.complete_json(
            build_verify_prompt(text, aspects),
            trace_key=f"verify-{'+'.join(aspects)}",
            temperature=0.0,
        )
        got = {d["aspect"]: int(d["sentiment"]) for d in data["sentiments"]}
        return [got[a] for a in aspects]  # KeyError 나면 아래 except가 잡음
    except Exception as e:  # noqa: BLE001 — 호출·파싱·스키마 오류를 스킵으로 흡수
        print(f"  ⚠️ 검수 실패({'+'.join(aspects)}): {e}")
        return None


async def generate_one(
    client,
    aspect,
    sentiment,
    seen_texts: set[str],
    stats: dict,
    retry: int = 3,
    verify: bool = True,
) -> dict | None:
    """1건 생성 + 중복 방지 + **라벨 검수 게이트**(2026-08-09 추가).

    이전엔 `sample_sentiment()`가 뽑은 값을 검수 없이 그대로 라벨로 저장했다. 즉 라벨이
    "생성 결과에 대한 관측"이 아니라 "생성 지시"였고, 생성기가 지시를 못 지키면 그대로
    오라벨이 됐다(700건 중 상당수).

    이제는 생성된 문장을 다시 읽혀서 요청 감성과 일치하는지 확인하고, **불일치하면 라벨을
    고치는 게 아니라 문장을 재생성**한다. 라벨을 고치면 golden이 모델 판단으로 오염되므로
    (같은 모델이 채점까지 하면 평가가 순환논리가 됨), 스펙인 라벨은 고정하고 문장을 맞춘다.

    ⚠️ 알려진 한계: 살아남는 문장은 "검수 모델이 동의한 문장"이라 평가가 낙관적으로
    편향된다. 이 게이트는 텍스트-라벨 정합성 보장용이지 품질 보증이 아니며, 모듈 docstring
    3번(사람 표본 검수)은 여전히 필수다. 거부율은 실행 끝에 출력된다 — 이 수치 자체가
    "요청 감성을 얼마나 못 지켰는지"의 지표다.
    """
    aspects = aspect if isinstance(aspect, list) else [aspect]
    requested = sentiment if isinstance(sentiment, list) else [sentiment]

    for _ in range(retry):
        prompt = build_prompt(aspect, sentiment)
        try:
            raw = await client.complete(prompt, trace_key=f"gen-{aspect}", temperature=0.9, json_mode=True)
            data = json.loads(raw)
            text = data.get("text", "").strip()
        except Exception as e:  # noqa: BLE001 — 1건 실패해도 나머지 생성은 계속한다
            print(f"  ⚠️ 생성 실패({aspect}): {e}")
            continue
        if not text or text in seen_texts:
            continue
        # 거부되더라도 같은 문장을 다시 뽑지 않도록 즉시 등록
        seen_texts.add(text)
        stats["generated"] += 1

        if not verify:
            return {"aspect": aspect, "sentiment": sentiment, "text": text}

        got = await verify_labels(client, text, aspects)
        if got is None:
            stats["verify_error"] += 1
            continue
        if got != requested:
            stats["rejected"] += 1
            stats["reject_samples"].append(
                {"aspect": aspects, "requested": requested, "verified": got, "text": text}
            )
            continue
        stats["accepted"] += 1
        return {"aspect": aspect, "sentiment": sentiment, "text": text}

    stats["dropped"] += 1
    print(f"  ⚠️ 생성/검수 {retry}회 실패 — {aspect} 1건 스킵")
    return None


def init_stats() -> dict:
    return {
        "generated": 0, "accepted": 0, "rejected": 0,
        "verify_error": 0, "dropped": 0, "reject_samples": [],
    }


async def run_generation(outfile: Path, seed: int, verify: bool = True) -> None:
    rng = random.Random(seed)
    client = get_llm_client()
    seen_texts: set[str] = set()
    rows: list[dict] = []
    stats = init_stats()

    print("=== 단일 aspect 생성 ===")
    for aspect, quota, ratio in QUOTA_PLAN:
        print(f"{aspect}: {quota}건 목표")
        tasks = []
        for _ in range(quota):
            sentiment = sample_sentiment(ratio, rng)
            tasks.append(generate_one(client, aspect, sentiment, seen_texts, stats, verify=verify))
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
        tasks.append(generate_one(client, pair, sentiments, seen_texts, stats, verify=verify))
    results = await asyncio.gather(*tasks)
    got = [r for r in results if r is not None]
    rows.extend(got)
    print(f"  → {len(got)}/{MULTI_ASPECT_QUOTA}건 생성됨")

    if verify:
        print()
        print("=== 라벨 검수 게이트 결과 ===")
        total_judged = stats["accepted"] + stats["rejected"]
        rate = stats["rejected"] / total_judged if total_judged else 0.0
        print(f"  생성 시도 {stats['generated']}건 / 통과 {stats['accepted']}건 / "
              f"거부 {stats['rejected']}건 (거부율 {rate:.1%})")
        print(f"  검수 오류 {stats['verify_error']}건 / 최종 스킵 {stats['dropped']}건")
        print("  ※ 거부율은 '생성기가 요청 감성을 못 지킨 비율'입니다. 높으면 감성 정의를 다시 보세요.")
        for s in stats["reject_samples"][:5]:
            print(f"    - 요청{s['requested']} → 검수{s['verified']}: {s['text'][:60]}")

    # 🔴 ID는 **생존 행 순번**이라 내용 기반이 아니다 (2026-08-09 PR 리뷰 지적).
    # 검수 게이트가 실패 행을 버리므로 재생성하면 GEN-#### 가 밀리고, 예전에 만든
    # 라벨 파일(relabel_runs/, relabel_manual_review.csv)을 반영하면 **다른 문장에
    # 라벨이 붙는다.** 재생성 시에는 그 파일들을 같이 폐기하거나 다시 만들 것.
    # relabel_generated_sentiment.py 의 check_text_drift() 가 반영 시점에 한 번 더 막는다.
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
    ap.add_argument(
        "--no-verify",
        action="store_true",
        help="라벨 검수 게이트 끄기(비용 절반, 대신 예전처럼 라벨=생성지시가 됨 — 권장 안 함)",
    )
    args = ap.parse_args()

    print(load_aspect_definitions_note())
    print()

    total = sum(q for _, q, _ in QUOTA_PLAN) + MULTI_ASPECT_QUOTA
    print(f"생성 계획: 단일aspect {sum(q for _, q, _ in QUOTA_PLAN)}건 + 다중aspect {MULTI_ASPECT_QUOTA}건 = 총 {total}건")
    for aspect, quota, ratio in QUOTA_PLAN:
        print(f"  {aspect}: {quota}건 (부정{ratio[0]:.0%}/중립{ratio[1]:.0%}/긍정{ratio[2]:.0%})")
    print(f"  다중aspect: {MULTI_ASPECT_QUOTA}건(2개 조합, 무작위)")

    verify = not args.no_verify
    print(f"  라벨 검수 게이트: {'ON (생성 1건당 검수 1콜 추가)' if verify else 'OFF ⚠️'}")

    if args.dry_run:
        print("\n[dry-run] LLM 호출 안 함.")
        return

    outfile = ROOT / args.outfile if not Path(args.outfile).is_absolute() else Path(args.outfile)
    asyncio.run(run_generation(outfile, args.seed, verify=verify))


if __name__ == "__main__":
    main()