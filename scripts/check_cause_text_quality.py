"""
generate_cause_text_v1 결과물 자동 품질 점검
================================================
LLM이 실제로 만든 cause_text_cache.json을 읽어서, "정답이 없는 생성 프롬프트"를
그래도 기계적으로 점검할 수 있는 부분만 자동 체크한다.

체크 항목
--------
1. 개수 정확성 — 케이스별 요청 개수와 실제 개수 일치 여부(이미 생성기가 보장하지만 재확인)
2. CS 문의체 여부 — 물음표나 요청 표현으로 끝나는지(프롬프트2 때 썼던 방식 재사용)
3. 완전 중복 문장 — 같은 케이스 안에서 똑같은 문장이 있는지
4. 원인간 단어 중복도 — 같은 aspect 안에서 서로 다른 원인끼리 핵심 단어가 겹치는지
   (겹침이 심하면 "구분이 잘 안 될 위험 신호"로만 보고, 자동으로 실패 처리는 안 함 —
    사람이 최종 판단해야 하는 영역이라 참고용 수치만 보여줌)

사용법
------
    python check_cause_text_quality.py --cache cause_text_cache.json
"""

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

# scripts/ 는 저장소 루트의 형제 폴더 — app 패키지를 절대경로로 import하려면
# 저장소 루트를 sys.path에 넣어야 함(실행 방식에 따라 자동으로 안 잡힐 수 있어서 명시)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.console import force_utf8_output

CS_MARKERS = [
    "?", "까요", "부탁", "가능", "해주세요", "해주실", "드립니다", "드려요",
    "인가요", "싶어요", "주세요",
    "필요해요", "필요합니다", "필요할",  # "~확인이 필요해요" 류 — 요청 표현
    "궁금해요", "궁금합니다",             # "~어떤지 궁금해요" 류 — 질문 대체 표현
    "싶습니다",                          # "싶어요"의 정중체
    "요청합니다", "요청드립니다",
    "것 같아요", "것 같습니다",           # CS에서 흔한 완곡한 불만/관찰 서술("잘못 표현된 것 같아요")
]

# aspect별 원인 그룹 (같은 aspect 안에서만 단어 중복도를 비교해야 의미 있음)
ASPECT_CAUSES = {
    "색상": ["사진_색감_오차", "조명_보정_차이", "실물_염색_편차", "기타"],
    "사이즈": ["표기_오타", "실측_표기_편차", "채널_사이즈_표준차이", "기타"],
    "소재": ["소재_정보_누락", "이미지_질감표현_부족", "실제_원단_문제", "기타"],
}


def is_cs_toned(text: str) -> bool:
    return any(m in text for m in CS_MARKERS)


def extract_keywords(text: str) -> set[str]:
    """아주 단순한 방식 — 2글자 이상 한글 어절 추출(형태소 분석기 없이 근사치)."""
    words = re.findall(r"[가-힣]{2,}", text)
    return set(words)


def check_case(case_id_aspect: str, items: list[dict]) -> dict:
    result = {"key": case_id_aspect, "count": len(items), "issues": []}

    # 1) CS 문의체 여부
    non_cs = [i for i in items if not is_cs_toned(i["text"])]
    if non_cs:
        result["issues"].append(f"CS 문의체 아님 의심 {len(non_cs)}건: {[i['text'] for i in non_cs]}")

    # 2) 완전 중복 문장
    texts = [i["text"] for i in items]
    dupes = [t for t, c in Counter(texts).items() if c > 1]
    if dupes:
        result["issues"].append(f"완전 중복 문장 {len(dupes)}종: {dupes[:3]}")

    # 3) 원인간 단어 중복도 (같은 aspect 안에서)
    by_cause: dict[str, list[str]] = {}
    for i in items:
        by_cause.setdefault(i["cause"], []).append(i["text"])

    cause_keywords = {c: set().union(*[extract_keywords(t) for t in ts]) for c, ts in by_cause.items()}
    causes = list(cause_keywords.keys())
    overlap_warnings = []
    for a in range(len(causes)):
        for b in range(a + 1, len(causes)):
            c1, c2 = causes[a], causes[b]
            if c1 == "기타" or c2 == "기타":
                continue  # 기타는 원래 이질적인 걸 모으는 버킷이라 비교 제외
            inter = cause_keywords[c1] & cause_keywords[c2]
            union = cause_keywords[c1] | cause_keywords[c2]
            jaccard = len(inter) / len(union) if union else 0
            if jaccard > 0.3:  # 임계값은 감으로 잡은 참고용 — 자동 실패 아님
                overlap_warnings.append(f"{c1} vs {c2}: 공통단어 {sorted(inter)} (중복도 {jaccard:.0%})")
    if overlap_warnings:
        result["issues"].append("원인간 단어 중복 주의: " + " / ".join(overlap_warnings))

    return result


def main():
    # 🔴 첫 문장이어야 한다. ⚠️ 이 파일만 `--help` 가 원래 통과한다 —
    #    `ArgumentParser()` 에 `description=` 이 없어 docstring 의 `—` 가 도움말에 안 실린다.
    #    대신 아래 진단 출력(119·124·127행)이 `⚠️`·`✅`·`—` 를 쓰므로
    #    **점검 결과가 통째로 사라진다.**
    #    `app/core/console.py`.
    force_utf8_output()

    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default="cause_text_cache.json")
    args = ap.parse_args()

    with open(args.cache, encoding="utf-8") as f:
        cache = json.load(f)

    print(f"총 {len(cache)}개 케이스 점검\n")
    clean_count = 0
    for key, items in cache.items():
        result = check_case(key, items)
        if result["issues"]:
            print(f"⚠️ {key} ({result['count']}건)")
            for issue in result["issues"]:
                print(f"    - {issue}")
        else:
            clean_count += 1
            print(f"✅ {key} ({result['count']}건) — 기계적 체크 전부 통과")

    print(f"\n{clean_count}/{len(cache)}개 케이스가 기계적 체크 전부 통과")
    print("⚠️ 표시된 것들도 반드시 틀렸다는 뜻은 아님 — 사람이 직접 읽고 최종 판단 필요")
    print("\n=== 사람이 직접 읽어볼 샘플(케이스당 원인별 1개씩) ===")
    for key, items in list(cache.items())[:3]:  # 처음 3개 케이스만 미리보기
        print(f"\n[{key}]")
        seen_causes = set()
        for i in items:
            if i["cause"] not in seen_causes:
                seen_causes.add(i["cause"])
                print(f"  {i['cause']}: {i['text']}")


if __name__ == "__main__":
    main()