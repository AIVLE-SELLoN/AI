"""실험⑦ 임베딩 모델 A/B — 컬렉션2(반려사유) 의미검색이 한국어에서 제 성능을 내는가.

Chroma 기본 임베딩 함수(all-MiniLM-L6-v2, 영어 전용)와 `EMBEDDING_MODEL`
(text-embedding-3-small, 다국어)을 **같은 문서·같은 쿼리**로 비교한다.

측정 대상은 컬렉션2 한 곳이다 — 컬렉션1 은 `get(where=...)` 정확 매칭이라 임베딩을
안 거친다(app/core/vectordb.py).

실행:
    python eval/run_embedding_eval.py          # 두 모델 비교, 실비용(임베딩 ~1천 토큰)
    python eval/run_embedding_eval.py --dry-run  # 표본만 출력, $0

⚠️ 컬렉션2 는 운영 데이터가 0건이라(HITL 미사용) **여기 표본은 우리가 만든 것**이다.
   `record_hitl_outcome()` 이 쓰는 문서 형식
   (`f"{root_cause_label} {cs_summary} {proposed_text}"`)과 `retrieve_context()` 의
   쿼리 형식(`alert.root_cause.label` + aspect 필터)을 그대로 따랐지만, 실제 반려
   사유의 문체와는 다를 수 있다. 숫자를 운영 성능으로 읽지 말 것.

⚠️ `label` 모드는 쿼리가 문서 안에 그대로 들어 있어 **어휘 일치만으로도 맞을 수 있다.**
   의미 이해를 보려면 `paraphrase` 모드를 볼 것 — 라벨 문자열을 뺀 고객 말투 쿼리다.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import chromadb
from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

from app.core.console import force_utf8_output
from app.core.constants import EMBEDDING_MODEL, SIMILAR_CASE_TOP_N
from app.core.vectordb import get_embedding_function

force_utf8_output()

# (aspect, root_cause_label, 개선안 본문, 라벨을 안 쓴 고객 말투 쿼리)
#
# 같은 aspect 안에 **서로 붙어 있는 원인**을 일부러 모아뒀다(사진_색감_오차·조명_보정_차이·
# 실물_염색_편차). 운영 조회가 `where={"aspect": ...}` 로 먼저 좁히므로, 변별이 필요한
# 구간이 정확히 여기다. 라벨 정의는 app/detection/prompts/classify_cause_v1.md.
SAMPLES: list[tuple[str, str, str, str]] = [
    (
        "색상",
        "사진_색감_오차",
        "상세페이지 대표 이미지를 보정 없이 재촬영하고 '화면 설정에 따라 색상이 다르게 보일 수 있습니다' 문구를 추가하세요.",
        "사진이랑 실물 색이 너무 달라요",
    ),
    (
        "색상",
        "조명_보정_차이",
        "자연광과 실내조명에서 각각 찍은 컷을 나란히 올려 조명별 색 차이를 미리 보여주세요.",
        "조명 때문인지 실내에서 보면 색이 다르게 보여요",
    ),
    (
        "색상",
        "실물_염색_편차",
        "염색 편차는 촬영으로 해결되지 않습니다. 입고 검수 기준을 점검하세요.",
        "원단 색이 얼룩덜룩한데 원래 이런 건가요",
    ),
    (
        "사이즈",
        "표기_오타",
        "상세페이지 사이즈표의 L 항목 수치가 실제 표기와 어긋납니다. 표를 정정하세요.",
        "L 주문했는데 상세페이지 사이즈표가 틀린 것 같아요",
    ),
    (
        "사이즈",
        "실측_표기_편차",
        "실측 기준(단면/총장)을 명시하고 허용 오차 범위를 함께 안내하세요.",
        "재보니까 적힌 것보다 2cm 작게 나왔어요",
    ),
    (
        "사이즈",
        "채널_사이즈_표준차이",
        "타 채널과 사이즈 기준이 다르다는 점을 상세페이지 상단에 안내하세요.",
        "다른 쇼핑몰에서 산 같은 사이즈보다 작아요",
    ),
    (
        "소재",
        "소재_정보_누락",
        "혼용률과 세탁 방법을 상세페이지 소재 항목에 추가하세요.",
        "무슨 소재인지 안 적혀 있어서 몰랐어요",
    ),
    (
        "소재",
        "이미지_질감표현_부족",
        "원단을 접었을 때의 두께와 비침 정도가 드러나는 근접 컷을 추가 촬영하세요.",
        "생각보다 훨씬 얇고 비침이 심해요",
    ),
    (
        "소재",
        "실제_원단_문제",
        "원단 품질 결함은 촬영·문구로 해결되지 않습니다. 입고 검수를 점검하세요.",
        "실이 계속 나오고 보풀이 심해요",
    ),
]

CS_SUMMARY_TEMPLATE = "CS 20건 중 14건이 '{label}' 관련 언급"


def build_document(label: str, proposed_text: str) -> str:
    """`record_hitl_outcome()` 과 같은 형식으로 조립한다 — 형식이 갈리면 비교가 무의미하다."""
    return f"{label} {CS_SUMMARY_TEMPLATE.format(label=label)} {proposed_text}"


def build_collection(client: Any, embedding_function: Any, tag: str) -> Any:
    collection = client.create_collection(
        name=f"embed_eval_{tag}_{uuid.uuid4().hex[:8]}",
        embedding_function=embedding_function,
    )
    collection.upsert(
        ids=[label for _, label, _, _ in SAMPLES],
        documents=[build_document(label, text) for _, label, text, _ in SAMPLES],
        metadatas=[{"aspect": aspect, "root_cause_label": label} for aspect, label, _, _ in SAMPLES],
    )
    return collection


def score(collection: Any, *, mode: str) -> tuple[int, list[str]]:
    """top-1 적중 수와 틀린 케이스 설명을 돌려준다.

    운영과 같은 조건으로 조회한다 — aspect 사전 필터 + `SIMILAR_CASE_TOP_N`
    (retrieve_context 는 그중 1위 하나만 쓴다).
    """
    hits = 0
    misses: list[str] = []
    for aspect, label, _, paraphrase in SAMPLES:
        query_text = label if mode == "label" else paraphrase
        result = collection.query(
            query_texts=[query_text],
            n_results=SIMILAR_CASE_TOP_N,
            where={"aspect": aspect},
        )
        ranked = (result.get("ids") or [[]])[0]
        if ranked and ranked[0] == label:
            hits += 1
        else:
            misses.append(f"{label} → {ranked[0] if ranked else '(결과 없음)'}  [{query_text}]")
    return hits, misses


def main(dry_run: bool = False) -> None:
    print(f"표본 {len(SAMPLES)}건 · aspect {len({a for a, _, _, _ in SAMPLES})}종")
    if dry_run:
        for aspect, label, text, paraphrase in SAMPLES:
            print(f"  [{aspect}] {label}")
            print(f"      문서: {build_document(label, text)[:70]}…")
            print(f"      쿼리(paraphrase): {paraphrase}")
        print("\n--dry-run 이라 임베딩 호출 없음.")
        return

    client = chromadb.EphemeralClient()
    arms = [
        ("기본(all-MiniLM-L6-v2, 영어)", DefaultEmbeddingFunction()),
        (f"신규({EMBEDDING_MODEL}, 다국어)", get_embedding_function()),
    ]

    for mode in ("label", "paraphrase"):
        header = "쿼리 = 원인 라벨 (운영과 동일)" if mode == "label" else "쿼리 = 고객 말투 (라벨 문자열 없음)"
        print(f"\n■ {header}")
        for name, embedding_function in arms:
            collection = build_collection(client, embedding_function, tag=mode)
            hits, misses = score(collection, mode=mode)
            print(f"  {name}: top-1 {hits}/{len(SAMPLES)}")
            for miss in misses:
                print(f"      MISS {miss}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="표본만 출력하고 임베딩은 안 부른다.")
    main(dry_run=parser.parse_args().dry_run)
