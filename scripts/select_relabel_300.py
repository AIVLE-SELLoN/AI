"""
71630 재라벨링 300건 선정
==========================
프롬프트1 정식 평가셋(하이브리드 1,000건 = 재라벨링 300 + LLM생성 700) 중
71630 재라벨링 몫 300건을 선정한다.

선정 기준
--------
- 소스: AI Hub 71630, Source=쇼핑몰, MainCategory=여성의류/남성의류
- Split: Training만 (Validation은 최종 1회 검증용으로 따로 보관, 여기서 안 씀)
- 제외: 프롬프트1·프롬프트2 few-shot에 이미 쓴 원문(총 17개) — 시험지 유출 방지
  (재라벨링 300건은 프롬프트1의 "시험지"이므로, 프롬프트1·2가 이미 "본 적 있는"
   문장이 다시 나오면 안 됨 — 이 방향의 미중복 원칙이 하이브리드1000건 결정 때 확정됨)
- 클린 조건: 리뷰 안에 대상 aspect(색상/사이즈+핏/소재)가 딱 하나만 언급된 것만
  (여러 aspect가 섞이면 재라벨링할 때 애매함이 커짐)
- 길이 100자 이하(너무 긴 리뷰는 재라벨링 난이도만 높아짐)

분배: 색상 100 / 사이즈 100 / 소재 100 (균등, 조정하려면 ASPECT_QUOTA 수정)

사용법
------
    python select_relabel_300.py --data-dir ./aihub71630 --seed 11 --outfile relabel_300.csv

--data-dir: 71630 zip을 풀어놓은 폴더(하위에 Training/Validation 폴더가 있는 구조)
"""

import argparse
import csv
import glob
import json
import random
import sys
from collections import defaultdict
from pathlib import Path

# scripts/ 는 저장소 루트의 형제 폴더 — app 패키지를 절대경로로 import하려면
# 저장소 루트를 sys.path에 넣어야 함(실행 방식에 따라 자동으로 안 잡힐 수 있어서 명시)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.console import force_utf8_output

ASPECT_QUOTA = {"색상": 100, "사이즈": 100, "소재": 100}

# 프롬프트1·2 few-shot에 이미 쓴 원문(앞부분 매칭) — 여기 빠진 게 있으면 추가해서 쓸 것
USED_PREFIXES = [
    # 프롬프트1(classify_aspect_v1.md) few-shot 원문
    "베이지색상은 화면과 달리",
    "가성비 좋은 렉스조끼",
    "네이비로 구매했는데 셔츠와 바지가",
    "편하고 간편하게 입을 수 있어서",
    "큰사이즈로 구매하라는",
    "조금 커서 한사이즈 작게",
    "고무밴드가 딱딱해서",
    "기모는 아닌데 모직바지느낌",
    "다른 상품 보다 사이즈가 타이트해서",
    "아들에게 선물했는데 불편하고",
    # 프롬프트2(classify_sentiment_v*.md) few-shot 원문
    "생각보다 원단이 좋네요",
    "구김이 심하고 상의는 66사이즈",
    "재질은 까칠하고 바지는 핏이",
    "핏은 좀 맘에 들지않지만",
    "특히 자켓이 넘작고",
    "완전 맘에들어요 한치수",
    "허벅지도 끼고 입으니",
    "소재,디자인도 좋아요 사이즈가",
    "텐션감이 좋아 착용감은",
    "뱃살이 좀 있는편이라",
    # 재검증으로 추가 — 위 4개(허벅지도/텐션감이/뱃살이 포함)가 v4 현재 텍스트와 매치 안 되는
    # 것을 발견해 v4의 실제 현재 문장으로 재작성하고 9~16번을 전부 추가했다. 기존 것도 다른
    # 버전에서 쓰일 수 있어 안 지우고 그대로 둔다(안전마진).
    "허벅지 부분이 너무 껴서",
    "스판끼가 좋아서 착용감은",
    "허벅지 살이 있는 편이라",
    "블랙이랑 그레이 두 개",
    "밑단이 살짝 우는 것만",
    "허리는 넉넉한데 총장이",
    "사이즈가 좀 크게 나오긴",
    "박음질이 아주 꼼꼼한",
    "색도 예쁘고 재질도",
]

TARGET_MAP = {"색상": "색상", "소재": "소재", "사이즈": "사이즈", "핏": "사이즈"}


def already_used(text: str) -> bool:
    return any(text.startswith(p) or p in text for p in USED_PREFIXES)


def load_71630(data_dir: str) -> list[dict]:
    files = sorted(glob.glob(f"{data_dir}/**/*.json", recursive=True))
    all_data = []
    for fp in files:
        if "__MACOSX" in fp or ".DS_Store" in fp:
            continue
        split = "Training" if "/Training/" in fp else ("Validation" if "/Validation/" in fp else "Unknown")
        with open(fp, encoding="utf-8") as f:
            recs = json.load(f)
        for r in recs:
            r["_split"] = split
            r["_file"] = fp
        all_data.extend(recs)
    return all_data


def build_candidates(all_data: list[dict], max_len: int = 100) -> dict[str, list[dict]]:
    filtered = [
        r for r in all_data
        if r.get("Source") == "쇼핑몰"
        and r.get("MainCategory") in ("여성의류", "남성의류")
        and r["_split"] == "Training"
    ]

    candidates: dict[str, list[dict]] = defaultdict(list)
    for r in filtered:
        if already_used(r["RawText"]) or len(r["RawText"]) > max_len:
            continue
        aspects_in_review = [TARGET_MAP[a["Aspect"]] for a in r["Aspects"] if a["Aspect"] in TARGET_MAP]
        if not aspects_in_review or len(set(aspects_in_review)) != 1:
            continue  # 대상 aspect가 없거나, 여러 개 섞여있으면 제외(클린 조건)

        asp = aspects_in_review[0]
        matched = [a for a in r["Aspects"] if TARGET_MAP.get(a["Aspect"]) == asp][0]
        r["_gold_aspect"] = matched["Aspect"]
        r["_gold_sentiment"] = matched["SentimentPolarity"]
        candidates[asp].append(r)
    return candidates


def select(candidates: dict[str, list[dict]], seed: int) -> list[dict]:
    rng = random.Random(seed)
    selected = []
    for asp, quota in ASPECT_QUOTA.items():
        pool = list(candidates[asp])
        rng.shuffle(pool)
        picked = pool[:quota]
        print(f"  {asp}: 후보 {len(pool)}개 중 {len(picked)}개 선정")
        if len(picked) < quota:
            print(f"    ⚠️ 후보 부족! 목표 {quota}개인데 {len(picked)}개만 있음")
        for r in picked:
            selected.append({
                "target_aspect": asp,
                "raw_text": r["RawText"],
                "gold_71630_aspect_label": r["_gold_aspect"],
                "gold_71630_sentiment": r["_gold_sentiment"],
                "source_file": r["_file"].split("/")[-1],
            })
    rng.shuffle(selected)  # aspect끼리 뭉치지 않게 섞기(재라벨링자 편향 방지)
    return selected


def write_csv(selected: list[dict], outfile: str):
    fieldnames = [
        "id", "target_aspect", "raw_text", "gold_71630_aspect_label", "gold_71630_sentiment",
        "source_file", "relabel_aspect", "relabel_sentiment", "relabel_note",
    ]
    with open(outfile, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for i, row in enumerate(selected, 1):
            row["id"] = f"RELABEL-{i:04d}"
            row["relabel_aspect"] = ""
            row["relabel_sentiment"] = ""
            row["relabel_note"] = ""
            w.writerow(row)


def main():
    # 첫 문장이어야 한다 — 아래 `parse_args()` 가 `--help` 를 먼저 찍고, 그 도움말
    # (`description=__doc__`)에 `—` 가 있다. `app/core/console.py`.
    force_utf8_output()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", required=True, help="71630 압축 풀어놓은 폴더")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--outfile", default="relabel_300.csv")
    args = ap.parse_args()

    print("71630 로딩 중...")
    all_data = load_71630(args.data_dir)
    print(f"전체 레코드 수: {len(all_data)}")

    candidates = build_candidates(all_data)
    print("\n선정 중...")
    selected = select(candidates, args.seed)

    write_csv(selected, args.outfile)
    print(f"\n총 {len(selected)}건 → {args.outfile} 저장 완료")


if __name__ == "__main__":
    main()