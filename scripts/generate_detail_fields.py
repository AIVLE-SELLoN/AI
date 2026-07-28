"""
input_detail_fields.csv / golden_detail_fields.csv 생성기
============================================================
Mock 데이터 정의서 §5-1, §9-1 기준.

- input_detail_fields.csv: 42상품 × 3채널 × 4aspect(색상/사이즈/소재/기타) = 504행
  이 중 Agent3가 실제 호출되는 15개 (product,channel,aspect) 조합만 LLM으로
  그라운딩 텍스트 작성, 나머지 489행은 "정보 없음".
- golden_detail_fields.csv: 위 15개 조합의 ground_truth_evidence(있음/없음/애매) 정답,
  input에서 분리(컨닝 방지) — 파이프라인 미진입, 평가 스크립트 전용.

사용법
------
    python generate_detail_fields.py \
        --products-config config_products.csv \
        --detail-prompt prompts/generate_detail_field_text_v1.md \
        --outdir ./output \
        --seed 11
"""

import argparse
import csv
import json
import re
from pathlib import Path

import asyncio
import sys
from pathlib import Path as _Path

sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))

from app.core.llm_client import get_llm_client
from app.core.exceptions import LlmCallError, LlmParseError

ASPECTS = ["색상", "사이즈", "소재", "기타"]
CHANNELS = ["COUPANG", "NAVER", "ZIGZAG"]

# Mock 정의서 §5-1 명시: 15개 (product,channel,aspect) 조합
# = SC-001~009(9) + SC-029·030·031(각1) + SC-032(2, 쿠팡·네이버) + SC-034(1)
# golden_group_id/channel/aspect/root_cause는 config_anomaly.csv에서 실측 확인됨(대화 중 검증 완료).
# evidence_level: 있음/없음/애매 — 원인 유형(copy_draft/image_guide/스코프한계) 기준 배치,
# SC-030만 팀 확정으로 "있음(고정)"(사진_색감_오차인데도 있음).
FIFTEEN_COMBOS = [
    {"case_id": "SC-001", "golden_group_id": "P019", "channel": "COUPANG", "aspect": "색상",
     "root_cause": "사진_색감_오차", "evidence_level": "없음"},
    {"case_id": "SC-002", "golden_group_id": "P009", "channel": "NAVER", "aspect": "색상",
     "root_cause": "조명_보정_차이", "evidence_level": "없음"},
    {"case_id": "SC-003", "golden_group_id": "P003", "channel": "ZIGZAG", "aspect": "색상",
     "root_cause": "실물_염색_편차", "evidence_level": "없음"},
    {"case_id": "SC-004", "golden_group_id": "P004", "channel": "NAVER", "aspect": "사이즈",
     "root_cause": "표기_오타", "evidence_level": "있음"},
    {"case_id": "SC-005", "golden_group_id": "P005", "channel": "COUPANG", "aspect": "사이즈",
     "root_cause": "실측_표기_편차", "evidence_level": "있음"},
    {"case_id": "SC-006", "golden_group_id": "P006", "channel": "ZIGZAG", "aspect": "사이즈",
     "root_cause": "채널_사이즈_표준차이", "evidence_level": "있음"},
    {"case_id": "SC-007", "golden_group_id": "P007", "channel": "ZIGZAG", "aspect": "소재",
     "root_cause": "이미지_질감표현_부족", "evidence_level": "애매"},
    {"case_id": "SC-008", "golden_group_id": "P008", "channel": "COUPANG", "aspect": "소재",
     "root_cause": "소재_정보_누락", "evidence_level": "없음"},
    {"case_id": "SC-009", "golden_group_id": "P010", "channel": "NAVER", "aspect": "소재",
     "root_cause": "실제_원단_문제", "evidence_level": "없음"},
    {"case_id": "SC-029", "golden_group_id": "P030", "channel": "COUPANG", "aspect": "색상",
     "root_cause": "사진_색감_오차", "evidence_level": "애매"},
    {"case_id": "SC-030", "golden_group_id": "P001", "channel": "COUPANG", "aspect": "색상",
     "root_cause": "사진_색감_오차", "evidence_level": "있음(고정)"},
    {"case_id": "SC-031", "golden_group_id": "P031", "channel": "COUPANG", "aspect": "색상",
     "root_cause": "사진_색감_오차", "evidence_level": "애매"},
    {"case_id": "SC-032", "golden_group_id": "P032", "channel": "COUPANG", "aspect": "색상",
     "root_cause": "", "evidence_level": "있음"},
    {"case_id": "SC-032", "golden_group_id": "P032", "channel": "NAVER", "aspect": "색상",
     "root_cause": "", "evidence_level": "애매"},
    {"case_id": "SC-034", "golden_group_id": "P034", "channel": "COUPANG", "aspect": "색상",
     "root_cause": "사진_색감_오차", "evidence_level": "애매"},
]
assert len(FIFTEEN_COMBOS) == 15
assert sum(1 for c in FIFTEEN_COMBOS if c["evidence_level"] in ("있음", "있음(고정)")) == 5
assert sum(1 for c in FIFTEEN_COMBOS if c["evidence_level"] == "없음") == 5
assert sum(1 for c in FIFTEEN_COMBOS if c["evidence_level"] == "애매") == 5


def load_products(path: str) -> dict[str, dict]:
    with open(path, encoding="utf-8-sig") as f:
        return {r["golden_group_id"]: r for r in csv.DictReader(f)}


def load_detail_prompt(path: str) -> str:
    text = Path(path).read_text(encoding="utf-8")
    m = re.search(r"## 핵심 설계 원칙\s*\n(.*)", text, re.S)
    body = m.group(1).strip() if m else text
    return body


def build_llm_items(products: dict[str, dict]) -> list[dict]:
    items = []
    for combo in FIFTEEN_COMBOS:
        gid = combo["golden_group_id"]
        items.append({
            "golden_group_id": gid,
            "concept_name": products[gid]["concept_name"],
            "channel": combo["channel"],
            "aspect": combo["aspect"],
            "evidence_level": combo["evidence_level"],
            "root_cause": combo["root_cause"] or "(미지정)",
        })
    return items


def generate_detail_texts(prompt_body: str, items: list[dict], use_llm: bool = True) -> dict[tuple, str]:
    """반환: (golden_group_id, channel, aspect) -> detail_text"""
    fallback = {(i["golden_group_id"], i["channel"], i["aspect"]): f"[PLACEHOLDER:{i['evidence_level']}]" for i in items}

    if not use_llm:
        return fallback

    items_json = json.dumps(items, ensure_ascii=False, indent=2)
    full_prompt = prompt_body.replace("{items_json}", items_json)

    user_msg = f"위 15개 항목 전부에 대해 detail_text를 생성하세요.\n\n{items_json}"
    combined_prompt = f"{full_prompt}\n\n---\n\n{user_msg}"  # system 파라미터 없어서 하나로 합침
    trace_key = "detail_fields:15combos"

    try:
        client = get_llm_client()
    except Exception as e:
        print(f"  ⚠️ [{trace_key}] LLM 클라이언트 생성 실패({e}) — 플레이스홀더로 대체")
        return fallback

    for business_attempt in range(1, 3):  # 개수 불일치 시 재시도(예외 재시도는 llm_client가 이미 담당)
        try:
            data = asyncio.run(client.complete_json(combined_prompt, trace_key=trace_key))
        except (LlmCallError, LlmParseError) as e:
            print(f"  ⚠️ [{trace_key}] LLM 호출 실패: {e}")
            break
        texts = data.get("texts", [])
        result = {}
        for t in texts:
            key = (t["golden_group_id"], t["channel"], t["aspect"])
            result[key] = t["detail_text"]
        if len(result) == 15:
            return result
        print(f"  ⚠️ [{trace_key}] 개수 불일치(기대15, 실제{len(result)}) → 재시도")

    print(f"  ⚠️ [{trace_key}] detail_text 생성 최종 실패 → 플레이스홀더 폴백")
    return fallback


def build_input_detail_fields(products: dict[str, dict], llm_texts: dict[tuple, str]) -> list[dict]:
    rows = []
    for gid in products:
        for channel in CHANNELS:
            for aspect in ASPECTS:
                key = (gid, channel, aspect)
                detail_text = llm_texts.get(key, "정보 없음")
                rows.append({
                    "product_group_id": gid,
                    "channel": channel,
                    "aspect": aspect,
                    "detail_text": detail_text,
                })
    return rows


def build_golden_detail_fields() -> list[dict]:
    rows = []
    for combo in FIFTEEN_COMBOS:
        evidence = combo["evidence_level"].replace("(고정)", "")  # golden엔 있음/없음/애매 3종만
        rows.append({
            "golden_group_id": combo["golden_group_id"],
            "channel": combo["channel"],
            "aspect": combo["aspect"],
            "ground_truth_evidence": evidence,
        })
    return rows


def write_csv(rows: list[dict], path: Path):
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--products-config", default="config_products.csv")
    ap.add_argument("--detail-prompt", default="prompts/generate_detail_field_text_v1.md")
    ap.add_argument("--outdir", default="./output")
    ap.add_argument("--no-llm", action="store_true", help="LLM 없이 플레이스홀더로(오프라인 테스트용)")
    args = ap.parse_args()

    products = load_products(args.products_config)
    print(f"상품 {len(products)}개 로딩 완료")

    prompt_body = load_detail_prompt(args.detail_prompt) if Path(args.detail_prompt).exists() else None
    if prompt_body is None:
        print(f"  ⚠️ 프롬프트 파일 {args.detail_prompt} 없음 — 전부 플레이스홀더로 나감")

    llm_items = build_llm_items(products)
    llm_texts = generate_detail_texts(prompt_body or "", llm_items, use_llm=(not args.no_llm and prompt_body is not None))

    input_rows = build_input_detail_fields(products, llm_texts)
    golden_rows = build_golden_detail_fields()

    print(f"input_detail_fields: {len(input_rows)}행 (기대 504)")
    print(f"golden_detail_fields: {len(golden_rows)}행 (기대 15)")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    write_csv(input_rows, outdir / "input_detail_fields.csv")
    write_csv(golden_rows, outdir / "golden_detail_fields.csv")
    print(f"저장 완료 → {outdir}/")


if __name__ == "__main__":
    main()