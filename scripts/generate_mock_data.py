"""
SELLON Mock 상품매핑 데이터 생성기
====================================

역할
----
products_config.csv (내용) 을 읽어서 두 개의 산출물을 만든다.
  1. Golden Mapping  — 정답 테이블. 매칭 알고리즘은 절대 참조하지 않음(채점 전용).
  2. 채널 원본 데이터  — 쿠팡/네이버/지그재그가 실제로 뱉을 법한 모양의 Mock 원본.
     시나리오(정상일치/정규화필요/완전누락)에 따라 일부러 표기를 다르게 하거나
     옵션 정보를 비운다.

설계 원칙
--------
- 상품 "내용"(이름, 옵션 수, 시나리오, 가격)은 코드에 하드코딩하지 않고
  products_config.csv 에서만 관리한다. 상품을 추가/수정하고 싶으면
  이 스크립트를 건드리지 말고 CSV만 고치면 된다.
- 랜덤 요소(옵션 조합 확장, 정규화필요 표기 왜곡, 가격 편차)는 --seed 로 고정
  가능 → 데모 재현성 확보.
- Golden Mapping은 시나리오와 무관하게 항상 완전한 진실을 담는다.
  "완전누락"은 원본 데이터 쪽에서만 구현되고, 정답 테이블은 늘 정확하다.

사용법
------
    python generate_mock_data.py \
        --config products_config.csv \
        --outdir ./output \
        --seed 11 \
        --price-variance 0.15

    # 검증만 다시 돌리고 싶을 때
    python generate_mock_data.py --config products_config.csv --outdir ./output --validate-only
"""

import argparse
import csv
import json
import random
import re
import sys
import time
import asyncio
from pathlib import Path
from pathlib import Path as _Path

# scripts/ 는 저장소 루트의 형제 폴더 — app 패키지를 절대경로로 import하려면
# 저장소 루트를 sys.path에 넣어야 함
sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))

from app.core.console import force_utf8_output
from app.core.llm_client import get_llm_client
from app.core.exceptions import LlmCallError, LlmParseError


# ────────────────────────────────────────────────────────────────
# 고정 상수 (색상/사이즈 풀, 채널 표기 관례) — 필요시 여기만 조정
# ────────────────────────────────────────────────────────────────

COLOR_CODE = {
    "블랙": "BLK", "화이트": "WHT", "베이지": "BEG", "네이비": "NVY", "카키": "KHK",
    "그레이": "GRY", "아이보리": "IVR", "브라운": "BRN", "핑크": "PNK", "레드": "RED",
    "옐로우": "YLW", "그린": "GRN", "블루": "BLU", "퍼플": "PPL", "오렌지": "ORG",
    "카멜": "CML", "데님블루": "DNM", "차콜": "CHR", "와인": "WIN",
}
SIZE_POOL = ["S", "M", "L", "XL", "FREE"]

SCENARIOS = ("정상일치", "정규화필요", "완전누락")

# ── 채널별 상품명 파생 ────────────────────────────────────────────────────
# 원래는 LLM 생성이 맞고, 크레딧 확보 전까지 임시로 동의어 사전 기반 템플릿 치환을 쓴다.
# LLM 예산이 잡히면 이 함수만 교체하면 된다.
SYNONYM_MAP = {
    "원피스": ["원피스", "드레스"],
    "니트": ["니트", "스웨터"],
    "팬츠": ["팬츠", "슬랙스"],
    "자켓": ["자켓", "재킷"],
    "셔츠": ["셔츠", "남방"],
    "스커트": ["스커트", "치마"],
}

NEEDS_NORM_TEMPLATES = [
    "[{size}]{color_kr}",
    "{color_kr}ㆍ{size}",
    "{size}-{color_kr}",
    "{color_kr}　{size}",          # 전각 공백
    "{color_code}_{size}_option",   # 영문코드 혼입 (정규화 난이도 최상)
]


# ────────────────────────────────────────────────────────────────
# 데이터 클래스 대신 단순 dict 사용 (팀 전체가 읽기 쉬운 형태 유지)
# ────────────────────────────────────────────────────────────────

def load_products(config_path: str) -> list[dict]:
    """config_products.csv 를 읽어 상품 목록(dict list)으로 반환.
    option_colors/option_sizes 는 '|' 구분(쉼표는 CSV 컬럼 구분자와 겹쳐 파이프 사용)."""
    with open(config_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    for r in rows:
        r["base_price"] = int(r["base_price"])
        r["colors"] = r["option_colors"].split("|")
        r["sizes"] = r["option_sizes"].split("|")
        r["demo_flag"] = r["demo_flag"].strip().upper() == "Y"
        for ch in ("coupang", "naver", "zigzag"):
            scen = r[f"{ch}_scenario"]
            if scen not in SCENARIOS:
                raise ValueError(
                    f"{r['golden_group_id']}: '{ch}_scenario' 값이 잘못됨 → {scen} "
                    f"(허용값: {SCENARIOS})"
                )
        for c in r["colors"]:
            if c not in COLOR_CODE:
                raise ValueError(f"{r['golden_group_id']}: 색상코드 미등록 → '{c}' (COLOR_CODE에 추가 필요)")
    return rows


# ────────────────────────────────────────────────────────────────
# 옵션 표기 생성 로직 (채널·시나리오별)
# ────────────────────────────────────────────────────────────────

def format_option(channel: str, color_kr: str, color_code: str, size: str,
                   scenario: str, rng: random.Random) -> str | None:
    """시나리오에 맞는 옵션 조합 원본 문자열을 만든다. 완전누락이면 None."""
    if scenario == "완전누락":
        return None

    if scenario == "정상일치":
        if channel == "COUPANG":
            return f"{color_kr}_{size}"
        if channel == "NAVER":
            return f"{color_kr} / {size}"
        if channel == "ZIGZAG":
            return f"{color_code}/{size}"

    if scenario == "정규화필요":
        template = rng.choice(NEEDS_NORM_TEMPLATES)
        return template.format(color_kr=color_kr, color_code=color_code, size=size)

    raise ValueError(f"알 수 없는 시나리오: {scenario}")





class NameGenerator:
    """채널별 상품명 파생 — LLM 생성 + 로컬 캐싱 + 실패 시 동의어 사전 폴백.

    캐시 파일(concept_name → 파생명 리스트)을 두는 이유:
    같은 concept_name에 대해 재실행할 때마다 API를 다시 부르면 비용·시간 낭비.
    한 번 생성한 건 캐시에 저장해두고 재사용한다(비용 통제 원칙: 결과 캐싱).
    """

    def __init__(self, cache_path: str = "channel_name_cache.json", use_llm: bool = True):
        self.cache_path = Path(cache_path)
        self.use_llm = use_llm
        self.cache: dict[str, list[str]] = {}
        if self.cache_path.exists():
            self.cache = json.loads(self.cache_path.read_text(encoding="utf-8"))
        self.llm_client = None
        if use_llm:
            try:
                self.llm_client = get_llm_client()
            except Exception as e:
                print(f"  ⚠️ LLM 클라이언트 생성 실패({e}) — 채널명은 동의어 사전으로 대체됩니다.")
                self.use_llm = False

    def save(self):
        self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")

    def _llm_generate(self, concept_name: str, n: int) -> list[str] | None:
        prompt = f"""다음은 한국 패션 이커머스에서 판매되는 상품의 내부 개념명입니다: "{concept_name}"

이 상품이 서로 다른 쇼핑몰(쿠팡, 네이버 스마트스토어, 지그재그)에 각각 등록될 때
실제로 쓰일 법한, 서로 다르지만 같은 상품임을 알아볼 수 있는 상품명을 {n}개 만들어주세요.

규칙:
- 동의어 치환(예: 원피스↔드레스, 팬츠↔슬랙스, 니트↔스웨터, 자켓↔재킷, 스커트↔치마)이나 어순 변경을 활용해 자연스럽게 다르게 만드세요
- 완전히 다른 상품처럼 보이면 안 됩니다 (과도한 변형 금지, 사람이 봐도 같은 상품임을 알 수 있어야 함)
- {n}개는 서로 겹치지 않게 만드세요
- 다른 설명 없이 JSON으로만 답하세요. 형식: {{"names": ["미디 원피스", "미디 드레스", "봄 미디 원피스"]}}"""

        trace_key = f"concept_name={concept_name}"
        for business_attempt in range(1, 3):  # 개수 부족 시 재시도(예외 재시도는 llm_client가 이미 담당)
            try:
                data = asyncio.run(self.llm_client.complete_json(prompt, trace_key=trace_key))
            except (LlmCallError, LlmParseError) as e:
                print(f"    ⚠️ [{trace_key}] LLM 이름 생성 실패({e}) → 동의어 사전으로 폴백")
                return None
            names = data.get("names") if isinstance(data, dict) else None
            if isinstance(names, list) and len(names) >= n:
                return names[:n]
            print(f"    ⚠️ [{trace_key}] 개수 부족(기대{n}, 실제{len(names) if names else 0}) → 재시도")
        print(f"    ⚠️ [{trace_key}] 개수 부족 재시도 소진 → 동의어 사전으로 폴백")
        return None

    def _synonym_fallback(self, concept_name: str, n: int, rng: random.Random) -> list[str]:
        results = []
        for _ in range(n):
            name = concept_name
            for base, synonyms in SYNONYM_MAP.items():
                if base in name and rng.random() < 0.6:
                    name = name.replace(base, rng.choice(synonyms))
                    break
            words = name.split()
            if len(words) > 1 and rng.random() < 0.4:
                rng.shuffle(words)
                name = " ".join(words)
            results.append(name)
        return results

    def get_variants(self, concept_name: str, n: int, rng: random.Random) -> list[str]:
        """concept_name에 대해 서로 다른 n개의 파생명을 반환 (캐시 우선)."""
        if n == 0:
            return []
        cached = self.cache.get(concept_name, [])
        if len(cached) >= n:
            return cached[:n]

        names = self._llm_generate(concept_name, n) if self.use_llm else None
        if names is None:
            names = self._synonym_fallback(concept_name, n, rng)

        self.cache[concept_name] = names
        return names


# ────────────────────────────────────────────────────────────────
# 메인 생성 로직
# ────────────────────────────────────────────────────────────────

CHANNEL_PREFIX = {"COUPANG": "C", "NAVER": "N", "ZIGZAG": "Z"}


def generate(products: list[dict], seed: int, price_variance: float, name_gen: "NameGenerator"):
    """
    golden_mapping 행 목록과 input_channel_products 행 목록을 함께 생성한다.
    (같은 rng 시퀀스를 공유해야 seed 하나로 둘 다 재현되므로 함께 처리)

    답 노출 방지 설계:
    - variant_row_id 는 상품·채널 정보를 전혀 담지 않는 opaque 일련번호.
      golden_mapping과 input_channel_products를 잇는 유일한 조인 키이며,
      이 값 자체로는 어떤 상품 그룹인지 추측할 수 없어야 한다.
    - channel_product_id 의 채널별 번호는 서로 독립적으로 섞는다.
      (쿠팡 101 / 네이버 101 / 지그재그 101 처럼 숫자가 그대로 맞아떨어지면
       그 상관관계 자체가 매칭 알고리즘에게 답을 알려주는 뒷문이 된다)
    - golden_group_id, mock_scenario_tag 는 golden_mapping에만 존재하고
      input_channel_products 에는 절대 포함하지 않는다.
    """
    rng = random.Random(seed)
    golden_rows: list[dict] = []
    raw_rows: list[dict] = []

    # 채널별 product_id 시퀀스를 각각 독립적으로 섞어서, 채널 간 번호 상관관계를 없앤다
    id_pool = {
        ch: rng.sample(range(1000, 1000 + len(products) * 3), len(products))
        for ch in ("COUPANG", "NAVER", "ZIGZAG")
    }

    variant_counter = 0  # 전역 opaque 일련번호 (상품/채널 정보 미포함)

    for p_idx, p in enumerate(products):
        gid = p["golden_group_id"]
        colors = p["colors"]
        sizes = p["sizes"]

        scenarios = {
            "COUPANG": p["coupang_scenario"],
            "NAVER": p["naver_scenario"],
            "ZIGZAG": p["zigzag_scenario"],
        }
        channel_product_id = {
            ch: f"{CHANNEL_PREFIX[ch]}{id_pool[ch][p_idx]}" for ch in scenarios
        }
        # 채널별 상품명: 정상일치 채널 수만큼만 LLM에게 한 번에 요청(상품당 1회 호출로 배치)
        normal_channels = [ch for ch, s in scenarios.items() if s == "정상일치"]
        variants = name_gen.get_variants(p["concept_name"], len(normal_channels), rng)
        names = {ch: p["concept_name"] for ch in scenarios}  # 기본값(정규화필요/완전누락은 원본 유지)
        for ch, variant_name in zip(normal_channels, variants):
            names[ch] = variant_name

        # 채널별 가격은 상품당 1회만 굴려서, 같은 상품 안의 모든 SKU가 같은 가격 정책을 공유하게 함
        channel_price = {}
        for ch in scenarios:
            variance = rng.uniform(-price_variance, price_variance)
            sale = int(round(p["base_price"] * (1 + variance) / 100)) * 100
            has_discount = rng.random() < 0.3
            original = int(sale * rng.uniform(1.05, 1.2) / 100) * 100 if has_discount else sale
            channel_price[ch] = (sale, original)

        for color_kr in colors:
            color_code = COLOR_CODE[color_kr]
            for size in sizes:
                for ch in ("COUPANG", "NAVER", "ZIGZAG"):
                    variant_counter += 1
                    vrid = f"VR-{variant_counter:04d}"  # opaque — 상품/채널 정보 없음
                    scen = scenarios[ch]

                    golden_rows.append({
                        "variant_row_id": vrid,
                        "golden_group_id": gid,
                        "canonical_option": f"{color_kr}/{size}",  # 표준 정답 표기 (channel 컬럼은 없음 — raw쪽에서 join으로 얻음)
                        "mock_scenario_tag": scen,
                    })

                    option_str = format_option(ch, color_kr, color_code, size, scen, rng)
                    sale, original = channel_price[ch]
                    raw_rows.append({
                        "variant_row_id": vrid,
                        "channel": ch,
                        "channel_product_id": channel_product_id[ch],
                        "channel_product_name": names[ch],
                        "option_group_names": None if option_str is None else "색상,사이즈",
                        "channel_option_name": option_str,
                        "sale_price": sale,
                        "original_price": original,
                        # golden_group_id, mock_scenario_tag 는 여기 넣지 않는다 (답 노출 방지)
                    })

    return golden_rows, raw_rows


# ────────────────────────────────────────────────────────────────
# 검증 (⑤ 단계에서 재사용할 수 있도록 여기 같이 둠)
# ────────────────────────────────────────────────────────────────

def validate(products: list[dict], golden_rows: list[dict], raw_rows: list[dict]) -> dict:
    """
    - 시나리오 실제 분포 vs 목표(60/25/15)
    - 모든 golden_group_id가 3채널 다 갖고 있는지(완결성)
    - golden_mapping과 input_channel_products가 variant_row_id로 1:1 정확히 맞물리는지
    - input_channel_products 쪽에 golden_group_id/mock_scenario_tag가 새고 있지 않은지
    """
    from collections import Counter, defaultdict

    scen_counter = Counter()
    group_channels: dict[str, set] = defaultdict(set)

    for p in products:
        for ch in ("coupang", "naver", "zigzag"):
            scen_counter[p[f"{ch}_scenario"]] += 1

    # golden_mapping엔 channel 컬럼이 없으므로(canonical_option 구조로 변경),
    # raw_rows에서 variant_row_id → channel 매핑을 만들어 join해서 완결성 체크
    vrid_to_channel = {r["variant_row_id"]: r["channel"] for r in raw_rows}
    for row in golden_rows:
        ch = vrid_to_channel.get(row["variant_row_id"])
        if ch:
            group_channels[row["golden_group_id"]].add(ch)

    total = sum(scen_counter.values())
    target = {"정상일치": 0.60, "정규화필요": 0.25, "완전누락": 0.15}
    distribution_report = {
        s: {
            "건수": scen_counter[s],
            "실제비율": round(scen_counter[s] / total, 4),
            "목표비율": target[s],
            "차이": round(scen_counter[s] / total - target[s], 4),
        }
        for s in SCENARIOS
    }

    incomplete_groups = [gid for gid, chs in group_channels.items() if len(chs) != 3]

    golden_ids = {r["variant_row_id"] for r in golden_rows}
    raw_ids = {r["variant_row_id"] for r in raw_rows}
    join_mismatch = golden_ids.symmetric_difference(raw_ids)

    leaked_fields = [k for k in raw_rows[0].keys() if k in ("golden_group_id", "mock_scenario_tag")] if raw_rows else []

    return {
        "총_product_channel_슬롯": total,
        "분포": distribution_report,
        "완결성_이상_그룹": incomplete_groups,
        "완결성_통과": len(incomplete_groups) == 0,
        "조인_불일치_건수": len(join_mismatch),
        "조인_통과": len(join_mismatch) == 0,
        "input_파일_답_노출_필드": leaked_fields,
        "답_비노출_통과": len(leaked_fields) == 0,
    }


# ────────────────────────────────────────────────────────────────
# csv 출력 (Mock Producer / 배치 적재 스크립트가 표준으로 읽는 포맷)
# ────────────────────────────────────────────────────────────────

def write_csv(rows: list[dict], path: Path):
    if not rows:
        path.touch()
        return
    headers = list(rows[0].keys())
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


# ────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────

def main():
    # 첫 문장이어야 한다 — 아래 `parse_args()` 가 `--help` 를 먼저 찍고, 그 도움말
    # (`description=__doc__` · `--golden-outdir` · `--name-cache`)에 `—` 가 있다.
    # `app/core/console.py`.
    force_utf8_output()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config_products.csv", help="상품 시나리오 설정 CSV")
    ap.add_argument("--outdir", default="./output", help="input_*.csv 출력 디렉토리")
    ap.add_argument("--golden-outdir", default=None,
                     help="golden_*.csv 출력 디렉토리(생략 시 --outdir와 동일 — 하위호환). "
                          "README 원칙상 data/golden/처럼 input과 분리된 경로 지정 권장")
    ap.add_argument("--seed", type=int, default=11, help="재현성을 위한 랜덤 시드")
    ap.add_argument("--price-variance", type=float, default=0.15, help="채널별 가격 편차 범위(±)")
    ap.add_argument("--validate-only", action="store_true", help="생성 없이 검증만 수행")
    ap.add_argument("--no-llm-names", action="store_true",
                     help="LLM 호출 없이 동의어 사전으로만 채널별 상품명 생성 (API 키 없을 때)")
    ap.add_argument("--name-cache", default="channel_name_cache.json",
                     help="채널별 상품명 캐시 파일 — 재실행 시 API 재호출 방지")
    args = ap.parse_args()

    products = load_products(args.config)
    name_gen = NameGenerator(cache_path=args.name_cache, use_llm=not args.no_llm_names)
    if not args.no_llm_names:
        print(f"채널별 상품명: LLM 생성 모드 (캐시: {args.name_cache})")
    else:
        print("채널별 상품명: 동의어 사전 모드 (--no-llm-names)")

    golden_rows, raw_rows = generate(products, seed=args.seed, price_variance=args.price_variance, name_gen=name_gen)
    name_gen.save()

    report = validate(products, golden_rows, raw_rows)
    print("\n=== 검증 리포트 ===")
    print(f"총 product×channel 슬롯: {report['총_product_channel_슬롯']}")
    for s, d in report["분포"].items():
        print(f"  {s}: {d['건수']}건 (실제 {d['실제비율']:.1%} / 목표 {d['목표비율']:.0%} / 차이 {d['차이']:+.1%})")

    print(f"완결성 검증: {'통과' if report['완결성_통과'] else '실패 → ' + str(report['완결성_이상_그룹'])}")
    join_status = "통과" if report["조인_통과"] else f"실패 → 불일치 {report['조인_불일치_건수']}건"
    print(f"조인 정합성(variant_row_id 1:1): {join_status}")
    print(f"답 비노출(input 파일에 golden_group_id/mock_scenario_tag 없음): "
          f"{'통과' if report['답_비노출_통과'] else '실패 → ' + str(report['input_파일_답_노출_필드'])}")

    if args.validate_only:
        return

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    golden_outdir = Path(args.golden_outdir) if args.golden_outdir else outdir
    golden_outdir.mkdir(parents=True, exist_ok=True)
    write_csv(golden_rows, golden_outdir / "golden_mapping.csv")
    write_csv(raw_rows, outdir / "input_channel_products.csv")
    print(f"\n생성 완료 → {golden_outdir}/golden_mapping.csv, {outdir}/input_channel_products.csv")


if __name__ == "__main__":
    main()