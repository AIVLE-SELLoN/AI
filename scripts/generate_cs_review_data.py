"""
SELLON CS·리뷰 Mock 데이터 생성기 — 뼈대(skeleton)
====================================================

역할
----
config_anomaly.csv(서영, 케이스 주문서) + config_products.csv(상품 정의)를 읽어서
input_cs_inquiries.csv / golden_cs_labels.csv / input_reviews.csv / golden_review_labels.csv
를 만든다.

지금 이 파일이 하는 것 / 안 하는 것
--------------------------------
- 한다: config 읽기 → 상품×채널×aspect×day별 "총 건수/부정 건수" 계산 → 그 개수만큼
        텍스트로 채워서 CSV 4종 출력. 분모용(denom) 텍스트는 templates.yaml에서 실제 채움(완료).
        원인분류 투입분(cause, 케이스당 20건 확정)도 LLM 배치 호출로 실제 문장 채움(완료).
- 안 한다(자리만 비워둠, TODO 표시):
    1. validate_against_config() — Fisher→BH-FDR→min_delta 정식 검산은 validate_anomaly.py로 별도 구현됨(완료).
       (참고: 소규모 개수 검증은 verify_counts.py 로 별도 제공)

사용법
------
    python generate_cs_review_data.py \
        --anomaly-config config_anomaly.csv \
        --products-config config_products.csv \
        --mapping-dir ./mapping_output \
        --outdir ./output \
        --anchor-date 2026-08-28 \
        --seed 11

--anchor-date: Day 60에 해당하는 실제 날짜(발표일). 지인님 요청사항(§1-2) —
발표일이 유동적이라 Day 번호만 config에 두고, 실제 날짜는 실행 시점에 주입.
"""

import argparse
import csv
import json
import random
import re
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import asyncio
import sys
from pathlib import Path as _Path

# scripts/ 는 저장소 루트의 형제 폴더 — app 패키지를 절대경로로 import하려면
# 저장소 루트를 sys.path에 넣어야 함(실행 방식에 따라 자동으로 안 잡힐 수 있어서 명시)
sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))

from app.core.console import force_utf8_output
from app.core.llm_client import get_llm_client
from app.core.exceptions import LlmCallError, LlmParseError
import yaml

# cause 프롬프트는 이 스크립트 옆(scripts/prompts/)에 산다. cwd 가 어디든 같은 파일을
# 가리켜야 한다 — 저장소 루트에서 돌렸을 때만 조용히 플레이스홀더로 빠지는 일을 막는다.
DEFAULT_CAUSE_PROMPT = Path(__file__).resolve().parent / "prompts" / "generate_cause_text_v3.md"
CAUSE_PLACEHOLDER_PREFIX = "[PLACEHOLDER:cause:"

# ────────────────────────────────────────────────────────────────
# 배경(비케이스) 상품용 baseline — 서영님 시나리오 정의서 §1 표 + 기타=3%(합의)
# ────────────────────────────────────────────────────────────────

BASELINE_RATE = {
    # aspect: {channel: rate}
    "색상": {"COUPANG": 0.05, "NAVER": 0.06, "ZIGZAG": 0.07},
    "사이즈": {"COUPANG": 0.08, "NAVER": 0.09, "ZIGZAG": 0.07},
    "소재": {"COUPANG": 0.04, "NAVER": 0.05, "ZIGZAG": 0.06},
    "파손": {"COUPANG": 0.02, "NAVER": 0.02, "ZIGZAG": 0.02},
    "오배송": {"COUPANG": 0.01, "NAVER": 0.01, "ZIGZAG": 0.01},
    "기타": {"COUPANG": 0.03, "NAVER": 0.03, "ZIGZAG": 0.03},  # 회신 확정값
}
ASPECTS = list(BASELINE_RATE.keys())
REVIEW_ASPECTS = ["색상", "사이즈", "소재"]
"""리뷰는 프롬프트2 스코프만 — 파손·오배송·기타 없음.

⚠️ **두 군데에 적지 말 것.** 이 목록의 길이가 `_negative_rate` 의 부정률 배수라,
   갈리면 라벨이 조용히 틀려진다(라우팅만 갈리던 예전과 다르다)."""
CHANNELS = ["COUPANG", "NAVER", "ZIGZAG"]
SOURCES = ["cs", "review"]

# ────────────────────────────────────────────────────────────────
# BASELINE_RATE 의 분모 — `--baseline-denominator`
# ────────────────────────────────────────────────────────────────
# "total"  : (상품,채널,source) 총문의 중 해당 aspect 부정 비율. **확정 스펙이자 기본값.**
# "aspect" : 그 aspect 로 배정된 문의 중 부정 비율. 2026-08-09 이전 배경 경로의 동작으로,
#            옛 데이터를 재현할 때만 쓴다.
#
# 왜 total 이 스펙인가 — 세 곳이 이미 total 로 못박혀 있다.
#   1. 「이상탐지 시나리오 정의서[확정]」 §1: 분모 = 해당 상품의 해당 채널 총 문의 수
#      (최소표본 항목에 "(상품,채널) 총문의(= 분모, aspect 무관)" 라고 직접 적혀 있다)
#   2. data/config/config_anomaly.csv: SC-001 쿠팡 색상 past_neg=40 / past_total=800.
#      800 = 28일 x 28건/일 = CS 전체 볼륨이지 색상만의 분모가 아니다.
#   3. app/detection/aggregate.py: 탐지 분모도 (product, channel, source) 총문의, aspect 무관.
#
# 즉 이건 정의 선택이 아니라 **구현이 확정 스펙을 어긴 결함**이다. 케이스 경로는
# past_neg/past_total 로 전체 분모에 정확 건수를 심는데(build_rows_for_window_group),
# 배경 경로만 aspect 내부 분모로 깔려서 한 파일 안에 규약이 두 개였다.
#
# 물증(2026-08-07 감사): TRUE config 33행 전부 past_rate == BASELINE_RATE 일치(33/33),
# 평균 case-past 관측률 5.03% vs 평균 순수 배경 0.85% = 6.04배. 6.04 는 CS aspect 수(6)다.
BASELINE_DENOMINATOR_TOTAL = "total"
BASELINE_DENOMINATOR_ASPECT = "aspect"


def _negative_rate(aspect: str, channel: str, n_aspects: int, denominator: str) -> float:
    """이 문서 1건이 `aspect` 부정일 확률.

    배경 경로는 문서를 aspect 로 먼저 쪼갠 뒤 그 안에서 뽑는다. 그래서 전체 분모 기준
    비율을 맞추려면 aspect 수만큼 되돌려 곱해야 한다 — 그러지 않으면 전체 분모로 볼 때
    `config / n_aspects` 로 희석된다(CS 1/6, 리뷰 1/3).

    ⚠️ 1.0 을 넘으면 **조용히 자르지 않고 죽는다.** 잘라내면 그 aspect 만 요청보다 낮은
       부정률로 생성되는데, 그게 정확히 이 함수가 고치려던 "명세와 구현이 조용히 갈리는"
       문제다. 현재 표의 최대치는 사이즈/NAVER 0.09 x 6 = 0.54 라 여유가 있다.
       (현진님 리뷰 5차)
    """
    rate = BASELINE_RATE[aspect][channel]
    if denominator == BASELINE_DENOMINATOR_TOTAL:
        rate *= n_aspects
    if rate > 1.0:
        raise ValueError(
            f"부정률이 1을 넘는다: {aspect}/{channel} "
            f"{BASELINE_RATE[aspect][channel]} x {n_aspects} = {rate:.3f}. "
            "BASELINE_RATE 를 올렸거나 aspect 를 늘렸다면 표 자체를 재검토할 것."
        )
    return rate

# 배경 상품 볼륨 (회신 확정값)
BG_CS_CUR_TOTAL, BG_CS_PAST_TOTAL = 42, 168     # 일 6건 x 7일 / x28일
BG_REVIEW_CUR_TOTAL, BG_REVIEW_PAST_TOTAL = 14, 56  # 일 2건


# ────────────────────────────────────────────────────────────────
# 1. config 읽기
# ────────────────────────────────────────────────────────────────

def load_anomaly_config(path: str) -> list[dict]:
    with open(path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k in ("window_start_day", "window_end_day", "past_neg", "past_total", "cur_neg", "cur_total"):
            r[k] = int(r[k])
    return rows


def load_products_config(path: str) -> list[dict]:
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def get_case_products(anomaly_rows: list[dict]) -> set[str]:
    return {r["golden_group_id"] for r in anomaly_rows}


def get_background_products(all_products: list[dict], case_products: set[str]) -> list[str]:
    return [p["golden_group_id"] for p in all_products if p["golden_group_id"] not in case_products]


# ────────────────────────────────────────────────────────────────
# 2. 상품ID → 채널별 channel_product_id 조회
#    (golden_mapping.csv + input_channel_products.csv 조인 — 상품매핑 산출물 재사용)
#    없으면 플레이스홀더로 대체(매핑 파일 아직 42상품 기준으로 재생성 전이면 이 경로를 탐)
# ────────────────────────────────────────────────────────────────

def load_channel_product_id_map(mapping_dir: str | None, golden_mapping_dir: str | None = None) -> dict[tuple[str, str], str]:
    """(golden_group_id, channel) -> channel_product_id
    golden_mapping_dir 생략 시 mapping_dir과 동일(하위호환 — golden/input이 한 폴더에 같이 있던 예전 구조)."""
    if not mapping_dir:
        return {}
    mapping_dir = Path(mapping_dir)
    golden_dir = Path(golden_mapping_dir) if golden_mapping_dir else mapping_dir
    golden_path = golden_dir / "golden_mapping.csv"
    raw_path = mapping_dir / "input_channel_products.csv"
    if not (golden_path.exists() and raw_path.exists()):
        print(f"  ⚠️ 매핑 파일을 찾을 수 없음(golden: {golden_path}, input: {raw_path}) — 플레이스홀더 ID로 대체")
        return {}

    with open(golden_path, encoding="utf-8-sig") as f:
        golden_rows = list(csv.DictReader(f))
    with open(raw_path, encoding="utf-8-sig") as f:
        raw_rows = {r["variant_row_id"]: r for r in csv.DictReader(f)}

    result = {}
    for g in golden_rows:
        vrid = g["variant_row_id"]
        if vrid in raw_rows:
            key = (g["golden_group_id"], raw_rows[vrid]["channel"])
            if key not in result:  # 대표값 1개면 충분(같은 상품·채널은 channel_product_id 동일)
                result[key] = raw_rows[vrid]["channel_product_id"]
    return result


def get_channel_product_id(pid_map: dict, golden_group_id: str, channel: str) -> str:
    return pid_map.get((golden_group_id, channel), f"PLACEHOLDER-{golden_group_id}-{channel}")


# ────────────────────────────────────────────────────────────────
# 3. 날짜 계산 — Day 번호 → 실제 날짜 (--anchor-date가 Day 60)
# ────────────────────────────────────────────────────────────────

def day_to_date(day: int, anchor_date: datetime, anchor_day: int = 60) -> datetime:
    return anchor_date - timedelta(days=(anchor_day - day))


# ────────────────────────────────────────────────────────────────
# 4. ⚠️ TODO — 실제 텍스트 생성 (다음 단계에서 채울 자리)
#    - 원인분류 투입분(케이스당 ~30건): LLM 생성 예정
#    - 분모용 나머지: 템플릿 변형 예정
#    지금은 자리표시자만 반환.
# ────────────────────────────────────────────────────────────────

CAUSE_SAMPLE_COUNT = 20  # 케이스당 원인분류 투입 문의 수 (Mock 문서 §부록G-2, 30건에서 20건으로 확정)


def parse_cause_distribution(s: str) -> dict[str, float]:
    """'사진_색감_오차:0.7,조명_보정_차이:0.1,...' → {'사진_색감_오차':0.7, ...}"""
    if not s:
        return {}
    result = {}
    for part in s.split(","):
        name, ratio = part.split(":")
        result[name.strip()] = float(ratio)
    return result


def compute_cause_counts(dist: dict[str, float], total: int = CAUSE_SAMPLE_COUNT) -> dict[str, int]:
    """비율을 total건에 맞춰 정수로 배분한다(largest-remainder method로 합계가 정확히 total이 되게)."""
    if not dist:
        return {}
    raw = {k: v * total for k, v in dist.items()}
    counts = {k: int(v) for k, v in raw.items()}
    remainder = total - sum(counts.values())
    fracs = sorted(raw.items(), key=lambda kv: kv[1] - int(kv[1]), reverse=True)
    for i in range(remainder):
        counts[fracs[i][0]] += 1
    return counts


class TextGenerator:
    """CS·리뷰 텍스트 생성.
    - is_cause_sample=False(분모용, denom): templates.yaml에서 랜덤 선택
    - cause(원인분류 투입분, 케이스당 20건): LLM 배치 호출(케이스당 1회)로 실제 생성, 로컬 캐싱
    """

    def __init__(self, templates_path: str | None, rng: random.Random,
                 cause_prompt_path: str | None = None,
                 cause_cache_path: str = "cause_text_cache.json", use_llm: bool = True):
        self.rng = rng
        self.templates = {}
        if templates_path and Path(templates_path).exists():
            with open(templates_path, encoding="utf-8") as f:
                self.templates = yaml.safe_load(f) or {}

        self.use_llm = use_llm
        self.llm_requested = use_llm
        self.llm_client = None
        self.llm_init_error: Exception | None = None
        if use_llm:
            try:
                self.llm_client = get_llm_client()
            except Exception as e:
                # 캐시가 100% 차 있으면 LLM 없이도 재생성할 수 있으므로 여기서 바로 죽이지
                # 않는다. 대신 첫 캐시 미스에서 중단한다. 정상 실행이 플레이스홀더로 조용히
                # 내려가는 것은 금지하고, 그 동작은 --no-llm-cause 에서만 허용한다.
                print(f"  ⚠️ LLM 클라이언트 생성 실패({e}) — 캐시 미스가 있으면 중단합니다.")
                self.llm_init_error = e
                self.use_llm = False
        self.cause_system_prompt = None
        if cause_prompt_path and Path(cause_prompt_path).exists():
            text = Path(cause_prompt_path).read_text(encoding="utf-8")
            m = re.search(r"## System Prompt\s*\n(.*)", text, re.S)
            self.cause_system_prompt = m.group(1).strip() if m else text

        self.cause_cache_path = Path(cause_cache_path)
        self.cause_cache: dict[str, list[dict]] = {}
        if self.cause_cache_path.exists():
            self.cause_cache = json.loads(self.cause_cache_path.read_text(encoding="utf-8"))

    def save_cause_cache(self):
        self.cause_cache_path.write_text(
            json.dumps(self.cause_cache, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def generate_cause_batch(self, case_id: str, aspect: str, cause_counts: dict[str, int]) -> list[dict]:
        """1라운드: cause_counts 전체를 한 번에 요청.
        2라운드부터: 부족한 원인만 "그 부족분만큼만" 추가 요청(전체 재생성 안 함 — 비용 절감).
        작은 개수 요청일수록 LLM이 정확하다는 게 실측으로 확인돼서(2·2·2는 항상 정확,
        14처럼 큰 수만 자주 부족), 부족분 보충 쪽이 오히려 성공률도 높고 저렴하다.
        반환: [{"cause": "...", "text": "..."}, ...] (개수 = sum(cause_counts.values()))"""
        cache_key = f"{case_id}:{aspect}"
        if cache_key in self.cause_cache:
            cached = self.cause_cache[cache_key]
            if self.llm_requested and any(
                str(item.get("text", "")).startswith(CAUSE_PLACEHOLDER_PREFIX)
                for item in cached
            ):
                raise RuntimeError(
                    f"[{cache_key}] cause 캐시에 정답 노출 플레이스홀더가 있습니다. "
                    "정상 cause_text_cache.json 으로 교체하세요."
                )
            return cached

        total = sum(cause_counts.values())
        trace_key = f"case_id={case_id};aspect={aspect}"

        def _placeholder(cause):
            return {"cause": cause, "text": f"[PLACEHOLDER:cause:{aspect}:{cause}]"}

        if not self.use_llm or not self.cause_system_prompt:
            if self.llm_requested:
                reason = (
                    f"LLM 클라이언트 초기화 실패: {self.llm_init_error}"
                    if self.llm_init_error
                    else "cause 프롬프트를 읽지 못함"
                )
                raise RuntimeError(
                    f"[{trace_key}] cause 캐시 미스인데 생성할 수 없습니다 ({reason}). "
                    "정답이 노출되는 플레이스홀더는 저장하지 않습니다. "
                    "정상 캐시를 받거나, 채점하지 않을 스모크 테스트라면 "
                    "--no-llm-cause 를 명시하세요."
                )
            items = [_placeholder(c) for c, n in cause_counts.items() for _ in range(n)]
            self.cause_cache[cache_key] = items
            return items

        collected: list[dict] = []
        remaining = dict(cause_counts)
        rounds_attempted = 0

        for round_num in range(1, 5):  # 1라운드(전체) + 부족분 보충 최대 3라운드
            rounds_attempted = round_num
            req_lines = "\n".join(f"- {c}: {n}건" for c, n in remaining.items() if n > 0)
            if round_num == 1:
                user_msg = f"aspect: {aspect}\n요청 개수:\n{req_lines}\n총 {total}건, 위 개수를 정확히 지켜서 생성하세요."
            else:
                shortfall = sum(remaining.values())
                already_written = "\n".join(f"- {i['text']}" for i in collected)
                user_msg = (
                    f"aspect: {aspect}\n이전 요청에서 아래 원인이 부족했습니다. "
                    f"정확히 이 개수만큼만 추가로 생성하세요(총 {shortfall}건):\n{req_lines}\n\n"
                    f"⚠️ 아래는 이미 작성된 문장들입니다. 이것들과 겹치지 않는 새로운 문장을 쓰세요:\n{already_written}"
                )
            combined_prompt = f"{self.cause_system_prompt}\n\n---\n\n{user_msg}"

            try:
                data = asyncio.run(
                    self.llm_client.complete_json(combined_prompt, trace_key=f"{trace_key};round={round_num}")
                )
            except (LlmCallError, LlmParseError) as e:
                print(f"    ⚠️ [{trace_key}] round{round_num} LLM 호출 실패: {e}")
                break  # llm_client 자체 재시도까지 다 실패 — 더 시도해도 소용없음

            items = data.get("texts", [])
            got_this_round = Counter()
            seen_texts = {i["text"] for i in collected}  # 이미 확보된 문장(전체 라운드 누적) — 중복 강제 차단
            for item in items:
                c = item.get("cause")
                text = item.get("text", "")
                if remaining.get(c, 0) > got_this_round[c] and text not in seen_texts:
                    collected.append(item)
                    got_this_round[c] += 1
                    seen_texts.add(text)
                # else: 중복 문장이거나 이미 채워진 원인 → 버리고, 그 개수는 여전히 "부족"으로 남김

            for c in list(remaining):
                remaining[c] -= got_this_round.get(c, 0)
            remaining = {c: n for c, n in remaining.items() if n > 0}

            if not remaining:
                self.cause_cache[cache_key] = collected
                return collected
            print(f"    ⚠️ [{trace_key}] round{round_num} 후에도 부족: {remaining} → 보충 요청")

        # 정상 실행에서 부족분을 플레이스홀더로 채우면 원인 라벨이 본문에 노출돼 [6] 채점이
        # 무효가 된다. 부분 결과도 캐시에 넣지 않고 전체 실행을 중단해 다음 실행이 다시
        # 생성하게 한다. 명시적인 --no-llm-cause 는 위의 전용 경로에서만 허용한다.
        raise RuntimeError(
            f"[{trace_key}] {rounds_attempted}라운드 시도 후에도 cause 텍스트 "
            f"{sum(remaining.values())}건이 "
            "부족합니다. 플레이스홀더·부분 캐시는 저장하지 않습니다."
        )

    def generate(self, aspect: str, sentiment: int, source: str, is_cause_sample: bool = False) -> str:
        sent_label = {-1: "부정", 0: "중립", 1: "긍정"}[sentiment]

        pool = (
            self.templates.get(source, {})
            .get(aspect, {})
            .get(str(sentiment), [])
        )
        if pool:
            return self.rng.choice(pool)

        # 해당 조합의 템플릿이 비어있으면(예: 파손/오배송의 중립·긍정처럼 애초에 없는 조합)
        # 조용히 실패하지 않고 명시적으로 표시 — 나중에 템플릿 누락을 바로 알아챌 수 있게
        return f"[TEMPLATE_MISSING:{source}:{aspect}:{sent_label}]"


def distribute_daily_negatives(total_neg: int, n_days: int, daily_pattern: str,
                                spike_day: int | None, spike_count: int | None = None) -> list[int]:
    """윈도우 안에서 하루하루 부정 건수를 어떻게 나눌지 결정.
    - uniform: 균등 분배
    - spike: spike_day(1-indexed, 윈도우 내 며칠째)에 spike_count건을 명시적으로 배정하고,
      나머지(total_neg - spike_count)는 남은 날짜에 균등분배.
      spike_count가 없으면(구버전 config 호환용) "균등분배 후 잔여를 스파이크일에 몰아준다"는
      예전 역산 방식으로 폴백 — 단, 이 폴백은 실제 spike_count와 다를 수 있어 안전망일 뿐임.
    """
    if daily_pattern != "spike" or not spike_day:
        base = total_neg // n_days
        rem = total_neg % n_days
        return [base + (1 if d < rem else 0) for d in range(n_days)]

    spike_idx = spike_day - 1  # 1-indexed → 0-indexed

    if spike_count is not None:
        # config에 명시된 정답을 그대로 사용 (역산 금지 — 이게 진짜 정답이므로)
        remaining = total_neg - spike_count
        other_days = n_days - 1
        base = remaining // other_days if other_days else 0
        rem = remaining % other_days if other_days else 0
        daily = []
        rem_used = 0
        for d in range(n_days):
            if d == spike_idx:
                daily.append(spike_count)
            else:
                extra = 1 if rem_used < rem else 0
                daily.append(base + extra)
                rem_used += extra
        return daily

    # ⚠️ 폴백(spike_count 없는 구버전 config용) — 균등분배 후 잔여를 스파이크일에 역산
    base = total_neg // n_days
    daily = [base] * n_days
    non_spike_sum = base * (n_days - 1)
    daily[spike_idx] = total_neg - non_spike_sum
    return daily


def group_anomaly_rows(anomaly_rows: list[dict]) -> dict:
    """같은 (상품,채널,source,윈도우)를 공유하는 행끼리 묶는다.
    SC-029처럼 "색상+파손 동시 편중"인 경우, 같은 채널·같은 윈도우에 aspect가 다른
    행이 2개 이상 나옴 — 이걸 각각 독립적으로 처리하면 같은 200건짜리 창을
    두 번(400건) 만들어버리는 사고가 생긴다. 반드시 묶어서 "한 창에 부정 몫만 여러 개"로
    처리해야 한다."""
    groups: dict[tuple, list[dict]] = {}
    for row in anomaly_rows:
        key = (row["golden_group_id"], row["channel"], row["source"],
               row["window_start_day"], row["window_end_day"])
        groups.setdefault(key, []).append(row)
    return groups

def build_rows_for_window_group(rows: list[dict], rng: random.Random, pid_map: dict,
                                 anchor_date: datetime, id_counters: dict, text_gen: "TextGenerator",
                                 baseline_denominator: str = BASELINE_DENOMINATOR_TOTAL) -> tuple[list, list]:
    """group_anomaly_rows()로 묶인 그룹(같은 상품·채널·source·윈도우, aspect만 다를 수 있음)
    → (문의/리뷰 행 리스트, 정답 행 리스트).
    SC-029(색상+파손 동시 편중)처럼 aspect가 2개 이상이어도, 창은 딱 한 번만 생성하고
    그 안에서 각 aspect의 부정 몫만 나눠 배정한다 — 절대로 aspect마다 따로 창을 만들지 않는다."""
    first = rows[0]
    source = first["source"]
    channel = first["channel"]
    gid = first["golden_group_id"]
    cpid = get_channel_product_id(pid_map, gid, channel)

    # 그룹 안 모든 행이 같은 total을 선언하고 있어야 정상(같은 창을 각자 관점에서 적었을 뿐이므로)
    for r in rows[1:]:
        assert r["past_total"] == first["past_total"] and r["cur_total"] == first["cur_total"], \
            f"같은 그룹인데 past_total/cur_total이 다름: {rows}"

    # ⚠️ set 이면 안 된다. 아래 reserved_neg 가 이 순서를 dict 키 순서로 물려받고,
    #    그게 "하루치 부정 슬롯을 어느 aspect 가 먼저 가져가나"를 정한다. 파이썬 str 해시는
    #    PYTHONHASHSEED 를 안 박으면 프로세스마다 무작위라, 같은 코드·같은 seed 로도
    #    실행마다 색상/파손 순서가 뒤집혀 다른 코퍼스가 나온다. 집계(aspect 별 부정 건수)는
    #    같아서 verify_counts 도 행수 검산도 통과한다 — 조용히 갈린다.
    #    dict.fromkeys 면 config 행 순서로 고정되고 `in` 은 그대로 O(1) 이다.
    group_aspects = dict.fromkeys(r["aspect"] for r in rows)  # 이 창에서 부정 몫이 정해진 aspect들

    data_rows, label_rows = [], []

    windows = [
        (first["window_start_day"], first["window_end_day"], first["cur_total"], "cur_neg", True),
        (first["window_start_day"] - 28, first["window_start_day"] - 1, first["past_total"], "past_neg", False),
    ]
    # ⚠️ 순서 반전(서영님 실험⑥ 피드백, 2026-08-05): cause_queue(aspect당 CAUSE_SAMPLE_COUNT개)를
    # 과거·현재 두 창이 공유하는데, 원래는 과거(28일)가 먼저 돌아서 cause_queue를 거의 다 소진하고
    # 현재 윈도우(7일)엔 하나도 안 남았음. 운영 [6] 원인분류는 현재 윈도우 텍스트만 읽으므로
    # (과거 윈도우 cause 텍스트는 어디서도 안 읽음 — 과거는 베이스라인 rate 계산용일 뿐),
    # 현재 윈도우가 cause_queue를 먼저 전부 가져가도록 순서를 바꿈. 배분비율은 100:0(현재:과거)
    # — 과거에 나눠줄 이유가 없음(아무도 안 읽는 텍스트에 유한한 LLM 생성 샘플을 나눠주는 건 낭비).

    # aspect별로 (일별 부정 배분, 원인분류 텍스트 큐) 미리 계산
    per_aspect = {}
    for r in rows:
        daily_pattern = r.get("daily_pattern", "uniform") or "uniform"
        spike_day = int(r["spike_day"]) if r.get("spike_day") else None
        spike_count = int(r["spike_count"]) if r.get("spike_count") else None
        cause_dist = parse_cause_distribution(r["cause_distribution"])
        # ⚠️ cause_distribution 컬럼이 같은 case_id의 모든 채널 행에 동일하게 채워져 있을 수 있음
        # (예: NAVER·ZIGZAG처럼 intended_answer=FALSE인 비발화 채널에도 값이 들어있는 경우).
        # 원인분류는 "편중형으로 발화한 채널"에서만 의미가 있으므로(로직 [6]),
        # intended_answer가 TRUE인 행에서만 cause 풀을 실제로 활성화한다.
        is_fired_row = r.get("intended_answer", "").strip().upper() == "TRUE"
        is_cause_pool = bool(cause_dist) and is_fired_row

        cause_queue = []
        if is_cause_pool:
            cause_counts = compute_cause_counts(cause_dist, total=CAUSE_SAMPLE_COUNT)
            cause_queue = list(text_gen.generate_cause_batch(r["case_id"], r["aspect"], cause_counts))
            # ⚠️ 캐시에 저장된 리스트를 그대로 쓰면 pop(0)이 캐시 원본까지 같이 지워버림
            # (list는 참조 타입) — 반드시 복사본을 만들어서 이번 그룹 전용으로 소모해야 함.

        per_aspect[r["aspect"]] = {
            "daily_pattern": daily_pattern, "spike_day": spike_day, "spike_count": spike_count,
            "past_neg": r["past_neg"], "cur_neg": r["cur_neg"],
            "is_cause_pool": is_cause_pool, "cause_queue": cause_queue,
        }

    for start_day, end_day, total, neg_key, is_current_window in windows:
        n_days = end_day - start_day + 1
        per_day = total // n_days
        remainder = total % n_days

        # aspect별 일별 부정 배분(현재윈도우만 daily_pattern 적용, 과거는 항상 균등)
        neg_by_day_per_aspect = {}
        for asp, cfg in per_aspect.items():
            pattern = cfg["daily_pattern"] if is_current_window else "uniform"
            neg_val = cfg["cur_neg"] if is_current_window else cfg["past_neg"]
            neg_by_day_per_aspect[asp] = distribute_daily_negatives(
                neg_val, n_days, pattern, cfg["spike_day"], cfg["spike_count"]
            )

        for d in range(n_days):
            day_no = start_day + d
            day_total = per_day + (1 if d < remainder else 0)
            date = day_to_date(day_no, anchor_date)

            # 오늘 이 (채널,창) 안에서 각 aspect가 정확히 몇 건씩 부정으로 확정됐는지
            reserved_neg = {asp: min(neg_by_day_per_aspect[asp][d], day_total) for asp in group_aspects}
            total_reserved = sum(reserved_neg.values())
            if total_reserved > day_total:
                # 안전장치: 여러 aspect의 부정 합이 그날 총건수를 넘으면 비례 축소(이론상 거의 안 일어남)
                scale = day_total / total_reserved
                reserved_neg = {a: int(v * scale) for a, v in reserved_neg.items()}

            # 아이템별로 "오늘 몫이 남은 aspect"부터 순서대로 채움
            remaining = dict(reserved_neg)
            for i in range(day_total):
                item_aspect = None
                for asp, left in remaining.items():
                    if left > 0:
                        item_aspect, sentiment = asp, -1
                        remaining[asp] -= 1
                        break

                if item_aspect is None:
                    # 이 그룹의 모든 aspect 부정 몫을 다 채웠음 — 나머지는 배경(무관한 문의)
                    # ⚠️ 리뷰는 프롬프트2 스코프(색상·사이즈·소재)만 — 파손·오배송·기타 없음
                    bg_aspect_pool = REVIEW_ASPECTS if source == "review" else ASPECTS
                    item_aspect = rng.choice(bg_aspect_pool)
                    if item_aspect in group_aspects:
                        # ⚠️ 이 그룹에 속한 aspect는 이미 위에서 "정확 건수"로 부정 몫을 다 심었음
                        # (plant 원칙=결정론). 배경에서 또 -1이 나오면 안 됨.
                        # ⚠️ 이 가드는 baseline_denominator 와 무관하게 유지해야 한다 —
                        #    여기서 -1 이 새면 config 의 cur_neg/past_neg 가 깨져 intended_answer 가 흔들린다.
                        sentiment = rng.choice([0, 1])
                    else:
                        item_rate = _negative_rate(
                            item_aspect, channel, len(bg_aspect_pool), baseline_denominator
                        )
                        sentiment = -1 if rng.random() < item_rate else rng.choice([0, 0, 1])

                cfg = per_aspect.get(item_aspect)
                use_cause = (
                    cfg is not None and cfg["is_cause_pool"] and cfg["cause_queue"]
                    and sentiment == -1 and item_aspect in group_aspects
                )
                cause_item = cfg["cause_queue"].pop(0) if use_cause else None

                id_counters[source] += 1
                if source == "cs":
                    rid = f"INQ-{id_counters[source]:06d}"
                    text = cause_item["text"] if cause_item else text_gen.generate(item_aspect, sentiment, source)
                    data_rows.append({
                        "inquiry_id": rid, "channel": channel, "channel_product_id": cpid,
                        "content": text, "inquired_at": date.strftime("%Y-%m-%dT%H:%M:%S"),
                    })
                    label_rows.append({
                        "inquiry_id": rid, "true_aspect": item_aspect, "true_sentiment": sentiment,
                        "true_cause": cause_item["cause"] if cause_item else "",
                    })
                else:
                    rid = f"RVW-{id_counters[source]:06d}"
                    text = text_gen.generate(item_aspect, sentiment, source)
                    rating = {-1: rng.choice([1, 2]), 0: 3, 1: rng.choice([4, 5])}[sentiment]
                    data_rows.append({
                        "review_id": rid, "channel": channel, "channel_product_id": cpid,
                        "content": text, "rating": rating, "created_at": date.strftime("%Y-%m-%dT%H:%M:%S"),
                    })
                    label_rows.append({
                        "review_id": rid, "true_aspect": item_aspect, "true_sentiment": sentiment,
                        # ⚠️ 항상 False — templates.yaml의 각 리뷰 문장은 단일 aspect·단일 감성만
                        # 담아서 만들어지므로(한 리뷰 안에서 같은 aspect가 상반된 감성으로 충돌하는
                        # 경우를 애초에 생성하지 않음), 이 Mock 데이터로는 mixed_signal=True 케이스를
                        # 재현하지 못한다. 프롬프트2의 mixed_signal 정확도는 71603 평가셋(ver1~3
                        # 성능테스트)으로 별도 검증해야 함 — 이 코퍼스의 한계로 기록.
                        "true_mixed_signal": False,
                    })

    return data_rows, label_rows


# ────────────────────────────────────────────────────────────────
# 6. 배경 상품 — Day1~60 전체에 baseline 비율로 채움 (변동 없음, 이상 없음)
# ────────────────────────────────────────────────────────────────

def get_covered_days(anomaly_groups: dict) -> dict[tuple, set[int]]:
    """(상품,채널,source) -> 케이스 창으로 이미 채워진 날짜 집합.
    이 집합 밖의 날짜가 "구멍"이고, 배경 수준으로 채워야 60일 연속 서비스가 재현된다."""
    covered: dict[tuple, set[int]] = {}
    for group_rows in anomaly_groups.values():
        first = group_rows[0]
        key = (first["golden_group_id"], first["channel"], first["source"])
        start = max(1, first["window_start_day"] - 28)
        end = min(60, first["window_end_day"])
        covered.setdefault(key, set()).update(range(start, end + 1))
    return covered


def get_hot_channels(anomaly_groups: dict) -> dict[str, set[tuple]]:
    """golden_group_id -> {(channel,source), ...} — 그 상품 안에서 "발화 채널"인 조합.
    Mock 정의서 산출 근거: 발화 채널은 케이스 창 밖에서도 일 28건(CS)/10건(리뷰)로 유지
    (같은 상품의 비발화 채널·순수 배경 상품은 일반 볼륨 6건/2건 그대로).

    ⚠️ 대부분 케이스(36개 중 34개)가 CS만 테스트하고 리뷰는 SC-034·035 2개만 명시적으로
    갖고 있음. 근데 "핫한 상품"이면 CS·리뷰 둘 다 평소 볼륨이 높아야 자연스러우므로,
    CS가 발화 채널인 (채널)은 같은 상품의 리뷰에도 그대로 미러링한다 —
    리뷰에 이상 신호가 없어도(config에 없어도), 평소 활발한 볼륨 자체는 유지."""
    hot: dict[str, set[tuple]] = {}
    for group_rows in anomaly_groups.values():
        first = group_rows[0]
        if any(r.get("intended_answer", "").strip().upper() == "TRUE" for r in group_rows):
            gid = first["golden_group_id"]
            hot.setdefault(gid, set()).add((first["channel"], first["source"]))

    # CS 발화 채널 → 같은 상품의 리뷰에도 미러링
    for gid, channels in hot.items():
        cs_hot = {ch for ch, src in channels if src == "cs"}
        for ch in cs_hot:
            channels.add((ch, "review"))

    return hot


def build_rows_for_product_background(gid: str, rng: random.Random, pid_map: dict,
                                       anchor_date: datetime, id_counters: dict, text_gen: "TextGenerator",
                                       day_scope: dict[tuple, list[int]],
                                       hot_channels: set[tuple] | None = None,
                                       baseline_denominator: str = BASELINE_DENOMINATOR_TOTAL) -> tuple[list, list]:
    """day_scope: {(channel, source): [day_no, ...]} — 이 상품의 이 채널·source에서
    baseline 수준으로 채워야 할 날짜 목록.
    - 순수 배경 상품(케이스 없음): 모든 채널×source에 대해 1~60일 전부가 여기 들어온다.
    - 케이스 상품: 자기 케이스 창(covered_days)을 뺀 "나머지" 날짜만 들어온다 — 60일 연속 재현용.

    hot_channels: {(channel, source), ...} — 이 상품 안에서 "발화 채널"인 (채널,source) 조합.
    Mock 정의서 산출 근거(케이스 채널은 일 28건/리뷰 10건, 나머지는 일 6건/리뷰 2건)에 따라
    발화 채널은 케이스 창 밖에서도 평소보다 높은 볼륨을 유지한다("핫한 상품"이니까).
    일별 볼륨: 일반 CS 6건·리뷰 2건 / 발화채널 CS 28건·리뷰 10건 — 6개 aspect에 baseline
    비율로 배정. 소수 볼륨은 확률적으로 반올림."""
    data_rows, label_rows = [], []
    NORMAL_VOLUME = {"cs": 6, "review": 2}
    HOT_VOLUME = {"cs": 28, "review": 10}
    hot_channels = hot_channels or set()

    for (channel, source), days in day_scope.items():
        if not days:
            continue
        cpid = get_channel_product_id(pid_map, gid, channel)
        volume = HOT_VOLUME if (channel, source) in hot_channels else NORMAL_VOLUME
        aspects_for_source = REVIEW_ASPECTS if source == "review" else ASPECTS
        per_aspect_daily = volume[source] / len(aspects_for_source)

        for day_no in days:
            date = day_to_date(day_no, anchor_date)
            for aspect in aspects_for_source:
                rate = _negative_rate(
                    aspect, channel, len(aspects_for_source), baseline_denominator
                )
                n = int(per_aspect_daily)
                if rng.random() < (per_aspect_daily - n):
                    n += 1
                for i in range(n):
                    sentiment = -1 if rng.random() < rate else 0
                    id_counters[source] += 1
                    if source == "cs":
                        rid = f"INQ-{id_counters[source]:06d}"
                        text = text_gen.generate(aspect, sentiment, source)
                        data_rows.append({
                            "inquiry_id": rid, "channel": channel, "channel_product_id": cpid,
                            "content": text, "inquired_at": date.strftime("%Y-%m-%dT%H:%M:%S"),
                        })
                        label_rows.append({
                            "inquiry_id": rid, "true_aspect": aspect, "true_sentiment": sentiment, "true_cause": "",
                        })
                    else:
                        rid = f"RVW-{id_counters[source]:06d}"
                        text = text_gen.generate(aspect, sentiment, source)
                        rating = {-1: rng.choice([1, 2]), 0: 3, 1: rng.choice([4, 5])}[sentiment]
                        data_rows.append({
                            "review_id": rid, "channel": channel, "channel_product_id": cpid,
                            "content": text, "rating": rating, "created_at": date.strftime("%Y-%m-%dT%H:%M:%S"),
                        })
                        label_rows.append({
                            "review_id": rid, "true_aspect": aspect, "true_sentiment": sentiment,
                            "true_mixed_signal": False,  # 위 build_rows_for_window_group과 동일 이유
                        })
    return data_rows, label_rows


# ────────────────────────────────────────────────────────────────
# 7. ⚠️ TODO — 검산 (Fisher → BH-FDR → min_delta, config의 intended_answer와 대조)
# ────────────────────────────────────────────────────────────────

def validate_against_config(anomaly_rows: list[dict], generated_cs_labels: list[dict],
                             generated_review_labels: list[dict]) -> dict:
    """TODO: 다음 단계에서 구현.
    순서 반드시 준수(서영님 §1-1): ① 풀 배치 구성 → ② Fisher 단측 p값 →
    ③ BH-FDR(q=0.05) → ④ min_delta(3%p) AND → ⑤ intended_answer と assert(공백은 스킵).
    m = 1,464 (42상품×36검정 − 보류채널4개×12검정), 보류는 채널 단위로 통째로 제외."""
    print("  ⚠️ validate_against_config() 미구현 — 스텁만 존재. 다음 단계에서 구현 예정.")
    return {"implemented": False}


# ────────────────────────────────────────────────────────────────
# 8. CSV 출력
# ────────────────────────────────────────────────────────────────

def write_csv(rows: list[dict], path: Path):
    if not rows:
        path.touch()
        return
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ────────────────────────────────────────────────────────────────
# 9. 메인
# ────────────────────────────────────────────────────────────────

def main():
    # 출력이 나가기 전에 부른다. 이 스크립트의 진단 문구는 `⚠️`·`❌`·`—` 를 쓰는데 cp949
    # (한국어 윈도우 기본 콘솔)에 없어서, 안 부르면 cause 프롬프트 가드에 닿기도 전에
    # UnicodeEncodeError 로 죽는다 — 멈춘 이유를 알리려고 만든 메시지가 통째로 사라지고
    # traceback 만 남는다. `--help` 도 같은 이유로 죽는다(모듈 docstring 에 `—` 가 있다).
    # `if __name__ == "__main__"` 이 아니라 여기 두는 이유는 배선을 테스트로 고정하기
    # 위해서다 (generate_monthly_reports.py · app/batch/daily.py 와 같은 관례).
    force_utf8_output()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--anomaly-config", default="config_anomaly.csv")
    ap.add_argument("--products-config", default="config_products.csv")
    ap.add_argument("--mapping-dir", default=None, help="input_channel_products.csv 위치")
    ap.add_argument("--golden-mapping-dir", default=None,
                     help="golden_mapping.csv 위치(생략 시 --mapping-dir와 동일 — 하위호환)")
    ap.add_argument("--templates", default="templates.yaml", help="분모용(denom) 텍스트 템플릿 사전")
    # ⚠️ 스크립트 위치 기준으로 잡는다. cwd 기준이면 저장소 루트에서 돌릴 때 못 찾고,
    #    못 찾으면 cause 텍스트가 [PLACEHOLDER:cause:색상:사진*색감*오차] 로 나간다.
    #    원인 라벨이 본문에 박혀 [6] 원인분류가 문장을 읽는 게 아니라 답을 베끼게 된다.
    ap.add_argument("--cause-prompt", default=str(DEFAULT_CAUSE_PROMPT),
                     help="원인분류 투입분 생성 프롬프트")
    ap.add_argument("--cause-cache", default="cause_text_cache.json", help="cause 텍스트 캐시(재실행 시 재호출 방지)")
    ap.add_argument("--no-llm-cause", action="store_true", help="LLM 없이 cause도 플레이스홀더로(오프라인 테스트용)")
    ap.add_argument("--outdir", default="./output", help="input_*.csv 출력 디렉토리")
    ap.add_argument("--golden-outdir", default=None,
                     help="golden_*.csv 출력 디렉토리(생략 시 --outdir와 동일 — 하위호환)")
    ap.add_argument("--anchor-date", required=True, help="Day 60에 해당하는 날짜, 예: 2026-08-28")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument(
        "--baseline-denominator",
        choices=[BASELINE_DENOMINATOR_TOTAL, BASELINE_DENOMINATOR_ASPECT],
        default=BASELINE_DENOMINATOR_TOTAL,
        help="BASELINE_RATE 의 분모. total=확정 스펙(기본), aspect=2026-08-09 이전 동작 재현용",
    )
    args = ap.parse_args()

    if args.baseline_denominator == BASELINE_DENOMINATOR_TOTAL:
        print("배경 baseline 분모: total — (상품,채널,source) 총문의 기준 (확정 스펙)")
    else:
        print(
            "⚠️ 배경 baseline 분모: aspect — 전체 분모로 보면 CS 1/6·리뷰 1/3 로 희석된다.\n"
            "   2026-08-09 이전 동작 재현용이다. 새 측정에 쓰지 말 것."
        )

    rng = random.Random(args.seed)
    anchor_date = datetime.strptime(args.anchor_date, "%Y-%m-%d")
    text_gen = TextGenerator(args.templates, rng, cause_prompt_path=args.cause_prompt,
                              cause_cache_path=args.cause_cache, use_llm=not args.no_llm_cause)
    if not Path(args.templates).exists():
        print(f"  ⚠️ 템플릿 파일 {args.templates} 없음 — denom 텍스트도 플레이스홀더로 나감")
    if args.no_llm_cause:
        print(
            "  ⚠️ --no-llm-cause: cause 텍스트가 [PLACEHOLDER:cause:<aspect>:<원인>] 으로 나갑니다.\n"
            "     원인 라벨이 본문에 박히므로 이 데이터로 [6] 원인분류를 채점하면 안 됩니다"
            " (오프라인 스모크 테스트 전용)."
        )
    if not args.no_llm_cause and not text_gen.cause_system_prompt:
        # 경고로 흘리면 안 된다. 프롬프트가 없으면 cause 텍스트가
        # [PLACEHOLDER:cause:색상:사진*색감*오차] 로 나가고, 원인 라벨이 본문에 그대로
        # 박힌 코퍼스가 만들어진다. [6] 원인분류가 문장을 읽는 게 아니라 답을 베끼게 돼
        # 채점 자체가 성립하지 않는다. 조용히 만들어져 쌓이느니 여기서 멈추는 게 낫다.
        raise SystemExit(
            f"❌ cause 프롬프트를 못 찾았습니다: {args.cause_prompt}\n"
            f"   기본값은 {DEFAULT_CAUSE_PROMPT} 입니다.\n"
            "   이대로 두면 원인 라벨이 본문에 박힌 코퍼스가 나와 [6] 채점이 무효가 됩니다.\n"
            "   의도적으로 플레이스홀더를 쓰려면 --no-llm-cause 를 명시하세요."
        )

    anomaly_rows = load_anomaly_config(args.anomaly_config)
    products = load_products_config(args.products_config)
    pid_map = load_channel_product_id_map(args.mapping_dir, args.golden_mapping_dir)

    anomaly_groups = group_anomaly_rows(anomaly_rows)
    multi_aspect_groups = [k for k, v in anomaly_groups.items() if len(v) > 1]
    if multi_aspect_groups:
        print(f"  같은 창을 공유하는 다중-aspect 그룹 {len(multi_aspect_groups)}개 발견 → 통합 처리")
        for k in multi_aspect_groups:
            print(f"    {k}: {[r['aspect'] for r in anomaly_groups[k]]}")

    case_products = get_case_products(anomaly_rows)
    background_products = get_background_products(products, case_products)
    print(f"케이스 상품 {len(case_products)}개, 배경 상품 {len(background_products)}개")

    id_counters = {"cs": 0, "review": 0}
    cs_data, cs_labels, review_data, review_labels = [], [], [], []

    for group_rows in anomaly_groups.values():
        data_rows, label_rows = build_rows_for_window_group(
            group_rows, rng, pid_map, anchor_date, id_counters, text_gen,
            baseline_denominator=args.baseline_denominator,
        )
        if group_rows[0]["source"] == "cs":
            cs_data.extend(data_rows); cs_labels.extend(label_rows)
        else:
            review_data.extend(data_rows); review_labels.extend(label_rows)

    # 🆕 60일 연속 서비스 재현 — 케이스 상품도 "자기 케이스 창 밖" 날짜는 배경 수준으로 채움.
    # 순수 배경 상품(4개)은 애초에 케이스가 없으니 covered_days가 비어있어서 60일 전부가 채워짐
    # (기존 동작과 동일). 이 부분이 이번에 새로 추가된 "구멍 메우기".
    covered_days = get_covered_days(anomaly_groups)
    hot_channels_by_product = get_hot_channels(anomaly_groups)
    all_products = [p["golden_group_id"] for p in products]
    total_gap_filled = 0

    for gid in all_products:
        day_scope = {}
        for channel in CHANNELS:
            for source in SOURCES:
                key = (gid, channel, source)
                covered = covered_days.get(key, set())
                gap_days = sorted(set(range(1, 61)) - covered)
                if gap_days:
                    day_scope[(channel, source)] = gap_days

        if not day_scope:
            continue  # 이 상품은 전 채널·source가 이미 케이스 창으로 60일 다 덮여있음(이론상 거의 없음)

        data_rows, label_rows = build_rows_for_product_background(
            gid, rng, pid_map, anchor_date, id_counters, text_gen, day_scope,
            hot_channels=hot_channels_by_product.get(gid, set()),
            baseline_denominator=args.baseline_denominator,
        )
        total_gap_filled += len(data_rows)
        for r in data_rows:
            (cs_data if "inquiry_id" in r else review_data).append(r)
        for r in label_rows:
            (cs_labels if "inquiry_id" in r else review_labels).append(r)

    print(f"구멍 메우기(60일 연속화)로 추가된 행: {total_gap_filled}건")
    print(f"생성됨 — CS 문의 {len(cs_data)}건 / 리뷰 {len(review_data)}건")
    if not args.no_llm_cause:
        placeholder_rows = [
            row for row in [*cs_data, *review_data]
            if str(row.get("content", "")).startswith(CAUSE_PLACEHOLDER_PREFIX)
        ]
        if placeholder_rows:
            sample_ids = [
                row.get("inquiry_id") or row.get("review_id")
                for row in placeholder_rows[:5]
            ]
            raise SystemExit(
                f"❌ cause 플레이스홀더 {len(placeholder_rows)}건 발견 — "
                "캐시와 CSV를 저장하지 않습니다. "
                f"예시 ID: {sample_ids}. 정상 cause_text_cache.json 을 사용하세요."
            )
    text_gen.save_cause_cache()
    print(f"cause 텍스트 캐시 저장 → {args.cause_cache}")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    golden_outdir = Path(args.golden_outdir) if args.golden_outdir else outdir
    golden_outdir.mkdir(parents=True, exist_ok=True)
    write_csv(cs_data, outdir / "input_cs_inquiries.csv")
    write_csv(cs_labels, golden_outdir / "golden_cs_labels.csv")
    write_csv(review_data, outdir / "input_reviews.csv")
    write_csv(review_labels, golden_outdir / "golden_review_labels.csv")
    print(f"저장 완료 → input: {outdir}/, golden: {golden_outdir}/")

    validate_against_config(anomaly_rows, cs_labels, review_labels)


if __name__ == "__main__":
    main()
