"""golden_anomaly.csv 생성 — config_anomaly.csv(설계 의도)에서 파생.

무엇을 만드나
-------------
실험①(탐지율·오탐률·편중/전역 판정 정확도)의 정답지.
행 단위: (case_id × channel × source) — mock 정의서 §7.

⚠️ **탐지 코드(app/detection)를 절대 쓰지 않는다.**
   우리 로직으로 정답을 만들면 자기 코드로 자기를 채점하는 순환이 된다.
   정답의 출처는 오직 config_anomaly.csv 의 `intended_answer`(= 서영이 설계한
   "이 채널이 울려야 하는가") 뿐이다.

파생 규칙 (전부 문서 근거)
--------------------------
- channel_significant  ← intended_answer (TRUE→Y / FALSE→N / 빈값→빈값)
- verdict              ← (case, source) 단위 Y 개수:  3개=전역형 / 1~2개=편중형 / 0개=정상
                         빈값이 하나라도 있으면 판정 불가 → 비움(scoring_included=N 케이스)
                         스키마 §6.2 "golden 에서 보류 값 제거, 5종 유지"
- is_anomaly           ← verdict ≠ 정상            (스키마 §6.2 파생 규칙)
- is_biased            ← verdict == 편중형 → true / 전역형·잠정전역 → false
                         정상·구분불가 → 빈값(null)  (mock 정의서 §7)
- main_aspect          ← 케이스 내 delta(cur_rate-past_rate) 최대 aspect
- sub_aspects          ← 나머지 aspect (SC-029 파손). 채점 대상 아님, 완결성용
- root_cause           ← cause_distribution 최다 라벨. 없으면 빈값(미특정 허용)
- window_start/end     ← Day 번호 + --anchor-date 기준 역산

수동 확인이 필요한 칸은 채우지 않고 비워서 리포트한다(§7 미결 항목).

실행:
    python scripts/build_golden_anomaly.py --anchor-date 2026-08-28
    python scripts/build_golden_anomaly.py --dry-run     # 파일 안 쓰고 요약만
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# scripts/ 는 저장소 루트의 형제 폴더 — app 패키지를 절대경로로 import하려면
# 저장소 루트를 sys.path에 넣어야 함(실행 방식에 따라 자동으로 안 잡힐 수 있어서 명시)
sys.path.insert(0, str(ROOT))

from app.core.console import force_utf8_output

CHANNELS = ("COUPANG", "NAVER", "ZIGZAG")

COLUMNS = [
    "case_id",
    "golden_group_id",
    "channel",
    "source",
    "window_start",
    "window_end",
    "channel_significant",
    "is_anomaly",
    "is_biased",
    "verdict",
    "main_aspect",
    "sub_aspects",
    "root_cause",
    "detection_confidence",
    "scoring_included",
    "linked_change_id",
]

# config 의 note 가 확신도를 명시한 케이스 (현재 SC-030 높음 / SC-031 중간 / SC-033 낮음).
# note 를 읽어 채우므로, config 에 확신도 문구가 늘면 자동으로 따라간다.
_CONFIDENCE_FROM_NOTE = {
    "확신도 높음": "높음",
    "확신도 중간": "중간",
    "확신도 낮음": "낮음",
}


def read_config(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def day_to_date(day: int, anchor: date, total_days: int) -> date:
    """Day 번호 → 실제 날짜. anchor 가 Day `total_days` 에 해당한다."""
    return anchor - timedelta(days=total_days - day)


def top_cause(distribution: str) -> str:
    """'사진_색감_오차:0.7,조명_보정_차이:0.1' → 비중 최대 라벨."""
    best, best_w = "", -1.0
    for part in distribution.split(","):
        if ":" not in part:
            continue
        label, _, weight = part.partition(":")
        try:
            w = float(weight)
        except ValueError:
            continue
        if w > best_w:
            best, best_w = label.strip(), w
    return best


def significance(intended: str) -> str:
    """intended_answer → channel_significant. 빈값은 '판정 불가' 로 그대로 전파."""
    value = intended.strip().upper()
    return {"TRUE": "Y", "FALSE": "N"}.get(value, "")


def decide_verdict(flags: list[str]) -> str:
    """한 (case, source) 의 채널별 유의 플래그 → verdict.

    ⚠️ '전부 발화 = 전역형' 판정은 채널 3개가 다 있을 때만 성립한다. config 에
    2채널만 적힌 케이스가 생기면 둘 다 Y 인 것이 전역형으로 잘못 찍힌다 —
    실제로는 나머지 한 채널이 안 울렸는지 알 수 없으므로 전역이라 단정할 수 없다.
    그런 케이스가 나오면 여기서 멈추고 config 를 먼저 확인할 것.
    """
    if any(f == "" for f in flags):
        return ""  # 보류/관찰 케이스 — scoring_included=N 으로만 제외 표기
    if len(flags) != len(CHANNELS):
        raise ValueError(
            f"채널이 {len(flags)}개뿐이라 전역/편중을 가릴 수 없다 "
            f"(기대 {len(CHANNELS)}개: {CHANNELS}). config_anomaly 를 확인할 것."
        )
    fired = flags.count("Y")
    if fired == 0:
        return "정상"
    return "전역형" if fired == len(flags) else "편중형"


def derive_confidence(notes: list[str], verdict: str) -> str:
    """확신도는 alert 가 발행된 경우에만 존재한다.

    - 정상: alert 자체가 없다 → 필드 없음(빈값). '해당없음'을 넣으면 안 된다.
    - 전역형·잠정 전역형: alert 는 나오나 [6] 미수행 → '해당없음'
      (decide_confidence 가 _CAUSE_SKIPPED_VERDICTS 에서 NOT_APPLICABLE 을 낸다)
    - 편중형: config note 가 명시한 케이스만(SC-030/031/033). 나머지는 비워 둔다
      — 시나리오 §4 가 확신도 채점 범위를 그 케이스들로 한정한다.
    """
    for note in notes:
        for needle, value in _CONFIDENCE_FROM_NOTE.items():
            if needle in note:
                return value
    if verdict in ("전역형", "잠정 전역형"):
        return "해당없음"
    return ""


def build(config_rows: list[dict], anchor: date, total_days: int) -> list[dict]:
    # (case, channel, source) 로 묶는다 — 한 슬롯에 aspect 가 여럿일 수 있다(SC-029).
    slots: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for row in config_rows:
        slots[(row["case_id"], row["channel"], row["source"])].append(row)

    # main_aspect 는 케이스 전체에서 delta 가 가장 큰 aspect (config note 의 '주aspect').
    case_main: dict[tuple[str, str], str] = {}
    for row in config_rows:
        key = (row["case_id"], row["source"])
        delta = float(row["cur_rate"]) - float(row["past_rate"])
        if key not in case_main or delta > case_main[key][1]:
            case_main[key] = (row["aspect"], delta)
    main_of = {k: v[0] for k, v in case_main.items()}

    # verdict 은 (case, source) 단위 — 채널 3개의 플래그를 모아 판정한다.
    by_case: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    for (case_id, channel, source), rows in slots.items():
        main = main_of[(case_id, source)]
        primary = next((r for r in rows if r["aspect"] == main), rows[0])
        by_case[(case_id, source)][channel] = significance(primary["intended_answer"])

    out: list[dict] = []
    for (case_id, channel, source), rows in sorted(slots.items()):
        main = main_of[(case_id, source)]
        primary = next((r for r in rows if r["aspect"] == main), rows[0])
        subs = sorted({r["aspect"] for r in rows} - {main})

        flags = by_case[(case_id, source)]
        verdict = decide_verdict([flags[c] for c in sorted(flags)])
        sig = significance(primary["intended_answer"])

        if verdict == "":
            is_anomaly, is_biased = "", ""
        else:
            is_anomaly = "true" if verdict != "정상" else "false"
            is_biased = {"편중형": "true", "전역형": "false"}.get(verdict, "")

        out.append(
            {
                "case_id": case_id,
                "golden_group_id": primary["golden_group_id"],
                "channel": channel,
                "source": source,
                "window_start": day_to_date(
                    int(primary["window_start_day"]), anchor, total_days
                ).isoformat(),
                "window_end": day_to_date(
                    int(primary["window_end_day"]), anchor, total_days
                ).isoformat(),
                "channel_significant": sig,
                "is_anomaly": is_anomaly,
                "is_biased": is_biased,
                "verdict": verdict,
                "main_aspect": main,
                "sub_aspects": "|".join(subs),
                "root_cause": top_cause(primary["cause_distribution"]),
                "detection_confidence": derive_confidence(
                    [r["note"] for r in rows], verdict
                ),
                "scoring_included": primary["scoring_included"],
                "linked_change_id": "",  # config 에 CHG ID 가 없다 — 아래 리포트 참조
            }
        )
    return out


def report(rows: list[dict], config_rows: list[dict]) -> None:
    from collections import Counter

    cases = {r["case_id"] for r in rows}
    print(f"생성 {len(rows)}행 / 케이스 {len(cases)}개")

    scored = [r for r in rows if r["scoring_included"] == "Y"]
    print(f"  채점 대상 {len(scored)}행 / 제외 {len(rows) - len(scored)}행")

    print("\n  verdict 분포 (케이스 수):")
    seen: dict[tuple[str, str], str] = {}
    for r in rows:
        seen[(r["case_id"], r["source"])] = r["verdict"]
    for v, n in Counter(seen.values()).most_common():
        print(f"    {v or '(비움 — 채점 제외)':22s} {n}")

    print("\n  ⚠️ 사람이 확인해야 하는 칸:")
    blank_conf = [r for r in scored if not r["detection_confidence"]]
    print(
        f"    detection_confidence 비어있음 {len(blank_conf)}행 "
        f"— 시나리오 §4 채점 범위는 SC-030/031 한정이라 나머지는 비워둠"
    )
    changed = [r for r in config_rows if r["create_change_event"] == "Y"]
    print(
        f"    linked_change_id 비어있음 (create_change_event=Y 인 "
        f"{len(changed)}건: {', '.join(sorted({r['case_id'] for r in changed}))}) "
        f"— config 에 CHG ID 가 없어 채울 수 없음"
    )
    subs = [r for r in rows if r["sub_aspects"]]
    if subs:
        print(f"    sub_aspects 있는 행 {len(subs)}개 — 채점 대상 아님(완결성용)")

    # root_cause 가 빈 편중형 중, 스코프 밖(파손·오배송)은 스키마상 null 이 정답이다.
    # 스코프 안인데 비어 있는 것만 사람이 정해야 한다.
    scope_in = {"색상", "사이즈", "소재"}
    seen_case: dict[tuple[str, str], dict] = {}
    for r in rows:
        seen_case.setdefault((r["case_id"], r["source"]), r)
    undecided = sorted(
        r["case_id"]
        for r in seen_case.values()
        if r["verdict"] == "편중형"
        and r["scoring_included"] == "Y"
        and not r["root_cause"]
        and r["main_aspect"] in scope_in
    )
    if undecided:
        print(
            f"    root_cause 미정 {len(undecided)}건 ({', '.join(undecided)}) "
            f"— 스코프 안인데 config 에 cause_distribution 이 없음. "
            f"'미특정'으로 둘지 결정 필요(스키마 §3 root_cause 2상태)"
        )


def main() -> None:
    # 🔴 첫 문장이어야 한다 — 아래 `parse_args()` 가 `--help` 를 먼저 찍고, 그 도움말
    #    (`description=__doc__`)에 `—`·`⚠️` 가 있다. `app/core/console.py`.
    force_utf8_output()

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--anomaly-config", default="data/config/config_anomaly.csv")
    ap.add_argument("--out", default="data/golden/golden_anomaly.csv")
    ap.add_argument(
        "--anchor-date", default="2026-08-28", help="Day N(마지막 날)에 해당하는 날짜"
    )
    ap.add_argument(
        "--total-days",
        type=int,
        default=60,
        help="--anchor-date 가 Day 몇에 해당하는지",
    )
    ap.add_argument("--dry-run", action="store_true", help="파일을 쓰지 않고 요약만")
    args = ap.parse_args()

    config_rows = read_config(ROOT / args.anomaly_config)
    anchor = date.fromisoformat(args.anchor_date)
    rows = build(config_rows, anchor, args.total_days)
    report(rows, config_rows)

    if args.dry_run:
        print("\n[dry-run] 파일 안 씀.")
        return

    out = ROOT / args.out
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n저장 → {out}")


if __name__ == "__main__":
    main()
