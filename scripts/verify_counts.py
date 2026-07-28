"""
소량(1~2케이스) 발화 개수 검증
================================
config_anomaly.csv에 적힌 숫자(past_neg/past_total/cur_neg/cur_total)와
실제로 생성된 데이터의 실제 건수가 정확히 일치하는지 확인한다.

정식 검산(Fisher→BH-FDR→min_delta, validate_against_config())의 축소판 —
"통계적으로 유의한지"는 아직 안 보고, "심으라고 한 개수만큼 정확히 심겼는지"만 본다.

동작 원리
--------
input_cs_inquiries.csv 와 golden_cs_labels.csv 는 생성기가 같은 순서로 한 행씩
같이 만들어서 저장한다(build_rows_for_case_row 참고). 그래서 두 파일을 zip으로
나란히 엮으면, 각 행이 어느 channel/날짜(data 파일)에 어떤 aspect/sentiment(label 파일)
인지 동시에 알 수 있다 — 별도 id 매칭 없이 바로 채널·기간·aspect로 필터링해서 카운트 가능.

사용법
------
    python verify_counts.py \
        --anomaly-config config_anomaly.csv \
        --generated-dir ./output \
        --case-ids SC-001,SC-024 \
        --anchor-date 2026-08-28
"""

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path

from generate_cs_review_data import load_channel_product_id_map, get_channel_product_id


def load_csv(path: str) -> list[dict]:
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def day_to_date_str(day: int, anchor_date: datetime, anchor_day: int = 60) -> str:
    return (anchor_date - timedelta(days=(anchor_day - day))).strftime("%Y-%m-%d")


def count_actual(data_rows: list[dict], label_rows: list[dict], channel: str, aspect: str,
                  date_start: str, date_end: str, date_field: str, channel_product_id: str) -> tuple[int, int]:
    """(총 건수, 부정 건수) 실측.
    서영님 정의(§3 [0]): 분모="해당 (sku,channel)의 총 문의수" — aspect 조건 없음.
    그래서 총건수는 aspect 무관하게 세고, 부정건수만 "이 케이스의 aspect + sentiment=-1"로 좁혀 센다."""
    total, neg = 0, 0
    for d, l in zip(data_rows, label_rows):
        if d["channel"] != channel:
            continue
        if d["channel_product_id"] != channel_product_id:
            continue
        date_str = d[date_field][:10]  # YYYY-MM-DD
        if not (date_start <= date_str <= date_end):
            continue
        total += 1  # aspect 무관 — 분모는 전체 문의
        if l["true_aspect"] == aspect and l["true_sentiment"] == "-1":
            neg += 1
    return total, neg


def verify_case(case_id: str, anomaly_rows: list[dict], data_by_source: dict,
                 labels_by_source: dict, anchor_date: datetime, pid_map: dict) -> bool:
    rows = [r for r in anomaly_rows if r["case_id"] == case_id]
    if not rows:
        print(f"  경고: {case_id} — config_anomaly.csv에서 못 찾음")
        return False

    print(f"\n=== {case_id} ===")
    all_pass = True
    for row in rows:
        channel, aspect, source, gid = row["channel"], row["aspect"], row["source"], row["golden_group_id"]
        cpid = get_channel_product_id(pid_map, gid, channel)
        date_field = "inquired_at" if source == "cs" else "created_at"
        data_rows = data_by_source[source]
        label_rows = labels_by_source[source]

        windows = [
            ("과거", int(row["window_start_day"]) - 28, int(row["window_start_day"]) - 1,
             int(row["past_total"]), int(row["past_neg"])),
            ("현재", int(row["window_start_day"]), int(row["window_end_day"]),
             int(row["cur_total"]), int(row["cur_neg"])),
        ]

        for window_name, start_day, end_day, expected_total, expected_neg in windows:
            date_start = day_to_date_str(start_day, anchor_date)
            date_end = day_to_date_str(end_day, anchor_date)

            actual_total, actual_neg = count_actual(
                data_rows, label_rows, channel, aspect, date_start, date_end, date_field, cpid
            )

            total_ok = actual_total == expected_total
            neg_ok = actual_neg == expected_neg
            status = "PASS" if (total_ok and neg_ok) else "FAIL"
            if not (total_ok and neg_ok):
                all_pass = False

            print(f"  [{status}] {window_name}/{channel}/{aspect} (상품ID:{cpid}) "
                  f"기대: 총{expected_total}/부정{expected_neg} -> "
                  f"실제: 총{actual_total}/부정{actual_neg}")

    return all_pass


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--anomaly-config", default="config_anomaly.csv")
    ap.add_argument("--generated-dir", default="./output")
    ap.add_argument("--mapping-dir", required=True, help="golden_mapping.csv/input_channel_products.csv 위치")
    ap.add_argument("--case-ids", default="SC-001", help="쉼표로 구분, 예: SC-001,SC-024")
    ap.add_argument("--anchor-date", required=True)
    args = ap.parse_args()

    anchor_date = datetime.strptime(args.anchor_date, "%Y-%m-%d")
    anomaly_rows = load_csv(args.anomaly_config)
    pid_map = load_channel_product_id_map(args.mapping_dir)

    gen_dir = Path(args.generated_dir)
    data_by_source = {
        "cs": load_csv(gen_dir / "input_cs_inquiries.csv"),
        "review": load_csv(gen_dir / "input_reviews.csv"),
    }
    labels_by_source = {
        "cs": load_csv(gen_dir / "golden_cs_labels.csv"),
        "review": load_csv(gen_dir / "golden_review_labels.csv"),
    }

    results = []
    for case_id in args.case_ids.split(","):
        results.append(verify_case(case_id.strip(), anomaly_rows, data_by_source, labels_by_source, anchor_date, pid_map))

    print("\n" + "=" * 40)
    print("전체 통과" if all(results) else "일부 불일치 있음 - 위에서 FAIL 확인")


if __name__ == "__main__":
    main()