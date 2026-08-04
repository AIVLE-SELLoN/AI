"""실험⑦ 리포팅 정량 실험 — 검증기 민감도 / 1차 통과율 / 위반 유형 분포 / 프롬프트 버전 비교.

## 무엇을 재나

리포팅은 "정답 문장"이 없다. 요약문이 좋은지 나쁜지는 골든으로 채점할 수 없다.
대신 **검증 가능한 것만** 잰다 — 문서 생성 스키마 §4-4 가 반려 사유로 못박은 것들이다.

  (A) 검증기 민감도 [$0, LLM 미호출]
      정상 출력에 "반드시 반려돼야 하는 오염"을 한 군데씩 주입해, 검증기가 잡아내는
      비율을 센다(재현율). 동시에 "반려되면 안 되는 정상 변형"(반올림 표기, 제외
      필드의 정책 수치)도 넣어 오탐률을 센다.
      → 검증기가 LLM 생성물의 최후 방어선이므로, 그 방어선 자체를 먼저 검증한다.

  (B) 1차 통과율·위반 유형 분포 [--live, LLM 과금]
      실제 LLM 에 한 번씩 생성시켜 재시도 없이 통과하는 비율과, 걸린 사유의 유형
      분포를 센다. 재시도를 태우지 않는 이유: 재시도까지 포함하면 "프롬프트가 좋은가"와
      "재시도 로직이 좋은가"가 섞여서 프롬프트 개선 효과를 볼 수 없다.

  (C) 프롬프트 버전 비교 [--compare v2,v3, LLM 과금]
      같은 입력에 서로 다른 프롬프트 버전을 물려 (B) 지표를 나란히 낸다.

  (D) 적재 정책 점검 [$0, LLM 미호출]
      코드가 계산하는 "다운로드 기한"이 S3 Lifecycle 설정과 같은지 확인한다.
      어긋나면 이미 지워진 파일을 받을 수 있다고 안내하게 된다.

## 한계 (인용할 때 반드시 같이 말할 것)

- 입력 케이스는 이 파일 안의 **합성 샘플 소수**다(월간 3 · CS 3). 실서비스 분포가
  아니므로 통과율은 상한으로 읽어야 한다.
- (A)의 오염은 우리가 만든 것이라, "검증기가 우리가 상상한 오염을 잡는다"까지만
  말할 수 있다. 상상 못 한 오염 유형은 측정 밖이다.
- (B)는 LLM 비결정성 때문에 실행마다 흔들린다 — `--repeat 3` 이상으로 평균을 쓴다.

실행:
    python eval/run_reporting_eval.py                      # (A) 검증기 민감도만, $0
    python eval/run_reporting_eval.py --live               # + (B) 1차 통과율, 실비용
    python eval/run_reporting_eval.py --live --repeat 3    # + 3회 평균 (권장)
    python eval/run_reporting_eval.py --compare monthly_report_v2,monthly_report_v3
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import get_settings
from app.core import constants
from app.core.llm_client import get_llm_client
from app.core.schemas import (
    CSGuidelineInput,
    CSGuidelineOutput,
    MonthlyReportInput,
    MonthlyReportOutput,
    PdfS3Meta,
)
from app.reporting import cs_reply_service, monthly_report_service
from app.reporting.cs_reply_validator import validate_cs_guideline
from app.reporting.monthly_report_validator import validate_monthly_report
from app.reporting.s3_uploader import (
    REPORT_TYPE_GUIDELINE,
    REPORT_TYPE_MONTHLY,
    resolve_storage_policy,
)

ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "eval" / "results"


# ── 샘플 케이스 (합성) ────────────────────────────────────────────────────
#
# 실데이터(data/)로 만들려면 분류 결과 집계가 선행돼야 해서, 단계별 severity 를
# 고루 덮는 합성 케이스를 여기 둔다. 케이스를 늘릴 때는 (입력, 정상출력) 쌍을
# 함께 추가해야 (A) 오염 실험이 성립한다.


def _monthly_case(
    *,
    case_id: str,
    severity: str | None,
    stage_label: str,
    hold_reason: str | None = None,
) -> tuple[str, MonthlyReportInput, MonthlyReportOutput]:
    judged = hold_reason is None
    pair: dict[str, Any] = {
        "comparison_pair": "COUPANG_VS_NAVER",
        "sample_size": 120 if judged else 12,
    }
    if judged:
        pair |= {
            "jsd_score": 0.42,
            "jsd_baseline": 0.18,
            "p_value": 0.001,
            "bh_significant": True,
            "is_crisis": severity != "SAFE",
            "severity": severity,
        }
    else:
        pair |= {"hold_reason": hold_reason}

    input_data = MonthlyReportInput(
        report_month="2026-07",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 31),
        master_product_code="P001",
        product_name="미디 원피스",
        total_voc_count=450,
        aspect_distributions=[
            {
                "aspect": "색상",
                "total_count": 200,
                "positive_ratio": 0.2,
                "neutral_ratio": 0.3,
                "negative_ratio": 0.5,
            },
            {
                "aspect": "사이즈",
                "total_count": 150,
                "positive_ratio": 0.5,
                "neutral_ratio": 0.3,
                "negative_ratio": 0.2,
            },
            {
                "aspect": "소재",
                "total_count": 100,
                "positive_ratio": 0.6,
                "neutral_ratio": 0.2,
                "negative_ratio": 0.2,
            },
        ],
        sentiment_drifts=[
            {"aspect": "색상", "drift_rate": 0.08, "status": "RISK"},
            {"aspect": "사이즈", "drift_rate": 0.01, "status": "NORMAL"},
            {"aspect": "소재", "drift_rate": -0.02, "status": "NORMAL"},
        ],
        channel_divergence={
            "calculated_at": datetime(2026, 8, 1, tzinfo=UTC),
            "worst_pair": "COUPANG_VS_NAVER",
            "is_crisis": (severity != "SAFE") if judged else None,
            "pairs": [pair],
        },
        recommended_id="REC-202607-P001",
    )

    output = MonthlyReportOutput(
        report_id="RPT-202607-P001",
        master_product_code="P001",
        report_month="2026-07",
        aspect_summaries=[
            {"aspect": "색상", "summary_text": "부정 의견이 전월 대비 8%p 올라 50%를 기록했습니다."},
            {"aspect": "사이즈", "summary_text": "부정 비율 20%로 전월과 비슷한 수준입니다."},
            {"aspect": "소재", "summary_text": "부정 비율 20%로 안정적인 흐름을 보였습니다."},
        ],
        channel_divergence_cause={
            "cause_title": f"쿠팡-네이버 채널 평판 격차 {stage_label}",
            "cause_description": "쿠팡 채널의 색상 불만 비중이 높아 이미지 운영 점검이 필요합니다.",
        },
        cause_analysis_results=["색상 부정 의견이 전체 450건 중 가장 큰 비중을 차지했습니다."],
        recommended_actions=["쿠팡 대표 이미지를 원본 색상 기준으로 교체하세요."],
    )
    return case_id, input_data, output


def build_monthly_cases() -> list[tuple[str, MonthlyReportInput, MonthlyReportOutput]]:
    """단계별로 하나씩 — CRISIS / CAUTION / 전 쌍 보류(단계 없음)."""
    return [
        _monthly_case(case_id="MR-CRISIS", severity="CRISIS", stage_label="위험 단계"),
        _monthly_case(case_id="MR-CAUTION", severity="CAUTION", stage_label="주의 단계"),
        _monthly_case(
            case_id="MR-HOLD",
            severity=None,
            stage_label="안정 단계",
            hold_reason="INSUFFICIENT_SAMPLE",
        ),
    ]


def _cs_case(
    *, case_id: str, with_root_cause: bool
) -> tuple[str, CSGuidelineInput, CSGuidelineOutput]:
    input_data = CSGuidelineInput(
        alert_id="ALT-20260528-P001-COUPANG",
        detected_at=datetime(2026, 5, 28, 9, 0, tzinfo=UTC),
        product_group_id="P001",
        product_name="미디 원피스",
        channel="COUPANG",
        main_aspect="색상",
        verdict="편중형",
        recommended_action="개선안 생성" if with_root_cause else "운영 점검 권장",
        detection_confidence="높음",
        stats={
            "cur_rate": 0.13,
            "past_rate": 0.05,
            "delta": 0.08,
            "cur_total": 200,
            "p_value": 0.002,
            "bh_significant": True,
        },
        root_cause={"label": "사진_색감_오차", "count": 18, "total": 26}
        if with_root_cause
        else None,
        linked_inquiries=[
            {
                "item_id": "INQ-000001",
                "raw_text": "색이 사진이랑 너무 달라요",
                "created_at": datetime(2026, 5, 27, 10, 0, tzinfo=UTC),
            },
            {
                "item_id": "INQ-000002",
                "raw_text": "화면에서 본 베이지랑 실물 색이 다릅니다",
                "created_at": datetime(2026, 5, 27, 14, 0, tzinfo=UTC),
            },
        ],
    )

    output = CSGuidelineOutput(
        guideline_id="GD-20260528-P001",
        alert_id="ALT-20260528-P001-COUPANG",
        summary={
            "issue_title": "쿠팡 색상 불만 급증 대응 가이드",
            "risk_level": "WARNING",
            "key_metric_text": "색상 부정 비율이 5%에서 13%로 8%p 상승했습니다 (문의 200건 기준).",
        },
        root_cause_summary="사진_색감_오차 18건 / 전체 26건 (69%)"
        if with_root_cause
        else "원인 미특정 상태로 집계됐습니다.",
        standard_guideline={
            "core_message": "촬영 조명 차이로 실물 색상이 다르게 보일 수 있음을 안내하고 무상 교환을 접수합니다.",
            "draft_reply": "안녕하세요 고객님, 색상 차이로 불편을 드려 죄송합니다. 무상 반품 및 교환을 도와드리겠습니다.",
            "key_talking_points": ["조명 차이 정중히 안내", "고객 과실 암시 표현 금지"],
        },
        ops_action_guide="쿠팡 대표 이미지의 색보정 상태를 점검하고 원본 기준으로 재등록하세요.",
        inquiry_specific_guides=[
            {"item_id": "INQ-000001", "recommended_point": "사과 후 무상 회수 접수를 우선 안내하세요."},
            {"item_id": "INQ-000002", "recommended_point": "색상 차이 확인 후 교환 절차를 안내하세요."},
        ],
    )
    return case_id, input_data, output


def build_cs_cases() -> list[tuple[str, CSGuidelineInput, CSGuidelineOutput]]:
    return [
        _cs_case(case_id="CS-ROOTCAUSE", with_root_cause=True),
        _cs_case(case_id="CS-NO-ROOTCAUSE", with_root_cause=False),
    ]


# ── (A) 검증기 민감도 — $0 ────────────────────────────────────────────────
#
# 오염 1건당 "반려돼야 한다", 정상 변형 1건당 "통과해야 한다"가 정답이다.
# 정답을 우리 코드가 아니라 §4-4 규칙에서 가져오므로 자기채점이 아니다.

MutationFn = Callable[[Any], None]


def _monthly_mutations() -> dict[str, tuple[MutationFn, bool]]:
    """{이름: (변형함수, 반려돼야_하는가)}"""

    def hallucinate_number(out: MonthlyReportOutput) -> None:
        out.aspect_summaries[0].summary_text = "부정 의견이 전월 대비 33%p 올랐습니다."

    def leak_p_value(out: MonthlyReportOutput) -> None:
        out.channel_divergence_cause.cause_description = "차이가 유의합니다 (p = 0.002)."

    def leak_fdr(out: MonthlyReportOutput) -> None:
        out.cause_analysis_results[0] = "BH-FDR 보정 후에도 격차가 유지됩니다."

    def drop_stage_label(out: MonthlyReportOutput) -> None:
        out.channel_divergence_cause.cause_title = "쿠팡-네이버 채널 평판 격차 발생"

    def mix_stage_label(out: MonthlyReportOutput) -> None:
        out.channel_divergence_cause.cause_title += " (일부는 안정 단계)"

    def wrong_product_code(out: MonthlyReportOutput) -> None:
        out.master_product_code = "P999"

    def drop_aspect(out: MonthlyReportOutput) -> None:
        out.aspect_summaries[2].aspect = out.aspect_summaries[0].aspect

    def round_numbers(out: MonthlyReportOutput) -> None:
        # 정상 변형: 반올림 표기는 허용 오차 안이라 통과해야 한다
        out.aspect_summaries[1].summary_text = "부정 비율 20%, 변동폭 1%p 수준입니다."

    return {
        "수치_환각": (hallucinate_number, True),
        "p값_노출": (leak_p_value, True),
        "FDR_노출": (leak_fdr, True),
        "단계라벨_누락": (drop_stage_label, True),
        "단계라벨_혼입": (mix_stage_label, True),
        "식별자_불일치": (wrong_product_code, True),
        "속성_누락": (drop_aspect, True),
        "반올림_표기(정상)": (round_numbers, False),
    }


def _cs_mutations() -> dict[str, tuple[MutationFn, bool]]:
    def ungrounded_id(out: CSGuidelineOutput) -> None:
        out.inquiry_specific_guides[0].item_id = "INQ-999999"

    def hallucinate_number(out: CSGuidelineOutput) -> None:
        out.summary.key_metric_text = "색상 부정 비율이 27%로 급등했습니다."

    def leak_p_value(out: CSGuidelineOutput) -> None:
        out.summary.key_metric_text = "부정률 상승이 유의합니다 (p = 0.002)."

    def leak_term(out: CSGuidelineOutput) -> None:
        out.ops_action_guide = "유의확률 기준으로 재점검하세요."

    def wrong_alert_id(out: CSGuidelineOutput) -> None:
        out.alert_id = "ALT-99999999-P999-NAVER"

    def drop_root_cause_label(out: CSGuidelineOutput) -> None:
        out.root_cause_summary = "여러 원인이 섞여 있습니다 18건 / 전체 26건 (69%)"

    def policy_numbers(out: CSGuidelineOutput) -> None:
        # 정상 변형: 제외 필드의 정책 상수는 팩트체크 대상이 아니라 통과해야 한다
        out.standard_guideline.core_message = "수령 후 7일 이내 무상 교환이 가능합니다."

    return {
        "미존재_문의ID": (ungrounded_id, True),
        "수치_환각": (hallucinate_number, True),
        "p값_노출": (leak_p_value, True),
        "통계용어_노출": (leak_term, True),
        "alert_id_불일치": (wrong_alert_id, True),
        "원인라벨_누락": (drop_root_cause_label, True),
        "정책수치(정상)": (policy_numbers, False),
    }


def run_storage_policy_check() -> dict[str, Any]:
    """(D) 적재 정책 점검 [$0] — S3 Lifecycle 설정과 코드가 어긋나지 않는지.

    코드가 계산한 `object_expires_at` 은 곧 UI 의 "다운로드 기한" 안내가 된다. 버킷의
    Lifecycle 을 바꾸면서 상수를 안 고치면 **이미 지워진 파일을 받을 수 있다고 안내**하게
    되므로, 배포 전에 두 값이 같은지 여기서 확인한다.

    검사 항목:
      - 문서 종류별 버킷이 분리돼 있는가
      - 보존 기간이 확정값(월간 6개월 / CS 24시간)과 같은가
      - 링크 만료가 객체 만료를 넘지 않는가 (넘으면 스키마가 거부한다)
      - 재컴파일 불가 문서(월간)의 보존이 충분히 긴가
    """
    monthly = resolve_storage_policy(REPORT_TYPE_MONTHLY)
    guideline = resolve_storage_policy(REPORT_TYPE_GUIDELINE)
    unknown = resolve_storage_policy("unknown-type")

    checks = {
        "버킷 분리": monthly.bucket_name != guideline.bucket_name,
        "월간 보존 = 6개월": monthly.retention_hours == constants.MONTHLY_RETENTION_DAYS * 24,
        "CS 보존 = 24시간": guideline.retention_hours == constants.GUIDELINE_RETENTION_HOURS,
        "월간 링크 ≤ 객체 수명": monthly.presigned_ttl_hours <= monthly.retention_hours,
        "CS 링크 ≤ 객체 수명": guideline.presigned_ttl_hours <= guideline.retention_hours,
        "월간은 재컴파일 불가로 표시": monthly.recompilable is False,
        "CS 는 재컴파일 가능으로 표시": guideline.recompilable is True,
        "미등록 종류는 짧은 보존으로": unknown.retention_hours == guideline.retention_hours,
        # 파일 산출물 공통 필수 4종(2026-08-03 확정) — optional 로 새면 메인이 파일을 못 찾는다
        "파일 메타 4종 필수": all(
            PdfS3Meta.model_fields[f].is_required()
            for f in ("original_file_name", "new_file_name", "created_at", "file_size_bytes")
        ),
    }

    passed = sum(1 for v in checks.values() if v)
    return {
        "policies": {
            "monthly": {
                "bucket": monthly.bucket_name,
                "retention": monthly.retention_label,
                "presigned_hours": monthly.presigned_ttl_hours,
                "recompilable": monthly.recompilable,
            },
            "cs_guideline": {
                "bucket": guideline.bucket_name,
                "retention": guideline.retention_label,
                "presigned_hours": guideline.presigned_ttl_hours,
                "recompilable": guideline.recompilable,
            },
        },
        "checks": checks,
        "summary": {"통과": f"{passed}/{len(checks)}"},
    }


def run_validator_sensitivity() -> dict[str, Any]:
    """(A) 오염을 잡는지 / 멀쩡한 걸 반려하지 않는지. LLM 호출 0회."""
    results: dict[str, Any] = {"monthly": {}, "cs": {}, "summary": {}}
    caught = missed = false_positive = clean_pass = 0

    for case_id, input_data, base_output in build_monthly_cases():
        # 단계 라벨이 없는 보류 케이스는 라벨 관련 오염이 성립하지 않으므로 제외한다
        skip = {"단계라벨_누락", "단계라벨_혼입"} if case_id == "MR-HOLD" else set()
        for name, (mutate, should_reject) in _monthly_mutations().items():
            if name in skip:
                continue
            output = base_output.model_copy(deep=True)
            mutate(output)
            is_valid, errors = validate_monthly_report(input_data, output)
            rejected = not is_valid
            ok = rejected == should_reject
            results["monthly"][f"{case_id}/{name}"] = {
                "should_reject": should_reject,
                "rejected": rejected,
                "correct": ok,
                "errors": errors[:2],
            }
            if should_reject:
                caught += ok
                missed += not ok
            else:
                clean_pass += ok
                false_positive += not ok

    for case_id, input_data, base_output in build_cs_cases():
        skip = {"원인라벨_누락"} if case_id == "CS-NO-ROOTCAUSE" else set()
        for name, (mutate, should_reject) in _cs_mutations().items():
            if name in skip:
                continue
            output = base_output.model_copy(deep=True)
            mutate(output)
            is_valid, errors = validate_cs_guideline(input_data, output)
            rejected = not is_valid
            ok = rejected == should_reject
            results["cs"][f"{case_id}/{name}"] = {
                "should_reject": should_reject,
                "rejected": rejected,
                "correct": ok,
                "errors": errors[:2],
            }
            if should_reject:
                caught += ok
                missed += not ok
            else:
                clean_pass += ok
                false_positive += not ok

    total_reject = caught + missed
    total_clean = clean_pass + false_positive
    results["summary"] = {
        "오염_탐지": f"{caught}/{total_reject}",
        "오염_탐지율": round(caught / total_reject, 4) if total_reject else None,
        "정상_통과": f"{clean_pass}/{total_clean}",
        "오탐률": round(false_positive / total_clean, 4) if total_clean else None,
    }
    return results


# ── (B) 1차 통과율 — --live ───────────────────────────────────────────────


def _classify_errors(errors: list[str]) -> list[str]:
    """반려 사유를 유형으로 접는다. 어떤 규칙이 프롬프트를 괴롭히는지 보려는 것."""
    kinds = set()
    for error in errors:
        if "수치 팩트체크" in error:
            kinds.add("수치_팩트체크")
        elif "금지 표현" in error:
            kinds.add("금지_표현")
        elif "단계 라벨" in error:
            kinds.add("단계_라벨")
        elif "Grounding" in error:
            kinds.add("문의ID_그라운딩")
        elif "불일치" in error:
            kinds.add("식별자_불일치")
        elif "JSON" in error or "스키마" in error:
            kinds.add("스키마_오류")
        else:
            kinds.add("기타")
    return sorted(kinds)


async def _single_shot(
    kind: str,
    input_data: Any,
    prompt_version: str,
) -> tuple[bool, list[str]]:
    """재시도 없이 1회만 생성하고 검증한다. (통과여부, 사유목록)."""
    client = get_llm_client()

    if kind == "monthly":
        prompt = monthly_report_service.build_prompt(input_data, prompt_version=prompt_version)
        model_cls, validate = MonthlyReportOutput, validate_monthly_report
    else:
        prompt = cs_reply_service.build_prompt(input_data, prompt_version=prompt_version)
        model_cls, validate = CSGuidelineOutput, validate_cs_guideline

    try:
        response = await client.complete_json(prompt=prompt, trace_key=f"eval|{kind}")
        output = model_cls.model_validate(response)
    except Exception as exc:  # noqa: BLE001 — 실패도 측정 대상(스키마 오류로 집계)
        return False, [f"JSON 파싱/스키마 오류: {exc!s}"]

    return validate(input_data, output)


async def run_first_pass(prompt_versions: dict[str, str], repeat: int) -> dict[str, Any]:
    """(B) 프롬프트 1회 생성의 통과율·위반 유형 분포. LLM 과금 구간."""
    cases: list[tuple[str, str, Any]] = [
        ("monthly", case_id, input_data) for case_id, input_data, _ in build_monthly_cases()
    ] + [("cs", case_id, input_data) for case_id, input_data, _ in build_cs_cases()]

    rounds: list[dict[str, Any]] = []
    for run_index in range(repeat):
        passed = 0
        violation_counter: Counter[str] = Counter()
        per_case: dict[str, Any] = {}

        for kind, case_id, input_data in cases:
            is_valid, errors = await _single_shot(kind, input_data, prompt_versions[kind])
            kinds = [] if is_valid else _classify_errors(errors)
            violation_counter.update(kinds)
            passed += is_valid
            per_case[case_id] = {"passed": is_valid, "violations": kinds}
            print(f"   [{run_index + 1}/{repeat}] {case_id}: {'통과' if is_valid else '반려 ' + str(kinds)}")

        rounds.append(
            {
                "1차_통과": f"{passed}/{len(cases)}",
                "1차_통과율": round(passed / len(cases), 4),
                "위반_유형_분포": dict(violation_counter),
                "케이스별": per_case,
            }
        )

    mean_rate = sum(r["1차_통과율"] for r in rounds) / len(rounds)
    return {
        "prompt_versions": prompt_versions,
        "n_cases": len(cases),
        "repeat": repeat,
        "평균_1차_통과율": round(mean_rate, 4),
        "회차별": rounds,
    }


# ── 실행 ─────────────────────────────────────────────────────────────────


def _print_sensitivity(result: dict[str, Any]) -> None:
    print("\n=== (A) 검증기 민감도 [LLM 호출 0회] ===")
    for name, row in {**result["monthly"], **result["cs"]}.items():
        mark = "O" if row["correct"] else "X"
        expected = "반려" if row["should_reject"] else "통과"
        actual = "반려" if row["rejected"] else "통과"
        print(f" [{mark}] {name}: 기대={expected} 실제={actual}")
    summary = result["summary"]
    print(f"\n 오염 탐지: {summary['오염_탐지']} | 정상 통과: {summary['정상_통과']}")


async def main_async(args: argparse.Namespace) -> None:
    settings = get_settings()
    stamp = datetime.now(UTC).astimezone().strftime("%Y%m%d_%H%M%S")

    result: dict[str, Any] = {
        # README 원칙: 실행일·모델명·프롬프트 버전·시드·표본 수 다섯 개를 반드시 남긴다
        "meta": {
            "실행일": datetime.now(UTC).astimezone().isoformat(),
            "모델": settings.llm_model,
            "프롬프트_버전": {
                "monthly": monthly_report_service.PROMPT_VERSION,
                "cs": cs_reply_service.PROMPT_VERSION,
            },
            "시드": "해당없음(합성 케이스 고정)",
            "표본수": {
                "monthly_cases": len(build_monthly_cases()),
                "cs_cases": len(build_cs_cases()),
            },
            "live": args.live or bool(args.compare),
        }
    }

    storage = run_storage_policy_check()
    result["storage_policy"] = storage
    print("\n=== (D) 적재 정책 점검 [LLM 호출 0회] ===")
    for name, ok in storage["checks"].items():
        print(f" [{'O' if ok else 'X'}] {name}")
    print(
        f" 월간 {storage['policies']['monthly']['bucket']}({storage['policies']['monthly']['retention']})"
        f" · CS {storage['policies']['cs_guideline']['bucket']}"
        f"({storage['policies']['cs_guideline']['retention']})"
    )

    sensitivity = run_validator_sensitivity()
    result["validator_sensitivity"] = sensitivity
    _print_sensitivity(sensitivity)

    if args.live:
        print("\n=== (B) 1차 통과율 [LLM 과금] ===")
        result["first_pass"] = await run_first_pass(
            {
                "monthly": monthly_report_service.PROMPT_VERSION,
                "cs": cs_reply_service.PROMPT_VERSION,
            },
            args.repeat,
        )
        print(f"\n 평균 1차 통과율: {result['first_pass']['평균_1차_통과율']:.1%}")

    if args.compare:
        print("\n=== (C) 프롬프트 버전 비교 [LLM 과금] ===")
        comparisons = {}
        for version in [v.strip() for v in args.compare.split(",") if v.strip()]:
            kind = "monthly" if version.startswith("monthly") else "cs"
            versions = {
                "monthly": monthly_report_service.PROMPT_VERSION,
                "cs": cs_reply_service.PROMPT_VERSION,
            }
            versions[kind] = version
            print(f" - {version}")
            comparisons[version] = await run_first_pass(versions, args.repeat)
        result["prompt_comparison"] = {
            v: r["평균_1차_통과율"] for v, r in comparisons.items()
        }
        result["prompt_comparison_detail"] = comparisons

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / f"reporting_eval_{stamp}.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n결과 저장: eval/results/{out.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="실험⑦ 리포팅 정량 실험")
    parser.add_argument(
        "--live", action="store_true", help="실제 LLM 생성으로 1차 통과율 측정 (과금)"
    )
    parser.add_argument(
        "--repeat", type=int, default=3, help="LLM 비결정성 흡수용 반복 횟수 (기본 3)"
    )
    parser.add_argument(
        "--compare",
        default="",
        help="비교할 프롬프트 버전 쉼표 구분 (예: monthly_report_v2,monthly_report_v3)",
    )
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
