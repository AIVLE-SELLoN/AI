"""담당: 지인 (원작 서영) — 일 1회 탐지 배치. 운영 진입점.

탐지의 시작점이 배치인 이유: 메인→AI 큐에 '탐지 요청' 이 없고, `/detect` 는 body 로 분류
결과 전량을 받는데 그걸 손에 든 건 분류를 수행하는 AI 노드 자신뿐이다. 그래서
`POST /detect` 는 운영 경로가 아니라 재현·디버깅 창구다.

`detection/service.py` 밖에 있는 이유: 탐지 → 개선안 → 가이드라인 → 발행 루프를 detection
안에 넣으면 detection 이 recommendation 을 import 하게 되어 "각 모듈은 core 에서만 가져다
쓴다" 는 규칙이 깨진다.

실행::

    python -m app.batch.daily --dry-run          # LLM 0회, 호출 횟수만 실측
    python -m app.batch.daily --max-alerts 3     # 비용 상한
    python -m app.batch.daily --window-end 2026-08-28
    python -m app.batch.daily --input-source golden --dry-run   # 평가·재현 (oracle)

입력은 주입받는다(`run_batch(load_inputs=...)`). 기본값은 raw DB 직접 읽기라, 목
파이프라인에서는 `mock_producer` 와 `classification_worker` 를 먼저 돌려야 배치가 돈다.

**이 모듈은 `data/golden/` 을 읽지 않는다** — `app/` 이 골든을 읽으면 컨닝이라 골든 로더를
`scripts/golden_inputs.py` 로 뺐다. 평가·재현일 때만 주입하고, 그때는 요약이 oracle 경고를
함께 낸다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

# 입력원과 상태 파일은 갈라져 있다(`inputs.py`·`state.py`). 여기서 다시 import 하는 것은
# 이 모듈이 쓰기 때문이기도 하고, **기존 호출부의 import 경로를 지키기 위해서**이기도
# 하다 — `scripts/detection_experiments/` 와 테스트가 `app.batch.daily` 에서 가져다 쓴다.
# 재노출 목록은 아래 `__all__` 이 정본이다.
from app.batch.inputs import (
    INPUT_WINDOW_DAYS,
    _classifier_versions_for,
    _read_inputs,
    load_inputs_from_db,
)
from app.batch.state import (
    GUIDELINE_RETRY_MAX_ATTEMPTS,
    PENDING_GUIDELINE_PATH,
    ROOT,
    STATE_PATH,
    STATE_RETENTION_DAYS,
    _as_date,
    load_pending_guidelines,
    load_prior_alerts,
    save_pending_guidelines,
    save_published,
)
from app.core.console import force_utf8_output
from app.core.constants import KST
from app.core.exit_codes import EXIT_CONFIG_ERROR, EXIT_RUNTIME_ERROR
from app.core.inquiries import build_linked_inquiries
from app.core.logging_setup import configure_logging_or_exit
from app.core.raw_db import connection_error_types
from app.core.schemas import ClassifiedItem, DetectionAlert
from app.detection.loader import check_coverage, unreliable_slots
from app.detection.service import DetectionDiagnostics, detect_anomaly

logger = logging.getLogger(__name__)

__all__ = [
    "GUIDELINE_RETRY_MAX_ATTEMPTS",
    "INPUT_WINDOW_DAYS",
    "PENDING_GUIDELINE_PATH",
    "ROOT",
    "STATE_PATH",
    "STATE_RETENTION_DAYS",
    "STUB_CAUSE",
    "CountingClient",
    "load_inputs_from_db",
    "load_pending_guidelines",
    "load_prior_alerts",
    "main",
    "print_summary",
    "run_batch",
    "save_pending_guidelines",
    "save_published",
]


# ── 담당자 미완성 함수 — import 폴백 ────────────────────────────
# 실물이 생기면 아래 except 가 안 타므로 이 파일은 손댈 필요가 없다.
#
# **모듈이 없을 때만 폴백한다.** `except ImportError` 로 통째로 삼키면, 모듈은
#    올라왔는데 그 안의 의존성(예: `aio_pika`)이 없어서 나는 ImportError 까지 먹고
#    조용히 no-op 이 된다 — 요약엔 "미연결"로 찍혀서 보는 사람은 "아직 안 만들었나"
#    로 읽고, 이벤트가 하나도 안 나가는데 배치는 정상 종료한다.


def _missing(exc: ImportError, module: str) -> bool:
    """그 모듈 자체가 없어서 난 ImportError 인가. 아니면 진짜 오류라 다시 던진다."""
    return exc.name == module or (exc.name or "").startswith(module + ".")


try:  # pragma: no cover - 실물이 생기면 이쪽
    from app.core.mq import (  # type: ignore[attr-defined]
        close_mq,
        new_trace_id,
        publish_anomaly_analyzed,
        publish_guideline_generated,
    )

    MQ_AVAILABLE = True
except ImportError as exc:  # pragma: no cover - 인프라 도입 전
    if not _missing(exc, "app.core.mq"):
        raise
    MQ_AVAILABLE = False

    def new_trace_id() -> str:
        return f"trace-{uuid.uuid4().hex[:16]}"

    async def publish_anomaly_analyzed(
        alert: Any, rec: Any, trace_id: str, classifier_versions: dict | None = None
    ) -> None:
        logger.info(
            "[MQ 미구현] ai.anomaly.analyzed 발행 생략 alert=%s", alert.alert_id
        )

    async def publish_guideline_generated(guideline: Any, trace_id: str) -> None:
        logger.info("[MQ 미구현] ai.guideline.generated 발행 생략")

    async def close_mq() -> None:
        return None


try:  # pragma: no cover
    from app.recommendation.pipeline import (
        generate_outcome_for_alert,  # type: ignore[attr-defined]
    )

    RECOMMENDATION_AVAILABLE = True
except ImportError as exc:  # pragma: no cover
    if not _missing(exc, "app.recommendation.pipeline"):
        raise
    RECOMMENDATION_AVAILABLE = False

    async def generate_outcome_for_alert(alert: Any, inquiries: Any) -> Any:
        logger.info("[Agent3 미연결] 개선안 생성 생략 alert=%s", alert.alert_id)
        # 아래 루프가 읽는 필드만 흉내낸다. is_evidence_gap=False 라 미연결 상태가
        # 데이터 갭으로 둔갑하지 않고 실패로 남는다 — Agent3 가 없는 건 갭이 아니다.
        return SimpleNamespace(
            recommendation=None, is_evidence_gap=False, detail="Agent3 미연결"
        )


try:  # pragma: no cover
    from app.reporting.cs_reply_service import (  # type: ignore[attr-defined]
        generate_guideline,
        is_guideline_target,
    )

    GUIDELINE_AVAILABLE = True
except ImportError as exc:  # pragma: no cover
    if not _missing(exc, "app.reporting.cs_reply_service"):
        raise
    GUIDELINE_AVAILABLE = False

    async def generate_guideline(
        alert: Any, inquiries: Any, *, product_name: str | None = None
    ) -> Any:
        logger.info("[가이드라인 미연결] 생성 생략 alert=%s", alert.alert_id)
        return None

    def is_guideline_target(alert: Any) -> bool:
        # 실물과 같은 규칙(빈 `evidence.inquiry_ids` 는 대상 아님). 폴백이 무조건 True 면
        # 미연결 환경의 dry-run 추정이 실물보다 크게 나와 추정값을 못 믿게 된다.
        return bool(alert.evidence.inquiry_ids)


# [2] 개선안 생성 게이트. `recommended_action == "개선안 생성"` 인 alert 만 Agent3 로
#     간다 — 큐 규약이 recommendation 을 그때만 non-null 로 못박고 있다.
#     Agent3 가 아직 안 붙은 지금도 **dry-run 호출 수 추정에 필요**하므로 폴백을 둔다
#     (조치 7종 중 개선안 생성은 1종뿐이라, 게이트 없이 세면 크게 과대추정된다).
try:  # pragma: no cover
    from app.recommendation.pipeline import should_generate
except ImportError as exc:  # pragma: no cover
    if not _missing(exc, "app.recommendation.pipeline"):
        raise

    def should_generate(alert: Any) -> bool:
        return getattr(alert.recommended_action, "value", "") == "개선안 생성"


# ── dry-run 스텁 ────────────────────────────────────────────────

STUB_CAUSE = "기타"
"""스텁이 돌려줄 합성 원인 라벨. 지원하는 색상·사이즈·소재 taxonomy에 모두 속한다.

`SCOPE_LIMIT_LABELS`(실물_염색_편차·실제_원단_문제)도 피한다 — 그 라벨이면 Agent3가
라우팅·생성을 통째로 건너뛰어 호출 수 추정이 어긋난다. 이 값은 실제 원인·조치 분포를
재현하지 않는다. dry-run에서 원인 검증 이후 게이트와 호출 수를 보존하기 위한 값이다.

`scripts/crosscheck_agent2_to_agent3.py` 도 이 값과 `CountingClient` 를 그대로
   가져다 쓴다. 두 도구가 같은 호출 수를 내야 하므로 복제하지 말 것."""


class CountingClient:
    """LLM 을 부르지 않고 호출 횟수만 세는 스텁. `--dry-run` 전용.

    `detect_anomaly(client=...)` 에 주입하면 [6] 원인분류가 **실제로 몇 번 불릴지**를
    돈 한 푼 안 쓰고 실측한다. 후보 수를 눈으로 세는 추정이 아니다.

    프롬프트에 박힌 cs_id 를 그대로 되돌려준다 — 개수를 맞춰야 `root_cause.total` 이
    입력 건수와 같아지고, 그래야 원인 검증 이후 게이트와 호출 수가 유지된다. 모든 문의에
    같은 합성 라벨을 주므로 실제 원인·조치 분포를 측정하는 스텁은 아니다.

    응답은 실제 프롬프트3 스키마를 모두 만족시킨다. `confidence` 는 런타임 판정에 쓰지
    않지만 Pydantic 계약의 필수 필드이고, `evidence` 는 원문 축자 인용 검증을 통과해야
    한다. 둘을 생략하면 dry-run 만 원인분류 실패로 다운그레이드되어 비용 추정이 0으로
    왜곡된다.

    프롬프트 전체를 정규식으로 훑지 않고 마지막 `입력:` JSON만 읽는다. 앞쪽 few-shot
    예시의 cs_id 까지 응답에 섞이면 ID 개수·순서 검증에서 청크 전체가 실패하기 때문이다.
    """

    def __init__(self) -> None:
        self.calls = 0
        self.empty_extractions = 0

    async def complete_json(
        self, prompt: str, *, trace_key: str = "-", **_: object
    ) -> dict:
        self.calls += 1
        try:
            input_data = json.loads(
                prompt.rsplit("입력:", 1)[1].split("\n출력:", 1)[0]
            )
            items = input_data["items"]
        except (IndexError, KeyError, TypeError, json.JSONDecodeError):
            self.empty_extractions += 1
            items = []
        return {
            "results": [
                {
                    "cs_id": item["cs_id"],
                    "cause": STUB_CAUSE,
                    "confidence": 1.0,
                    "evidence": item["raw_text"],
                    "aspect_match": True,
                }
                for item in items
            ]
        }


# ── 배치 본체 ───────────────────────────────────────────────────


def _failure(target_key: str, stage: str, error: BaseException | str) -> dict[str, str]:
    """배치 요약에 실을 실패 1건.

    식별자는 `target_key` 하나다 — `print_summary` 가 이 키만 읽으므로 `alert_id` 처럼
    다른 이름을 섞으면 요약에서 "식별자 없음" 으로 찍힌다.

    예외를 그대로 받아 `repr()` 을 **여기서** 한다. 호출부마다 문자열로 만들면 한 곳만
    `str()` 로 적어도 요약의 형식이 조용히 갈린다. 이미 사람이 읽을 문장인 경우
    (`RecommendationOutcome.detail`)는 그대로 통과시킨다.
    """
    return {
        "target_key": target_key,
        "stage": stage,
        "error": error if isinstance(error, str) else repr(error),
    }


@dataclass
class _Tally:
    """알림을 돌며 쌓이는 값. 한 알림 처리의 산출물이 여섯 갈래라 묶어서 넘긴다.

    묶는 진짜 이유는 `evidence_gaps`·`routing_misses` 다 — int 라 인자로 넘겨서는
    증가시킬 수 없다. 리스트·Counter 만 있었으면 그냥 넘겨도 됐다.

    셋을 한 자루에 담지 않는 이유: `failures` 는 **종료코드에 실리고**, 그 둘은
    개선안이 안 나온 사유이긴 해도 **실패가 아니라서 안 실린다**(`counts_as_failure`).
    합치면 상세페이지 미등록 같은 데이터 갭이 배치를 상시 exit 1 로 만든다.
    """

    counts: Counter[str] = field(default_factory=Counter)
    failures: list[dict] = field(default_factory=list)
    evidence_gaps: int = 0
    routing_misses: int = 0
    delivered: list[DetectionAlert] = field(default_factory=list)
    guideline_pending: list[DetectionAlert] = field(default_factory=list)
    """알림은 나갔는데 가이드라인만 못 나간 건. 다음 배치가 재시도한다(§4)."""

    def fail(self, target_key: str, stage: str, error: BaseException | str) -> None:
        self.failures.append(_failure(target_key, stage, error))



def _count_dry_run(alert: DetectionAlert, tally: _Tally) -> None:
    """LLM 을 안 부르고 **몇 번 부를지만** 센다. 추정이 아니라 실측이다.

    게이트를 실제로 태운다 — 안 태우면 조치 7종 중 1종만 해당하는 개선안 비용이 크게
    과대추정된다. 가이드라인도 `is_guideline_target()` 을 태운다: 스코프 밖 알림
    (파손·오배송)은 LLM 을 아예 안 부르는데 세면 그만큼 부풀려진다.

    개선안 수치는 **상한**이다. dry-run 은 근거 조회(ChromaDB·CS 원문)를 안 하므로 근거
    0건으로 걸러질 알림을 미리 알 수 없다 — 실제 실행은 그것들을 `no_evidence` 로 빼서
    더 작게 나온다. 어긋난 게 아니라 원리적으로 못 맞추는 값이다.
    """
    if should_generate(alert):
        tally.counts["개선안"] += 1
    if is_guideline_target(alert):
        tally.counts["가이드라인"] += 1
    tally.counts["발행:이상"] += 1


async def _process_alert(
    alert: DetectionAlert,
    *,
    documents: list[dict],
    trace_id: str,
    classifier_versions: dict[str, str] | None,
    tally: _Tally,
) -> None:
    """알림 1건 — CS 원문 조회 → 개선안 → 가이드라인 → 발행 2종.

    **예외를 밖으로 내보내지 않는다.** 호출부 루프를 감싸는 try 에는 except 가 없고
    `finally: close_mq()` 뿐이라, 여기서 던지면 `run_batch` 밖으로 나간다. 그러면
    `save_published()` 가 그 try/finally **뒤**라 같이 건너뛰어 **이미 발행에 성공한
    앞쪽 알림이 캐시에 안 들어가고**, 다음 배치가 같은 알림을 다시 만들며 LLM 비용을
    또 쓴다. 그래서 모든 단계가 각자 격리돼 실패를 `tally` 에 적기만 한다.

    **단계가 실패해도 알림 발행은 막지 않는다.** 알림은 통계로 서고 CS 원문과 무관하다 —
    건너뛰면 셀러가 이상 자체를 못 본다
    (test_silent_recommendation_failure_still_shows_up).
    """
    # 개선안과 가이드라인이 같은 CS 원문을 쓴다. 한 번 만들어 둘 다에게 넘긴다 — 각자
    # 만들면 같은 매핑이 두 벌이 된다.
    #
    # **`continue` 하지 않는다 — 알림 자체는 발행한다.** 알림은 통계로 서고 CS 원문과
    # 무관해서, 건너뛰면 셀러가 이상 자체를 못 본다. 빈 리스트로 내려보내면
    # `generate_guideline` 이 ValueError 로 올려 그쪽 단계에도 정직하게 남는다.
    try:
        inquiries = build_linked_inquiries(alert, documents)
    except Exception as exc:  # noqa: BLE001 - 배치 격리가 목적
        inquiries = []
        tally.fail(alert.alert_id, "CS 원문 매핑", exc)

    # alert 1건이 터져도 배치는 계속한다. 여기서 던지면 **이미 LLM 비용을 쓴
    #    앞쪽 알림들까지 발행되지 않고 날아간다.** 실패는 모아서 끝에 요약한다.
    rec = guideline = None
    # §4 — 대기열 적재 판정용. 아래 두 예외 지점이 후보만 표시하고, 실제 적재는
    # 알림 발행 성공 여부까지 보고 이 반복의 끝에서 한다.
    anomaly_delivered = False
    guideline_undelivered = False
    if should_generate(alert):
        try:
            outcome = await generate_outcome_for_alert(alert, inquiries)
            rec = outcome.recommendation
        except Exception as exc:  # noqa: BLE001 - 배치 격리가 목적
            tally.counts["개선안"] += 1
            tally.fail(alert.alert_id, "개선안", exc)
        else:
            # `generate_outcome_for_alert` 는 계약상 예외를 안 던지고 실패를 "개선안 없는
            # 결과" 로 돌려준다. 위 except 만 두면 개선안이 하나도 안 붙은 배치가 "성공" 으로
            # 끝난다. `else` 인 이유는 except 와 둘 다 타면 실패 1건이 2건으로 잡혀서다.
            #
            # 개선안이 없는 사유를 셋으로 가른다. 근거 0건과 라우팅 미스는 근본 원인이 같고
            # (상세페이지 미등록, mock 504행 중 489행) 실패로 세면 배치가 상시 exit 1 이라
            # 진짜 장애가 묻힌다. 대신 따로 세서 요약에 남긴다.
            # 판정은 `RecommendationOutcome` 이 한다 — 여기서 다시 판정하면 두 곳이 갈린다.
            if outcome.is_evidence_gap:
                # 라우팅 전에 걸러지므로 LLM 호출은 0회다 — 개선안 카운트에 안 넣는다.
                tally.evidence_gaps += 1
            else:
                # 라우팅까지는 갔으므로 LLM 을 썼다(미스여도 마찬가지).
                tally.counts["개선안"] += 1
                if outcome.is_routing_miss:
                    tally.routing_misses += 1
                elif outcome.counts_as_failure:
                    tally.fail(
                        alert.alert_id,
                        "개선안",
                        outcome.detail
                        or "생성 실패 — 사유는 app.recommendation.pipeline 로그 참고",
                    )

    try:
        guideline = await generate_guideline(alert, inquiries)
        # `None` 은 **생성 대상이 아니라는 뜻**이지 실패가 아니다
        #    (`is_guideline_target()` — `evidence.inquiry_ids` 가 빈 스코프 밖 알림).
        #    그것까지 세면 dry-run 추정과 실제 집계가 서로 다른 것을 세게 되고,
        #    비용 추정이 위로 어긋난다. 실패(FAILED_*)는 콜백을 돌려주므로 여기 든다.
        if guideline is not None:
            tally.counts["가이드라인"] += 1
    except Exception as exc:  # noqa: BLE001
        # §4 — 생성 예외 = 대상 알림인데 백엔드가 아무것도 못 들었다 → 대기 후보.
        #    FAILED_* 는 예외가 아니라 반환값이라 여기 안 온다. 그 콜백은 아래
        #    발행이 성공하는 순간 백엔드가 종결 상태를 들은 것이므로 재시도하지
        #    않는다(재시도하면 FAILED 행을 SUCCESS 로 덮는 계약 변경이 된다).
        guideline_undelivered = True
        tally.fail(alert.alert_id, "가이드라인", exc)

    try:
        await publish_anomaly_analyzed(alert, rec, trace_id, classifier_versions)
        tally.counts["발행:이상"] += 1
        tally.delivered.append(alert)
        anomaly_delivered = True
    except Exception as exc:  # noqa: BLE001
        tally.fail(alert.alert_id, "발행:이상", exc)

    if guideline is not None:
        try:
            await publish_guideline_generated(guideline, trace_id)
            tally.counts["발행:가이드"] += 1
        except Exception as exc:  # noqa: BLE001
            # §4 본체 — 만들어 놓고 백엔드가 못 들었다 → 대기 후보.
            guideline_undelivered = True
            tally.fail(alert.alert_id, "발행:가이드", exc)

    # §4 — 대기열은 "알림은 **나갔는데** 가이드라인만 못 나간" 건만 받는다.
    # 알림 발행까지 실패한 건은 캐시에 안 들어가 다음 배치가 그 알림을 통째로
    # 재처리한다(가이드라인도 그 경로에서 다시 만들어진다) — 대기열에도 넣으면
    # 같은 guideline_id 가 두 번 발행되고 LLM·S3 를 이중 지불한다.
    if guideline_undelivered and anomaly_delivered:
        tally.guideline_pending.append(alert)


async def run_batch(
    *,
    window_end: date | None = None,
    max_alerts: int | None = None,
    dry_run: bool = False,
    state_path: Path = STATE_PATH,
    pending_path: Path | None = None,
    load_inputs: Callable[..., tuple[list[ClassifiedItem], list[dict]]] | None = None,
) -> dict:
    """탐지 → 개선안 → 가이드라인 → 발행. 배치 1회.

    Args:
        window_end: 현재 윈도우 마지막 날. 없으면 입력의 최신 날짜.
        max_alerts: Agent3·가이드라인에 태울 alert 수 상한 (비용 통제).
            **가이드라인 재시도(§4)도 이 예산을 나눠 쓴다** — 신규 target 이 먼저
            쓰고 남는 만큼만 재시도한다. 밖에 두면 상한이 재시도로 우회된다.
        dry_run: LLM 을 한 번도 부르지 않고 **몇 번 부를지만 실측**한다.
        state_path: 발행 기록 캐시 경로 (테스트 주입용).
        pending_path: 가이드라인 대기열 경로. 기본은 state_path 를 따라간다 —
            운영 기본이면 PENDING_GUIDELINE_PATH, 커스텀이면 그 옆 파생 이름.
            디버그 실행(--state-path)이 운영 대기열을 소비·저장하지 않게 하기 위해서다.
        load_inputs: 입력원. 기본은 원본 DB(`load_inputs_from_db`).
            평가·재현은 `scripts.golden_inputs.load_golden_inputs` 를 주입한다.
            **골든을 주입하면 분류 오차가 0 이라 결과가 탐지 성능이 아니다** —
            요약에 그 경고가 함께 출력된다.

    Returns:
        배치 요약 dict. 실패는 모아서 담고 **중간에 던지지 않는다.**
    """
    loader = load_inputs or load_inputs_from_db
    classifier_versions = _classifier_versions_for(loader)
    if pending_path is None:
        # 커스텀 state_path 는 **파일명에서** 파생한다 — 디렉토리 기준이면 "파일명만 바꿔
        # 운영 디렉토리를 준" 디버그 실행이 운영 대기열을 그대로 소비·저장한다.
        pending_path = (
            PENDING_GUIDELINE_PATH
            if state_path == STATE_PATH
            else state_path.with_name(state_path.stem + ".pending_guidelines.json")
        )
    trace_id = new_trace_id()
    # 경과시간 전용이라 시간대는 UTC 로 고정한다 — 벽시계 값이 아니라 **차이만** 쓴다.
    # naive `datetime.now()` 는 로컬 시각이라 배치가 도는 중 DST·시간대 변경이 걸리면
    # elapsed_sec 가 통째로 어긋난다. 아래 `elapsed_sec` 와 **짝이라 같이 바꿔야 한다**
    # (한쪽만 aware 로 두면 뺄셈이 TypeError 다).
    started = datetime.now(timezone.utc)
    logger.info("배치 시작 trace_id=%s dry_run=%s", trace_id, dry_run)

    items, documents, input_dropped = _read_inputs(loader, window_end)

    # **window_end 를 여기서 한 번만 확정한다.** 로드·탐지·저장이 같은 값을 써야 한다.
    #    읽기는 실행 시각(`date.today()`), 쓰기는 데이터 시각(`window_end`)이면, 데이터가
    #    오늘보다 STATE_RETENTION_DAYS 이상 뒤처진 상태(백필·유입 지연)에서 로드가 방금
    #    저장한 캐시를 통째로 버려 **매 배치가 첫 실행처럼 굴러간다.** 억제 모듈이 경과일을
    #    데이터 시각으로 세는 것과 같은 이유다.
    if window_end is None and documents:
        window_end = max(_as_date(d["created_at"]) for d in documents)

    # check_coverage 를 두 번 돌리지 않는다 — detect_anomaly 도 안에서 같은 계산을 한다.
    # 넘겨주면 128k 스캔이 한 번 줄고, "경고에 찍힌 슬롯 = 실제로 family 에서 빠진 슬롯"
    # 이 보장된다.
    coverage_gaps = check_coverage(documents, items)
    unreliable = unreliable_slots(coverage_gaps)
    if unreliable:
        missing_documents = sum(
            gap["documents"] - gap["classified"] for gap in coverage_gaps
        )
        logger.warning(
            "분류 커버리지 미달 %d슬롯, 부모 분류 레코드 누락 %d건 — 검정에서 "
            "제외됩니다",
            len(unreliable),
            missing_documents,
        )
        for gap in coverage_gaps:
            logger.warning(
                "분류 커버리지 미달 상세 product=%s channel=%s source=%s day=%s "
                "classified=%d/%d",
                gap["product"],
                gap["channel"],
                gap["source"],
                date.fromordinal(gap["day"]),
                gap["classified"],
                gap["documents"],
            )

    # 문서가 0건이라 window_end 를 못 정한 경우에만 타는 분기. `date.today()` 는 호스트
    # 로컬이라 UTC 컨테이너에서 KST 보다 하루 이르다 — 날짜 경계가 KST 로 못박혀 있으므로
    # 여기서도 같은 기준을 쓴다. 운영 사고를 막는 코드가 아니라 계약 일관성용이다(그 경우
    # `prior` 는 아래 로그 건수에만 쓰이고 상태 파일도 안 바뀐다).
    prior = load_prior_alerts(window_end or datetime.now(KST).date(), state_path)
    logger.info(
        "입력 items=%d documents=%d prior_alerts=%d window_end=%s",
        len(items),
        len(documents),
        len(prior),
        window_end,
    )

    # §4 가이드라인 재시도 대기열 — "알림은 나갔는데 가이드라인만 못 나간" 건들.
    # window_end 가 없으면(문서 0건) 로드하지 않는다 — 보관 컷오프를 잴 기준이 없고
    # documents 가 비어 재시도가 성공할 수도 없다. 파일은 그대로 남는다.
    pending = load_pending_guidelines(window_end, pending_path) if window_end else []
    if pending:
        logger.info("가이드라인 재시도 대기 %d건", len(pending))

    # dry-run 이어도 [6] 원인분류는 detect_anomaly 안에서 돈다. 스텁을 안 주면
    #    "LLM 0회"라고 해놓고 실제로 과금된다.
    stub = CountingClient() if dry_run else None
    detection_diagnostics = DetectionDiagnostics()

    alerts, suppressed = await detect_anomaly(
        items,
        documents=documents,
        window_end=window_end,
        prior_alerts=prior,
        # 백엔드가 어디서 줄지 미정 — 정해질 때까지 빈 집합.
        resolved_alert_ids=set(),
        unreliable_denominators=unreliable,
        client=stub,
        diagnostics=detection_diagnostics,
    )

    targets = alerts if max_alerts is None else alerts[:max_alerts]
    tally = _Tally()
    for failure in detection_diagnostics.cause_failures:
        tally.fail(
            f"{failure['product']}/{failure['aspect']}/"
            f"{failure['channel']}/{failure['source']}",
            "원인분류",
            failure["error"],
        )
    # 두 상태 파일(대기열·억제 캐시)을 원자적으로 같이 못 쓴다. 직전 실행이 그 사이에서
    # 죽으면 "대기열엔 있는데 억제는 안 된" 알림이 남아 이번에 신규 target 으로 다시 뜨고,
    # 그러면 같은 guideline_id 가 두 번 발행된다. 겹치는 대기 항목을 걷어내 한 경로만 태운다.
    target_ids = {a.alert_id for a in targets}
    superseded = sum(1 for e in pending if e["alert"].alert_id in target_ids)
    if superseded:
        logger.info(
            "가이드라인 대기 %d건이 이번 실행의 신규 target 과 겹쳐 대기열에서 뺍니다"
            " (메인 루프가 처리)",
            superseded,
        )
        pending = [e for e in pending if e["alert"].alert_id not in target_ids]
    # 재시도 패스가 안 돌면(dry-run·documents 0건) 대기열은 그대로 다음 배치로 넘어간다.
    still_pending: list[dict] = list(pending)
    retried_ok = 0
    retry_exhausted = 0
    # §4 — 재시도도 --max-alerts 예산 안에서 돈다: 신규 target 이 먼저 쓰고 남는 만큼만.
    # 상한을 우회하면 장애 복구 직후 대기열 규모만큼 LLM·S3 비용이 한 번에 나간다
    # 예산 밖 대기 건은 attempts 를 안 쓰고 다음 배치로 넘어간다.
    retry_budget = (
        len(pending) if max_alerts is None else max(0, max_alerts - len(targets))
    )
    if dry_run and pending:
        # 재시도도 다음 실제 실행이 지불할 비용이다 — 안 세면 추정이 아래로 어긋난다.
        # 실제 실행과 같은 예산을 적용해야 추정이 실측과 일치한다.
        tally.counts["가이드라인"] += min(len(pending), retry_budget)

    # MQ 연결은 프로세스당 재사용이라 반드시 닫는다. 안 닫고 이벤트 루프가 내려가면
    # 재연결 태스크가 남아 "Task was destroyed but it is pending" 이 뜨고, 반복 호출되면
    # 연결이 샌다. 루프 도중 예외가 나가도 닫히도록 finally 다(발행이 없었으면 no-op).
    try:
        for alert in targets:
            if dry_run:
                _count_dry_run(alert, tally)
                continue
            await _process_alert(
                alert,
                documents=documents,
                trace_id=trace_id,
                classifier_versions=classifier_versions,
                tally=tally,
            )

        # ── §4 가이드라인 재시도 패스 ────────────────────────────
        # 메인 루프 **뒤**다 — 대기 건의 알림은 억제돼 있어 targets 에 없으므로 두
        # 경로가 같은 알림을 겹쳐 처리하지 않는다. 같은 try 안이라 finally 가 MQ 를
        # 닫아준다. 재시도 = **재생성**이다(PENDING_GUIDELINE_PATH docstring).
        if pending and not dry_run:
            if not documents:
                # 원문이 없으면 generate_guideline 이 ValueError 로 죽어 attempts 만
                # 태운다. 대기열을 그대로 두고 다음 배치로 넘긴다.
                logger.info("documents 0건 — 가이드라인 재시도 %d건 보류", len(pending))
            else:
                retry_now = pending[:retry_budget]
                held = pending[retry_budget:]
                if held:
                    logger.info(
                        "가이드라인 재시도 %d건은 --max-alerts 예산 밖 — 다음 배치로 보류",
                        len(held),
                    )
                remaining: list[dict] = []
                for entry in retry_now:
                    p_alert: DetectionAlert = entry["alert"]
                    try:
                        retry_inquiries = build_linked_inquiries(p_alert, documents)
                        retry_guideline = await generate_guideline(
                            p_alert, retry_inquiries
                        )
                        if retry_guideline is None:
                            # 대상 아닌 알림은 애초에 대기열에 안 들어온다. 그래도
                            # 조용히 눌러앉지 않게 종결로 뺀다 — 게이트 정책이 바뀌거나
                            # 가이드라인 미연결 환경으로 파일이 넘어와도 안전하다.
                            logger.info(
                                "가이드라인 재시도 대상 아님 — 종결 alert=%s",
                                p_alert.alert_id,
                            )
                            continue
                        tally.counts["가이드라인"] += 1
                        await publish_guideline_generated(retry_guideline, trace_id)
                        tally.counts["발행:가이드"] += 1
                        retried_ok += 1
                    except Exception as exc:  # noqa: BLE001 - 배치 격리가 목적
                        entry["attempts"] += 1
                        tally.fail(p_alert.alert_id, "가이드라인 재시도", exc)
                        if entry["attempts"] >= GUIDELINE_RETRY_MAX_ATTEMPTS:
                            retry_exhausted += 1
                            logger.warning(
                                "가이드라인 재시도 %d회 소진 — 포기 alert=%s (%r)",
                                GUIDELINE_RETRY_MAX_ATTEMPTS,
                                p_alert.alert_id,
                                exc,
                            )
                        else:
                            remaining.append(entry)
                still_pending = remaining + held

    finally:
        await close_mq()

    # 대기열을 억제 캐시보다 **먼저** 저장한다(write-ahead). 순서가 반대면 두 쓰기 사이에서
    # 죽었을 때 알림은 억제되는데 대기 항목이 없어 가이드라인이 영구 유실된다. 저장이
    # 실패하면 그 알림을 delivered 에서 빼 다음 배치가 통째로 재처리하게 한다 — 영구 유실보다
    # 중복 비용을 택한다. 반대 방향의 크래시 창은 실행 초입의 대기열-target 조정이 걷어낸다.
    pending_after = still_pending + [
        {"alert": a, "attempts": 0} for a in tally.guideline_pending
    ]
    if not dry_run and window_end:
        try:
            save_pending_guidelines(pending_after, pending_path)
        except Exception as exc:  # noqa: BLE001 - 저장 실패가 배치를 못 세우게
            tally.fail("(가이드라인 대기열)", "대기열 저장", exc)
            undeliverable = {a.alert_id for a in tally.guideline_pending}
            tally.delivered = [
                a for a in tally.delivered if a.alert_id not in undeliverable
            ]

    # 캐시는 dry-run 에서 건드리지 않는다 — 안 보낸 걸 보냈다고 기록하지 않는다.
    cached = (
        save_published(tally.delivered, window_end, state_path)
        if (not dry_run and tally.delivered and window_end)
        else 0
    )

    return {
        "trace_id": trace_id,
        "dry_run": dry_run,
        # 입력원을 결과에 박아둔다 — 골든(oracle)으로 돌린 숫자를 "탐지 성능"으로
        # 인용하는 사고를 막는 게 목적이다. eval/README §68 이 실험① 에 붙인 경고와
        # 같은 이유이고, 거기서는 사람이 문서에 적었지만 여기서는 코드가 매번 낸다.
        "input_source": getattr(loader, "__name__", str(loader)),
        "elapsed_sec": round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "items": len(items),
        "documents": len(documents),
        # 입력에서 버린 행의 사유별 건수. 경고 로그로만 남기면 CronJob 로그를 아무도 안 봐서
        # 미매핑이 늘어도 모른다. **종료코드에는 안 싣는다** — 미매핑은 상류의 데이터 갭이고
        # 사람이 손으로 재매핑하는 흐름이라 배치를 세워도 할 수 있는 게 없다(갭은 카운터로,
        # 고장은 종료코드로). `None` 은 0건이 아니라 "이 입력원은 보고하지 않는다" 이다.
        "input_dropped": input_dropped,
        "coverage_gap_slots": len(unreliable),
        "coverage_missing_documents": sum(
            gap["documents"] - gap["classified"] for gap in coverage_gaps
        ),
        "prior_alerts": len(prior),
        "published": len(alerts),
        # suppressed 도 정상 alert_id 를 갖고 있다. 구분 없이 세면 나중에 "발행된 건가
        # 억제된 건가"를 알 수 없으므로 따로 센다.
        "suppressed": len(suppressed),
        "processed": len(targets),
        # 발행에 성공해 캐시에 들어간 건수. published(탐지) 와 다를 수 있고,
        # 그 차이가 곧 "다음 배치에서 다시 시도될 알림"이다.
        "delivered": len(tally.delivered),
        "llm_calls": dict(tally.counts),
        "cause_calls": stub.calls if stub else None,
        "cause_failures": len(detection_diagnostics.cause_failures),
        # 개선안이 안 나왔지만 실패가 아닌 두 사유. failures 와 **별개**라 종료코드에
        # 안 실린다. `no_evidence` 가 계속 크면 상세페이지 시딩·CS 원문 조회를,
        # `routing_miss` 가 계속 크면 라우팅 프롬프트를 볼 것.
        "no_evidence": tally.evidence_gaps,
        "routing_miss": tally.routing_misses,
        # §4 가이드라인 재시도. retried = 이번 실행이 재발행에 성공한 건수 /
        # pending = 다음 배치가 다시 시도할 건수 / exhausted = 상한 소진으로 포기.
        "guideline_retried": retried_ok,
        "guideline_pending": len(pending_after),
        "guideline_retry_exhausted": retry_exhausted,
        "failures": tally.failures,
        "state_cached": cached,
    }


def print_summary(summary: dict) -> None:
    print("\n" + "=" * 62)
    print(f"배치 요약  trace_id={summary['trace_id']}  {summary['elapsed_sec']}초")
    print("=" * 62)
    print(
        f"  입력          items {summary['items']} / documents {summary['documents']}"
        f"  [{summary['input_source']}]"
    )
    # 요약에 실어 두기만 하면 절반이다 — 사람이 실제로 보는 것은 이 화면이다.
    #    비어 있으면(또는 관측 불가면) 줄을 안 낸다: 매번 "0건" 을 찍으면 눈에 안 띄는
    #    줄이 하나 늘 뿐이고, 이 항목의 목적은 **늘었을 때 보이는 것**이다.
    if summary.get("input_dropped"):
        detail = " / ".join(
            f"{reason} {count}건" for reason, count in summary["input_dropped"].items()
        )
        print(f"  ⚠️ 입력 제외   {detail}  ← 분모에서 빠진 원문")
    print(
        f"  분류 coverage 제외 슬롯 {summary.get('coverage_gap_slots', 0)} / "
        f"부모 레코드 누락 {summary.get('coverage_missing_documents', 0)}건"
    )
    if summary["input_source"] == "load_golden_inputs":
        print(
            "  ⚠️ 골든 라벨(oracle) 입력 — 분류 오차 0%. 이 숫자는 탐지 성능이 아니다."
        )
    print(f"  prior_alerts  {summary['prior_alerts']}건")
    print(f"  탐지          {summary['published']}건")
    print(f"  억제          {summary['suppressed']}건  ← 발행 아님")
    print(f"  후속 처리     {summary['processed']}건")
    if not summary["dry_run"]:
        print(
            f"  발행 성공     {summary['delivered']}건  ← 캐시(prior_alerts)에 들어간 것"
        )
        missed = summary["processed"] - summary["delivered"]
        if missed > 0 or summary["published"] > summary["processed"]:
            print(
                f"  ↳ 캐시에 안 넣음 {summary['published'] - summary['delivered']}건"
                " (상한으로 잘렸거나 발행 실패) — 다음 배치에서 다시 시도됩니다"
            )
    if summary.get("no_evidence"):
        print(
            f"  개선안 생략   {summary['no_evidence']}건  ← 근거 0건(상세페이지 미등록·CS"
            " 원문 없음). 실패 아님"
        )
    if summary.get("routing_miss"):
        # 종료코드에서 뺐으니 요약에서라도 눈에 띄어야 한다 — 안 그러면 라우팅 미스가
        # 조용히 쌓이고 프롬프트를 손볼 근거가 사라진다.
        print(
            f"  라우팅 미스   {summary['routing_miss']}건  ← 근거가 있는 쪽을 모델이 안"
            " 골랐음. 실패 아님 / 프롬프트 재측정 대상"
        )
    if summary.get("cause_failures"):
        print(
            f"  원인분류 실패 {summary['cause_failures']}건  ← 탐지는 계속했지만 "
            "배치는 실패 상태로 종료"
        )
    if (
        summary.get("guideline_retried")
        or summary.get("guideline_pending")
        or summary.get("guideline_retry_exhausted")
    ):
        print(
            f"  가이드라인 재시도  성공 {summary.get('guideline_retried', 0)}건 / "
            f"대기 {summary.get('guideline_pending', 0)}건 / "
            f"포기 {summary.get('guideline_retry_exhausted', 0)}건"
        )
    if summary["dry_run"]:
        print("\n  [dry-run] LLM 호출 0회. 실제로 돌리면:")
        print(
            f"     Agent2 [6] 원인분류 : {summary['cause_calls']}회  ← 스텁이 가로채 실측"
        )
        print(f"     후속 단계           : {summary['llm_calls']}")
        print(
            "     개선안은 recommended_action=='개선안 생성' 인 alert 만 (1건당 LLM 2~4회)."
        )
    if summary["failures"]:
        print(f"\n  ⚠️ 실패 {len(summary['failures'])}건 (배치는 계속 진행됨)")
        for f in summary["failures"][:10]:
            failure_key = f.get("target_key", "식별자 없음")
            print(f"     {failure_key} [{f['stage']}] {f['error'][:80]}")
    missing = [
        name
        for name, ok in [
            ("RabbitMQ(app.core.mq)", MQ_AVAILABLE),
            ("Agent3(generate_outcome_for_alert)", RECOMMENDATION_AVAILABLE),
            ("가이드라인(generate_guideline)", GUIDELINE_AVAILABLE),
        ]
        if not ok
    ]
    if missing:
        print(f"\n  ℹ️ 미연결: {', '.join(missing)} — 해당 단계는 no-op 입니다.")


def main() -> None:
    # **argparse 를 만들기 전에 부른다.** 예전엔 `parse_args()` 뒤에 있었는데,
    #    아래 `--state-path` 도움말에 `—` 가 들어 있어 **`--help` 나 잘못된 인자만으로
    #    cp949 콘솔에서 죽었다**(exit 1, UnicodeEncodeError).
    #    argparse 는 우리 코드가 첫 줄을 찍기 한참 전에 자기 출력을 내보낸다.
    force_utf8_output()

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument(
        "--window-end", default=None, help="현재 윈도우 마지막 날 (YYYY-MM-DD)"
    )
    ap.add_argument(
        "--max-alerts",
        type=int,
        default=None,
        help="후속 처리할 alert 수 상한 (비용 통제). 가이드라인 재시도도 이 예산을 나눠 쓴다",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="LLM 0회 — 몇 번 부를지만 실측한다"
    )
    ap.add_argument(
        "--input-source",
        choices=["db", "golden"],
        default="db",
        help="db(기본, 운영) / golden(평가·재현 전용 — scripts/golden_inputs.py)."
        " golden 은 분류 오차가 0 이라 결과가 탐지 성능이 아니다. 요약이 경고를 낸다.",
    )
    ap.add_argument(
        "--state-path",
        default=None,
        help=f"발행 기록 캐시 경로 (기본 {STATE_PATH}). 디버그 실행이 운영 캐시를"
        " 오염시키지 않게 따로 지정할 것 — 캐시가 오염되면 그 알림이 억제 기간 내내"
        " 셀러에게 안 간다. 가이드라인 대기열(pending_guidelines)도 이 경로에서"
        " 파생돼 같이 옮겨간다.",
    )
    args = ap.parse_args()

    loader = None
    if args.input_source == "golden":
        # app/ 안에서 골든을 읽지 않기 위해 **여기서만** 늦게 import 한다
        # (eval/README §232). 모듈 최상단에 두면 운영 import 경로에 골든이 딸려온다.
        from scripts.golden_inputs import load_golden_inputs

        loader = load_golden_inputs

    # 자리가 고정돼 있다. `parse_args()` 앞으로 옮기면 설정 오타 하나로 `--help` 조차 못
    # 보고, `force_utf8_output()` 앞으로는 못 간다(그건 첫 문장이어야 한다).
    # `get_settings()` 가 `@lru_cache` 라 여기서 한 번 읽으면 깊은 호출부까지 덮인다 —
    # 실패해도 `run_batch` 전에 exit 2 로 끝나서, 아래 "실패가 있음"(=1)과 구분된다.
    configure_logging_or_exit("배치")
    # 바깥 껍질 — 부팅 가드가 못 보는 **실행 중 환경 전제**도 exit 2 로 가른다. 배치에서
    # 제일 잦은 실패가 그쪽이다(raw DB 경로·옛 스키마·분류 결과 테이블 부재). 전부 재시작해도
    # 같으니 exit 1 로 보고하면 k8s 가 영원히 재시도한다.
    #
    # 세 타입인 이유: `RuntimeError` 는 `app/` 전체에서 전제 검사뿐이고(AST 가드가 잠근다),
    # `FileNotFoundError` 는 "raw DB 없음" 하나다. `psycopg.Error` 는 둘 중 어느 것도 아니라
    # `connection_error_types()` 를 안 넣으면 Postgres 실패가 전부 exit 1 + traceback 이 된다.
    #
    # 메시지를 자르지 않는다(부팅 경로와 다르다) — 전제 검사들이 조치 방법까지 담고 있어서
    # 첫 줄만 남기면 정작 필요한 안내를 잃는다.
    try:
        summary = asyncio.run(
            run_batch(
                window_end=(
                    date.fromisoformat(args.window_end) if args.window_end else None
                ),
                max_alerts=args.max_alerts,
                dry_run=args.dry_run,
                state_path=Path(args.state_path) if args.state_path else STATE_PATH,
                load_inputs=loader,
            )
        )
    except (FileNotFoundError, RuntimeError, *connection_error_types()) as exc:
        print(f"환경이 준비되지 않아 배치를 돌리지 못했습니다: {exc}", file=sys.stderr)
        sys.exit(EXIT_CONFIG_ERROR)

    print_summary(summary)

    # 계속 도는 것과 성공으로 보고하는 것은 다르다. 실패가 있으면 비-0 으로 끝내야
    #    cron·k8s Job 이 알아챈다 — 안 그러면 모든 알림이 발행 실패해도 성공한 배치다.
    #
    # **값(1)은 그대로다 — 출처만 상수로 바꿨다.** 여기는 "배치가 돌긴 했는데 일부가
    #    실패" 라 `EXIT_RUNTIME_ERROR` 의 정의(*"재시작하면 나을 수 있다"*)에 정확히 맞는다.
    #    설정 오류(재시작해도 같음)는 위 `configure_logging_or_exit()` 이 2 로 가른다.
    if summary["failures"]:
        sys.exit(EXIT_RUNTIME_ERROR)


if __name__ == "__main__":
    main()
