"""담당: 지인 (2026-08-06 인수, 원작 서영) — 일 1회 탐지 배치. **운영 진입점.**

왜 여기 있나
------------
백엔드 확정 구조상 탐지의 시작점은 **AI 노드 안에서 매일 도는 배치**다. 메인→AI 방향
큐에 '탐지 요청'이 없고(셀러 피드백 2종뿐), `/detect` 는 body 로 items(분류 결과 전량)
를 받는 API 인데 그걸 손에 든 건 분류를 수행하는 AI 노드 자신뿐이다 — 백엔드는 분류를
하지 않는다. 그래서 `POST /detect` 는 운영 경로가 아니라 **재현·디버깅 창구**로 남는다.
(2026-08-05 지인님 결선 정리)

이 모듈이 `detection/service.py` 밖에 있는 이유
    탐지 → 개선안 → CS 가이드라인 → 발행 루프를 detection 안에 넣으면 detection 이
    recommendation 을 import 하게 되어 **"각 모듈은 core 에서만 가져다 쓴다"**는 팀
    규칙이 깨진다. 그래서 양쪽 바깥에 있는 이 모듈이 둘 다 부른다.

실행 (스케줄링은 아직 안 붙인다 — 주체가 백엔드인지 AI 노드인지 미정)::

    python -m app.batch.daily --dry-run          # LLM 0회, 호출 횟수만 실측
    python -m app.batch.daily --max-alerts 3     # 비용 상한
    python -m app.batch.daily --window-end 2026-08-28
    python -m app.batch.daily --input-source golden --dry-run   # 평가·재현 (oracle)

입력은 **주입**받는 형태다 — `run_batch(load_inputs=...)`. 기본값
`load_inputs_from_db()` 는 raw DB(`cs`·`reviews`·`classified_item`)를 직접 읽는다.
목 파이프라인에서는 그 테이블을 `scripts/mock_producer.py` 와
`scripts/classification_worker.py` 가 채우므로, **둘을 먼저 돌려야 배치가 돈다.**

🔴 **이 모듈은 `data/golden/` 을 읽지 않는다.** `eval/README.md` §232("`data/golden/`
   은 `eval/` 만 읽는다 — `app/` 이 import 하면 컨닝이다")를 지키기 위해 골든 로더를
   `scripts/golden_inputs.py` 로 뺐다(2026-08-06). 평가·재현으로 배치를 돌릴 때만
   `--input-source golden` 으로 주입하고, 그때는 요약이 oracle 경고를 함께 낸다.
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
from app.core.raw_db import (
    connection_error_types,
)
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
# 시그니처는 지인님 결선 정리(2026-08-05)의 예시 코드를 그대로 따른다.
# 실물이 생기면 아래 except 가 안 타므로 이 파일은 손댈 필요가 없다.
#
# ⚠️ **모듈이 없을 때만 폴백한다.** `except ImportError` 로 통째로 삼키면, 모듈은
#    올라왔는데 그 안의 의존성(예: `aio_pika`)이 없어서 나는 ImportError 까지 먹고
#    조용히 no-op 이 된다 — 요약엔 "미연결"로 찍혀서 보는 사람은 "아직 안 만들었나"
#    로 읽고, 이벤트가 하나도 안 나가는데 배치는 정상 종료한다.
#    (지인님 PR 리뷰 §6, 2026-08-06)


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

⚠️ `scripts/crosscheck_agent2_to_agent3.py` 도 이 값과 `CountingClient` 를 그대로
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

    # ⚠️ **window_end 를 여기서 한 번만 확정한다.** 로드·탐지·저장이 같은 값을 써야 한다.
    #    읽기는 실행 시각(`date.today()`), 쓰기는 데이터 시각(`window_end`)이면, 데이터가
    #    오늘보다 STATE_RETENTION_DAYS 이상 뒤처진 상태(백필·유입 지연)에서 로드가 방금
    #    저장한 캐시를 통째로 버려 **매 배치가 첫 실행처럼 굴러간다.** 억제 모듈이 경과일을
    #    데이터 시각으로 세는 것과 같은 이유다. (지인님 PR 리뷰 §5, 2026-08-06)
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

    # **KST 로 오늘을 정한다.** 문서가 하나도 없어 window_end 를 데이터에서 못 정했을
    # 때만 타는 분기다. `date.today()` 는 호스트 로컬이라 UTC 컨테이너에서는 KST 보다
    # 하루 이른 날짜가 나오는데, **날짜 경계는 §3 이 KST 로 못박았으므로** 여기서도
    # 같은 기준을 쓴다.
    #
    # ⚠️ **운영 사고를 막는 코드가 아니다 — 계약 일관성용이다** (서영님 사후 리뷰, PR #68).
    #    이 분기에서 `prior` 는 바로 아래 로그의 건수에만 쓰인다: documents 가 0건이라
    #    `detect_anomaly` 가 빈 rows 로 즉시 반환하고(`service.py` 의 `if not rows`),
    #    `save_published` 도 window_end 가 None 이라 건너뛴다. `load_prior_alerts` 는
    #    읽기 전용이라 상태 파일도 안 바뀐다.
    #
    #    처음엔 "기록이 하루 일찍 잘려 억제가 빨리 풀린다"고 적었는데 **방향이 반대다** —
    #    여기서 넘긴 날짜로 `cutoff = 그 날짜 - STATE_RETENTION_DAYS` 를 잡고
    #    **`alert.window_end >= cutoff`** 인 기록을 보관하므로(`load_prior_alerts`),
    #    날짜가 이르면 cutoff 도 일러져 오히려 **더 오래** 남는다
    #    (실측: today=1/28 → 3건 보관, today=1/29 → 2건).
    #    ⚠️ 두 `window_end` 는 다른 값이다 — 앞은 이 함수의 인자(기준일), 뒤는 보관
    #       후보 알림의 필드다. 이름이 같아 자기 자신과 비교하는 것처럼 읽힌다.
    #       (서영님 PR #69 리뷰 잔가지)
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

    # ⚠️ dry-run 이어도 [6] 원인분류는 detect_anomaly 안에서 돈다. 스텁을 안 주면
    #    "LLM 0회"라고 해놓고 실제로 과금된다.
    stub = CountingClient() if dry_run else None
    detection_diagnostics = DetectionDiagnostics()

    alerts, suppressed = await detect_anomaly(
        items,
        documents=documents,
        window_end=window_end,
        prior_alerts=prior,
        # 백엔드가 어디서 줄지 미정 — 정해질 때까지 빈 집합 (지인님 결선 §8).
        resolved_alert_ids=set(),
        unreliable_denominators=unreliable,
        client=stub,
        diagnostics=detection_diagnostics,
    )

    targets = alerts if max_alerts is None else alerts[:max_alerts]
    failures: list[dict] = [
        {
            "target_key": (
                f"{failure['product']}/{failure['aspect']}/"
                f"{failure['channel']}/{failure['source']}"
            ),
            "stage": "원인분류",
            "error": failure["error"],
        }
        for failure in detection_diagnostics.cause_failures
    ]
    counts: Counter[str] = Counter()
    # 개선안이 안 나왔지만 **실패가 아닌** 두 사유의 건수. failures 와 분리해 두는
    # 이유는 종료코드에 안 실리게 하기 위해서다(루프 안 주석 참고).
    evidence_gaps = 0
    routing_misses = 0
    # §4 — 두 상태 파일(대기열·억제 캐시)은 원자적으로 같이 못 쓴다. 직전 실행이
    # 대기열 저장(선행)과 억제 캐시 저장(후행) **사이**에서 죽으면 "대기열엔 있는데
    # 억제는 안 된" 알림이 남고, 그 알림은 이번 실행에 신규 target 으로 다시 뜬다 —
    # 메인 루프가 가이드라인까지 다시 만드므로 겹치는 대기 항목을 여기서 걷어내
    # **한 경로만** 태운다 (PR #90 리뷰 2회전 P1 실측: 같은 guideline_id 2회 발행).
    # 걷힌 건의 attempts 는 버려진다 — 메인 루프가 실패하면 attempts=0 으로 다시
    # 들어오는데, 크래시 창 한정이라 재시도가 늘어나는 방향의 오차만 있다.
    target_ids = {a.alert_id for a in targets}
    superseded = sum(1 for e in pending if e["alert"].alert_id in target_ids)
    if superseded:
        logger.info(
            "가이드라인 대기 %d건이 이번 실행의 신규 target 과 겹쳐 대기열에서 뺍니다"
            " (메인 루프가 처리)",
            superseded,
        )
        pending = [e for e in pending if e["alert"].alert_id not in target_ids]
    # §4 — 이 배치에서 "가이드라인만 못 나간" 알림. 끝에 대기열로 들어간다.
    new_pending: list[DetectionAlert] = []
    # 재시도 패스가 안 돌면(dry-run·documents 0건) 대기열은 그대로 다음 배치로 넘어간다.
    still_pending: list[dict] = list(pending)
    retried_ok = 0
    retry_exhausted = 0
    # §4 — 재시도도 --max-alerts 예산 안에서 돈다: 신규 target 이 먼저 쓰고 남는 만큼만.
    # 상한을 우회하면 장애 복구 직후 대기열 규모만큼 LLM·S3 비용이 한 번에 나간다
    # (PR #90 리뷰 P2). 예산 밖 대기 건은 attempts 를 안 쓰고 다음 배치로 넘어간다.
    retry_budget = (
        len(pending) if max_alerts is None else max(0, max_alerts - len(targets))
    )
    if dry_run and pending:
        # 재시도도 다음 실제 실행이 지불할 비용이다 — 안 세면 추정이 아래로 어긋난다.
        # 실제 실행과 같은 예산을 적용해야 추정이 실측과 일치한다.
        counts["가이드라인"] += min(len(pending), retry_budget)
    # ⚠️ **발행에 성공한 것만** 캐시에 넣는다. save_published docstring 참고.
    delivered: list[DetectionAlert] = []

    # ⚠️ **연결을 반드시 닫는다.** app/core/mq.py 가 프로세스당 연결·채널을 재사용하는데,
    #    닫지 않고 이벤트 루프가 내려가면 connect_robust 의 재연결 태스크가 정리되지 않아
    #    "Task was destroyed but it is pending" 이 뜨고, 나중에 이 배치가 장수 프로세스에
    #    얹히거나 반복 호출되면 연결이 샌다. 루프 도중 예외가 나가도 닫히도록 finally 다.
    #    한 번도 발행하지 않았으면(dry-run 등) 연결 자체가 없어서 no-op 이다.
    #    (서영님 PR 리뷰 §1, 2026-08-07)
    try:
        for alert in targets:
            # [2] 개선안 게이트 — 조치 7종 중 '개선안 생성' 일 때만 Agent3 가 돈다.
            wants_recommendation = should_generate(alert)

            if dry_run:
                # 실제로 몇 번 부를지만 센다. 추정이 아니라 실측이다 — 게이트를 안 태우면
                # Agent3 비용이 크게 과대추정된다(조치 7종 중 1종만 해당).
                #
                # ⚠️ **개선안 수치는 상한이다.** dry-run 은 근거 조회(ChromaDB·CS 원문)를
                #    안 하므로 근거 0건으로 걸러질 알림을 미리 알 수 없다. 실제 실행은
                #    그것들을 `no_evidence` 로 빼므로 여기보다 작게 나온다 — **어긋난 게
                #    아니라 원리적으로 못 맞추는 것**이다(가이드라인 쪽은 게이트가
                #    `evidence.inquiry_ids` 만 보므로 조회 없이도 맞출 수 있어 태운다).
                if wants_recommendation:
                    counts["개선안"] += 1
                # 가이드라인도 **게이트를 태워서** 센다. `evidence.inquiry_ids` 가 빈 알림
                # (스코프 밖 — 파손·오배송)은 `is_guideline_target()` 이 걸러 LLM 을 아예
                # 안 부르는데, 세면 그만큼 과대추정된다. 개선안과 달리 가이드라인은 발화한
                # 알림 거의 전부에 돌아서 건수가 그대로 비용이다.
                if is_guideline_target(alert):
                    counts["가이드라인"] += 1
                counts["발행:이상"] += 1
                continue

            # 개선안·가이드라인이 **같은 CS 원문**을 근거로 쓴다. 여기서 한 번 만들어 둘 다
            # 에게 넘긴다 — 각자 만들면 같은 매핑이 두 벌이 되고, C4(item_id ↔ cs/reviews PK)
            # 가 풀려 DB 조회로 바뀔 때 고칠 곳이 두 곳이 된다.
            #
            # 🔴 **아래 격리 안에 있어야 한다 — 밖에 두면 배치가 통째로 죽는다.** 이 루프를
            #    감싸는 try(위 `finally: close_mq()`)엔 except 가 없어서 여기서 던지면
            #    run_batch 밖으로 나가고, `save_published()` 가 try/finally **뒤**라 같이
            #    건너뛴다 — **이미 발행에 성공한 앞쪽 알림이 캐시에 안 들어가서 다음 배치가
            #    같은 알림을 다시 만들고 LLM 비용을 또 쓴다.** documents 한 행이 이상해서
            #    죽을 수 있는 자리라(값 검증은 `LinkedCSInquiry` 가 한다) 격리 대상이다.
            #
            # ⚠️ **`continue` 하지 않는다 — 알림 자체는 발행한다.** 알림은 통계로 서고
            #    CS 원문과 무관하다. 여기서 건너뛰면 셀러가 그 이상 자체를 못 본다.
            #    이 루프의 다른 단계도 전부 같은 규율이다(`개선안`·`가이드라인` 실패가
            #    발행을 막지 않는다 — test_silent_recommendation_failure_still_shows_up).
            #    빈 리스트로 내려보내면 `generate_guideline` 이 "대상 알림인데 원문이
            #    0건" 을 ValueError 로 올려서 그쪽 단계에도 정직하게 남는다(그 함수
            #    docstring 의 Raises 가 바로 이 경우다). 두 항목이 남지만 단계 이름이
            #    달라서 어느 쪽이 근본 원인인지 구분된다.
            try:
                inquiries = build_linked_inquiries(alert, documents)
            except Exception as exc:  # noqa: BLE001 - 배치 격리가 목적
                inquiries = []
                failures.append(
                    {
                        "target_key": alert.alert_id,
                        "stage": "CS 원문 매핑",
                        "error": repr(exc),
                    }
                )

            # ⚠️ alert 1건이 터져도 배치는 계속한다. 여기서 던지면 **이미 LLM 비용을 쓴
            #    앞쪽 알림들까지 발행되지 않고 날아간다.** 실패는 모아서 끝에 요약한다.
            rec = guideline = None
            # §4 — 대기열 적재 판정용. 아래 두 예외 지점이 후보만 표시하고, 실제 적재는
            # 알림 발행 성공 여부까지 보고 이 반복의 끝에서 한다 (PR #90 리뷰 P2).
            anomaly_delivered = False
            guideline_undelivered = False
            if wants_recommendation:
                try:
                    outcome = await generate_outcome_for_alert(alert, inquiries)
                    rec = outcome.recommendation
                except Exception as exc:  # noqa: BLE001 - 배치 격리가 목적
                    counts["개선안"] += 1
                    failures.append(
                        {
                            "target_key": alert.alert_id,
                            "stage": "개선안",
                            "error": repr(exc),
                        }
                    )
                else:
                    # ⚠️ `generate_outcome_for_alert` 는 **계약상 예외를 안 던지고** 실패를
                    #    개선안 없는 결과로 돌려준다. 위 except 만 두면 실패가 요약에도
                    #    종료코드에도 안 남아서, 개선안이 하나도 안 붙은 배치가 "성공"으로
                    #    끝난다. else 인 이유: except 와 둘 다 타면 실패 1건이 요약에 2건으로
                    #    잡혀 배치 요약의 실패 건수를 못 믿게 된다.
                    #
                    # 🔴 **개선안이 없는 사유를 셋으로 가른다 (2026-08-10).**
                    #    상세페이지 미등록은 흔한 **데이터 갭**이고(mock 504행 중 489행이
                    #    "정보 없음"), 그걸 실패로 세면 배치가 상시 종료코드 1로 끝나서 진짜
                    #    장애가 묻힌다. 근거 0건(`is_evidence_gap`)과 모델이 빈 쪽을 고른 것
                    #    (`is_routing_miss`)은 **근본 원인이 같아** 둘 다 실패에서 뺀다.
                    #    대신 각각 따로 세서 요약에 남긴다 — 라우팅 미스가 조용해지면
                    #    프롬프트 v3 를 손볼 근거가 사라진다.
                    #    ⚠️ 판정은 `RecommendationOutcome` 이 한다(`counts_as_failure`).
                    #    여기서 사유를 다시 판정하면 사유가 늘 때 두 곳이 갈린다.
                    if outcome.is_evidence_gap:
                        # 라우팅 전에 걸러지므로 LLM 호출은 0회다 — 개선안 카운트에 안 넣는다.
                        evidence_gaps += 1
                    else:
                        # 라우팅까지는 갔으므로 LLM 을 썼다(미스여도 마찬가지).
                        counts["개선안"] += 1
                        if outcome.is_routing_miss:
                            routing_misses += 1
                        elif outcome.counts_as_failure:
                            failures.append(
                                {
                                    "target_key": alert.alert_id,
                                    "stage": "개선안",
                                    "error": outcome.detail
                                    or "생성 실패 — 사유는 app.recommendation.pipeline 로그 참고",
                                }
                            )

            try:
                guideline = await generate_guideline(alert, inquiries)
                # ⚠️ `None` 은 **생성 대상이 아니라는 뜻**이지 실패가 아니다
                #    (`is_guideline_target()` — `evidence.inquiry_ids` 가 빈 스코프 밖 알림).
                #    그것까지 세면 dry-run 추정과 실제 집계가 서로 다른 것을 세게 되고,
                #    비용 추정이 위로 어긋난다. 실패(FAILED_*)는 콜백을 돌려주므로 여기 든다.
                if guideline is not None:
                    counts["가이드라인"] += 1
            except Exception as exc:  # noqa: BLE001
                # §4 — 생성 예외 = 대상 알림인데 백엔드가 아무것도 못 들었다 → 대기 후보.
                #    FAILED_* 는 예외가 아니라 반환값이라 여기 안 온다. 그 콜백은 아래
                #    발행이 성공하는 순간 백엔드가 종결 상태를 들은 것이므로 재시도하지
                #    않는다(재시도하면 FAILED 행을 SUCCESS 로 덮는 계약 변경이 된다).
                guideline_undelivered = True
                failures.append(
                    {
                        "target_key": alert.alert_id,
                        "stage": "가이드라인",
                        "error": repr(exc),
                    }
                )

            try:
                await publish_anomaly_analyzed(alert, rec, trace_id, classifier_versions)
                counts["발행:이상"] += 1
                delivered.append(alert)
                anomaly_delivered = True
            except Exception as exc:  # noqa: BLE001
                failures.append(
                    {
                        "target_key": alert.alert_id,
                        "stage": "발행:이상",
                        "error": repr(exc),
                    }
                )

            if guideline is not None:
                try:
                    await publish_guideline_generated(guideline, trace_id)
                    counts["발행:가이드"] += 1
                except Exception as exc:  # noqa: BLE001
                    # §4 본체 — 만들어 놓고 백엔드가 못 들었다 → 대기 후보.
                    guideline_undelivered = True
                    failures.append(
                        {
                            "target_key": alert.alert_id,
                            "stage": "발행:가이드",
                            "error": repr(exc),
                        }
                    )

            # §4 — 대기열은 "알림은 **나갔는데** 가이드라인만 못 나간" 건만 받는다.
            # 알림 발행까지 실패한 건은 캐시에 안 들어가 다음 배치가 그 알림을 통째로
            # 재처리한다(가이드라인도 그 경로에서 다시 만들어진다) — 대기열에도 넣으면
            # 같은 가이드라인이 두 경로에서 두 번 생성·발행된다 (PR #90 리뷰 P2 실측:
            # 같은 guideline_id 가 2회 발행 + LLM·S3 이중 지불).
            if guideline_undelivered and anomaly_delivered:
                new_pending.append(alert)

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
                        counts["가이드라인"] += 1
                        await publish_guideline_generated(retry_guideline, trace_id)
                        counts["발행:가이드"] += 1
                        retried_ok += 1
                    except Exception as exc:  # noqa: BLE001 - 배치 격리가 목적
                        entry["attempts"] += 1
                        failures.append(
                            {
                                # failures 식별자 키는 target_key 하나다 (#80 후속,
                                # `5adc025`). print_summary 가 이 키만 읽는다.
                                "target_key": p_alert.alert_id,
                                "stage": "가이드라인 재시도",
                                "error": repr(exc),
                            }
                        )
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

    # §4 대기열을 억제 캐시보다 **먼저** 저장한다(write-ahead) — 순서가 반대면 두 쓰기
    # 사이에서 죽었을 때 알림은 억제되는데 대기 항목이 없어, 이 PR 이 막으려는 구멍
    # (가이드라인 영구 유실)이 그대로 재현된다 (PR #90 리뷰 P1). 저장이 실패하면 해당
    # 알림을 delivered 에서 빼서 다음 배치가 통째로 재처리하게 한다 — 영구 유실보다
    # 중복 비용(재발행·재생성)을 택한다.
    #
    # 같은 alert_id 가 대기열과 신규 target 에 **정상 경로에서는** 겹치지 않지만(대기열은
    # 알림 발행 성공 건만 받아 자기 window 동안 억제된다), 두 파일을 원자적으로 같이 쓸
    # 수 없어 "대기열 저장 성공 + 억제 캐시 저장 실패" 크래시 창에서는 겹친다 — 그건
    # 실행 초입의 대기열-target 조정이 걷어낸다(리뷰 2회전 P1). 억제 만료·갱신으로 다시
    # 뜨는 알림은 window_end 가 달라 alert_id 도 다르다(별개 알림 = 각자 가이드라인).
    # dry-run 은 읽기만 하고, window_end 가 없으면 로드도 안 했으므로 파일을 안 건드린다.
    pending_after = still_pending + [
        {"alert": a, "attempts": 0} for a in new_pending
    ]
    if not dry_run and window_end:
        try:
            save_pending_guidelines(pending_after, pending_path)
        except Exception as exc:  # noqa: BLE001 - 저장 실패가 배치를 못 세우게
            failures.append(
                {
                    "target_key": "(가이드라인 대기열)",
                    "stage": "대기열 저장",
                    "error": repr(exc),
                }
            )
            undeliverable = {a.alert_id for a in new_pending}
            delivered = [a for a in delivered if a.alert_id not in undeliverable]

    # 캐시는 dry-run 에서 건드리지 않는다 — 안 보낸 걸 보냈다고 기록하지 않는다.
    cached = (
        save_published(delivered, window_end, state_path)
        if (not dry_run and delivered and window_end)
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
        # 입력에서 **버린** 행의 사유별 건수. 지금까지 경고 로그로만 남아서, 미매핑이
        # 늘어도 아무도 몰랐다 — CronJob 로그를 여는 사람이 없다는 것이 이 저장소가
        # 반복해서 전제해 온 사실이다.
        #
        # ⚠️ **종료코드에 안 싣는다.** 미매핑은 상류(백엔드 상품 매핑)의 데이터 갭이지
        #    우리 배치의 고장이 아니고, 사람이 손으로 재매핑하는 흐름이라(2026-08-18 규리
        #    확인) 배치를 세워도 그 자리에서 할 수 있는 게 없다. `no_evidence` 를 실패에서
        #    뺀 것과 같은 기준이다 — **갭은 카운터로, 고장은 종료코드로.**
        #
        # ⚠️ 값이 `None` 이면 "0건" 이 아니라 **"이 입력원은 보고하지 않는다"** 이다
        #    (`_read_inputs`).
        "input_dropped": input_dropped,
        "coverage_gap_slots": len(unreliable),
        "coverage_missing_documents": sum(
            gap["documents"] - gap["classified"] for gap in coverage_gaps
        ),
        "prior_alerts": len(prior),
        "published": len(alerts),
        # suppressed 도 정상 alert_id 를 갖고 있다. 구분 없이 세면 나중에 "발행된 건가
        # 억제된 건가"를 알 수 없으므로 따로 센다 (지인님 결선 §6-②).
        "suppressed": len(suppressed),
        "processed": len(targets),
        # 발행에 성공해 캐시에 들어간 건수. published(탐지) 와 다를 수 있고,
        # 그 차이가 곧 "다음 배치에서 다시 시도될 알림"이다.
        "delivered": len(delivered),
        "llm_calls": dict(counts),
        "cause_calls": stub.calls if stub else None,
        "cause_failures": len(detection_diagnostics.cause_failures),
        # 개선안이 안 나왔지만 실패가 아닌 두 사유. failures 와 **별개**라 종료코드에
        # 안 실린다. `no_evidence` 가 계속 크면 상세페이지 시딩·CS 원문 조회를,
        # `routing_miss` 가 계속 크면 라우팅 프롬프트를 볼 것.
        "no_evidence": evidence_gaps,
        "routing_miss": routing_misses,
        # §4 가이드라인 재시도. retried = 이번 실행이 재발행에 성공한 건수 /
        # pending = 다음 배치가 다시 시도할 건수 / exhausted = 상한 소진으로 포기.
        "guideline_retried": retried_ok,
        "guideline_pending": len(pending_after),
        "guideline_retry_exhausted": retry_exhausted,
        "failures": failures,
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
    # 🔴 요약에 실어 두기만 하면 절반이다 — 사람이 실제로 보는 것은 이 화면이다.
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
    # 🔴 **argparse 를 만들기 전에 부른다.** 예전엔 `parse_args()` 뒤에 있었는데,
    #    아래 `--state-path` 도움말에 `—` 가 들어 있어 **`--help` 나 잘못된 인자만으로
    #    cp949 콘솔에서 죽었다**(2026-08-14 재현: exit 1, UnicodeEncodeError).
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

    # ⚠️ **`parse_args()` 뒤에 둔다.** 앞으로 옮기면 설정 오타 하나로 `--help` 조차 못 본다.
    #    ⚠️ 반대로 `force_utf8_output()` 보다 앞으로는 못 간다 — 그건 첫 문장이어야 한다
    #       (`tests/test_console_encoding.py` 가 강제).
    #
    # 🔴 **여기서 설정을 한 번 읽는 것이 아래 깊은 호출부까지 덮는다.** `get_settings()` 가
    #    `@lru_cache` 라, 이 시점에 성공하면 `_active_version_params()`·`load_inputs_from_db()`
    #    가 나중에 부를 때 같은 인스턴스를 받는다. 실패하면 `run_batch` 에 들어가기 전에
    #    exit 2 로 끝난다 — 예전엔 그 깊은 호출부에서 미포착 `ValidationError` 가 터져
    #    **exit 1 + raw traceback** 이었고, 그건 아래 "실패가 있음"(=1)과 구분이 안 됐다.
    #
    # ⚠️ 로깅 레벨이 `logging.INFO` 고정에서 **`LOG_LEVEL` 을 따르는 것으로 바뀐다.**
    #    기본값이 `INFO` 라 평소 동작은 그대로고, 다른 두 진입점과 같아진다.
    configure_logging_or_exit("배치")
    # 🔴 **바깥 껍질 — 실행 중 드러나는 환경 전제도 2 로 가른다.**
    #    부팅 가드(`configure_logging_or_exit`)는 `Settings` 로 읽히는 값만 본다. 그런데
    #    배치에서 **제일 자주 나는 실패는 그 다음**이다 — raw DB 경로가 틀렸거나(볼륨 마운트
    #    누락) 스키마가 옛 버전이거나 분류 결과 테이블이 없는 것. 전부 **재시작해도 같으니**
    #    exit 1("재시작하면 나을 수 있다")로 보고하면 k8s 가 영원히 재시도한다.
    #    (용준님 PR #98 리뷰 ①, 재현 확인)
    #
    #    ⚠️ **왜 이 타입들인가.** `raise RuntimeError` 는 `app/` 전체에서 이 파일의 전제
    #       검사 3곳뿐이고(`test_runtime_error_stays_confined_to_preconditions` 가 잠근다),
    #       `FileNotFoundError` 는 `raw_db.connect_readonly()` 의 "raw DB 없음" 하나다.
    #       ⚠️ `raw_db` 쪽 타입을 바꾸는 안은 못 쓴다 — `app/recommendation/service.py` 가
    #       `except FileNotFoundError` 로 **의도적 degrade** 를 하고 `inquiries.py` 가 그걸
    #       계약으로 문서화해 뒀다.
    #
    #    🔴 **`connection_error_types()` 는 Postgres 문을 같은 계약 안으로 넣는다.**
    #       위 두 타입만으로 환경 전제가 다 덮인다는 것은 **sqlite 일 때만** 참이다 —
    #       `psycopg.Error` 는 둘 중 어느 것도 아니라서(실측) DSN 오타·DB 미기동·뷰 없음·
    #       GRANT 누락이 전부 **여기를 그냥 지나 exit 1 + raw traceback** 이 된다.
    #       #98 이 없앤 상태가 백엔드만 바뀌어 그대로 돌아오는 자리이고, 하필 첫 연동에서
    #       제일 잦다. 목록·근거는 `raw_db.connection_error_types()` docstring.
    #
    #    ⚠️ 라이브러리가 던진 `RuntimeError` 가 여기 걸리면 "환경 문제" 로 오분류된다.
    #       CronJob 이라 **다음 예약 실행은 그대로 돌아서** 비용이 비대칭이다 — 지금(exit 1)은
    #       영구 오류에 무한 재시도이고, 오분류는 이번 실행 한 번을 포기하는 것뿐이다.
    #
    #    ⚠️ **여기서는 메시지를 자르지 않는다**(부팅 경로와 다르다). 위 전제 검사들은
    #       여러 줄로 **조치 방법까지** 담고 있어서(`LLM_MODEL 오타면 설정을 고치세요` 등)
    #       첫 줄만 남기면 정작 필요한 안내를 잃는다. raw traceback 이 아닌 것으로 충분하다.
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

    # ⚠️ 계속 도는 것과 성공으로 보고하는 것은 다르다. 실패가 있으면 비-0 으로 끝내야
    #    cron·k8s Job 이 알아챈다 — 안 그러면 모든 알림이 발행 실패해도 성공한 배치다.
    #    (지인님 PR 리뷰 §4, 2026-08-06)
    #
    # ⚠️ **값(1)은 그대로다 — 출처만 상수로 바꿨다.** 여기는 "배치가 돌긴 했는데 일부가
    #    실패" 라 `EXIT_RUNTIME_ERROR` 의 정의(*"재시작하면 나을 수 있다"*)에 정확히 맞는다.
    #    설정 오류(재시작해도 같음)는 위 `configure_logging_or_exit()` 이 2 로 가른다.
    if summary["failures"]:
        sys.exit(EXIT_RUNTIME_ERROR)


if __name__ == "__main__":
    main()
