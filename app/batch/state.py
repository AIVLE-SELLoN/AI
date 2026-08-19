"""담당: 지인 — 배치가 실행 사이에 들고 가는 상태. 발행 기록 캐시와 가이드라인 대기열.

`daily.py` 에서 갈라져 나왔다. 둘 다 **JSON 파일 하나 = 역할 하나**이고, 손상·부분
기록에 견디는 읽기(깨진 항목은 버리고 경고)와 원자적 쓰기가 공통이라 한 모듈로 묶었다.

두 파일은 원자적으로 같이 쓸 수 없다. 그 창에서 나는 불일치는 쓰는 쪽이 아니라 읽는 쪽
(`run_batch` 초입의 대기열-target 조정)이 조정한다 — 자세한 건 `run_batch` 참고.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from app.core.constants import CURRENT_WINDOW_DAYS, PAST_WINDOW_DAYS
from app.core.schemas import DetectionAlert

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]

STATE_PATH = ROOT / "data" / "batch_state" / "published_alerts.json"
"""발행 기록 캐시. **`prior_alerts` 의 출처.**

alert 저장의 정본은 서비스 DB(백엔드 단독 소유)이고 AI 는 직접 접근할 수 없다. 하지만
재알림 억제(`filter_suppressed`)와 기준선 오염 방지(`_alert_days`)가 `prior_alerts` 를
필요로 하는데, 그건 **AI 가 자기가 발행한 것이라 이미 안다.** 그래서 서비스 DB 쓰기가
아니라 '자기 발행 기록 캐시'로 둔다. 백엔드 조회 API 가 생기면 이 자리만 교체한다.

⚠️ 이게 없으면 매일 배치가 첫 실행처럼 굴러서 ① 같은 알림이 억제 기간 내내 매일 나가고
   ② 더 나쁘게는 `_alert_days` 가 비어 **지속되는 이상이 과거 윈도우에 섞여 '새로운
   평소'로 굳고 알림이 스스로 꺼진다.** 매일 도는 배치에서는 며칠 안에 실제로 난다.

🔴 **컨테이너로 올릴 때 이 경로에 볼륨을 붙일 것** (지인님 지적, 2026-08-05).
   이미지 안 임시 파일로 두면 재시작마다 날아가서 **캐시가 없는 것과 같아진다** —
   위 ①② 가 그대로 재현된다. compose 예시::

       volumes:
         - ./data/batch_state:/app/data/batch_state

   `data/**` 는 .gitignore 대상이라 저장소에도 안 올라간다. 배포 시 호스트 경로를
   반드시 확보할 것.
"""

STATE_RETENTION_DAYS = CURRENT_WINDOW_DAYS + PAST_WINDOW_DAYS
"""캐시 보관 기간(일). 임의값이 아니라 두 소비처가 요구하는 범위의 합이다.

`_alert_days` 가 과거 윈도우(28일) 안의 알림 구간을 제외하고, 억제 판정은 현재
윈도우(7일) 기준으로 경과일을 센다. 그보다 짧게 자르면 그 경계에서 조용히 억제가
풀리고 기준선이 오염된다.
"""

PENDING_GUIDELINE_PATH = ROOT / "data" / "batch_state" / "pending_guidelines.json"
"""가이드라인 전달 대기열 — **알림 발행은 성공했는데 가이드라인만 백엔드가 못 들은
건**을 담는다 (§4, 2026-08-14). 알림 발행까지 실패한 건은 여기 안 온다 — 억제 캐시에
없어 다음 배치가 알림을 통째로 재처리하고, 대기열에도 있으면 가이드라인이 두 경로에서
두 번 만들어진다 (PR #90 리뷰 P2).

`published_alerts.json`(억제 캐시)과 파일을 일부러 나눈다:
  - 억제 캐시는 §2 조회 API(`GET /internal/alerts/active`)가 붙으면 통째로 걷어낼
    파일이지만, "가이드라인을 받았는지"는 그 응답에 없다(백엔드에 요청하지 않기로
    확정). 한 파일에 섞으면 §2 때 지워도 되는 키와 안 되는 키가 섞인다.
  - 새 파일은 배포 첫 실행에 비어 있다 — 기존 캐시 35일치가 재시도 대상으로 둔갑해
    LLM 비용이 한 번에 나가는 사고가 구조적으로 없다.

항목: {"alert": DetectionAlert.model_dump(), "attempts": int}
  attempts = 소진한 **재시도** 횟수. 본배치 실패로 처음 들어올 때 0.
  알림 전문을 담는 이유: `build_guideline_input` 이 알림에서 18개 필드를 읽는데
  §2 응답은 5개뿐이라 alert_id 만으로는 재생성이 불가능하다.

재시도는 **재생성**이다(payload 재발행 아님) — Pre-signed URL·S3 객체 수명(7일)
시계가 생성 시점에 시작되므로, 콜백을 캐시했다 재발행하면 보장 기간이 깎인 채로
(배치 장기 중단 뒤엔 만료된 링크가) SUCCESS 로 나간다. `GenerationCallback` 을
저장하면 리포팅 계약에 결합되는 문제도 있다.

볼륨 요구는 STATE_PATH 와 같다(같은 디렉토리) — 추가 마운트 없음.
"""

GUIDELINE_RETRY_MAX_ATTEMPTS = 3
"""알림 1건당 가이드라인 재시도 상한. 소진하면 경고를 남기고 포기한다.

상한이 없으면 영구 실패(S3 미구성 등)가 같은 건을 매일 재시도한다 — 재시도 1회가
곧 LLM·S3 재지불이다. 배치 상태 정책이라 core/constants.py 가 아니라 여기 둔다.
"""


def _as_date(value: Any) -> date:
    """documents 의 `created_at`(str 또는 datetime)에서 날짜만 뽑는다."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value.date() if isinstance(value, datetime) else value


def _read_state(path: Path) -> list[dict]:
    """캐시 파일을 **항목 단위로 방어하며** 읽는다.

    ⚠️ 이게 유일한 상태 저장소인데 자가복구 경로가 없으면, 쓰다 만 JSON 하나로 배치가
       영구히 못 뜬다. 사람이 파일을 지워야 다시 도는데 그러면 억제·기준선이 첫 실행으로
       리셋된다 — `STATE_PATH` docstring ①② 가 경고한 그 시나리오다.
       `DetectionAlert` 에 필수 필드가 하나 추가되는 것만으로도 같은 상태가 된다
       (스키마는 전원 회의 확정 사항이라 실제로 바뀐다).
       (지인님 PR 리뷰 §3, 2026-08-06)
    """
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("발행 기록 캐시가 손상됐습니다 — 빈 상태로 진행합니다 (%s)", exc)
        return []
    if not isinstance(raw, list):
        logger.warning("발행 기록 캐시 형식이 예상과 다릅니다 — 빈 상태로 진행합니다")
        return []
    return [a for a in raw if isinstance(a, dict) and "alert_id" in a]


def _atomic_write(path: Path, text: str) -> None:
    """임시파일에 쓰고 `os.replace` 로 갈아끼운다. 도중에 죽어도 반쪽 파일이 안 남는다."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def load_prior_alerts(
    window_end: date, path: Path = STATE_PATH
) -> list[DetectionAlert]:
    """캐시에서 `prior_alerts` 를 읽는다. 없으면 빈 리스트(첫 실행).

    깨진 항목은 버리고 경고만 남긴다 — 하나 때문에 배치 전체가 멈추면 안 된다.
    """
    cutoff = date.fromordinal(window_end.toordinal() - STATE_RETENTION_DAYS)
    alerts: list[DetectionAlert] = []
    dropped = 0
    for item in _read_state(path):
        try:
            alert = DetectionAlert.model_validate(item)
        except ValidationError:
            dropped += 1
            continue
        if alert.window_end >= cutoff:
            alerts.append(alert)
    if dropped:
        logger.warning(
            "발행 기록 %d건이 현재 스키마와 안 맞아 건너뜁니다 (억제가 그만큼 약해집니다)",
            dropped,
        )
    return alerts


def save_published(
    published: list[DetectionAlert], window_end: date, path: Path = STATE_PATH
) -> int:
    """발행된 알림을 캐시에 누적한다. 반환값은 저장 후 총 건수.

    ⚠️ **여기 넣는 것은 "실제로 발행에 성공한 알림"뿐이다.**
       - `suppressed` 는 안 넣는다 — 정의가 "과거 **발행된** 알림"이라 억제분을 넣으면
         다음 배치가 그걸 기준으로 또 억제해 이중 억제가 된다.
       - **`--max-alerts` 로 잘린 알림, 발행이 실패한 알림도 안 넣는다.** 넣으면 다음
         배치가 그걸 직전 알림으로 보고 `RENOTIFY_BLOCK_DAYS` 만큼 억제해서 **셀러가
         그 알림을 영영 못 본다.** `resolved_alert_ids` 가 빈 집합이라 조기 해제 경로도
         없다. MQ 가 5분 죽으면 그날 알림이 7일간 침묵하는 형태다.
         (지인님 PR 리뷰 §1, 2026-08-06)

    같은 `alert_id` 는 덮어쓴다(같은 날 재실행 시 중복 누적 방지).
    """
    existing: dict[str, dict] = {a["alert_id"]: a for a in _read_state(path)}
    for alert in published:
        existing[alert.alert_id] = alert.model_dump(mode="json")

    cutoff = date.fromordinal(window_end.toordinal() - STATE_RETENTION_DAYS)
    kept = [
        a for a in existing.values() if date.fromisoformat(a["window_end"]) >= cutoff
    ]
    _atomic_write(path, json.dumps(kept, ensure_ascii=False, indent=2))
    return len(kept)


def load_pending_guidelines(
    window_end: date, path: Path = PENDING_GUIDELINE_PATH
) -> list[dict]:
    """대기열을 읽는다. 항목: {"alert": DetectionAlert, "attempts": int}.

    깨진 항목은 버리고 경고만 남긴다(`load_prior_alerts` 와 같은 규율).
    `window_end` 가 STATE_RETENTION_DAYS 보다 오래된 항목도 버린다 — 그 구간의
    CS 원문이 오늘 documents(35일 창) 밖이라 재생성이 성공할 수 없다.
    """
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(
            "가이드라인 대기열이 손상됐습니다 — 빈 대기열로 진행합니다 (%s)", exc
        )
        return []
    if not isinstance(raw, list):
        logger.warning("가이드라인 대기열 형식이 예상과 다릅니다 — 빈 대기열로 진행합니다")
        return []

    cutoff = date.fromordinal(window_end.toordinal() - STATE_RETENTION_DAYS)
    entries: list[dict] = []
    dropped = expired = 0
    for item in raw:
        if not isinstance(item, dict):
            dropped += 1
            continue
        try:
            alert = DetectionAlert.model_validate(item.get("alert"))
        except ValidationError:
            dropped += 1
            continue
        if alert.window_end < cutoff:
            expired += 1
            continue
        attempts = item.get("attempts")
        entries.append(
            {
                "alert": alert,
                "attempts": attempts if isinstance(attempts, int) and attempts >= 0 else 0,
            }
        )
    if dropped:
        logger.warning("가이드라인 대기 %d건이 현재 스키마와 안 맞아 버립니다", dropped)
    if expired:
        logger.warning(
            "가이드라인 대기 %d건이 보관 기간(%d일)을 넘겨 포기합니다",
            expired,
            STATE_RETENTION_DAYS,
        )
    return entries


def save_pending_guidelines(entries: list[dict], path: Path) -> None:
    """대기열을 통째로 다시 쓴다. 빈 리스트면 빈 파일 — '대기 없음'도 상태다."""
    _atomic_write(
        path,
        json.dumps(
            [
                {"alert": e["alert"].model_dump(mode="json"), "attempts": e["attempts"]}
                for e in entries
            ],
            ensure_ascii=False,
            indent=2,
        ),
    )
