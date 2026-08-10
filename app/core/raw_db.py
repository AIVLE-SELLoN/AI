"""raw DB(`cs`·`reviews`·`classified_item`) 읽기 연결. **읽는 쪽 공용.**

AI 노드는 원본 DB 에 **읽기 권한만** 있다(「Raw DB 스키마 확정 (8/7)」 §5-2 — 쓰기는
분류 워커의 `classified_item` 계열뿐이고, 서비스 DB 는 main server 단독 소유다).

⚠️ 이게 `app/core/` 에 있는 이유: **읽는 쪽이 둘로 갈려 있다.** 탐지 배치
(`app/batch/daily.py`)와 CS 원문 조회(`app/core/inquiries.py`)가 같은 파일을 여는데,
한쪽에만 두면 나머지가 연결 문자열을 다시 적게 되고 `mode=ro` 같은 조건이 조용히
갈린다 — `raw_schema.py` 가 core 에 있는 것과 같은 사유다.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from app.config import get_settings


def connect_readonly(db_path: str | None = None) -> sqlite3.Connection:
    """raw DB 를 **읽기 전용**으로 연다. 없으면 만들지 않고 던진다.

    `mode=ro` 인 이유가 둘이다:
      1. 권한 그대로다 — 읽는 쪽이 원문을 고칠 일이 없다.
      2. **경로 오타를 조용히 넘기지 않는다.** 기본 연결은 없는 경로에 빈 DB 를 새로
         만들어서, 조회가 0건으로 성공하고 배치는 알림 없이 정상 종료한다.

    Args:
        db_path: DB 경로. 기본은 `settings.raw_db_path`.

    Raises:
        FileNotFoundError: 파일이 없을 때. 목 파이프라인은 `scripts/mock_producer.py`
            가 원문을, `scripts/classification_worker.py` 가 분류 결과를 채운다.
    """
    path = Path(db_path or get_settings().raw_db_path).resolve()
    if not path.exists():
        raise FileNotFoundError(
            f"raw DB 가 없습니다: {path} — 목 파이프라인은 scripts/mock_producer.py 로"
            " 원문을 적재한 뒤 scripts/classification_worker.py 로 분류해야 합니다."
        )

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn