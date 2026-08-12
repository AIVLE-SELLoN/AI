"""담당: 지인 — `app/core/raw_db.connect_readonly()` (raw DB 읽기 연결).

이 모듈이 지키는 계약 2개. **둘 다 조용히 깨지는 종류라 테스트가 유일한 방어선이다:**
  1. **읽기 전용이다** — 읽는 쪽이 원문을 고칠 수 없다(§5-2 권한 그대로).
  2. **없는 경로를 조용히 넘기지 않는다** — 기본 연결은 빈 DB 를 새로 만들어서, 조회가
     0건으로 성공하고 배치가 알림 없이 **정상 종료**한다.

LLM·네트워크 없음. sqlite 파일만 만든다.
"""

import sqlite3

import pytest

from app.core.raw_db import connect_readonly


def _seed(path) -> None:
    """읽어서 구분할 수 있는 표식 1행. 엉뚱한 파일을 열면 이 행이 없다."""
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE marker (x INTEGER)")
    conn.execute("INSERT INTO marker VALUES (1)")
    conn.commit()
    conn.close()


def test_path_with_hash_opens_the_real_file(tmp_path):
    """🔴 경로에 `#` 이 있어도 **그 DB 를 읽기 전용으로 연다.**

    `f"file:{path}?mode=ro"` 로 만들면 `#` 뒤가 URI fragment 로 잘려 **`mode=ro` 가
    통째로 날아가고**, 남은 앞부분(`.../we`)을 경로로 잡아 **빈 DB 를 새로 만든다.**
    그러면 위 계약 두 개가 **동시에** 깨진다 — 쓰기가 열리고, 경로 오타가 "문서 0건"
    으로 조용히 통과한다. `as_uri()` 가 `#` 을 `%23` 으로 인코딩해서 막는다.

    ⚠️ `#` 은 지어낸 입력이 아니다. Windows 사용자 폴더·브랜치명이 섞인 워크트리 경로에
       실제로 들어간다(`RAW_DB_PATH` 는 `.env` 로 각자 지정한다).

    세 가지를 따로 본다. 하나만 보면 옛 형태로 되돌려도 통과하는 조합이 생긴다:
      - 표식 행이 읽힌다 → **그 파일**을 열었다
      - 쓰기가 거부된다 → `mode=ro` 가 살아 있다
      - 옆에 파일이 안 생긴다 → 빈 DB 를 새로 만들지 않았다
    """
    weird_dir = tmp_path / "we#ird"
    weird_dir.mkdir()
    db = weird_dir / "raw.db"
    _seed(db)

    conn = connect_readonly(str(db))
    try:
        assert conn.execute("SELECT x FROM marker").fetchone()["x"] == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("INSERT INTO marker VALUES (2)")
    finally:
        conn.close()

    # 옛 형태는 여기에 `we` 라는 0바이트 DB 를 만든다(2026-08-11 리뷰 ③ 실측).
    assert [p.name for p in tmp_path.iterdir()] == ["we#ird"]


def test_missing_path_raises_instead_of_creating_an_empty_db(tmp_path):
    """경로가 틀리면 던진다 — 빈 DB 를 만들어 '문서 0건' 으로 통과하면 안 된다."""
    missing = tmp_path / "없음.db"

    with pytest.raises(FileNotFoundError):
        connect_readonly(str(missing))

    assert not missing.exists()
