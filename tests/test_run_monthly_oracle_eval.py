"""월간 oracle 평가기가 공용 읽기 전용 DB 연결을 지키는지 검증한다."""

import sqlite3

from app.core import raw_schema
from eval import run_monthly_oracle_eval


def test_build_oracle_connection_handles_hash_in_source_path(tmp_path, monkeypatch):
    """`#`이 든 경로도 URI fragment로 잘리지 않고 원본 DB를 열어야 한다."""
    db_path = tmp_path / "raw#snapshot.db"
    source = sqlite3.connect(db_path)
    raw_schema.create_source_tables(source)
    source.commit()
    source.close()
    monkeypatch.setattr(run_monthly_oracle_eval, "load_golden_inputs", lambda: ([], []))

    conn, before, after = run_monthly_oracle_eval.build_oracle_connection(db_path)
    try:
        assert before == 0
        assert after == 0
    finally:
        conn.close()
