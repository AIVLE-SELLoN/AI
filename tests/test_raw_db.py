"""담당: 지인 — `app/core/raw_db.connect_readonly()` (raw DB 읽기 연결).

이 모듈이 지키는 계약 2개. **둘 다 조용히 깨지는 종류라 테스트가 유일한 방어선이다:**
  1. **읽기 전용이다** — 읽는 쪽이 원문을 고칠 수 없다(§5-2 권한 그대로).
  2. **없는 경로를 조용히 넘기지 않는다** — 기본 연결은 빈 DB 를 새로 만들어서, 조회가
     0건으로 성공하고 배치가 알림 없이 **정상 종료**한다.

Postgres 이식 1단계(2026-08-16)로 계약이 하나 늘었고, ⓑ-0(2026-08-18)에서 접속 정보가
커넥션 문자열 한 벌에서 **원자값**으로 바뀌었다:
  3. **백엔드를 가르는 것은 `RAW_DB_HOST` 하나다** — 비어 있으면 sqlite 그대로다.
     데모가 이 기본값 위에서 돌기 때문에, 이 갈림이 뒤집히면 데모가 빈 Postgres 를 본다.
  4. **접속 문자열은 원자값에서 우리가 조립한다** — 남의 커넥션 문자열을 파싱하지 않고,
     빈 값은 싣지 않으며, `sslmode` 는 항상 싣는다(빼면 libpq 가 평문으로 내려간다).

⚠️ Postgres **실연결** 검증은 `tests/test_raw_db_postgres.py` 에 있고 DSN 이 있을 때만
   돈다. 여기는 DB 없이 확인할 수 있는 것만 둔다 — LLM·네트워크·DB 없음.
"""

import sqlite3

import pytest
from psycopg.conninfo import conninfo_to_dict
from pydantic import ValidationError

from app.config import Settings
from app.core import raw_db
from app.core.logging_setup import _describe
from app.core.raw_db import (
    connect_readonly,
    conninfo_from_settings,
    describe_target,
    translate_placeholders,
)

# 원자값을 **전부** 지정한 최소 조합. 개별 테스트는 여기서 한 항목만 바꾼다.
_ATOMS = {
    "raw_db_host": "db.internal",
    "raw_db_port": 5432,
    "raw_db_name": "rawdb",
    "raw_db_username": "sellon_ai",
    "raw_db_password": "S3cr3t",
}


def _settings(monkeypatch, **overrides) -> Settings:
    """`.env` 와 `os.environ` 을 **차단하고** 원자값만으로 Settings 를 만든다.

    🔴 두 겹으로 막는 이유: `env_file` 뿐 아니라 `app.config.load_dotenv()` 가 import
       시점에 `.env` 를 **`os.environ` 에도** 넣는다. `_env_file=None` 만으로는 그쪽이
       안 막혀서, `RAW_DB_*` 를 `.env` 에 둔 개발자에게만 결과가 달라진다 — 이 저장소가
       반복해서 밟은 계열이다(`pin_company_id`·`block_local_raw_db` 참고).
    """
    for key in (
        "RAW_DB_HOST",
        "RAW_DB_PORT",
        "RAW_DB_NAME",
        "RAW_DB_USERNAME",
        "RAW_DB_PASSWORD",
        "RAW_DB_SSLMODE",
        "RAW_DB_SSLROOTCERT",
    ):
        monkeypatch.delenv(key, raising=False)
    return Settings(_env_file=None, **{**_ATOMS, **overrides})


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


# ── 백엔드 갈림 (Postgres 이식 1단계) ────────────────────────────────────────


def test_empty_dsn_keeps_the_sqlite_path(tmp_path):
    """🔴 **기본값이 계약이다** — 접속 문자열이 비면 sqlite 로 간다.

    데모가 이 경로 위에서 돈다. 갈림이 뒤집히면 배치가 빈 Postgres 를 읽고 **문서 0건
    으로 정상 종료**하는데, 그건 이 파일 계약②가 막으려던 바로 그 모양이다.
    """
    db = tmp_path / "raw.db"
    _seed(db)

    conn = connect_readonly(str(db), dsn="")
    try:
        assert conn.execute("SELECT x FROM marker").fetchone()["x"] == 1
    finally:
        conn.close()


def test_dsn_switches_to_postgres_without_touching_the_file(tmp_path, monkeypatch):
    """DSN 이 있으면 sqlite 경로를 **아예 안 본다.**

    파일 존재 검사가 먼저 돌면 DSN 을 넣은 환경이 `FileNotFoundError` 로 죽는다 —
    `RAW_DB_PATH` 에 DSN 을 넣지 못하는 이유가 그 검사였다.
    """
    opened = {}

    class _Stub:
        dialect = raw_db.POSTGRES

        def __init__(self, dsn):
            opened["dsn"] = dsn

    monkeypatch.setattr(raw_db, "PostgresConnection", _Stub)

    conn = connect_readonly(str(tmp_path / "없음.db"), dsn="postgresql://x@h/rawdb")

    assert isinstance(conn, _Stub)
    assert opened["dsn"] == "postgresql://x@h/rawdb"


# ── 원자값 → 접속 문자열 (ⓑ-0, 2026-08-18) ──────────────────────────────────


def test_no_host_means_sqlite(monkeypatch):
    """🔴 **갈림은 `RAW_DB_HOST` 하나다** — 나머지가 차 있어도 비면 sqlite 로 간다.

    데모가 이 기본값 위에서 돈다. 뒤집히면 배치가 빈 Postgres 를 읽고 **문서 0건으로
    정상 종료**하는데, 그건 이 파일 계약②가 막으려던 바로 그 모양이다.
    """
    assert conninfo_from_settings(_settings(monkeypatch, raw_db_host="")) == ""


def test_conninfo_carries_every_atom(monkeypatch):
    """원자값이 **하나도 빠짐없이** 실린다. 빠지면 libpq 가 기본값으로 채워 조용히 틀린다."""
    info = conninfo_to_dict(conninfo_from_settings(_settings(monkeypatch)))

    assert info["host"] == "db.internal"
    assert info["port"] == "5432"
    assert info["dbname"] == "rawdb"
    assert info["user"] == "sellon_ai"
    assert info["password"] == "S3cr3t"


def test_sslmode_is_always_carried(monkeypatch):
    """🔴 **`sslmode` 가 빠지면 libpq 기본값 `prefer` 로 떨어진다.**

    `prefer` 는 SSL 을 시도하다 **서버가 거부하면 평문으로 붙고 실패하지 않는다**
    (실측: `pq.Conninfo.get_defaults()` → `prefer`). 즉 이 항목이 빠지는 회귀는 접속
    실패가 아니라 **조용한 암호화 상실**로 나타난다 — 테스트가 유일한 방어선이다.
    """
    assert conninfo_to_dict(conninfo_from_settings(_settings(monkeypatch)))["sslmode"] == (
        "require"
    )


@pytest.mark.parametrize(
    "password",
    [
        "p w",  # 공백 — 따옴표로 감싸지 않으면 다음 키로 잘린다
        "p'w",  # 작은따옴표 — 키워드 형식의 인용 부호와 충돌한다
        "p\\w",  # 역슬래시 — 이스케이프 문자로 먹힌다
        "pw=x sslmode=disable",  # 🔴 값이 **다른 키를 주입**할 수 있는 모양
    ],
)
def test_special_characters_survive_assembly(monkeypatch, password):
    """🔴 값에 특수문자가 있어도 **그대로** 전달된다 — f-string 조립을 막는 이유.

    비밀번호는 공백·따옴표·역슬래시가 흔한 자리인데, 손으로 붙이면 키워드 형식이
    어긋나 **인증 실패로만 보이고 원인이 안 드러난다.** 마지막 케이스는 더 나쁘다 —
    값이 `sslmode` 를 덮어써 암호화가 조용히 꺼진다.
    """
    info = conninfo_to_dict(
        conninfo_from_settings(_settings(monkeypatch, raw_db_password=password))
    )

    assert info["password"] == password
    assert info["sslmode"] == "require", "값이 다른 키를 주입했습니다"


def test_empty_optionals_are_omitted_not_blank(monkeypatch):
    """🔴 **`password=''` 는 "생략" 과 다른 뜻이다** — 빈 값은 싣지 않는다.

    libpq 에 빈 문자열을 명시하면 *빈 비밀번호를 쓰겠다*는 뜻이라 `.pgpass` 조회가
    막힌다(실측: `make_conninfo(password='')` → `password=''` 가 실제로 실린다).
    `RAW_DB_SSLROOTCERT` 를 **키만 두고 비워 두는** 것이 안전한 근거이기도 하다 —
    빈 경로가 실리면 libpq 가 그 파일을 찾다 실패한다.
    """
    info = conninfo_to_dict(
        conninfo_from_settings(
            _settings(monkeypatch, raw_db_password="", raw_db_sslrootcert="")
        )
    )

    assert "password" not in info
    assert "sslrootcert" not in info


def test_ca_bundle_is_carried_when_set(monkeypatch):
    """반대편 — 채워져 있으면 실린다. 위 생략 규칙이 CA 를 통째로 삼키면 안 된다."""
    info = conninfo_to_dict(
        conninfo_from_settings(
            _settings(
                monkeypatch,
                raw_db_sslmode="verify-full",
                raw_db_sslrootcert="/etc/ssl/rds-ca.pem",
            )
        )
    )

    assert info["sslrootcert"] == "/etc/ssl/rds-ca.pem"
    assert info["sslmode"] == "verify-full"


# ── 부팅 가드 (설정 조합) ────────────────────────────────────────────────────


def test_verify_mode_without_ca_is_refused_at_boot(monkeypatch):
    """🔴 `verify-*` 인데 CA 가 비면 **부팅에서** 세운다.

    안 세우면 나중에 보안을 조이려고 `verify-full` 만 켠 사람이 **런타임에 알 수 없는
    이유로** 접속 실패를 본다(2026-08-18 결정). 여기서 걸리면 진입점의
    `configure_logging_or_exit()` 이 사유 한 줄 + exit 2 로 끝낸다.
    """
    with pytest.raises(ValidationError, match="CA 번들이 필요합니다"):
        _settings(monkeypatch, raw_db_sslmode="verify-full", raw_db_sslrootcert="")


@pytest.mark.parametrize("missing", ["raw_db_name", "raw_db_username"])
def test_host_without_name_or_user_is_refused_at_boot(monkeypatch, missing):
    """호스트만 있고 DB 이름·계정이 없으면 세운다.

    libpq 는 빠진 자리를 **OS 사용자 이름**으로 채운다 — 컨테이너에서는 `root` 라
    `database "root" does not exist` 로 죽고, 원인이 메시지에 안 드러난다.
    """
    with pytest.raises(ValidationError, match="도 있어야 합니다"):
        _settings(monkeypatch, **{missing: ""})


def test_unknown_sslmode_is_refused_at_boot(monkeypatch):
    """libpq 값이 아닌 `sslmode` 는 접속 시점이 아니라 부팅에서 거른다."""
    with pytest.raises(ValidationError, match="libpq 값이 아닙니다"):
        _settings(monkeypatch, raw_db_sslmode="requre")


def test_retired_dsn_key_is_refused_loudly(monkeypatch):
    """🔴 폐기된 `RAW_DB_DSN` 이 남아 있으면 **부팅에서 세운다.**

    `extra="ignore"` 라 그냥 두면 **아무 말 없이 무시되고 sqlite 를 읽는다.** 직전
    `.env.example` 이 *"주석 해제 → 검증 → 다시 주석"* 을 안내했으므로 남겨둔 사람이
    반드시 있고, 그 사람은 Postgres 를 본다고 믿으면서 목 데이터를 본다.

    ⚠️ **호스트가 비어 있어도 걸려야 한다** — 피해자가 정확히 "원자값을 아직 안 넣은
       사람" 이라, 이 검사가 호스트 게이트 안으로 들어가면 아무도 못 본다.
    """
    monkeypatch.setenv("RAW_DB_DSN", "postgresql://u:pw@db.internal:5432/rawdb")

    with pytest.raises(ValidationError, match="더 이상 쓰지 않습니다") as exc:
        _settings(monkeypatch, raw_db_host="")

    assert "pw" not in _describe(exc.value), "폐기 안내에 접속 문자열을 싣지 않는다"


def test_sqlite_default_skips_the_raw_db_guards(monkeypatch):
    """🔴 **호스트가 비면 아무것도 안 본다** — 데모·팀원 로컬·테스트가 걸리면 안 된다.

    위 세 가드는 전부 Postgres 를 설정한 배포에만 해당한다. 여기가 뒤집히면 설정을
    안 건드린 사람이 갑자기 부팅 실패를 본다(*"설정을 안 건드리면 이전과 같다"* 계약).
    """
    settings = _settings(
        monkeypatch,
        raw_db_host="",
        raw_db_name="",
        raw_db_username="",
        raw_db_sslmode="이것은-libpq-값이-아니다",
    )

    assert conninfo_from_settings(settings) == ""


def test_boot_guard_message_is_readable_and_leaks_nothing(monkeypatch):
    """🔴 가드가 터질 때 **비밀번호가 로그로 안 나가고, 사유는 읽힌다.**

    진입점이 이 예외를 `logging_setup._describe()` 로 한 줄 요약해 stderr 에 찍는다
    (실측: `python -m app.batch.daily --dry-run` → exit 2 + 아래 문장).
      - 값이 실리면 배치 로그에 박혀 회수가 안 된다. `_describe` 가 `loc`·`msg` 만
        쓰는 것이 방어선이라, 가드 메시지에 값을 넣으면 그 방어선을 우회한다.
      - ⚠️ **`model_validator` 는 `loc` 이 비어서** 그대로 붙이면 `": 사유"` 로 나간다.
        이 저장소의 첫 모델 단위 검증이라 그 처리도 여기서 같이 잠근다.
    """
    with pytest.raises(ValidationError) as exc:
        _settings(monkeypatch, raw_db_sslmode="verify-full", raw_db_sslrootcert="")

    described = _describe(exc.value)
    assert "S3cr3t" not in described
    assert "CA 번들이 필요합니다" in described, f"사유가 안 남으면 진단이 안 된다: {described}"
    assert not described.startswith(":"), f"빈 loc 접두어가 남았습니다: {described}"


@pytest.mark.parametrize(
    "dsn",
    [
        # URI — 자격증명이 `@` 앞에
        "postgresql://sellon_ai:S3cr3t@db.internal:5432/rawdb",
        # URI — 자격증명이 쿼리 인자로
        "postgresql://db.internal/rawdb?password=S3cr3t",
        # 🔴 키워드 형식. psycopg 가 이것도 받는데 `@`·`?` 가 없어서, 문자열을 잘라내는
        #    방식으로는 **통째로 샌다**(2026-08-16 용준님 리뷰 §2, 실측).
        "host=db.internal dbname=rawdb user=sellon_ai password=S3cr3t",
        "dbname=rawdb password=S3cr3t",
    ],
)
def test_describe_target_never_leaks_the_password(dsn):
    """🔴 오류 메시지·로그에 raw DB 비밀번호를 싣지 않는다 — **DSN 형식 불문.**

    `_require_classified_tables` 가 "어느 DB 를 봤는지" 를 메시지에 넣는데, DSN 이 새면
    그 문자열이 배치 요약·로그로 그대로 나가 회수가 안 된다.

    ⚠️ **URI 만 재면 안 된다.** 세 번째 케이스가 옛 구현에서 실제로 통째로 샜다 —
       한 형식만 잠그면 나머지 형식으로 같은 사고가 그대로 재발한다.
    """
    described = describe_target(dsn=dsn)

    assert "S3cr3t" not in described
    assert "sellon_ai" not in described


def test_describe_target_reports_where_it_looked():
    """가리되 **쓸모는 남긴다** — 어느 호스트·DB 를 봤는지는 나와야 진단이 된다."""
    assert describe_target(
        dsn="postgresql://u:pw@db.internal:5432/rawdb"
    ) == "Postgres db.internal:5432/rawdb"
    assert describe_target(
        dsn="host=db.internal dbname=rawdb user=u password=pw"
    ) == "Postgres db.internal/rawdb"


def test_describe_target_survives_an_unreadable_dsn():
    """🔴 읽기 실패해도 **던지지 않고, 원문도 안 싣는다.**

    이 함수는 인자 자리에서 항상 평가되므로(`_require_classified_tables(conn,
    describe_target(...))`) 여기서 던지면 **진단이 진짜 원인을 가린다.** 그렇다고 폴백에
    DSN 을 넣으면 막으려던 것이 폴백으로 새어나간다 — 둘 다 안 되게 잠근다.
    """
    described = describe_target(dsn="=== password=S3cr3t ===")

    assert "S3cr3t" not in described
    assert "Postgres" in described


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT ? , ?", "SELECT %s , %s"),
        # 리터럴 안의 `?` 는 바인딩이 아니다 — 바꾸면 개수가 어긋난다.
        ("SELECT '? 물음표', ?", "SELECT '? 물음표', %s"),
        # `''` 이스케이프를 지나도 리터럴 판정이 유지돼야 한다.
        ("SELECT 'it''s ?', ?", "SELECT 'it''s ?', %s"),
        # psycopg 는 인자를 넘길 때 `%` 를 서식 문자로 읽는다.
        ("SELECT x WHERE y LIKE '%키워드%'", "SELECT x WHERE y LIKE '%%키워드%%'"),
    ],
)
def test_translate_placeholders(sql, expected):
    """`?` 바인딩 SQL 을 psycopg 의 `%s` 로 옮긴다. **한 벌로 쓰는 근거다.**"""
    assert translate_placeholders(sql) == expected


def test_null_safe_comparison_spelling_works_on_sqlite():
    """🔴 `IS NOT DISTINCT FROM` 이 sqlite 에서도 `IS` 와 같은 뜻이어야 한다.

    sqlite·Postgres 양쪽에서 같은 뜻이라 이 철자 하나로 쓰는데, **sqlite 3.39 미만이면 구문 오류**다.
    그 환경에서는 탐지 배치와 분류 워커가 통째로 못 돈다 — 팀원 로컬마다 파이썬이
    다르므로 "내 PC 에서 되니까" 로 넘길 수 없어 테스트로 잠근다.
    (호스트 3.49 · 런타임 이미지 python:3.12-slim 3.46 실측, 2026-08-16)
    """
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE t (a TEXT)")
        conn.execute("INSERT INTO t VALUES (NULL)")
        # NULL 끼리도 참이어야 한다 — `=` 는 여기서 NULL 을 돌려준다.
        assert conn.execute(
            "SELECT COUNT(*) FROM t WHERE a IS NOT DISTINCT FROM ?", (None,)
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM t WHERE NOT (a IS NOT DISTINCT FROM ?)", ("v5",)
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_connection_error_types_covers_both_psycopg_bases():
    """🔴 Postgres 실패가 두 베이스로 갈린다 — 한쪽만 잡으면 절반이 샌다.

    호출부(`daily.main()` 의 exit 2 분류, `service.generate_recommendation` 의 degrade)가
    이 목록으로 "환경 탓" 을 가른다. `psycopg.OperationalError` 로 좁히면 **DSN 형식 오타 ·
    DB 이름 틀림 · 뷰 없음 · GRANT 누락**이 전부 빠져나간다(전부 `ProgrammingError` 계열).
    하필 그 넷이 첫 연동에서 제일 잦다.

    ⚠️ **`FileNotFoundError`·`RuntimeError` 가 아닌 것까지 같이 본다.** 그게 이 함수가
       존재하는 이유이므로(둘 중 하나였다면 호출부가 이미 잡고 있었다), 그 전제가
       psycopg 버전이 올라가며 바뀌면 여기서 먼저 알려준다.
    """
    import psycopg

    covered = raw_db.connection_error_types()
    assert covered, "드라이버가 있는데 빈 튜플이면 호출부가 아무것도 새로 못 잡는다"

    for code, kind in [
        ("28P01", "비밀번호 틀림"),
        ("3D000", "DB 이름 틀림"),
        ("42P01", "뷰·테이블 없음"),
        ("42501", "GRANT 누락"),
    ]:
        exc_type = psycopg.errors.lookup(code)
        assert issubclass(exc_type, covered), f"{kind}({code}) 이 안 잡힌다: {exc_type}"
        assert not issubclass(exc_type, FileNotFoundError | RuntimeError), (
            f"{kind}({code}) 이 이미 호출부 분기에 걸린다면 이 함수가 필요 없다"
        )
