"""담당: 지인 — raw DB 를 **누가 열고 어디에 쓰는지**를 소스에서 유도하는 가드.

여기 두 가드는 성격이 다르다. 합치지 말 것:

  ① 호출부 등록      raw DB 연결을 여는 모듈이 늘면 걸린다.
                     → 새 호출부가 Postgres 접속 실패를 안 다루면 CronJob 이 무한 재시도한다.
  ② 쓰기 대상 고정   분류 워커가 **AI 소유 4개 밖**에 쓰면 걸린다.
                     → 인프라가 RW 전면 부여로 회신해 DB 가 더는 안 막는다.

둘 다 **손으로 적은 목록과 대조하지 않고 소스에서 유도**한다. 선례는
`tests/test_console_encoding.py` 다 — `force_utf8_output()` 을 진입점마다 손으로 붙이다가
**셋이 단언 문자열까지 같은데도 다음 진입점을 못 막아서** 유도형으로 갔다.

LLM·네트워크·DB 없음. 소스를 AST 로 읽기만 한다.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from app.core import raw_schema

ROOT = Path(__file__).resolve().parents[1]
SCANNED_DIRS = ("app", "scripts", "eval")


def _python_files() -> list[Path]:
    files: list[Path] = []
    for directory in SCANNED_DIRS:
        files += sorted(p for p in (ROOT / directory).rglob("*.py") if "__pycache__" not in p.parts)
    return files


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


# ── ① raw DB 연결을 여는 모듈 ────────────────────────────────────────────────

CONNECT_FUNCTIONS = frozenset({"connect_readonly", "connect_readwrite"})

RAW_DB_CALLERS: dict[str, str] = {
    "app/batch/inputs.py": (
        "던지는 쪽이다. 잡는 곳은 app/batch/daily.py 의 main() "
        "— (FileNotFoundError, RuntimeError, *connection_error_types()) 를 잡아 exit 2"
    ),
    "app/core/inquiries.py": (
        "던지는 쪽이다. 잡는 곳은 app/recommendation/service.py 의 degrade "
        "— 그쪽이 connection_error_types() 를 같이 잡는다(PR #101)"
    ),
    "scripts/classification_worker.py": "open_db() 가 잡아 사유 한 줄 + exit 1",
    "scripts/generate_monthly_reports.py": "run_aggregate() 가 잡아 사유 한 줄 + return 1",
    "scripts/mock_producer.py": "open_raw_db() 가 잡아 사유 한 줄 + exit 1",
    "eval/run_monthly_oracle_eval.py": (
        "dsn='' 로 못박혀 **sqlite 전용**이다(Connection.backup() 을 쓴다) — "
        "Postgres 로 갈 수 없으므로 접속 예외도 안 난다"
    ),
}
"""**raw DB 연결을 여는 모듈 전부. 늘어나면 이 테스트가 걸린다.**

여기 이름을 추가할 때는 *"Postgres 접속 실패를 누가 어떻게 다루나"* 를 사유로 적을 것.
안 다루면 `psycopg.Error` 가 raw traceback 으로 나가는데, 배포되는 것(배치·워커)은 k8s 가
같은 설정으로 **무한 재시도**하므로 실패가 로그 밖에서는 안 보인다.

`psycopg.Error` 는 `FileNotFoundError` 도 `RuntimeError` 도 `OSError` 도 **아니다** —
   기존 분기를 그냥 통과한다. 목록은 `raw_db.connection_error_types()` 가 준다.
"""

# 사유상 예외인 모듈 — 접속 예외 자체가 안 나는 경로.
EXEMPT_FROM_ERROR_HANDLING = frozenset({"eval/run_monthly_oracle_eval.py"})


def _modules_opening_raw_db() -> dict[str, set[str]]:
    """`connect_readonly` / `connect_readwrite` 를 부르는 모듈 → 부른 함수 이름."""
    found: dict[str, set[str]] = {}
    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else func.id
                if isinstance(func, ast.Name)
                else None
            )
            if name in CONNECT_FUNCTIONS:
                found.setdefault(_rel(path), set()).add(name)
    # 정의부 자신은 호출부가 아니다.
    found.pop("app/core/raw_db.py", None)
    return found


def test_every_raw_db_call_site_is_registered():
    """raw DB 연결을 여는 모듈이 늘면 여기서 걸린다.

    **이게 이 파일의 이유다.** 접속 실패 처리를 사람이 기억해서 붙이는 동안 두 번
       빠졌다(배치 · REST). 세 번째가 이 워커였고, 빠지면 CronJob 이 exit 1 로 무한
       재시도한다 — 접속 실패 분류가 막으려던 그 상태다.
    """
    found = set(_modules_opening_raw_db())

    assert found == set(RAW_DB_CALLERS), (
        "raw DB 연결 호출부가 바뀌었습니다. RAW_DB_CALLERS 에 "
        "'Postgres 접속 실패를 누가 어떻게 다루나' 를 사유로 적고 등록하세요. "
        f"발견={sorted(found)} / 등록={sorted(RAW_DB_CALLERS)}"
    )


@pytest.mark.parametrize("module", sorted(set(RAW_DB_CALLERS) - EXEMPT_FROM_ERROR_HANDLING))
def test_raw_db_call_sites_handle_connection_errors(module: str):
    """등록된 호출부는 `connection_error_types()` 를 실제로 언급한다.

    **약한 가드다** — 이름을 언급하기만 해도 통과한다(예외를 실제로 잡는지는 안 본다).
       그래도 값이 있는 이유는, 지금까지 빠진 방식이 전부 *"그런 게 있는 줄 몰랐다"* 였지
       *"잡는 척만 했다"* 가 아니었기 때문이다. 던지는 쪽(`inquiries.py`)은 잡는 모듈
       이름을 docstring 에 적어 두는 것으로 대신한다.
    """
    source = (ROOT / module).read_text(encoding="utf-8")
    assert "connection_error_types" in source, (
        f"{module} 이 raw DB 를 여는데 connection_error_types() 를 안 봅니다 — "
        "Postgres 접속 실패가 raw traceback 으로 나갑니다."
    )


# ── ② 분류 워커의 쓰기 대상 ──────────────────────────────────────────────────

AI_OWNED_TABLES = frozenset(
    {
        "classified_item",
        "classified_item_aspect",
        "classification_failure",
        "classification_cursor",
    }
)
"""확정 문서 §1 소유 표에서 **AI 노드가 쓰는** 테이블. 나머지 6개는 main server 소유다."""

AI_OWNED_OBJECTS = AI_OWNED_TABLES | {raw_schema.VOC_DOCUMENT}
"""DDL 이 만들어도 되는 것 — 위 4개 + 우리 읽기 모델(뷰). 08-18 확정으로 뷰도 우리가 만든다."""

# `UPDATE` 뒤에 `SET` 이 오는 경우를 뺀다 — upsert 의 `DO UPDATE SET ...` 이 걸려서
#    `set` 이라는 테이블에 쓰는 것으로 잡힌다(실제로 처음에 그렇게 나왔다). 그 오탐은
#    "가드가 시끄럽다" 로 끝나지 않는다: 다음 사람이 허용 목록에 `set` 을 넣어 버리면
#    가드가 조용해진 채로 남는다.
WRITE_STATEMENT = re.compile(
    r"\b(?:INSERT\s+INTO|UPDATE\s+(?!SET\b)|DELETE\s+FROM)\s*([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)
CREATE_OR_DROP = re.compile(
    r"\b(?:CREATE|DROP)\s+(?:TABLE|VIEW|INDEX|OR\s+REPLACE\s+VIEW)?\s*"
    r"(?:IF\s+NOT\s+EXISTS\s+|IF\s+EXISTS\s+)?(?:\S+\s+ON\s+)?([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)


def _sql_literals(path: Path) -> list[str]:
    """그 모듈의 SQL 문자열 상수. **docstring 은 뺀다.**

    소스 전체를 정규식으로 훑으면 안 된다 — 이 저장소는 주석·docstring 에 SQL 을
       설명으로 적는 일이 흔해서(`INSERT OR IGNORE 였다가...`), 설명 한 줄이 "쓰기" 로
       잡히면 가드가 오탐으로 시끄러워지고 결국 꺼진다.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            body = getattr(node, "body", None)
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                docstrings.add(id(body[0].value))
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ]


def _upsert_helper_tables(path: Path) -> set[str]:
    """`raw_db.upsert_sql("t", ...)` 로 조립되는 대상 테이블.

    **문자열만 훑으면 이게 통째로 안 잡힌다.** 헬퍼가 만든 SQL 은 소스 어디에도
       `INSERT INTO ...` 로 안 적혀 있어서, 정규식 가드가 "쓰기 문장 0건" 으로 조용히
       통과한다 — 가드가 있는데 아무것도 안 보는 상태가 제일 나쁘다.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    tables: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "upsert_sql"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            tables.add(node.args[0].value)
    return tables


def test_worker_only_writes_to_ai_owned_tables():
    """분류 워커의 쓰기 대상이 AI 소유 4개 밖으로 나가면 걸린다.

    **DB 가 더는 안 막아 준다.** 인프라가 *"모든 테이블에 대한 RW 권한을 기능마다 따로
    부여할 순 없다"* 며 AI 노드에 raw DB **RW 전면 부여**로 회신했다 — 계정
    분리는 이미 거절된 요청이라 다시 올리지 않는다. 읽는 쪽은 `connect_readonly()` 의 세션
    read-only 가 막지만 **쓰기 연결에는 그 한 줄도 없다.**

    그래서 오타 하나로 main server 소유 `cs`·`reviews` 에 쓸 수 있고, 그 사고는 원문이
    바뀌는 것이라 **되돌릴 수단이 없다.** 남은 방어선이 이 테스트다.
    """
    path = ROOT / "scripts/classification_worker.py"
    targets = {
        match.group(1).lower()
        for sql in _sql_literals(path)
        for match in WRITE_STATEMENT.finditer(sql)
    }
    targets |= _upsert_helper_tables(path)

    # 가드가 헛돌지 않는지 — 실제로 쓰기 문장을 보고 있어야 한다.
    assert targets, "워커에서 쓰기 문장을 하나도 못 찾았습니다 — 가드가 헛돌고 있습니다."
    assert targets <= AI_OWNED_TABLES, (
        "분류 워커가 AI 소유가 아닌 테이블에 씁니다: "
        f"{sorted(targets - AI_OWNED_TABLES)} (§1 소유 표 기준 허용={sorted(AI_OWNED_TABLES)})"
    )


def test_worker_ddl_only_creates_ai_owned_objects():
    """워커가 부르는 DDL(`create_classified_tables`)도 AI 소유 밖을 만들지 않는다.

    쓰기 문장과 **다른 축이다.** `create_source_tables()` 는 main server 6개를 만드는데,
       그건 목 프로듀서만 부른다 — 워커가 그걸 부르기 시작하면 여기서 걸려야 한다.
    """
    statements = [
        *raw_schema._CLASSIFIED_DDL[raw_schema.raw_db.SQLITE],
        *raw_schema._CLASSIFIED_DDL[raw_schema.raw_db.POSTGRES],
        *raw_schema.CLASSIFIED_INDEXES,
        *raw_schema._VOC_DOCUMENT_DDL.values(),
    ]
    targets = {
        match.group(1).lower() for sql in statements for match in CREATE_OR_DROP.finditer(sql)
    }
    assert targets, "DDL 에서 대상 객체를 하나도 못 찾았습니다 — 가드가 헛돌고 있습니다."
    assert targets <= AI_OWNED_OBJECTS, (
        f"AI 소유가 아닌 객체를 만듭니다: {sorted(targets - AI_OWNED_OBJECTS)}"
    )
