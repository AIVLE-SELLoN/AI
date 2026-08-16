"""**진입점**이 콘솔 인코딩을 바꾸는지 고정한다 — 배포되는 것 + 출력이 깨질 수 있는 것.

왜 배선을 손으로 세는 테스트를 그만두는가
------------------------------------------
같은 가드를 진입점마다 손으로 하나씩 붙여 왔다 — `test_batch_daily.py` ·
`test_monthly_batch.py` · `test_consumer_entrypoint.py`. 셋이 단언 문자열까지 글자
그대로 같고, **그래도 다음 진입점은 못 막는다.** 새로 쓰는 파일이 그 테스트를 같이
안 만들면 그만이기 때문이다(2026-08-14, PR #86 리뷰에서 용준님 지적).

`tests/test_timestamp_timezone.py` 가 같은 이유로 만들어졌다 — 값을 하나씩 단언하는
대신 **집합을 기계적으로 유도해서** 검사한다. 이 파일은 그 방식을 인코딩에 적용한 것이다.

무엇이 문제인가 — 로그가 아니라 메시지가 사라진다
--------------------------------------------------
윈도우 기본 콘솔은 cp949 이고 `—`(U+2014) · `⚠️` · `ℹ️` · `❌` 가 **거기 없다**.
진입점이 `force_utf8_output()` 을 안 부르면:

  1. `emit()` 이 인코딩에 실패해 **그 줄이 안 나간다**
  2. `handleError()` 가 traceback 을 같은 스트림에 쓰는데, 거기 실린 **소스 라인**에
     그 문자가 있으면 또 터진다. handleError 는 `OSError` 만 삼키므로
     `UnicodeEncodeError` 가 호출부로 **탈출한다**
  3. 컨슈머에서는 그게 `ValueError` 하위라 `consume()` 이 계약 위반으로 분류해
     `nack(requeue=False)` → **DLX. 메시지가 유실된다** (PR #86 에서 실측)

스크립트에서는 탈출이 크래시로 보이는데, **할 일을 다 끝낸 뒤 마지막 print 에서** 나서
종료코드가 "성공"과 "아무것도 못 함"을 구분하지 못하게 만든다(`app/core/console.py`).

🔴 왜 정규식·tokenize 가 아니라 `ast` 인가
------------------------------------------
초안은 소스를 문자열로 훑었는데 **네 방향으로 뚫렸다**(2026-08-14, 용준님이 전부 재현):

  - f-string 안의 언급을 호출로 셌다 — 파이썬 **3.12 부터 f-string 본문이
    `FSTRING_MIDDLE`** 이라 `tokenize.STRING` 으로 안 지워진다. 이 저장소는 3.12 고정이라
    **항상** 해당됐다. `--help` 문구에 이름만 적어도 통과한다
  - `def force_utf8_output():` 정의를 호출로 셌다 — **사설 복사본으로 되돌아가는 것**이
    이 가드가 제일 잡아야 할 회귀인데(`app/core/console.py` 가 "떠나온 안티패턴" 이라고
    적어 둔 그것) 그걸 못 잡았다
  - `if __name__ == '__main__':`(홑따옴표)를 놓치고, docstring 안 언급을 진입점으로 셌다
  - 폼피드(`\f`)가 있으면 `splitlines()` 와 `tokenize` 의 행 번호가 어긋나 주석이 살아남았다

전부 "소스를 텍스트로 보면 생기는" 구멍이라 정규식을 덧대는 대신 **파서에게 맡긴다.**
덤으로 `test_timestamp_timezone._offending_lines` 와 겹치던 13줄도 사라졌다 — 두 곳에서
같은 버그를 고칠 일이 없다.
"""

from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

COMPOSE = ROOT / "docker-compose.yml"

DOCKERFILE = ROOT / "Dockerfile"

DOCKER_COPY_DIR = re.compile(r"^COPY\s+(?:--\S+\s+)*([\w.-]+)/\s", re.MULTILINE)
"""`COPY app/ ./app/` 형태에서 **소스 디렉터리**만 뽑는다.

⚠️ `COPY requirements.txt .` 처럼 파일 하나를 옮기는 줄은 안 잡는다(끝에 `/` 가 없다).
⚠️ `--from=builder` 같은 플래그가 붙어도 잡는다.
"""

HELPER = "force_utf8_output"

HELPER_MODULE = "app.core.console"
"""helper 의 정본 위치. 진입점은 **여기서** import 해야 한다(사설 복사본 금지)."""

YAML_COMMENT = re.compile(r"(?:^|\s)#.*$")
"""compose 의 주석 — **전체줄과 인라인을 모두** 지운다.

🔴 **주석을 빼지 않으면 오탐이 난다** — 이 파일 주석이 사용법으로
`python scripts/classification_worker.py` · `setup_local_mq.py` · `app/core/mq.py` 를
적어 두고 있어서, 서비스가 아닌 것들이 "배포되는 진입점"으로 잡힌다. 실제로 처음에
그렇게 걸렸다(2026-08-14). 안내 문구를 고쳤다고 가드가 요구사항을 늘리면 안 된다.

⚠️ 범위를 넓힌 뒤로는 앞의 둘이 **다른 사유**(위험 문자)로 어차피 대상이라 이 제외가
   덜 보이지만, `app/core/mq.py` 는 여전히 `__main__` 블록이 없는 라이브러리 모듈이다 —
   주석 제외가 깨지면 그게 요구 대상이 되고, 넣을 자리조차 없어 영영 빨간 테스트가 된다.

⚠️ 처음엔 `^\\s*#` 로 **전체줄만** 지웠는데 `app/core/mq.py` 하나가 계속 남았다 —
   포트 매핑 뒤 **인라인** 주석(`- "5672:5672"  # AMQP … app/core/mq.py …`)이었다.
"""

COMMAND_KEY = re.compile(r"^(\s*)-?\s*(?:command|entrypoint)\s*:\s*(.*)$")
"""`command:` · `entrypoint:` 키. **이 블록 안에서만** 대상을 찾는다.

🔴 초안은 `findall` 을 **파일 전체**에 돌려서, docstring 이 *"`command:` 가 가리키는"*
   이라고 적어 둔 계약과 구현이 갈려 있었다. 볼륨 마운트에 `.py` 가 하나 생기면
   진입점도 아닌 파일이 요구 대상이 된다.
"""

SCRIPT_TARGET = re.compile(r"(?:^|[\s\"',\[])\.?/?((?:app|scripts|eval)/[\w./-]+\.py)")
"""`command:` 값 안의 파이썬 파일.

⚠️ **exec-form(`["python", "-u", "scripts/x.py"]`)도 잡아야 한다** — 이 compose 가 이미
   그 관용구를 쓰고 있다(`entrypoint: ["/bin/bash", "-c"]` · healthcheck `test:`).
   그래서 앞에 따옴표·대괄호·쉼표가 와도 매칭한다.
⚠️ `[\\w/]` 만 쓰면 `scripts/my-tool/run.py` 같은 하이픈·점 경로를 놓친다.
⚠️ `./scripts/foo.py` 의 `./` 는 선택적으로 흡수한다.
"""

MODULE_TARGET = re.compile(r"-m\s+([\w.]+)")
"""`python -m app.consumer` 형태. 모듈 경로를 파일 경로로 바꿔서 본다."""


def _parse(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, ValueError):  # pragma: no cover - 파싱 불가 파일은 건너뛴다
        return None


def _is_main_guard(node: ast.stmt) -> bool:
    """그 문장이 `if __name__ == "__main__":` 인지.

    ⚠️ 문자열 매칭이 아니라 **AST** 다 — 홑따옴표도 잡고, docstring·주석 안의 언급은
       애초에 노드가 아니라 안 잡힌다(초안이 두 방향으로 다 틀렸던 자리).
    """
    if not isinstance(node, ast.If):
        return False
    test = node.test
    if not isinstance(test, ast.Compare) or len(test.comparators) != 1:
        return False
    left, right = test.left, test.comparators[0]
    return (
        isinstance(left, ast.Name)
        and left.id == "__name__"
        and isinstance(right, ast.Constant)
        and right.value == "__main__"
    )


def _has_main_block(tree: ast.Module) -> bool:
    """모듈 최상단에 `__main__` 가드가 있는지."""
    return any(_is_main_guard(node) for node in tree.body)


def _skip_docstring(stmts: list[ast.stmt]) -> list[ast.stmt]:
    """맨 앞 docstring 한 문장을 건너뛴다."""
    if (
        stmts
        and isinstance(stmts[0], ast.Expr)
        and isinstance(stmts[0].value, ast.Constant)
        and isinstance(stmts[0].value.value, str)
    ):
        return stmts[1:]
    return stmts


def _delegation_target(stmts: list[ast.stmt], tree: ast.Module) -> ast.stmt | None:
    """`__main__` 블록이 **위임 한 줄**뿐이면 그 함수 노드. 아니면 `None`.

    `main()` · `asyncio.run(main())` 둘 다 본다.
    """
    if len(stmts) != 1 or not isinstance(stmts[0], ast.Expr):
        return None
    call = stmts[0].value
    name = _call_name(call)
    if name == "run" and isinstance(call, ast.Call) and call.args:
        name = _call_name(call.args[0])
    if not name:
        return None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == name:
            return node
    return None


def _entry_body(tree: ast.Module) -> list[ast.stmt] | None:
    """진입 지점의 문장 목록 — **`__main__` 블록이 기준**이다.

    블록이 *위임 한 줄*(`main()` · `asyncio.run(main())`)뿐일 때만 그 함수로 내려간다.
    그때는 함수 첫 문장이 곧 프로세스의 첫 문장이라 같은 검사가 성립한다.

    🔴 **예전 규칙("모듈 레벨 `def main` 이 있으면 무조건 그쪽")은 두 방향으로 틀렸다.**

       - **오탐** — `scripts/seed_vectordb.py` 는 `__main__` 블록이 argparse 를 돌린 뒤
         `main(reset=...)` 을 부르는 형태다. 즉 그 `main()` 은 진입점이 아니라 **작업 함수**인데
         옛 규칙이 그쪽을 봐서 실패로 잡았다. 그 말을 믿고 호출을 `main()` 안으로 옮기면
         **argparse 가 다시 앞서게 되어 PR #89 가 잡은 `daily.py` 버그가 되살아난다.**
       - **미탐** — `ast.FunctionDef` 만 봐서 `async def main()` 을 못 찾고 조용히
         `__main__` 블록으로 폴백했다. 규칙이 파일 모양에 따라 말없이 바뀌던 셈이다.

    ⚠️ 그래서 `async def main()` 파일들은 호출이 `__main__` 블록에 있다
       (`asyncio.run()` **앞**이라 오히려 더 이르다). 이 규칙은 그 배치를 그대로 통과시킨다 —
       `AsyncFunctionDef` 를 인식하도록 "고치면" 그 파일들이 전부 깨진다.
    """
    guard = next((node for node in tree.body if _is_main_guard(node)), None)
    if guard is None:
        return None
    stmts = _skip_docstring(list(guard.body))
    target = _delegation_target(stmts, tree)
    return list(target.body) if target is not None else stmts


def _call_name(node: ast.expr) -> str | None:
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _imports_canonical_helper(tree: ast.Module) -> bool:
    """`app.core.console` 의 helper 를 **모듈 최상단에서, `__main__` 가드보다 먼저** import 하는지.

    🔴 **호출만 검사하면 이 회귀를 통째로 놓친다**(서영님 PR #97 리뷰, 재현함). import 한 줄만
       지우고 호출을 남기면 `_calls_helper_first` 는 **통과**하는데 CLI 는 첫 문장에서
       `NameError` 로 죽는다. 이름이 같은 무엇이든 첫 문장이면 되는 게 아니라, **그 이름이
       공용 helper 여야** 한다.

    세 가지를 한 번에 막는다.
      - **import 없음** — 위 회귀
      - **가드 뒤 import** — 모듈은 위에서 아래로 실행되므로 `__main__` 블록이 먼저 돌아
        역시 `NameError` 다. 그래서 줄 번호를 비교한다
      - **사설 동명 함수** — `def force_utf8_output(): ...` 로 되돌아가는 것
        (`app/core/console.py` 가 "떠나온 안티패턴" 이라 적어 둔 그것). `ImportFrom` 이
        아니므로 안 잡힌다

    ⚠️ `import app.core.console` **후 `app.core.console.force_utf8_output()`** 형태는 일부러
       통과시키지 않는다 — 저장소에 0건이고, 관용구를 하나로 두는 편이 낫다. 그 형태를 쓰려면
       이 함수를 넓히고 실패 메시지도 같이 고칠 것.
    """
    guard_line = next(
        (node.lineno for node in tree.body if _is_main_guard(node)), None
    )
    if guard_line is None:
        return False
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == HELPER_MODULE
        and any(alias.name == HELPER for alias in node.names)
        and node.lineno < guard_line
        for node in tree.body
    )


def _calls_helper_first(tree: ast.Module) -> bool:
    """`force_utf8_output()` 이 진입 지점의 **첫 문장**인지(docstring 제외).

    🔴 **왜 "어딘가에 있으면" 이 아니라 "첫 문장" 인가 — 실버그로 확인됐다.**
       초안은 "진입 지점의 직속 문장이면 통과" 였는데, 그 규칙 아래서
       `app/batch/daily.py` 가 **통과하면서 실제로는 죽었다**:

           $ PYTHONIOENCODING=cp949 python -m app.batch.daily --help
           UnicodeEncodeError: 'cp949' codec can't encode character '\\u2014'

       `--state-path` 도움말에 `—` 가 있는데 **argparse 가 그걸 찍는 시점이
       `force_utf8_output()` 보다 앞**이었다. 우리 코드가 첫 줄을 찍기 한참 전이다.
       지켜야 할 것은 "진입 지점에서 실행된다" 가 아니라 **"무엇도 출력되기 전에
       실행된다"** 이고, 그걸 기계적으로 보장하는 유일한 형태가 첫 문장이다.

    🔴 이 규칙은 세 가지를 한 번에 막는다.
       - **정의를 호출로 세지 않는다** — `def force_utf8_output(): pass` 만 있는 사설
         복사본(`app/core/console.py` 가 "떠나온 안티패턴" 이라 적어 둔 그것)
       - **분기 안으로 숨기지 못한다** — `if False:` 안에 넣어도 첫 문장이 아니다
       - **다른 출력 뒤로 밀지 못한다** — 위 daily.py 사고

    ⚠️ 앞에 무언가를 꼭 둬야 하는 진입점이 나오면 그때 완화하되 **왜 완화하는지 근거를
       남길 것.** 인코딩 전환은 의존성이 없는 환경 설정이라 앞에 둘 것이 실제로 없다.
    """
    body = _entry_body(tree)
    if not body:
        return False

    stmts = _skip_docstring(list(body))
    if not stmts:
        return False
    head = stmts[0]
    return isinstance(head, ast.Expr) and _call_name(head.value) == HELPER


def _compose_command_blocks(text: str) -> list[str]:
    """`command:` · `entrypoint:` 값 블록만 뽑는다(주석 제거 후).

    값이 같은 줄에 있을 수도(`command: python x.py`), 블록 스칼라·리스트로 다음 줄에
    이어질 수도 있다(`command: >` · `command:` + 들여쓴 리스트). 키보다 **더 들여쓴**
    줄을 값의 연속으로 본다.
    """
    lines = [YAML_COMMENT.sub("", line) for line in text.splitlines()]
    blocks: list[str] = []
    i = 0
    while i < len(lines):
        match = COMMAND_KEY.match(lines[i])
        if not match:
            i += 1
            continue
        indent = len(match.group(1))
        collected = [match.group(2)]
        i += 1
        while i < len(lines):
            nxt = lines[i]
            if nxt.strip() and len(nxt) - len(nxt.lstrip()) <= indent:
                break
            collected.append(nxt)
            i += 1
        blocks.append("\n".join(collected))
    return blocks


def _has_uncp949_literal(tree: ast.Module) -> bool:
    """문자열 리터럴에 cp949 로 인코딩 못 하는 문자가 있는지(docstring 포함).

    ⚠️ **한글은 cp949 에 있다.** 걸리는 건 `—`(U+2014) · `⚠️` · `ℹ️` · `❌` 같은 것들이라,
       한국어를 쓴다고 대상이 되지는 않는다. 그래서 이 검사가 실제로 집합을 좁힌다.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for char in node.value:
                try:
                    char.encode("cp949")
                except UnicodeEncodeError:
                    return True
    return False


def _image_source_dirs() -> list[str]:
    """운영 이미지에 실리는 소스 디렉터리 — `Dockerfile` 의 `COPY <dir>/ ...` 에서 읽는다.

    🔴 **손으로 적지 않는 이유.** 예전에는 `app/**` 를 코드에 박아뒀는데, PR #95 가
       `COPY scripts/ ./scripts/` 를 추가하면서 **그 목록이 조용히 낡았다.** 분류 워커는
       노션 CI_방향성 §8-1 대로 k8s CronJob(`python scripts/classification_worker.py`)으로
       도는데, k8s 매니페스트는 이 저장소에 없어 compose 만 보던 옛 기준으로는 영영
       안 잡혔다. Dockerfile 은 우리 저장소에 있으므로 여기서 유도할 수 있다.
    """
    return sorted(set(DOCKER_COPY_DIR.findall(DOCKERFILE.read_text(encoding="utf-8"))))


def _guarded_entrypoints() -> list[Path]:
    """가드 대상 진입점. 손으로 적은 목록이 아니라 **두 가지 사유로 유도**한다.

    1. **우리가 실제로 돌리는 것** — 운영 이미지에 실리는 디렉터리(`Dockerfile` 의 `COPY`)의
       `__main__` 진입점 + `docker-compose.yml` 의 `command:`/`entrypoint:` 가 가리키는 파일.
    2. **깨질 수 있는 것** — `app/`·`scripts/`·`eval/` 의 `__main__` 진입점 중 문자열에
       cp949 불가 문자를 가진 것.

    ⚠️ **1 의 근거를 오해하지 말 것 — "컨테이너에서 깨진다" 가 아니다.** 이미지는 리눅스라
       기본 인코딩이 UTF-8 이고 거기서는 이 헬퍼가 사실상 no-op 다. 1 이 의미가 있는 이유는
       **"이미지에 실린다 = 우리가 진짜 쓰는 도구다"** 이고, 그 도구를 **윈도우에서 직접
       치는 것이 실제 경로**이기 때문이다(분류 워커를 로컬에서 돌린 이력). 즉 1 은 위험의
       원인이 아니라 **"사람이 칠 확률" 의 대리 지표**다.

    ⚠️ **두 사유를 합집합으로 두는 이유.** 한쪽이 다른 쪽을 대체하지 않는다 — 출력이 ASCII
       뿐인 배포물은 2 에서 빠지고, `eval/` 은 `.dockerignore` 라 1 에서 빠진다.

    🔴 **왜 `scripts/`·`eval/` 로 넓혔나.** 예전에는 "배포되는 것" 으로 끊고 나머지는
       `MANUALLY_CONVERTED` 라는 손 목록에 뒀는데, **그 목록이 대장 노릇을 못 했다** —
       유도 대상에도 목록에도 없이 helper 를 부르는 파일이 19개까지 늘었고 아무 테스트도
       그것들을 안 잡았다(용준님 PR #92 리뷰 2회전). 소유자별 3 PR(#92·#93·#94)로 파일이
       전부 전환됐으므로, 이제 손 목록을 지우고 기준을 기계로 유도한다.

    🔴 **여기에 개수를 적지 말 것.** 예전 docstring 이 *"28개"* 와 특정 파일의 줄 번호를
       박아뒀는데 **그 파일을 고치는 PR 이 같은 문장을 거짓으로 만들었다**(같은 리뷰 A-2).

    ⚠️ `app/main.py` 는 대상이 아니다 — `__main__` 블록이 없고 Dockerfile 이
       `uvicorn app.main:app` 으로 띄운다. 스트림 소유자가 uvicorn 이라 우리가 끼어들
       자리가 아니다. (그 파일의 설정 오류 처리는 별건 — PR #96)
    """
    found: set[Path] = set()
    shipped = set(_image_source_dirs())

    for folder in ("app", "scripts", "eval"):
        for path in (ROOT / folder).rglob("*.py"):
            tree = _parse(path)
            if tree is None or not _has_main_block(tree):
                continue
            if folder in shipped or _has_uncp949_literal(tree):
                found.add(path)

    for block in _compose_command_blocks(COMPOSE.read_text(encoding="utf-8")):
        for rel in SCRIPT_TARGET.findall(block):
            target = ROOT / rel
            if target.exists():
                found.add(target)
        for dotted in MODULE_TARGET.findall(block):
            target = ROOT / (dotted.replace(".", "/") + ".py")
            if target.exists():
                found.add(target)

    return sorted(found)


def test_guarded_entrypoints_switch_console_encoding() -> None:
    """🔴 가드 대상 진입점은 전부 `force_utf8_output()` 을 불러야 한다.

    걸렸다면 **진입 지점의 첫 문장**으로 넣을 것:

        from app.core.console import force_utf8_output    # 모듈 최상단
        ...
        force_utf8_output()

    어디가 "진입 지점" 인지는 `_entry_body()` 가 정한다 — `__main__` 블록이 기준이고,
    그 블록이 `main()` 위임 한 줄뿐일 때만 그 함수 안이다.

    🔴 **호출과 import 를 둘 다 본다.** 호출만 보면 `import` 한 줄만 지운 회귀를 놓친다 —
       가드는 통과하는데 CLI 는 첫 문장에서 `NameError` 로 죽는다(서영님 PR #97 리뷰에서
       실측). 사유는 `_imports_canonical_helper`.

    ⚠️ **import 는 모듈 최상단, `__main__` 가드보다 앞.** 함수 안에 두면 실행은 되더라도
       배선을 테스트로 고정할 수 없고, 가드보다 뒤에 두면 그대로 `NameError` 다.
    """
    no_call: list[str] = []
    no_import: list[str] = []
    for path in _guarded_entrypoints():
        rel = path.relative_to(ROOT).as_posix()
        tree = _parse(path)
        if tree is None or not _calls_helper_first(tree):
            no_call.append(rel)
        # ⚠️ 두 목록을 **따로** 모은다. 합쳐 보고하면 어느 쪽이 빠졌는지 몰라 고치는 사람이
        #    호출을 또 넣거나 import 를 또 넣는다.
        if tree is not None and not _imports_canonical_helper(tree):
            no_import.append(rel)

    assert not no_call, (
        "진입 지점의 첫 문장에서 force_utf8_output() 을 안 부릅니다:\n  "
        + "\n  ".join(no_call)
    )
    assert not no_import, (
        f"모듈 최상단(=`__main__` 가드보다 앞)에서 `from {HELPER_MODULE} import {HELPER}` 를 "
        "하지 않습니다 — 호출만 있으면 CLI 가 NameError 로 죽습니다:\n  "
        + "\n  ".join(no_import)
    )


def test_the_derivation_actually_finds_the_known_entrypoints() -> None:
    """가드가 **무엇을 보고 있는지** 고정한다.

    이게 없으면 유도가 조용히 아무것도 안 잡게 바뀌어도 위 테스트가 통과한다 —
    "테스트가 있는데 안 잡는" 상태가 제일 나쁘다.
    """
    found = {p.relative_to(ROOT).as_posix() for p in _guarded_entrypoints()}

    # 사유 1(배포된다) — compose 파싱이 죽으면 여기가 먼저 터진다.
    assert "app/batch/daily.py" in found
    assert "app/consumer.py" in found
    assert "scripts/mock_producer.py" in found, "compose command 파싱이 죽었습니다"
    assert "eval/run_detection_eval.py" in found, "compose command 파싱이 죽었습니다"

    # 사유 2(깨질 수 있다) — compose 밖·`app/` 밖인데 위험 문자가 있는 것들.
    # 🔴 예전에는 이 셋이 **대상 아님**으로 단언돼 있었다(compose 주석에만 있으므로).
    #    범위를 넓히면서 사유가 바뀌었다 — 배포 여부와 무관하게 문자로 잡힌다.
    #    특히 `classification_worker.py` 는 PR #95 로 k8s CronJob 배포가 됐는데,
    #    k8s 매니페스트는 이 저장소에 없어서 compose 만 보던 옛 기준으로는 영영 안 잡혔다.
    assert "scripts/classification_worker.py" in found
    assert "scripts/verify_counts.py" in found
    assert "eval/run_pipeline_eval.py" in found

    # 사유 1(이미지에 실린다) — 위험 문자가 **없는데도** 대상인 것.
    # 🔴 이 파일은 한글만 써서 사유 2 로는 안 잡힌다. Dockerfile 이 `scripts/` 를 COPY 하기
    #    때문에 대상이고, 그래서 `_image_source_dirs()` 가 죽으면 여기가 먼저 터진다.
    assert "scripts/detection_experiments/audit_mock_baselines.py" in found

    # uvicorn 이 스트림을 소유하므로 대상이 아니다(`_guarded_entrypoints` docstring 참고).
    assert "app/main.py" not in found


# ── 실제 크래시 경로 (구조가 아니라 동작을 잰다) ────────────────────────

CP949_HELP_SMOKE = [
    "eval/run_reporting_eval.py",
    "scripts/build_mapped_data_input.py",
    "scripts/classification_worker.py",
    "scripts/smoke_mq.py",
]
"""cp949 에서 `--help` 만으로 **실제로 죽었던** 진입점 표본.

⚠️ **대장이 아니라 표본이다.** 전수 검사는 위 `test_guarded_entrypoints_...` 가 AST 로 한다.
   여기는 그 구조 검사가 못 보는 것 — *"`force_utf8_output()` 자체가 망가지면?"* — 을 잡는
   유일한 자리라, 실제로 프로세스를 띄워 종료코드를 본다.

🔴 **전수로 돌리지 말 것.** 유도 집합에는 **argparse 가 없는 파일이 섞여 있다.**
   그런 파일에 `--help` 를 주면 파싱이 없으니 **스크립트가 그대로 실행된다**
   (`scripts/detection_experiments/demo_sim.py` 등은 LLM·파일 쓰기까지 간다).
   그래서 표본은 argparse 가 확실한 것만 손으로 고른다.
"""


@pytest.mark.parametrize("rel", CP949_HELP_SMOKE)
def test_sampled_entrypoints_survive_cp949_help(rel: str) -> None:
    """cp949 콘솔에서 `--help` 만 요청해도 죽지 않아야 한다.

    argparse 는 우리 코드가 첫 줄을 찍기 전에 자기 출력을 내보내므로, 전환이
    `parse_args()` 뒤로 밀리거나 통째로 빠지면 여기서 exit 1 로 잡힌다.

    ⚠️ **구조 가드와 역할이 다르다.** 위 `test_guarded_entrypoints_...` 는 AST 로
       *"호출이 첫 문장인가"* 만 본다. 그건 `force_utf8_output()` **자신이** 망가져도
       통과한다. 여기는 실제로 프로세스를 띄워 종료코드를 보므로 그 경우를 잡는다.

    🔴 **`text=True` 를 쓰지 말 것 — bytes 로 받는다.** 자식은 `force_utf8_output()`
       때문에 UTF-8 로 쓰는데 부모는 `PYTHONIOENCODING` 이 아니라 **자기 locale** 로
       디코드한다. 한국어 윈도우에서 그 둘이 어긋나 reader thread 가
       `UnicodeDecodeError` 로 죽고 **`stdout` 이 조용히 `None` 이 된다**(실측). 종료코드는
       0 그대로라 단언은 통과하면서 출력만 사라진다 — PR #66 이 `test_generate_determinism`
       에서 고친 바로 그 버그다.
    ⚠️ `env` 를 통째로 교체하지 않고 `os.environ` 을 물려준다. 교체하면 자식이 `PATH` 를
       잃는다(같은 PR #66 건).
    """
    proc = subprocess.run(
        [sys.executable, str(ROOT / rel), "--help"],
        capture_output=True,
        env={**os.environ, "PYTHONIOENCODING": "cp949"},
        cwd=ROOT,
        check=False,
    )

    assert proc.returncode == 0, (
        f"{rel} 이 cp949 콘솔에서 --help 만으로 죽습니다:\n"
        + proc.stderr.decode("utf-8", "replace")[-2000:]
    )
    # 종료코드만 보면 "아무것도 안 찍고 0" 도 통과한다 — 도움말이 실제로 나왔는지까지 본다.
    assert proc.stdout, f"{rel} 이 --help 에 아무것도 출력하지 않았습니다"


# ⚠️ PR #92 의 monkeypatch 배선 테스트(`module.main()` 을 직접 호출)는 여기서 지웠다.
#    그때는 세 파일이 유도 집합 **밖**이라 그것만이 호출을 잠갔지만, 범위를 넓힌 지금은
#    구조 가드가 같은 것을 전수로 본다. 되살리지 말 것 — `main()` 을 실제로 부르는
#    테스트는 argparse 없는 파일이 목록에 들어오는 순간 **본 작업을 실행한다.**


# ── 검출기 자체를 고정한다 (전부 초안이 실제로 틀렸던 입력) ──────────────

MAIN_GUARD = 'if __name__ == "__main__":\n    main()\n'
"""위임 한 줄짜리 `__main__` 블록. 아래 표본에서 `main()` 을 진입점으로 만들어 준다."""

ASYNC_DELEGATION = (
    'async def main():\n    force_utf8_output()\nif __name__ == "__main__":\n'
    "    asyncio.run(main())\n"
)
"""#94 의 `demo_sim` 계열 모양 — 옛 규칙은 `AsyncFunctionDef` 를 못 봤다."""

WORKER_MAIN_OK = (
    'def main(reset=False):\n    do_work()\nif __name__ == "__main__":\n'
    "    force_utf8_output()\n    ap = build_parser()\n"
    "    main(reset=ap.parse_args().reset)\n"
)
"""`seed_vectordb` 모양 — `main()` 은 진입점이 아니라 **작업 함수**다."""

WORKER_MAIN_BAD = (
    'def main(reset=False):\n    force_utf8_output()\nif __name__ == "__main__":\n'
    "    ap = build_parser()\n    main(reset=ap.parse_args().reset)\n"
)
"""위와 같은 모양인데 호출이 작업 함수 안 — argparse 가 이미 앞섰다."""


def _tmp_module(tmp_path: Path, body: str) -> ast.Module:
    path = tmp_path / "sample.py"
    path.write_text(body, encoding="utf-8")
    tree = _parse(path)
    assert tree is not None
    return tree


@pytest.mark.parametrize(
    "body",
    [
        'if __name__ == "__main__":\n    main()\n',
        "if __name__ == '__main__':\n    main()\n",  # 🔴 홑따옴표 — 초안이 놓쳤다
    ],
)
def test_main_block_is_detected_regardless_of_quote_style(
    tmp_path: Path, body: str
) -> None:
    assert _has_main_block(_tmp_module(tmp_path, body))


def test_main_block_ignores_mentions_in_docstrings_and_comments(tmp_path: Path) -> None:
    """🔴 설명이 진입점을 만들면 안 된다 — 초안은 docstring 언급을 셌다."""
    body = (
        '"""아래에 if __name__ == "__main__": 를 둔다."""\n'
        '# if __name__ == "__main__":\n'
        'HELP = "if __name__ == \\"__main__\\":"\n'
    )
    assert not _has_main_block(_tmp_module(tmp_path, body))


CANONICAL_IMPORT = f"from {HELPER_MODULE} import {HELPER}\n"

CALL_WITHOUT_IMPORT = f"def main():\n    {HELPER}()\n{MAIN_GUARD}"
"""🔴 서영님 PR #97 리뷰에서 실측된 회귀 — 호출만 남기고 import 를 지운 모양."""

IMPORT_AFTER_GUARD = f"def main():\n    {HELPER}()\n{MAIN_GUARD}{CANONICAL_IMPORT}"
"""import 가 `__main__` 가드보다 뒤 — 실행 순서상 그대로 `NameError`."""

PRIVATE_HELPER_DEF = (
    f"def {HELPER}():\n    pass\ndef main():\n    {HELPER}()\n{MAIN_GUARD}"
)
"""사설 동명 함수 — `app/core/console.py` 가 "떠나온 안티패턴" 이라 적어 둔 그것."""

IMPORT_INSIDE_FUNCTION = (
    f"def main():\n    from {HELPER_MODULE} import {HELPER}\n    {HELPER}()\n{MAIN_GUARD}"
)
"""함수 안 import — 실행은 되지만 배선을 테스트로 고정할 수 없다."""

IMPORT_FROM_OTHER_MODULE = (
    f"from app.core.constants import KST\ndef main():\n    {HELPER}()\n{MAIN_GUARD}"
)
"""엉뚱한 모듈에서 온 import — 이름만 맞다고 통과하면 안 된다."""


@pytest.mark.parametrize(
    "body, expected, why",
    [
        (
            f"{CANONICAL_IMPORT}def main():\n    force_utf8_output()\n{MAIN_GUARD}",
            True,
            "모듈 최상단 import + 가드보다 앞",
        ),
        (
            CALL_WITHOUT_IMPORT,
            False,
            "🔴 import 없이 호출만 — 가드는 통과하는데 CLI 는 NameError (서영님 #97 실측)",
        ),
        (
            IMPORT_AFTER_GUARD,
            False,
            "🔴 import 가 __main__ 가드보다 **뒤** — 모듈은 위에서 아래로 도니 역시 NameError",
        ),
        (
            PRIVATE_HELPER_DEF,
            False,
            "🔴 사설 동명 함수 — 공용 helper 로부터의 import 가 아니다",
        ),
        (
            IMPORT_INSIDE_FUNCTION,
            False,
            "🔴 함수 안 import — 모듈 최상단이 아니라 배선을 고정할 수 없다",
        ),
        (
            IMPORT_FROM_OTHER_MODULE,
            False,
            "🔴 다른 모듈에서 온 import 는 근거가 안 된다",
        ),
    ],
)
def test_import_detector_requires_the_canonical_helper_import(
    tmp_path: Path, body: str, expected: bool, why: str
) -> None:
    """🔴 호출 검사와 **짝**이다. 하나만 있으면 반대쪽 회귀를 통째로 놓친다."""
    assert _imports_canonical_helper(_tmp_module(tmp_path, body)) is expected, why


@pytest.mark.parametrize(
    "body, expected, why",
    [
        (
            f"def main():\n    force_utf8_output()\n{MAIN_GUARD}",
            True,
            "위임 한 줄 → main() 첫 문장",
        ),
        (
            f'name = "x"\ndef main():\n    print(f"hint: force_utf8_output() {{name}}")\n{MAIN_GUARD}',
            False,
            "🔴 f-string 안 언급 — 3.12 는 FSTRING_MIDDLE 이라 tokenize 로 안 지워졌다",
        ),
        (
            f"def force_utf8_output():\n    pass\ndef main():\n    pass\n{MAIN_GUARD}",
            False,
            "🔴 정의를 호출로 세면 사설 복사본 회귀를 못 잡는다",
        ),
        (
            f"def main():\n    if False:\n        force_utf8_output()\n{MAIN_GUARD}",
            False,
            "🔴 안 닿는 분기로 숨기면 안 된다",
        ),
        (
            f"def main():\n    ap = build_parser()\n    force_utf8_output()\n{MAIN_GUARD}",
            False,
            "🔴 다른 문장 뒤로 밀면 안 된다 — daily.py 가 이 모양으로 `--help` 에서 죽었다",
        ),
        (
            f"X = 1\n\x0c\ndef main():\n    # force_utf8_output() 주석뿐\n    pass\n{MAIN_GUARD}",
            False,
            "🔴 폼피드가 있으면 splitlines/tokenize 행이 어긋나 주석이 살아남았다",
        ),
        (
            f'def main():\n    """force_utf8_output() 을 부른다."""\n    pass\n{MAIN_GUARD}',
            False,
            "docstring 언급",
        ),
        (
            'if __name__ == "__main__":\n    force_utf8_output()\n    run()\n',
            True,
            "main() 없이 __main__ 블록에서 직접 부르는 형태",
        ),
        # 🔴 아래 셋이 새 `_entry_body` 규칙의 핵심이다 (PR #92 후속에서 확인된 실제 모양).
        (
            ASYNC_DELEGATION,
            True,
            "🔴 async def main() 도 위임 한 줄이면 그 안을 본다 — 옛 규칙은 못 찾았다",
        ),
        (
            WORKER_MAIN_OK,
            True,
            "🔴 seed_vectordb 모양 — main() 은 작업 함수다. 옛 규칙은 이걸 실패로 오탐했다",
        ),
        (
            WORKER_MAIN_BAD,
            False,
            "🔴 위와 반대 — 호출이 작업 함수 안이면 argparse 가 이미 앞선다",
        ),
    ],
)
def test_call_detector_counts_only_real_calls_at_entry(
    tmp_path: Path, body: str, expected: bool, why: str
) -> None:
    assert _calls_helper_first(_tmp_module(tmp_path, body)) is expected, why


@pytest.mark.parametrize(
    "line, expected",
    [
        ("command: python -u scripts/mock_producer.py", "scripts/mock_producer.py"),
        # 🔴 exec-form — 이 compose 가 이미 쓰는 관용구인데 초안이 통째로 놓쳤다
        ('command: ["python", "-u", "scripts/mock_producer.py"]', "scripts/mock_producer.py"),
        ("command: python ./scripts/mock_producer.py", "scripts/mock_producer.py"),
        ('entrypoint: ["python", "eval/run_detection_eval.py"]', "eval/run_detection_eval.py"),
        # `python -m app.consumer` 는 모듈 표기라 파일로 환원해야 한다
        ("command: python -m app.consumer", "app/consumer.py"),
    ],
)
def test_compose_command_forms_are_all_understood(line: str, expected: str) -> None:
    blocks = _compose_command_blocks(f"services:\n  x:\n    {line}\n")
    hits = {
        rel for b in blocks for rel in SCRIPT_TARGET.findall(b)
    } | {
        dotted.replace(".", "/") + ".py" for b in blocks for dotted in MODULE_TARGET.findall(b)
    }
    assert expected in hits, f"{line!r} 에서 {expected} 를 못 찾았습니다"


def test_targets_outside_command_blocks_are_ignored() -> None:
    """🔴 `command:` 밖의 `.py` 는 진입점이 아니다 — 볼륨 마운트 등.

    초안은 `findall` 을 파일 전체에 돌려서, docstring 이 적어 둔 계약
    (*"`command:` 가 가리키는"*)과 구현이 갈려 있었다.
    """
    compose = (
        "services:\n"
        "  x:\n"
        "    command: python scripts/mock_producer.py\n"
        "    volumes:\n"
        "      - ./scripts/seed_vectordb.py:/app/seed.py:ro\n"
    )
    hits = {rel for b in _compose_command_blocks(compose) for rel in SCRIPT_TARGET.findall(b)}

    assert "scripts/mock_producer.py" in hits
    assert "scripts/seed_vectordb.py" not in hits


@pytest.mark.parametrize("char", ["—", "⚠", "ℹ", "❌"])
def test_the_characters_this_guard_exists_for_are_really_absent_from_cp949(
    char: str,
) -> None:
    """이 가드의 전제 — 그 문자들이 정말 cp949 에 없다.

    전제가 조용히 바뀌면(예: 누가 `—` 를 ASCII `-` 로 일괄 치환) 가드는 남아 있는데
    막는 대상이 없어진다. 그 경우 이 테스트가 먼저 실패해서 알려준다.
    """
    with pytest.raises(UnicodeEncodeError):
        char.encode("cp949")


def test_the_arrow_is_encodable_so_only_one_char_per_line_blows_up() -> None:
    """대조군 — `←`(U+2190)는 **cp949 에 있다.**

    그래서 같은 줄에서 한 문자만 골라 터지는 것처럼 보인다(`app/core/console.py`).
    ⚠️ 초안은 이 단언을 위 파라미터라이즈 테스트 끝에 **assert 없는 맨 표현식**으로
       뒀는데, 그러면 지워져도 아무것도 안 깨진다(용준님 잔가지 지적).

    ⚠️ 바이트값을 박지 않는다 — 처음에 손으로 적었다가 틀렸다(`\\xa1\\xe7` 인데
       `\\xa1\\xf9` 로 썼다). 여기서 고정할 것은 **인코딩이 되느냐**이지 특정 바이트가
       아니다.
    """
    assert "←".encode("cp949")  # 예외가 나면 이 줄에서 실패한다
