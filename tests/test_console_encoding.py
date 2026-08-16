"""**배포되는 진입점**이 콘솔 인코딩을 바꾸는지 고정한다.

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
import importlib
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

COMPOSE = ROOT / "docker-compose.yml"

HELPER = "force_utf8_output"

YAML_COMMENT = re.compile(r"(?:^|\s)#.*$")
"""compose 의 주석 — **전체줄과 인라인을 모두** 지운다.

🔴 **주석을 빼지 않으면 오탐이 6건 난다** — 이 파일 주석이 사용법으로
`python scripts/classification_worker.py` · `setup_local_mq.py` · `app/core/mq.py` 를
적어 두고 있어서, 서비스가 아닌 것들이 "배포되는 진입점"으로 잡힌다. 실제로 처음에
그렇게 걸렸다(2026-08-14). 안내 문구를 고쳤다고 가드가 요구사항을 늘리면 안 된다.

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


def _entry_body(tree: ast.Module) -> list[ast.stmt] | None:
    """진입 지점의 문장 목록. `main()` 이 있으면 그쪽, 없으면 `__main__` 블록.

    ⚠️ `main()` 을 먼저 보는 이유: 대부분의 진입점은 `__main__` 블록이
       `main()` 한 줄이라, 블록을 보면 "첫 문장이 `main()` 이다" 가 되어 검사가 무의미해진다.
    """
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            return node.body
    for node in tree.body:
        if _is_main_guard(node):
            return node.body
    return None


def _call_name(node: ast.expr) -> str | None:
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


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

    stmts = list(body)
    first = stmts[0]
    if (
        isinstance(first, ast.Expr)
        and isinstance(first.value, ast.Constant)
        and isinstance(first.value.value, str)
    ):
        stmts = stmts[1:]  # docstring 은 건너뛴다

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


def _deployed_entrypoints() -> list[Path]:
    """**배포되는** 진입점 집합. 손으로 적은 목록이 아니라 유도한다.

    두 곳에서 온다:
      - `app/**` 의 `__main__` 블록 — 우리가 배포하는 패키지 코드
      - `docker-compose.yml` 의 `command:`/`entrypoint:` 가 가리키는 파이썬 파일

    ⚠️ **범위를 왜 여기서 끊었나.** `scripts/`·`eval/` 전체로 넓히면 대상이 수십 개다.
       전부 두 줄짜리 기계적 변경이지만 대부분 남의 파일이라 한 PR 에 몰면 리뷰가
       불가능하다. 넓히려면 이 함수만 고치면 되고, 그때는 파일 소유자별로 나눠 올릴 것
       (그렇게 진행 중이다 — PR #92·#93·#94).

    🔴 **여기에 개수를 적지 말 것.** 예전 docstring 이 *"28개"* 와
       *"`run_reporting_eval.py:53` 에 손수 `sys.stdout.reconfigure()` 가 남아 있다"* 를
       박아뒀는데, **그 파일을 고치는 PR 이 같은 문장을 거짓으로 만들었다**(용준님 PR #92
       리뷰 A-2). 스윕이 진행되는 동안 이 숫자는 PR 마다 바뀐다 — 세고 싶으면
       `MANUALLY_CONVERTED` 아래 주석의 명령으로 그때그때 유도할 것.

    ⚠️ 남은 사설 사본들은 stderr 를 안 바꾸고 `contextlib.suppress` 도 없어, 범위 확대
       때 한꺼번에 걷는 편이 낫다. **범위 밖 파일을 임의로 몇 개만 더 걷으면 "배포되는
       것만" 이라는 기준이 흐려진다** — 그래서 손으로 전환한 것은 `MANUALLY_CONVERTED`
       에 모아 두고 별도 테스트로 잠근다. (용준님 PR #89 2회전 · #92 리뷰 지적)

    ⚠️ `app/main.py` 는 대상이 아니다 — `__main__` 블록이 없고 Dockerfile 이
       `uvicorn app.main:app` 으로 띄운다. 스트림 소유자가 uvicorn 이라 우리가 끼어들
       자리가 아니다.
    """
    found: set[Path] = set()

    for path in (ROOT / "app").rglob("*.py"):
        tree = _parse(path)
        if tree is not None and _has_main_block(tree):
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


def test_deployed_entrypoints_switch_console_encoding() -> None:
    """🔴 배포되는 진입점은 전부 `force_utf8_output()` 을 불러야 한다.

    걸렸다면 `main()` 안에 **직속 문장으로** 넣을 것:

        from app.core.console import force_utf8_output
        ...
        force_utf8_output()

    ⚠️ **import 를 모듈 최상단에 둘 것.** 함수 안에서 import 하면 배선 테스트가
       몽키패치를 못 걸어 호출 여부를 고정할 수 없다.
    """
    missing = [
        p.relative_to(ROOT).as_posix()
        for p in _deployed_entrypoints()
        if (tree := _parse(p)) is None or not _calls_helper_first(tree)
    ]

    assert not missing, (
        "배포되는 진입점인데 진입 지점에서 force_utf8_output() 을 안 부릅니다:\n  "
        + "\n  ".join(missing)
    )


def test_the_derivation_actually_finds_the_known_entrypoints() -> None:
    """가드가 **무엇을 보고 있는지** 고정한다.

    이게 없으면 정규식이 조용히 아무것도 안 잡게 바뀌어도 위 테스트가 통과한다 —
    "테스트가 있는데 안 잡는" 상태가 제일 나쁘다.
    """
    found = {p.relative_to(ROOT).as_posix() for p in _deployed_entrypoints()}

    assert "app/batch/daily.py" in found
    assert "app/consumer.py" in found
    assert "scripts/mock_producer.py" in found, "compose command 파싱이 죽었습니다"
    assert "eval/run_detection_eval.py" in found, "compose command 파싱이 죽었습니다"

    # uvicorn 이 스트림을 소유하므로 대상이 아니다(위 docstring 참고).
    assert "app/main.py" not in found

    # 🔴 compose **주석**에만 있는 것은 서비스가 아니다.
    for mentioned_only_in_comments in (
        "scripts/classification_worker.py",
        "scripts/setup_local_mq.py",
        "app/core/mq.py",
    ):
        assert mentioned_only_in_comments not in found, (
            f"{mentioned_only_in_comments} 은 compose 주석에만 있습니다 — "
            "주석 제외가 깨졌습니다"
        )


# ── 범위 밖인데 손으로 전환한 진입점 ────────────────────────────────────

MANUALLY_CONVERTED = [
    "eval/run_reporting_eval.py",
    "scripts/build_mapped_data_input.py",
    "scripts/classification_worker.py",
]
"""`_deployed_entrypoints()` 밖이지만 **손으로 전환한** 진입점.

🔴 여기 적는 것 = *"범위 밖인데 예외로 걷었다"* 는 선언이다. 늘릴 때는 PR 에 사유를
   남길 것 — 목록이 조용히 자라면 `_deployed_entrypoints()` 의 기준이 흐려진다.
   범위가 확대되면 이 목록은 지우고 유도 대상에 흡수시킨다. (용준님 PR #92 리뷰 A-1)

남은 사설 `sys.stdout.reconfigure()` 사본을 세려면(숫자를 박지 말고 그때그때 유도할 것)::

    git ls-tree -r --name-only HEAD | grep '\\.py$' | while read f; do
      grep -q 'sys\\.stdout\\.reconfigure(' "$f" && ! grep -q force_utf8_output "$f" && echo "$f"
    done | wc -l
"""


@pytest.mark.parametrize("rel", MANUALLY_CONVERTED)
def test_manually_converted_entrypoints_survive_cp949_help(rel: str) -> None:
    """cp949 콘솔에서 `--help` 만 요청해도 죽지 않아야 한다.

    argparse 는 우리 코드가 첫 줄을 찍기 전에 자기 출력을 내보내므로, 전환이
    `parse_args()` 뒤로 밀리거나 통째로 빠지면 여기서 exit 1 로 잡힌다. 이 세 파일은
    유도 집합 밖이라 **이 테스트가 없으면 고친 줄을 지워도 스위트가 초록이다**
    (용준님 PR #92 리뷰 A-1).

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


@pytest.mark.parametrize("rel", MANUALLY_CONVERTED)
def test_manually_converted_entrypoints_call_the_helper_before_parsing(
    rel: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`main()` 이 `parse_args()` **전에** `force_utf8_output()` 을 부르는지.

    🔴 **위 서브프로세스 테스트만으로는 부족하다 — 3개 중 1개를 못 잡는다.**
       `eval/run_reporting_eval.py` 는 `description` 이 리터럴(`"실험⑦ …"`)이고
       `⑦`(U+2465)은 **cp949 에 있어서** `--help` 가 애초에 안 죽는다. 호출을 통째로
       지워도 서브프로세스 테스트는 초록이다(실측). 그 파일의 위험은 `--help` 가 아니라
       **채점 결과 출력**에 있는데 그건 LLM 비용이 들어 테스트에서 돌릴 수 없다.

    `--help` 는 `SystemExit(0)` 을 던지므로, 호출이 `parse_args()` 뒤로 밀리면
    여기 도달하기 전에 빠져나가 `calls` 가 비고 실패한다 — 순서까지 같이 잠근다.
    """
    module = importlib.import_module(rel.removesuffix(".py").replace("/", "."))

    calls: list[str] = []
    monkeypatch.setattr(module, "force_utf8_output", lambda: calls.append("utf8"))
    monkeypatch.setattr(module.sys, "argv", [rel, "--help"])

    with pytest.raises(SystemExit):
        module.main()

    assert calls, f"{rel} 의 main() 이 force_utf8_output() 을 (먼저) 부르지 않았습니다"


# ── 검출기 자체를 고정한다 (전부 초안이 실제로 틀렸던 입력) ──────────────


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


@pytest.mark.parametrize(
    "body, expected, why",
    [
        (
            "def main():\n    force_utf8_output()\n",
            True,
            "직속 호출",
        ),
        (
            'name = "x"\ndef main():\n    print(f"hint: force_utf8_output() {name}")\n',
            False,
            "🔴 f-string 안 언급 — 3.12 는 FSTRING_MIDDLE 이라 tokenize 로 안 지워졌다",
        ),
        (
            "def force_utf8_output():\n    pass\ndef main():\n    pass\n",
            False,
            "🔴 정의를 호출로 세면 사설 복사본 회귀를 못 잡는다",
        ),
        (
            "def main():\n    if False:\n        force_utf8_output()\n",
            False,
            "🔴 안 닿는 분기로 숨기면 안 된다",
        ),
        (
            "def main():\n    ap = build_parser()\n    force_utf8_output()\n",
            False,
            "🔴 다른 문장 뒤로 밀면 안 된다 — daily.py 가 이 모양으로 `--help` 에서 죽었다",
        ),
        (
            "X = 1\n\x0c\ndef main():\n    # force_utf8_output() 주석뿐\n    pass\n",
            False,
            "🔴 폼피드가 있으면 splitlines/tokenize 행이 어긋나 주석이 살아남았다",
        ),
        (
            'def main():\n    """force_utf8_output() 을 부른다."""\n    pass\n',
            False,
            "docstring 언급",
        ),
        (
            'if __name__ == "__main__":\n    force_utf8_output()\n    run()\n',
            True,
            "main() 없이 __main__ 블록에서 직접 부르는 형태",
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
