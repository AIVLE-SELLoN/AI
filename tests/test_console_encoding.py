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
"""

from __future__ import annotations

import io
import re
import tokenize
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

COMPOSE = ROOT / "docker-compose.yml"

MAIN_BLOCK = re.compile(r'__name__\s*==\s*"__main__"')
CALL = re.compile(r"\bforce_utf8_output\s*\(")

# docker-compose 의 `command:` 에 적힌 파이썬 대상. `python -u scripts/x.py` ·
# `python eval/y.py` 를 잡는다.
#
# ⚠️ **`yaml` 로 파싱하지 않는다.** PyYAML 이 `requirements.txt` 에 없다(전이 의존으로
#    깔려 있을 뿐이라 남의 환경에서는 없을 수 있다). 테스트가 선언되지 않은 의존성에
#    기대면 "내 로컬에서만 통과"가 된다 — boto3 가 없어 리포팅 테스트 9건이 실패했던
#    것과 같은 계열이다(2026-08-07).
COMPOSE_TARGET = re.compile(r"(?:^|\s)((?:app|scripts|eval)/[\w/]+\.py)")

YAML_COMMENT = re.compile(r"(?:^|\s)#.*$")
"""compose 의 주석 — **전체줄과 인라인을 모두** 지운다.

🔴 **주석을 빼지 않으면 오탐이 6건 난다** — 이 파일 주석이 사용법으로
`python scripts/classification_worker.py` · `setup_local_mq.py` · `app/core/mq.py` 를
적어 두고 있어서, 서비스가 아닌 것들이 "배포되는 진입점"으로 잡힌다. 실제로 처음에
그렇게 걸렸다(2026-08-14). 안내 문구를 고쳤다고 가드가 요구사항을 늘리면 안 된다.

⚠️ 처음엔 `^\\s*#` 로 **전체줄만** 지웠는데 `app/core/mq.py` 하나가 계속 남았다 —
   포트 매핑 뒤 **인라인** 주석(`- "5672:5672"  # AMQP … app/core/mq.py …`)이었다.
   두 형태를 다 지워야 한다.
"""


def _deployed_entrypoints() -> list[Path]:
    """**배포되는** 진입점 집합. 손으로 적은 목록이 아니라 유도한다.

    두 곳에서 온다:
      - `app/**` 의 `__main__` 블록 — 우리가 배포하는 패키지 코드
      - `docker-compose.yml` 의 `command:` 가 가리키는 파이썬 파일 — 컨테이너로 뜬다

    ⚠️ **범위를 왜 여기서 끊었나.** `scripts/`·`eval/` 전체로 넓히면 대상이 **28개**가
       된다(2026-08-14 실측). 전부 두 줄짜리 기계적 변경이지만 대부분 남의 파일이라
       한 PR 에 몰면 리뷰가 불가능하다. 넓히려면 이 함수만 고치면 되고, 그때는
       파일 소유자별로 나눠 올릴 것.

    ⚠️ `app/main.py` 는 대상이 아니다 — `__main__` 블록이 없고 Dockerfile 이
       `uvicorn app.main:app` 으로 띄운다. 스트림 소유자가 uvicorn 이라 우리가 끼어들
       자리가 아니다.
    """
    found: set[Path] = set()

    for path in (ROOT / "app").rglob("*.py"):
        if MAIN_BLOCK.search(path.read_text(encoding="utf-8")):
            found.add(path)

    compose = "\n".join(
        YAML_COMMENT.sub("", line)
        for line in COMPOSE.read_text(encoding="utf-8").splitlines()
    )
    for rel in COMPOSE_TARGET.findall(compose):
        target = ROOT / rel
        if target.exists():
            found.add(target)

    return sorted(found)


def _calls_in_code(path: Path) -> bool:
    """그 파일이 **코드로** `force_utf8_output()` 을 부르는지. 주석·문자열은 뺀다.

    ⚠️ 주석을 빼는 이유는 `test_timestamp_timezone._offending_lines` 와 같다 —
       이 함수를 **설명하는** 주석이 여러 파일에 있어서, 그것까지 세면 설명만 적어 두고
       실제로는 안 부르는 파일이 통과한다.
    """
    source = path.read_text(encoding="utf-8")
    lines = source.splitlines()
    blanked = list(lines)

    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (tokenize.TokenError, SyntaxError):  # pragma: no cover - 파싱 불가 파일
        return False

    for tok in tokens:
        if tok.type not in (tokenize.COMMENT, tokenize.STRING):
            continue
        (r1, c1), (r2, c2) = tok.start, tok.end
        for row in range(r1, r2 + 1):
            i = row - 1
            if i >= len(blanked):
                continue
            start = c1 if row == r1 else 0
            end = c2 if row == r2 else len(blanked[i])
            blanked[i] = blanked[i][:start] + " " * (end - start) + blanked[i][end:]

    return any(CALL.search(code) for code in blanked)


def test_deployed_entrypoints_switch_console_encoding() -> None:
    """🔴 배포되는 진입점은 전부 `force_utf8_output()` 을 불러야 한다.

    걸렸다면 `main()` 첫 줄에 넣을 것:

        from app.core.console import force_utf8_output
        ...
        force_utf8_output()

    ⚠️ **import 를 모듈 최상단에 둘 것.** 함수 안에서 import 하면 배선 테스트가
       몽키패치를 못 걸어 호출 여부를 고정할 수 없다.
    """
    missing = [
        p.relative_to(ROOT).as_posix()
        for p in _deployed_entrypoints()
        if not _calls_in_code(p)
    ]

    assert not missing, (
        "배포되는 진입점인데 force_utf8_output() 을 안 부릅니다:\n  "
        + "\n  ".join(missing)
    )


def test_the_derivation_actually_finds_the_known_entrypoints() -> None:
    """가드가 **무엇을 보고 있는지** 고정한다.

    이게 없으면 정규식이 조용히 아무것도 안 잡게 바뀌어도 위 테스트가 통과한다 —
    "테스트가 있는데 안 잡는" 상태가 제일 나쁘다. `test_timestamp_timezone` 의
    `test_the_guard_actually_matches_the_forbidden_forms` 와 같은 역할이다.
    """
    found = {p.relative_to(ROOT).as_posix() for p in _deployed_entrypoints()}

    # app/ 의 __main__ 진입점
    assert "app/batch/daily.py" in found
    assert "app/consumer.py" in found
    # docker-compose 가 컨테이너로 띄우는 것
    assert "scripts/mock_producer.py" in found, "compose command 파싱이 죽었습니다"
    assert "eval/run_detection_eval.py" in found, "compose command 파싱이 죽었습니다"

    # uvicorn 이 스트림을 소유하므로 대상이 아니다(위 docstring 참고).
    assert "app/main.py" not in found

    # 🔴 compose **주석**에만 있는 것은 서비스가 아니다. 이 셋은 사용법 안내로 적혀
    #    있을 뿐이라, 여기 들어오면 가드가 안내 문구를 요구사항으로 착각한 것이다.
    for mentioned_only_in_comments in (
        "scripts/classification_worker.py",
        "scripts/setup_local_mq.py",
        "app/core/mq.py",
    ):
        assert mentioned_only_in_comments not in found, (
            f"{mentioned_only_in_comments} 은 compose 주석에만 있습니다 — "
            "주석 제외가 깨졌습니다"
        )


def test_the_call_detector_ignores_comments_and_strings(tmp_path: Path) -> None:
    """탐지기가 **코드**만 센다 — 주석·문자열에 이름만 적힌 파일은 통과하면 안 된다."""
    only_mentions = tmp_path / "mentions.py"
    only_mentions.write_text(
        '"""force_utf8_output() 을 부르는 게 좋다."""\n'
        "# force_utf8_output() 을 여기서 부른다\n"
        'HELP = "force_utf8_output()"\n',
        encoding="utf-8",
    )
    assert not _calls_in_code(only_mentions)

    real = tmp_path / "real.py"
    real.write_text(
        "from app.core.console import force_utf8_output\n"
        "def main():\n"
        "    force_utf8_output()\n",
        encoding="utf-8",
    )
    assert _calls_in_code(real)


@pytest.mark.parametrize("char", ["—", "⚠", "ℹ", "❌"])
def test_the_characters_this_guard_exists_for_are_really_absent_from_cp949(
    char: str,
) -> None:
    """이 가드의 전제 — 그 문자들이 정말 cp949 에 없다.

    전제가 조용히 바뀌면(예: 누가 `—` 를 ASCII `-` 로 일괄 치환) 가드는 남아 있는데
    막는 대상이 없어진다. 그 경우 이 테스트가 먼저 실패해서 알려준다.

    ⚠️ `←`(U+2190) 는 **cp949 에 있다** — 그래서 같은 줄에서 한 문자만 골라 터지는
       것처럼 보인다(`app/core/console.py`).
    """
    with pytest.raises(UnicodeEncodeError):
        char.encode("cp949")

    "←".encode("cp949")  # 대조군: 이건 통과한다
