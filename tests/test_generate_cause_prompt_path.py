"""cause 프롬프트 기본 경로가 **실제로 존재하는 파일**을 가리키는지 고정한다.

기본값이 `prompts/generate_cause_text_v3.md`(cwd 기준)였던 적이 있다. 실제 파일은
`scripts/prompts/` 에 있어서, 저장소 루트에서 생성기를 돌리면 못 찾았다. 그런데 못 찾아도
에러가 아니라 경고 한 줄이었고, cause 텍스트가 이렇게 나갔다:

    [PLACEHOLDER:cause:색상:사진*색감*오차]

**원인 라벨이 본문에 그대로 박힌다.** 이 코퍼스로 [6] 원인분류를 채점하면 모델이 문장을
읽는 게 아니라 답을 베끼는 것이라 채점이 성립하지 않는다.

캐시(`cause_text_cache.json`)가 이미 있는 사람은 프롬프트를 보러 가지 않아 이 갈림길을
안 밟는다. 그래서 **캐시 없는 사람만** 오염된 데이터를 만들게 되고, 행수 검산(96,524 /
31,639)은 양쪽 다 통과한다 — 조용히 갈린다.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


def test_default_cause_prompt_exists():
    """기본값이 존재하는 파일을 가리켜야 한다 — cwd 와 무관하게."""
    import generate_cs_review_data as gen

    assert gen.DEFAULT_CAUSE_PROMPT.exists(), (
        f"cause 프롬프트 기본값이 없는 파일을 가리킨다: {gen.DEFAULT_CAUSE_PROMPT}"
    )
    assert gen.DEFAULT_CAUSE_PROMPT.is_absolute(), "cwd 에 따라 달라지면 안 된다"


def test_default_is_resolved_from_script_not_cwd(tmp_path, monkeypatch):
    """엉뚱한 디렉토리에서 import 해도 같은 파일을 가리킨다."""
    import generate_cs_review_data as gen

    before = gen.DEFAULT_CAUSE_PROMPT
    monkeypatch.chdir(tmp_path)
    assert gen.DEFAULT_CAUSE_PROMPT == before
    assert gen.DEFAULT_CAUSE_PROMPT.exists()


def test_missing_prompt_is_fatal_not_a_warning():
    """프롬프트가 없으면 **멈춰야** 한다. 경고만 내고 플레이스홀더로 나가면 안 된다.

    실제 실행으로 확인한다 — 인자 파싱부터 종료까지가 대상이라 함수 단위로는 못 잡는다.
    """
    proc = subprocess.run(
        [sys.executable, "scripts/generate_cs_review_data.py",
         "--cause-prompt", "없는파일_xyz.md", "--anchor-date", "2026-08-28"],
        cwd=ROOT, capture_output=True, text=True, timeout=120, check=False,
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode != 0, f"프롬프트가 없는데 종료코드가 0 이다:\n{out}"
    assert "cause 프롬프트" in out, f"왜 멈췄는지 알려줘야 한다:\n{out[:500]}"
    assert "PLACEHOLDER" not in proc.stdout or "채점" in out, (
        "플레이스홀더로 조용히 진행하면 안 된다"
    )
