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

import os
import random
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


def test_default_cause_prompt_exists():
    """기본값이 존재하는 파일을 가리켜야 한다 — cwd 와 무관하게."""
    import generate_cs_review_data as gen

    assert gen.DEFAULT_CAUSE_PROMPT.exists(), (
        f"cause 프롬프트 기본값이 없는 파일을 가리킨다: {gen.DEFAULT_CAUSE_PROMPT}"
    )
    assert gen.DEFAULT_CAUSE_PROMPT.is_absolute(), "cwd 에 따라 달라지면 안 된다"


def test_default_is_resolved_from_script_not_cwd(monkeypatch):
    """엉뚱한 디렉토리에서 import 해도 같은 파일을 가리킨다."""
    import generate_cs_review_data as gen

    before = gen.DEFAULT_CAUSE_PROMPT
    monkeypatch.chdir(ROOT / "tests")
    assert gen.DEFAULT_CAUSE_PROMPT == before
    assert gen.DEFAULT_CAUSE_PROMPT.exists()


def test_missing_prompt_is_fatal_not_a_warning():
    """프롬프트가 없으면 **멈춰야** 한다. 경고만 내고 플레이스홀더로 나가면 안 된다.

    실제 실행으로 확인한다 — 인자 파싱부터 종료까지가 대상이라 함수 단위로는 못 잡는다.
    """
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    proc = subprocess.run(
        [sys.executable, "scripts/generate_cs_review_data.py",
         "--cause-prompt", "없는파일_xyz.md", "--anchor-date", "2026-08-28"],
        cwd=ROOT, env=env, capture_output=True, text=True, encoding="utf-8",
        timeout=120, check=False,
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode != 0, f"프롬프트가 없는데 종료코드가 0 이다:\n{out}"
    assert "cause 프롬프트" in out, f"왜 멈췄는지 알려줘야 한다:\n{out[:500]}"
    assert "PLACEHOLDER" not in proc.stdout or "채점" in out, (
        "플레이스홀더로 조용히 진행하면 안 된다"
    )


def test_llm_init_failure_stops_on_cache_miss(monkeypatch):
    """정상 실행은 LLM 초기화 실패를 플레이스홀더로 숨기면 안 된다."""
    import generate_cs_review_data as gen

    def fail_client():
        raise RuntimeError("API key 없음")

    monkeypatch.setattr(gen, "get_llm_client", fail_client)
    text_gen = gen.TextGenerator(
        templates_path=None,
        rng=random.Random(11),
        cause_prompt_path=str(gen.DEFAULT_CAUSE_PROMPT),
        cause_cache_path=str(ROOT / "tests" / "_nonexistent_cause_cache.json"),
        use_llm=True,
    )

    with pytest.raises(RuntimeError, match="캐시 미스"):
        text_gen.generate_cause_batch("SC-TEST", "색상", {"사진_색감_오차": 1})
    assert text_gen.cause_cache == {}


class _AlwaysEmptyClient:
    def __init__(self):
        self.calls = 0

    async def complete_json(self, prompt, *, trace_key="-"):
        self.calls += 1
        return {"texts": []}


def test_llm_shortfall_does_not_cache_placeholders(monkeypatch):
    """4라운드가 부족해도 정답 노출 플레이스홀더와 부분 캐시를 남기지 않는다."""
    import generate_cs_review_data as gen

    client = _AlwaysEmptyClient()
    monkeypatch.setattr(gen, "get_llm_client", lambda: client)
    text_gen = gen.TextGenerator(
        templates_path=None,
        rng=random.Random(11),
        cause_prompt_path=str(gen.DEFAULT_CAUSE_PROMPT),
        cause_cache_path=str(ROOT / "tests" / "_nonexistent_cause_cache.json"),
        use_llm=True,
    )

    with pytest.raises(RuntimeError, match="라운드 시도 후"):
        text_gen.generate_cause_batch("SC-TEST", "색상", {"사진_색감_오차": 1})
    assert client.calls == 4
    assert text_gen.cause_cache == {}


def test_existing_placeholder_cache_is_rejected(monkeypatch):
    """예전에 오염된 캐시도 정상 실행에서 재사용하면 안 된다."""
    import generate_cs_review_data as gen

    monkeypatch.setattr(gen, "get_llm_client", lambda: _AlwaysEmptyClient())
    text_gen = gen.TextGenerator(
        templates_path=None,
        rng=random.Random(11),
        cause_prompt_path=str(gen.DEFAULT_CAUSE_PROMPT),
        cause_cache_path=str(ROOT / "tests" / "_nonexistent_cause_cache.json"),
        use_llm=True,
    )
    text_gen.cause_cache["SC-TEST:색상"] = [{
        "cause": "사진_색감_오차",
        "text": "[PLACEHOLDER:cause:색상:사진_색감_오차]",
    }]

    with pytest.raises(RuntimeError, match="플레이스홀더"):
        text_gen.generate_cause_batch("SC-TEST", "색상", {"사진_색감_오차": 1})
