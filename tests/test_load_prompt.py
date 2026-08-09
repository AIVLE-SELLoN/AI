"""담당: 지인 — pipeline.load_prompt() 테스트.

프롬프트 md 의 머리말(개발자용)은 모델에게 가면 안 된다. 토큰도 토큰이지만,
변경 이력에 "예전엔 이렇게 하면 통과했다" 같은 과거 결함 설명이 들어가면
**모델에게 우회로를 알려주는 셈**이 된다.

LLM 호출 없음 — 파일 파싱만 본다.
"""

from app.recommendation import pipeline

_PROMPT_PATHS = (
    pipeline.COPY_DRAFT_PROMPT_PATH,
    pipeline.IMAGE_GUIDE_PROMPT_PATH,
    pipeline.ROUTING_PROMPT_PATH,
    pipeline.FALLBACK_GUIDE_PROMPT_PATH,
)


def test_strips_developer_header(tmp_path):
    path = tmp_path / "p.md"
    path.write_text(
        "# 제목\n구버전 삭제 금지.\n\n---\n\n## 지시\n본문입니다.\n",
        encoding="utf-8",
    )

    assert pipeline.load_prompt(path) == "## 지시\n본문입니다."


def test_keeps_whole_file_when_no_separator(tmp_path):
    """머리말 없이 쓴 프롬프트가 통째로 사라지면 안 된다 — 없으면 전체를 보낸다."""
    path = tmp_path / "p.md"
    path.write_text("## 지시\n본문뿐입니다.\n", encoding="utf-8")

    assert pipeline.load_prompt(path) == "## 지시\n본문뿐입니다."


def test_handles_crlf_checkout(tmp_path):
    """저장소가 CRLF 로 체크아웃되는 환경이 있다 — 줄바꿈 때문에 구분선을 놓치면 안 된다."""
    path = tmp_path / "p.md"
    path.write_bytes("# 제목\r\n\r\n---\r\n\r\n## 지시\r\n본문\r\n".encode())

    assert pipeline.load_prompt(path) == "## 지시\n본문"


def test_body_separator_does_not_truncate_body(tmp_path):
    """첫 구분선만 경계다 — 본문 안의 `---` 는 본문을 자르지 않는다."""
    path = tmp_path / "p.md"
    path.write_text(
        "# 제목\n\n---\n\n## 지시\n앞\n\n---\n\n## 출력\n뒤\n", encoding="utf-8"
    )

    body = pipeline.load_prompt(path)

    assert "앞" in body and "뒤" in body


def test_no_prompt_leaks_its_header_to_the_model():
    """실제 프롬프트 4종 — 머리말 고유 문구가 본문에 남아 있으면 안 된다."""
    for path in _PROMPT_PATHS:
        body = pipeline.load_prompt(path)
        assert "구버전 삭제 금지" not in body, f"{path.name} 머리말이 모델에게 간다"
        assert "## 지시" in body, f"{path.name} 본문이 통째로 잘렸다"


def test_all_prompts_declare_a_header_separator():
    """관례를 파일마다 지키는지 — 구분선이 없으면 머리말이 조용히 모델에게 간다."""
    for path in _PROMPT_PATHS:
        lines = path.read_text(encoding="utf-8").splitlines()
        assert any(
            line.strip() == pipeline.PROMPT_HEADER_SEPARATOR for line in lines
        ), f"{path.name} 에 `---` 구분선이 없다"
