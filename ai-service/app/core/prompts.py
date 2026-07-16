"""프롬프트 파일 로더.

⚠️ 문서 4장 core/ 목록에 없는 파일입니다. 4개 모듈이 전부 같은 로직을 필요로 해서
   추가했습니다. core/ 변경이므로 팀 합의 대상 — 반대 있으면 각 모듈로 흩어도 됩니다.

협업 규칙 2: 프롬프트는 코드에 하드코딩 금지, prompts/ 별도 파일.
버전 비교 실험을 해야 하므로 파일명의 v1/v2 를 인자로 받는다.

사용 예:
    from app.core.prompts import load_prompt

    template = load_prompt("classification", "classify_aspect_v1")
    prompt = template.format(review_text=text)
"""

from functools import lru_cache
from pathlib import Path

_APP_DIR = Path(__file__).resolve().parent.parent


@lru_cache
def load_prompt(module: str, name: str) -> str:
    """`app/{module}/prompts/{name}.md` 를 읽어 문자열로 반환.

    Args:
        module: 모듈 폴더명 (`classification`, `detection`, ...)
        name: 확장자 뺀 프롬프트 파일명 (`classify_aspect_v1`)

    Raises:
        FileNotFoundError: 파일이 없을 때. 오타를 조용히 넘기지 않는다.
    """
    path = _APP_DIR / module / "prompts" / f"{name}.md"
    if not path.is_file():
        raise FileNotFoundError(f"프롬프트 파일 없음: {path}")
    return path.read_text(encoding="utf-8")
