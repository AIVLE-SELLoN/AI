"""프롬프트 파일 로더.

⚠️ 문서 4장 core/ 목록에 없는 파일입니다. 4개 모듈이 전부 같은 로직을 필요로 해서
   추가했습니다. core/ 변경이므로 팀 합의 대상 — 반대 있으면 각 모듈로 흩어도 됩니다.

협업 규칙 2: 프롬프트는 코드에 하드코딩 금지, prompts/ 별도 파일.
버전 비교 실험을 해야 하므로 파일명의 v1/v2 를 인자로 받는다.

**프롬프트 파일에는 프롬프트만 넣는다.** 입출력 계약·후처리·미결사항 같은 설계 메모는
service.py docstring 으로. load_prompt 는 파일 전체를 그대로 반환하므로, 메모를 같이
넣으면 그게 전부 LLM 에 전송된다. v1/v2 를 통째로 갈아끼우는 실험도 프롬프트만 들어
있어야 깔끔하다.

⚠️ **str.format() 을 쓰지 말 것.** 우리 프롬프트는 출력 형식·few-shot 예시로 JSON 을
가지고 있어서, format() 이 JSON 의 중괄호를 플레이스홀더로 오해하고 KeyError 로 터진다.
string.Template 과 $플레이스홀더를 쓴다:

    import json
    from string import Template
    from app.core.prompts import load_prompt

    template = Template(load_prompt("detection", "classify_cause_v1"))
    input_json = json.dumps({"cs_id": cs_id, "raw_text": text}, ensure_ascii=False)
    prompt = template.substitute(input_json=input_json)

원문 텍스트를 프롬프트에 끼워 넣을 때는 반드시 json.dumps 로 직렬화할 것.
따옴표·줄바꿈이 든 CS 원문을 그대로 넣으면 프롬프트의 JSON 이 깨진다.
substitute() 는 채우지 못한 $플레이스홀더가 있으면 KeyError 를 낸다 (오타를 조용히
넘기지 않으므로 safe_substitute() 보다 낫다).
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
