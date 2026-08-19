"""두 requirements 파일이 갈리지 않는지 본다.

🔴 **`requirements-mock-producer.txt` 는 `requirements.txt` 의 부분집합이고, 버전은 거기와
   같아야 한다** — 슬림 이미지 머리말이 스스로 그렇게 규정한다. 갈리면 *"로컬(전체 의존성)
   에서는 되는데 producer 이미지에서만 깨지는"* 클래스가 생기는데, 그건 목 파이프라인을
   돌리는 사람이 제일 늦게 알아채는 자리다.

⚠️ **이 가드는 계층 1(`test.yml`)에 있어야 한다.** `mock-producer.yml` 의 `paths:` 필터에는
   `requirements.txt` 가 없어서, 거기서만 `pydantic` 을 올리는 PR 은 그 워크플로가 아예 안
   돈다 — **드리프트를 만드는 바로 그 PR 에서 게이트가 안 생긴다.** pytest 는 항상 돈다.

🔴 **비교할 패키지 목록을 여기 적지 말 것.** 두 파일의 교집합으로 유도한다. 적으면 패키지가
   하나 늘 때 조용히 거짓이 된다 — `exit_codes.py` 가 진입점 목록을 안 적는 것과 같은 사유다.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FULL = ROOT / "requirements.txt"
SLIM = ROOT / "requirements-mock-producer.txt"


def _pins(path: Path) -> dict[str, str]:
    """`이름 -> 원문 한 줄`. extras 도 값에 그대로 남긴다 — `psycopg[binary]` 에서 extras 만
    빠지는 변경도 드리프트라, 이름만 맞추고 넘어가면 못 잡는다."""
    pins: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        name = line.split("==")[0].split("[")[0].strip().lower().replace("_", "-")
        pins[name] = line
    return pins


def test_shared_pins_do_not_drift():
    """🔴 두 파일에 다 있는 패키지는 **줄이 글자 그대로 같아야** 한다(extras 포함)."""
    full, slim = _pins(FULL), _pins(SLIM)
    shared = sorted(set(full) & set(slim))
    assert shared, "교집합이 비었다 — 파싱이 깨졌거나 슬림 파일이 통째로 갈렸다"

    drifted = {name: (full[name], slim[name]) for name in shared if full[name] != slim[name]}
    assert not drifted, (
        "requirements.txt 와 requirements-mock-producer.txt 의 핀이 갈렸습니다: "
        + ", ".join(f"{n}: 전체={f!r} 슬림={s!r}" for n, (f, s) in drifted.items())
    )


def test_slim_requirements_stay_a_subset():
    """⚠️ 슬림 파일에만 있는 패키지는 없어야 한다.

    슬림 이미지가 전체 이미지에 없는 것을 깔고 있으면, 로컬·테스트·AI 노드 어디에서도
    안 돌아본 코드 경로가 producer 에만 생긴다. 방향이 반대(전체에만 있는 것)인 건
    정상이다 — 그게 이 파일을 나눈 이유다.
    """
    only_in_slim = sorted(set(_pins(SLIM)) - set(_pins(FULL)))
    assert not only_in_slim, (
        "슬림 파일에만 있는 패키지: " + ", ".join(only_in_slim)
        + " — requirements.txt 에도 같은 핀으로 넣거나, 슬림에서 빼야 합니다"
    )
