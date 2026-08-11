"""같은 seed 로 돌리면 **어느 프로세스에서 돌려도** 같은 코퍼스가 나와야 한다.

`build_rows_for_window_group` 의 `group_aspects` 가 `set` 이던 적이 있다. 그 순회 순서가
`reserved_neg` 의 dict 키 순서로 넘어가고, 그게 "하루치 부정 슬롯을 어느 aspect 가 먼저
가져가나"를 정한다. 파이썬 `str` 해시는 `PYTHONHASHSEED` 를 안 박으면 프로세스마다
무작위라, **같은 코드·같은 seed 로도 실행마다 색상/파손 순서가 뒤집혔다.**

조용히 갈리는 게 핵심이다:
    - aspect 별 부정 건수(`cur_neg`/`past_neg`)는 순서와 무관하게 그대로 → `verify_counts`
      246 슬롯 PASS
    - 행수·id·채널·날짜·`true_sentiment` 도 그대로 → 행수 검산 통과
    - 갈리는 건 **어느 id 에 어느 문장이 붙느냐** 뿐 → `data_fingerprint` 만 달라진다

실측(2026-08-11): 팀원과 `1fb05ed9` vs `07276bc5` 로 갈렸고, 리뷰 산출물은 바이트 동일한데
CS 만 달랐다. 다중-aspect 그룹 3개가 전부 `source=cs` 라서다.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

# 해시 시드를 바꿔가며 같은 결과가 나오는지 본다. set 이면 여기서 갈린다.
# ⚠️ 몇 개로는 부족하다. 항목이 2개뿐이라 시드가 달라도 같은 순서가 자주 나온다
#    (실측: 1~10 중 파손 먼저가 3개, 색상 먼저가 7개). 아래 test_the_broken_form_really_is_
#    unstable 이 이 범위가 충분한지 매번 확인한다.
SEEDS = tuple(str(i) for i in range(1, 11))


def _order_under(hashseed: str, expr: str) -> str:
    """해시 시드를 박은 **별도 프로세스**에서 순회 순서를 찍는다.

    같은 프로세스 안에서는 해시 시드가 이미 정해져 있어 재현이 안 된다.
    """
    code = f"rows=[{{'aspect':'색상'}},{{'aspect':'파손'}}]; print(','.join({expr}))"
    out = subprocess.run(
        [sys.executable, "-c", code],
        env={"PYTHONHASHSEED": hashseed, "PATH": "/usr/bin:/bin"},
        capture_output=True, text=True, check=True,
    )
    return out.stdout.strip()


FIXED = "dict.fromkeys(r['aspect'] for r in rows)"
BROKEN = "{r['aspect'] for r in rows}"


def test_group_aspects_is_not_a_set():
    """`set` 이면 순회 순서가 프로세스마다 달라진다 — 소스에서 직접 막는다."""
    src = (ROOT / "scripts" / "generate_cs_review_data.py").read_text(encoding="utf-8")
    assert 'group_aspects = {r["aspect"] for r in rows}' not in src, (
        "group_aspects 가 set 이면 하루치 부정 슬롯의 배분 순서가 실행마다 뒤집힌다"
    )
    assert "group_aspects = dict.fromkeys(" in src, "config 행 순서로 고정돼야 한다"


def test_order_is_stable_across_hash_seeds():
    """해시 시드를 바꿔도 config 행 순서(색상 먼저)가 유지된다."""
    orders = {_order_under(s, FIXED) for s in SEEDS}
    assert orders == {"색상,파손"}, (
        f"해시 시드에 따라 순서가 갈린다: {orders} — 코퍼스가 실행마다 달라진다"
    )


def test_the_broken_form_really_is_unstable():
    """이 테스트가 헛돌지 않는지 확인한다 — set 은 실제로 시드에 따라 갈려야 한다.

    이게 없으면 위 테스트가 "원래 안 갈리는 걸 안 갈린다고 확인" 하는 것일 수 있다.
    """
    orders = {_order_under(s, BROKEN) for s in SEEDS}
    assert len(orders) > 1, (
        f"set 이 모든 시드에서 같은 순서({orders})라면 이 파일의 다른 테스트는 의미가 없다"
        " — 재현 조건을 다시 봐야 한다"
    )


def test_membership_still_works():
    """`in` 검사가 그대로 동작해야 한다 (dict 도 O(1) 이다)."""
    import generate_cs_review_data as gen  # noqa: F401  임포트 가능성만 확인

    rows = [{"aspect": "색상"}, {"aspect": "파손"}, {"aspect": "색상"}]
    ga = dict.fromkeys(r["aspect"] for r in rows)
    assert "색상" in ga and "파손" in ga
    assert list(ga) == ["색상", "파손"], "중복이 제거되고 첫 등장 순서가 유지돼야 한다"
