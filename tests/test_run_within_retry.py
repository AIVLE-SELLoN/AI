"""회차 **안에서** 무응답을 재시도하는지 고정한다.

회차는 LLM 흔들림을 평균 내려고 나눈 것이라 서로 독립이어야 한다. 무응답을 다음 회차
실행으로 미루면 앞 회차가 뒤 실행에서 채워져 커버리지가 회차마다 달라지고, ± 가
흔들림이 아니라 커버리지 비대칭을 재게 된다.

실측(2026-08-10): `--runs 1` 에서 회차 1 이 60.0%(7슬롯 제외)였는데 `--runs 2` 를 돌리자
같은 회차가 84.0% 가 됐다. 모델이 아니라 채점 대상이 달라진 것이다.

⚠️ **두 경로 다 테스트한다.** 배치(CS)만 고치고 per_item(리뷰 420건)을 빼먹었던 적이
   있다 — 리뷰는 프롬프트2 에 배치 조립기가 없어 항상 per_item 이다.
"""

import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "eval"))

import run_pipeline_eval as rpe


def _docs(n: int) -> list[dict]:
    return [
        {
            "id": f"INQ-{i:04d}", "text": f"문의 {i}", "source": "cs",
            "product": "P001", "channel": "COUPANG",
            "created_at": datetime(2026, 8, 28, 12, 0, tzinfo=UTC),
        }
        for i in range(n)
    ]


def _run(coro):
    return asyncio.run(coro)


# ── 배치 경로 ────────────────────────────────────────────────────


def test_batch_retries_within_run(monkeypatch):
    """첫 호출에서 빠진 id 를 **같은 회차 안에서** 다시 불러 채운다."""
    calls: list[set] = []

    async def fake(rows, chunk_size, concurrency):
        ids = {r["inquiry_id"] for r in rows}
        calls.append(ids)
        if len(calls) == 1:            # 1차: 마지막 하나를 응답에서 빠뜨린다
            miss = max(ids)
            return {i: [] for i in ids - {miss}}, [miss]
        return {i: [] for i in ids}, []

    monkeypatch.setattr(rpe, "run_batch_chunks", fake)
    cache: dict = {}
    _done, failed = _run(rpe._run_batch(_docs(3), cache, lambda: None, concurrency=1))

    assert failed == 0, "회차 안에서 채웠으므로 무응답이 남으면 안 된다"
    assert len(cache) == 3
    assert len(calls) == 2, "재시도가 일어나야 한다"
    assert calls[1] == {"INQ-0002"}, "재시도는 빠진 건만 부른다"


def test_batch_gives_up_after_limit(monkeypatch):
    """재시도를 다 써도 안 오면 캐시에 넣지 않는다 — 커버리지 검사가 슬롯을 뺀다.

    호출 패스 수도 함께 고정한다 — 예산 상수를 바꿨을 때 실제 패스 수가 따라오는지를
    건다. 비용 근거("최대 100여 건")가 여기 걸려 있다.

    ⚠️ 예산은 **가드 두 겹**이 정확히 같은 지점에서 끊는다.
        `range(1, RETRY_WITHIN_RUN + 2)`        → attempt 1·2·3
        `if ... or attempt > RETRY_WITHIN_RUN`  → attempt 3 에서 break
    그래서 어느 한쪽만 느슨하게 해도(range 를 `+ 3` 으로, break 를 `+ 1` 로, break 를
    아예 제거) 패스 수는 3 그대로다. 실제로 4패스가 되려면 **둘 다** 건드려야 하고,
    그때 이 assert 가 문다. 한쪽만 바꿔보고 "테스트가 안 무네"로 읽지 말 것.
    """
    calls: list[set] = []

    async def always_drop(rows, chunk_size, concurrency):
        ids = {r["inquiry_id"] for r in rows}
        calls.append(ids)
        miss = max(ids)
        return {i: [] for i in ids - {miss}}, [miss]

    monkeypatch.setattr(rpe, "run_batch_chunks", always_drop)
    cache: dict = {}
    _done, failed = _run(rpe._run_batch(_docs(3), cache, lambda: None, concurrency=1))

    assert failed == 1
    assert "INQ-0002" not in cache, "무응답을 0 으로 세면 분자가 조용히 깎인다"
    assert len(calls) == 1 + rpe.RETRY_WITHIN_RUN, (
        f"1차 + 재시도 {rpe.RETRY_WITHIN_RUN}회 = {1 + rpe.RETRY_WITHIN_RUN} 패스여야 하는데"
        f" {len(calls)} 패스다 — 오프바이원이면 비용이 예상과 달라진다"
    )


# ── 건당 경로 (리뷰가 쓰는 길) ───────────────────────────────────


class _Err(Exception):
    pass


class _Res:
    """classify_aspect 반환 형태 최소 스텁 — aspects 만 본다."""

    class _A:
        def __init__(self):
            self.aspect = type("E", (), {"value": "색상"})()
            self.sentiment = -1

    def __init__(self):
        self.aspects = [self._A()]


def test_per_item_retries_within_run(monkeypatch):
    """건당 경로도 같은 회차에서 재시도한다. (리뷰 420건이 이 길을 쓴다)"""
    seen: list[list[str]] = []

    async def fake(items):
        ids = [i.item_id for i in items]
        seen.append(ids)
        if len(seen) == 1:                       # 1차: 마지막 건만 실패
            return [_Res() for _ in ids[:-1]] + [_Err("일시 실패")]
        return [_Res() for _ in ids]

    monkeypatch.setattr(rpe, "classify_aspect", fake)
    cache: dict = {}
    _done, failed = _run(rpe._run_per_item(_docs(3), cache, lambda: None, concurrency=1))

    assert failed == 0, "per_item 도 회차 안에서 채워야 한다"
    assert len(cache) == 3
    assert seen[1] == ["INQ-0002"], "재시도는 실패한 건만 부른다"


def test_per_item_gives_up_after_limit(monkeypatch):
    """건당 경로도 재시도 소진 후에는 캐시에 안 넣는다. 패스 수도 같이 고정한다."""
    seen: list[list[str]] = []

    async def always_fail_last(items):
        ids = [i.item_id for i in items]
        seen.append(ids)
        return [_Res() for _ in ids[:-1]] + [_Err("계속 실패")]

    monkeypatch.setattr(rpe, "classify_aspect", always_fail_last)
    cache: dict = {}
    _done, failed = _run(rpe._run_per_item(_docs(3), cache, lambda: None, concurrency=1))

    assert failed == 1
    assert "INQ-0002" not in cache
    assert len(seen) == 1 + rpe.RETRY_WITHIN_RUN, (
        f"1차 + 재시도 {rpe.RETRY_WITHIN_RUN}회여야 하는데 {len(seen)} 패스다"
    )


def test_retry_budget_is_bounded():
    """무한 재시도가 아니다 — 예산이 상수로 고정돼 있어야 비용이 예측된다."""
    assert rpe.RETRY_WITHIN_RUN >= 1
    assert rpe.RETRY_WITHIN_RUN <= 5, "예산이 크면 무응답 많은 날 비용이 튄다"
