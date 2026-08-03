"""담당: 현진 — 분류 테스트.

성격: "코드가 안 깨졌나" 검증(자동·무료) — tests/eval 성격 구분(README) 원칙에 따라
실제 LLM 호출 없이 app.core.llm_client를 가짜(mock)로 대체해서 로직만 검증한다.

🔴 확인 필요: 이 파일의 기존 docstring엔 "완료 기준 = fixture 100건 분류 정확도
측정치 첨부"라고 돼 있었으나, tests/fixtures/README.md의 설계 원칙(정답 없음·몇 건)과
상충함을 발견함(팀 확인 대기 중). "정확도 측정"은 LLM 실제 호출·golden 정답이 필요해
eval/의 역할(README: eval="성능이 얼마나 나오나", 수동·과금)에 해당하는 것으로 보여,
이 파일에서는 다루지 않음 — 프롬프트1·2의 실제 정확도는 42건 파일럿(브라우저 Artifact)
및 71603 하이브리드 1,000건 평가(eval/) 결과로 별도 보고함.

fixture: tests/fixtures/raw_cs_reviews.json (협업 규칙 4, 정답 없는 계약 예시 10건).
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from app.classification.service import ClassifyRequestItem, _classify_one, _parse_llm_response, explode_to_rows
from app.core.exceptions import LlmParseError
from app.core.schemas import AspectSentiment, Aspect, Channel, ClassifiedItem, Sentiment, Source
from app.main import app

client = TestClient(app)

FIXTURES_PATH = Path(__file__).parent / "fixtures" / "raw_cs_reviews.json"


@pytest.fixture
def raw_cs_reviews() -> list[dict]:
    """계약 예시 10건(정답 없음) 로딩."""
    return json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))


def _make_item(gid: str = "P001") -> ClassifiedItem:
    return ClassifiedItem(
        item_id="INQ-000001",
        source=Source.CS,
        channel=Channel.COUPANG,
        product_group_id=gid,
        raw_text="색상도 다르고 사이즈도 안 맞아요",
        aspects=[
            AspectSentiment(aspect=Aspect.COLOR, sentiment=Sentiment.NEGATIVE),
            AspectSentiment(aspect=Aspect.SIZE, sentiment=Sentiment.NEGATIVE),
        ],
        created_at=datetime(2026, 5, 1, 10, 0, 0),
    )


# ── 1. fixture 자체가 스키마(ClassifyRequestItem)와 맞는지 ──────────────────────


def test_fixture_matches_classify_request_item_shape(raw_cs_reviews):
    """계약 예시 10건이 전부 ClassifyRequestItem으로 파싱되는지(스키마 정합성)."""
    items = [ClassifyRequestItem(**row) for row in raw_cs_reviews]
    assert len(items) == 10
    assert sum(1 for i in items if i.source == Source.CS) == 6
    assert sum(1 for i in items if i.source == Source.REVIEW) == 4


# ── 2. explode_to_rows — 순수 로직, LLM 불필요 ──────────────────────────────


def test_explode_to_rows_splits_multi_aspect_into_multiple_rows():
    """§2 규약: aspect 여러 개 → 행도 그만큼 분리."""
    item = _make_item()
    rows = explode_to_rows(item)
    assert len(rows) == 2
    assert {r["aspect"] for r in rows} == {"색상", "사이즈"}


def test_explode_to_rows_cs_mixed_signal_always_none():
    """CS는 mixed_signal이 항상 None으로 나가야 함(스키마 규칙)."""
    item = _make_item()
    rows = explode_to_rows(item)
    assert all(r["mixed_signal"] is None for r in rows)


def test_explode_to_rows_single_aspect_item():
    item = ClassifiedItem(
        item_id="INQ-000002", source=Source.CS, channel=Channel.NAVER,
        product_group_id="P004", raw_text="사이즈가 너무 커요",
        aspects=[AspectSentiment(aspect=Aspect.SIZE, sentiment=Sentiment.NEGATIVE)],
        created_at=datetime(2026, 5, 1, 10, 0, 0),
    )
    rows = explode_to_rows(item)
    assert len(rows) == 1


# ── 3. _parse_llm_response — LLM 응답 파싱, 실제 호출 없이 가짜 JSON으로 검증 ──


def test_parse_llm_response_cs_valid():
    data = {"aspects": [{"aspect": "색상", "sentiment": -1}]}
    result = _parse_llm_response(data, Source.CS, trace_key="test")
    assert len(result) == 1
    assert result[0].aspect == Aspect.COLOR
    assert result[0].sentiment == Sentiment.NEGATIVE
    assert result[0].mixed_signal is None  # CS는 강제로 None


def test_parse_llm_response_review_keeps_mixed_signal():
    data = {"aspects": [{"aspect": "사이즈", "sentiment": 1, "mixed_signal": True}]}
    result = _parse_llm_response(data, Source.REVIEW, trace_key="test")
    assert result[0].mixed_signal is True


def test_parse_llm_response_missing_aspects_key_raises():
    with pytest.raises(LlmParseError):
        _parse_llm_response({}, Source.CS, trace_key="test")


def test_parse_llm_response_invalid_aspect_value_raises():
    """LLM이 존재하지 않는 aspect를 환각(hallucination)으로 뱉으면 LlmParseError로 통일."""
    data = {"aspects": [{"aspect": "배송속도", "sentiment": -1}]}  # 6종 밖의 값
    with pytest.raises(LlmParseError):
        _parse_llm_response(data, Source.CS, trace_key="test")


def test_parse_llm_response_invalid_sentiment_value_raises():
    data = {"aspects": [{"aspect": "색상", "sentiment": 5}]}  # -1/0/1 밖의 값
    with pytest.raises(LlmParseError):
        _parse_llm_response(data, Source.CS, trace_key="test")


# ── 4. _classify_one — LLM을 가짜로 대체해서 프롬프트1/2 분기 검증 ────────────


@pytest.mark.asyncio
async def test_classify_one_cs_uses_prompt1_and_no_mixed_signal():
    """source=cs면 프롬프트1이 쓰이고, 결과 mixed_signal이 None인지."""
    fake_client = AsyncMock()
    fake_client.complete_json.return_value = {"aspects": [{"aspect": "색상", "sentiment": -1}]}

    item = ClassifyRequestItem(
        item_id="INQ-000001", source=Source.CS, channel=Channel.COUPANG,
        product_group_id="P001", raw_text="색상이 달라요",
        created_at=datetime(2026, 5, 1, 10, 0, 0),
    )

    with patch("app.classification.service.get_llm_client", return_value=fake_client):
        result = await _classify_one(item)

    assert result.aspects[0].mixed_signal is None
    # trace_key에 item_id가 포함됐는지(컨벤션 4장: 로그 추적 키)
    _, kwargs = fake_client.complete_json.call_args
    assert "INQ-000001" in kwargs["trace_key"]


@pytest.mark.asyncio
async def test_classify_one_review_uses_prompt2_and_keeps_mixed_signal():
    """source=review면 프롬프트2가 쓰이고, mixed_signal이 그대로 반영되는지."""
    fake_client = AsyncMock()
    fake_client.complete_json.return_value = {
        "aspects": [{"aspect": "사이즈", "sentiment": -1, "mixed_signal": True}]
    }

    item = ClassifyRequestItem(
        item_id="RVW-000001", source=Source.REVIEW, channel=Channel.NAVER,
        product_group_id="P004", raw_text="사이즈가 이상해요",
        created_at=datetime(2026, 5, 1, 10, 0, 0),
    )

    with patch("app.classification.service.get_llm_client", return_value=fake_client):
        result = await _classify_one(item)

    assert result.aspects[0].mixed_signal is True


@pytest.mark.asyncio
async def test_classify_one_review_with_damage_aspect_fails_schema_validation():
    """LLM이 리뷰인데 파손을 뱉는 극단적 오류 상황 — ClassifiedItem 생성 자체가 막혀야 함."""
    fake_client = AsyncMock()
    fake_client.complete_json.return_value = {"aspects": [{"aspect": "파손", "sentiment": -1}]}

    item = ClassifyRequestItem(
        item_id="RVW-000002", source=Source.REVIEW, channel=Channel.ZIGZAG,
        product_group_id="P007", raw_text="테스트",
        created_at=datetime(2026, 5, 1, 10, 0, 0),
    )

    with patch("app.classification.service.get_llm_client", return_value=fake_client):
        with pytest.raises(Exception):  # Pydantic ValidationError
            await _classify_one(item)


# ── 5. 라우터 — LLM을 가짜로 대체해서 엔드포인트 계약만 검증 ──────────────────


def test_classify_endpoint_empty_request_returns_422():
    """items 필드 없이 보내면 검증 실패(501 아님 — 구현 끝났다는 증거)."""
    response = client.post("/api/v1/classify", json={})
    assert response.status_code == 422


def test_classify_endpoint_success_with_mocked_llm(raw_cs_reviews):
    """fixture 1건으로 엔드포인트 전체 흐름(요청 파싱 → 분류 → 응답)을 LLM 없이 검증."""
    fake_client = AsyncMock()
    fake_client.complete_json.return_value = {"aspects": [{"aspect": "색상", "sentiment": -1}]}

    payload = {"items": [raw_cs_reviews[0]]}  # cs 아이템 1건

    with patch("app.classification.service.get_llm_client", return_value=fake_client):
        response = client.post("/api/v1/classify", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert len(body["results"]) == 1
    assert body["results"][0]["item_id"] == raw_cs_reviews[0]["item_id"]

# ── 프롬프트 플레이스홀더 렌더링 ─────────────────────────────────
# 배경: classify_sentiment_v4가 $review_text 대신 {review_text}로 작성돼,
# string.Template.substitute()가 조용히 무시하는 바람에 리뷰 원문이 LLM에
# 전달되지 않는 버그가 있었다(PR 리뷰에서 발견). substitute()는 안 쓰인
# 키워드 인자를 예외 없이 무시하므로 런타임에도 안 터진다 — 그래서 테스트로 막는다.


@pytest.mark.parametrize(
    "prompt_path",
    sorted((Path(__file__).parent.parent / "app" / "classification" / "prompts").glob("*.md")),
    ids=lambda p: p.stem,
)
def test_prompt_placeholder_substitutes(prompt_path: Path):
    """모든 분류 프롬프트가 $placeholder 형식이라 실제로 원문이 렌더링되는지 검증.

    파일을 glob으로 수집하므로 새 버전(v6...)을 추가해도 자동으로 커버된다.
    """
    from string import Template

    from app.classification.service import load_llm_prompt

    key = "cs_text" if "aspect" in prompt_path.stem else "review_text"
    rendered = Template(load_llm_prompt("classification", prompt_path.stem)).substitute(
        **{key: "__SENTINEL__"}
    )

    assert "__SENTINEL__" in rendered, (
        f"{prompt_path.name}: 원문이 렌더링되지 않음. "
        f"플레이스홀더가 ${key} 형식인지 확인할 것."
    )
    assert f"{{{key}}}" not in rendered, (
        f"{prompt_path.name}: 치환 안 된 중괄호 플레이스홀더가 남아있음."
    )