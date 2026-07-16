"""커스텀 예외. 네이밍 규칙: `~Error` 접미사."""


class AiServiceError(Exception):
    """이 서비스의 모든 커스텀 예외의 최상위. except 로 한 번에 잡을 때 사용."""


class LlmParseError(AiServiceError):
    """LLM 응답을 기대한 형식(JSON 등)으로 파싱하지 못함. 재시도 대상."""


class LlmCallError(AiServiceError):
    """LLM API 호출 자체가 실패 (타임아웃·rate limit·인증 등)."""


class EvidenceNotFoundError(AiServiceError):
    """인용 검증 실패 — 생성된 근거가 원문에 없음. '근거 없음' 경로로 분기."""


class VectorDbError(AiServiceError):
    """벡터DB 조회/적재 실패."""
