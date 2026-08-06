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


class MqDisabledError(AiServiceError):
    """MQ_ENABLED=false — 발행하지 않았다.

    no-op 이 아니라 예외인 이유: 호출부가 예외 없음을 발행 성공으로 보고 그 알림을
    prior_alerts 캐시에 넣는다. 조용히 넘기면 안 나간 알림이 7일간 억제된다.
    """


class MqPublishError(AiServiceError):
    """MQ 접속·발행 실패. 재시도 대상(다음 배치가 다시 시도한다)."""
