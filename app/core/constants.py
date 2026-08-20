"""전역 상수.

매직넘버 금지. 정량 실험 때 바꿔가며 돌려야 하는 값들이라 전부 여기 모아둔다.
값을 바꾸면 실험 결과가 통째로 달라지므로 변경 전 팀 합의 필수.
"""

from datetime import timedelta, timezone

# --- 시간대 ---

KST = timezone(timedelta(hours=9))
"""날짜 경계의 기준 시간대. UTC 로 자르면 KST 오전 9시 이전 문의가 전날로 밀린다.

확정 문서의 `AT TIME ZONE 'Asia/Seoul'` 은 Postgres 문법이라 로컬 sqlite 에 없다 — 절단은
파이썬에서 한다(`daily._to_kst`). 탐지 입력 로더와 탐지 시각 기본값이 같은 경계를 써야 해서
core 에 둔다.
"""

# --- 윈도우 (detection) ---

CURRENT_WINDOW_DAYS = 7
"""현재 윈도우 길이(일). 일별 슬라이딩이라 매일 이 7일 구간으로 재탐지한다."""

PAST_WINDOW_DAYS = 28
"""과거 기준 윈도우 길이(일). 기준선은 표본이 커야 흔들리지 않아 길게 잡는다.

이 구간에 이미 알림이 나간 날짜는 제외한다(기준선 오염 방지).
"""

# --- 통계 검정 (detection) ---

ALPHA = 0.05
"""유의수준. 통계 표준값(관습). Fisher 단측 검정의 raw p 기준."""

MIN_SAMPLE_SIZE = 10
"""[관문①] 현재 윈도우 (상품,채널) 총문의(aspect 무관)가 이 미만이면 판정 보류.

분모가 작으면 비율이 널뛴다(3건 중 2건=67%). 보류는 채널 단위라 그 채널의 모든 aspect 가
함께 빠진다.
"""

MIN_DELTA = 0.03
"""[관문③] 최소 상승폭 3%p. p<ALPHA 발화분에 AND 로 겹친다.

**다중검정 게이트가 아니다** — 배경 볼륨의 상승폭 표준편차가 4.10%p 라 0.73 SD 밖에 안 되는
non-binding 값이고, 다중검정 방어는 BH-FDR(관문②) 담당이다. 실제 역할은 N 이 클 때
통계적으론 유의하나 실무적으로 무의미한 상승(+0.5%p 등) 차단이다.
"""

BH_FDR_Q = 0.05
"""[관문②] BH-FDR 의 목표 FDR. 보정이 없으면 정상 상품 1개가 하루 54.8% 확률로 오탐한다.

**family 는 소비자마다 다르다** — 일별 이상탐지는 상품별(`statistics.decide_fires`), 월간
JSD 는 전체 (상품 x 채널쌍)(`metrics_calculator.apply_bh_fdr`).

각 family 의 기대 거짓발견률을 제어할 뿐 **최종 발행 알림의 헛알림률이 5% 이하라는 뜻이
아니다** — 결합·억제·채점을 거친 데모 헛알림률은 별도 실측 지표다.
"""

# --- 원인 분류 일관성 (detection) ---

CONSISTENT_RATIO = 0.50
"""[6] 최다 원인이 이 비율 이상이어야 '원인 일관'. 미달이면 [7]에서 확신도 낮음 경로."""

CONSISTENT_COUNT = 5
"""[6] 그리고 최다 원인 건수가 이 이상이어야 '원인 일관'. 저건수 우연 일치 방지."""

# --- 알림 억제 (detection) ---

RENOTIFY_BLOCK_DAYS = 7
"""같은 (상품, aspect, 채널) 조합의 재알림 금지 기간(일).

**불변식: RENOTIFY_BLOCK_DAYS <= CURRENT_WINDOW_DAYS. 지금 여유가 0 이다.**
억제된 날은 알림이 없어 `prior_alerts` 에 안 남는데도 기준선에서 빠지는 이유는, 억제가 풀린
뒤 나가는 알림의 윈도우가 그 구간을 덮어주기 때문이다(`service._alert_days`). 윈도우보다
키우면 두 알림 사이에 틈이 생기고 그 틈의 이상 구간이 과거 기준선에 섞여 **'새로운 평소'로
굳는다 → 알림이 스스로 꺼진다.** 늘리려면 윈도우를 함께 늘리거나 억제된 날짜를 따로 기록해
`_alert_days` 에 넘겨야 한다. tests/test_pipeline.py 가 이 불변식을 고정한다.
"""

RENOTIFY_DELTA_JUMP = 0.05
"""억제 중이라도 부정률이 직전 알림 대비 이만큼 더 오르면 갱신 알림 허용.

갱신은 새 alert_id 를 발급하고 updates_alert_id 에 원본을 담는다.
"""

# --- LLM 공통 ---

MAX_RETRY = 2
"""LLM 호출/파싱 실패 시 재시도 횟수. 총 시도 = 1 + MAX_RETRY."""

# --- 인용 검증 (recommendation) ---

GROUNDING_SIMILARITY_THRESHOLD = 0.8
"""인용문 grounding 유사도 임계값. 완전 부분일치 실패 시의 완화 기준.

완전일치는 조사 하나만 달라도 깨지고(오탐) 너무 낮추면 환각을 통과시킨다. quote 와
source_text 정규화 문자열의 최장 연속 일치 길이 / quote 길이로 계산한다.
"""

SIMILAR_CASE_TOP_N = 3
"""컬렉션2(과거·반려 사례) 유사도 조회 상위 건수."""

CS_QUOTE_TOP_N = 5
"""image_guide 근거로 프롬프트에 싣는 CS 원문 건수 상한.

전부 실으면 프롬프트 길이보다 **인용 대상이 흩어지는 게 문제다** — LLM 이 어느 문의를
인용했는지 흐려지고 citations 대조도 느슨해진다. 앞에서부터 자르므로 탐지가 정한 우선순위는
유지된다.
"""

# --- 문서 생성 (reporting) — 계약 정본은 docs/reporting_schema.md ---

MONTHLY_ASPECT_COUNT = 3
"""월간 리포트가 다루는 aspect 개수(색상·사이즈·소재) 고정 길이.

aspect_distributions·sentiment_drifts·aspect_summaries 세 배열의 길이가 전부 이 값이며 서로
aspect 집합이 같아야 한다. CS 탐지용 6종(Aspect enum 전체)과 다르다.
"""

MAX_CHANNEL_PAIRS = 3
"""월간 채널쌍 최대 개수 = C(채널 3종, 2) = 3.

채널이 늘면 함께 키워야 한다 — 검증기가 "입력 pairs 전부에 분석이 있을 것"을 요구하므로,
입력 쌍이 상한을 넘으면 스키마가 잘라내 **영구 FAILED_VALIDATION** 이 된다(재시도 무의미).
"""

DRIFT_RISK_THRESHOLD = 0.03
"""sentiment_drifts[].status 가 RISK 가 되는 ΔP_neg 하한. 경고 박스 스타일을 가른다.

detection 의 MIN_DELTA 와 수치는 같지만 다른 계약이라 별도로 둔다 — 한쪽을 조정할 때 다른
쪽이 딸려가면 안 된다.
"""

RATIO_SUM_TOLERANCE = 0.005
"""비율 합 검증 허용 오차("세 비율 합 = 1.00 ±0.005"). delta 검증에도 함께 쓴다.

부동소수 반올림 흡수용이라 이보다 키우면 집계 버그를 놓친다.
"""

MAX_PDF_SIZE_BYTES = 10_485_760
"""생성 PDF 용량 상한(10MB). 초과하면 S3 업로드 전에 FAILED_SIZE_EXCEEDED 로 차단한다."""

# --- S3 보존 정책 — S3 Lifecycle 규칙과 **반드시 같은 값**이어야 한다 ---
#
# 코드가 계산하는 `object_expires_at` 이 이 상수에서 나온다. 버킷 Lifecycle 을 바꾸면서
# 여기를 안 고치면 실제로는 삭제된 파일을 "아직 받을 수 있다"고 안내하게 된다.

GUIDELINE_RETENTION_HOURS = 7 * 24
"""CS 가이드라인 PDF 자동 삭제까지의 시간 = 7일.

화면 플로우에 **운영 MD 승인이라는 사람 단계**가 있어 하루로는 부족하다 — 메일이 승인 뒤에
나가는데 승인이 하루를 넘기면 객체가 이미 지워져 발송할 것이 없다. 화면의 '재시도' 는
재생성이 아니라 S3 재조회라 만료 뒤에는 그 버튼으로 복구되지 않는다.
"""

PRESIGNED_URL_TTL_HOURS = 7 * 24
"""Pre-signed URL 만료까지의 시간 — 문서 종류 무관 7일 고정. SigV4 서명의 상한이다.

업로드 시점에 최초 1회 발급하고 메인 서버가 유효한 동안 재사용한다. **객체보다 링크가
오래 살 수는 없다** — 월간 리포트는 객체 6개월 / 링크 7일이라 링크가 먼저 만료된다.
"""

MONTHLY_RETENTION_DAYS = 180
"""월간 리포트 PDF 자동 삭제까지의 일수(6개월).

월간은 원본 데이터를 보관하지 않는다(PDF 가 유일 산출물). 즉 **만료 = 영구 소실**이라
줄이면 아직 볼 수 있어야 할 리포트가 사라진다. 변경 전 합의 필수.
"""

SEVERITY_STAGE_LABEL: dict[str, str] = {
    "SAFE": "안정 단계",
    "CAUTION": "주의 단계",
    "CRISIS": "위험 단계",
}
"""severity → cause_title 에 반드시 포함돼야 하는 단계 라벨.

다른 단계의 라벨이 섞이면 반려한다. 키는 Severity enum 의 value 와 1:1.
"""

HOLD_INSUFFICIENT_DATA_NOTICE = (
    "해당 상품의 월간 CS 표본 수는 부족으로 인하여 보고서 생성이 보류되었습니다. "
    "데이터가 누적되면 분석이 재개됩니다."
)
"""HOLD_INSUFFICIENT_DATA 콜백의 고정 안내 문구.

LLM 이 생성하는 문장이 아니라 문서에 못박힌 고정 문자열이다 — 임의로 바꾸지 말 것.
"""

MIN_VOC_COUNT_FOR_REPORT = 10
"""월간 보고서 생성 최소 표본. 미만이면 LLM 을 아예 안 돌리고 HOLD_INSUFFICIENT_DATA.

detection 의 MIN_SAMPLE_SIZE 와 수치는 같지만 다른 계약이라 별도로 둔다.
"""

NOTICE_MAX_CHARS = 255
"""콜백 `notice_message` **전체**의 글자 수 상한. `_build_excluded_notice` 가 보장한다.

두 가지가 계약이다. 상한은 **조립된 최종 문자열**에 걸고(구절마다 예산을 나눠 주면 구절
수와 고정 문구만큼 천장이 같이 올라가 상한이 안 지켜진다), 자르기는 개수가 아니라
**길이**로 한다(`_fetch_product_names()` 가 커머스 노출명을 자르지 않고 그대로 싣기 때문에,
목 데이터 이름 7자로 223자인 조합이 실제 38자 이름에서는 368자가 된다).

접어도 되는 근거는 보류 상세가 합본 PDF 의 보류 페이지에 있다는 것인데, **보류에만**
성립한다 — `pdf_compiler` 는 `held` 만 페이지로 만들고 `failed_products` 는 지면에 안 남는다
(배치 요약 JSON 에는 전체 목록이 남는다).

255 는 VARCHAR(255) **가정값**이고 백엔드 컬럼의 실제 제한은 아직 확인 못 했다. 확인할 때
글자 수인지 바이트인지도 같이 물을 것 — 전부 한글이면 UTF-8 로 약 700바이트다.
"""

HOLD_IN_BOOK_NOTICE = (
    f"해당 상품은 월간 VOC 가 {MIN_VOC_COUNT_FOR_REPORT}건 미만이라 "
    "리포트 생성이 보류되었습니다."
)
"""합본 PDF 의 **보류 상품 페이지**에 찍는 문구. 위 콜백용 문구와 쓰임이 다르다.

보류 상품이 합본에서 통째로 빠지면 **PDF 만 받아 보는 사람은 자기 상품이 왜 없는지 알
방법이 없다**(목차도 표지도 없어 빠졌다는 사실조차 안 보인다). 기준은 **미만**(`< 10`)이다 —
'10건 이하'로 쓰면 정확히 10건인 상품이 생성되는데도 보류라고 안내하게 된다.
"""

# --- 채널 분열 판정 (reporting) ---

JSD_DELTA_MIN = 0.10
"""δ_min (bits). excess = jsd_score − jsd_baseline 를 이 값과 비교해 단계를 가른다.

excess < δ_min 또는 미유의 → SAFE / δ_min ≤ excess < 2δ_min 이고 유의 → CAUTION /
excess ≥ 2δ_min 이고 유의 → CRISIS. 게이지 문구 단계를 정하므로 변경 시 합의 필요.
"""

JSD_GATE_MIN_TOTAL = 30
"""[게이트] 두 채널 부정 문서 합 N 의 하한. min(n_A, n_B) ≥ 1 과 AND 로 묶인다.

미충족이면 판정 6개 값을 전부 null 로 두고 hold_reason 을 세팅한다(반쪽 상태 금지).
"""

PERMUTATION_B = 10_000
"""순열검정 반복 횟수 B. p값 해상도가 1/B 라 이보다 줄이면 BH-FDR 이 무뎌진다."""

# --- 출력 검증 (reporting) ---

FORBIDDEN_METRIC_EXPRESSIONS = (
    "p-value",
    "p값",
    "p 값",
    "FDR",
    "유의확률",
)
"""출력 텍스트 금지 표현. 통계 용어가 셀러용 문서에 노출되면 안 된다.

`p = 0.0x` 형태는 FORBIDDEN_P_VALUE_PATTERN 이 따로 잡는다.
"""

FORBIDDEN_P_VALUE_PATTERN = r"p\s*[=<>≤≥]\s*0?\.\d+"
"""`p = 0.03`, `p<0.05` 같은 p값 노출 패턴. FORBIDDEN_METRIC_EXPRESSIONS 와 함께 검사한다."""

FACTCHECK_NUMBER_TOLERANCE = 0.5
"""수치 팩트체크 허용 오차. 출력의 `13%`/`8%p`/`450건` 이 입력 수치와 이 값 이내여야 한다.

단위는 표기된 수치 그대로(%, %p, 건)이고 반올림 표기를 흡수하는 폭이다.
"""

FACTCHECK_SCORE_TOLERANCE = 0.005
"""단위 없는 소수(JSD 점수 등)의 팩트체크 허용 오차.

`0.54` 를 %/건과 같은 ±0.5 로 재면 전혀 다른 점수(0.18)까지 통과해서 따로 둔다.
"""

ROOT_CAUSE_UNSPECIFIED_TEXT = "원인 미특정"
"""root_cause 가 null 일 때 root_cause_summary 에 반드시 포함돼야 하는 대체 문구.

원본 문서의 해당 칸이 캡처에서 판독되지 않아 임시로 정한 값이다 — 확정 문구가 확인되면
교체할 것.
"""

# --- 벡터DB 컬렉션 이름 ---

COLLECTION_DETAIL_PAGES = "detail_pages"
"""컬렉션1 — 상세페이지. 개선안 생성의 인용 근거."""

COLLECTION_REJECTION_REASONS = "rejection_reasons"
"""컬렉션2 — 반려 사유. 다음 생성 시 참고 (B5 반려 → 적재)."""

EMBEDDING_MODEL = "text-embedding-3-small"
"""두 컬렉션 공통 임베딩 모델(다국어).

이 값을 바꾸면 **두 컬렉션 다 재시딩해야 한다** — Chroma 가 컬렉션 설정에 임베딩 함수를
저장해두고 불일치 시 열기를 거부한다(`vectordb._get_collection`).
"""
