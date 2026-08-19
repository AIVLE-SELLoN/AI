"""담당: 지인 — 탐지 배치의 입력원. raw DB 에서 분모(원문)와 분자(분류 결과)를 읽는다.

`daily.py` 에서 갈라져 나왔다. 분리 기준은 **바뀌는 이유**다 — 이쪽은 raw DB 스키마와
분류기 버전 정책이 바뀔 때 움직이고, `daily.py` 는 배치 흐름이 바뀔 때 움직인다.

호출부는 `run_batch(load_inputs=...)` 하나이고, 평가·재현은 같은 시그니처로
`scripts/golden_inputs.load_golden_inputs` 를 주입한다.

주의 — raw DB 연결을 여는 모듈이라 `tests/test_raw_db_write_scope.py` 의
`RAW_DB_CALLERS` 에 등록돼 있다. 접속 실패는 던지는 쪽이고, 잡는 곳은 `daily.main()` 이다 —
그쪽이 `connection_error_types()` 를 같이 잡아 exit 2 로 끝낸다.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable
from datetime import date, datetime

from pydantic import ValidationError

# 활성 프롬프트 버전 — 탐지의 조회 조건이다. 다른 선택적 의존성과 달리 **폴백을 두지
# 않는다.** 값이 틀리면 조회가 0건이 되고 그건 "알림이 안 나간다" 라서 조용하다 —
# 미연결 폴백이 오히려 더 위험한 자리다. 못 읽으면 ImportError 로 세우는 게 맞다.
from app.classification.service import PROMPT_ASPECT_VERSION, PROMPT_SENTIMENT_VERSION
from app.config import get_settings
from app.core import raw_schema
from app.core.constants import CURRENT_WINDOW_DAYS, KST, PAST_WINDOW_DAYS
from app.core.raw_db import (
    RawDbConnection,
    connect_readonly,
    describe_target,
    existing_tables,
)
from app.core.schemas import Channel, ClassifiedItem, Source
from app.core.versions import CLASSIFIER_PIPELINE_VERSION

logger = logging.getLogger(__name__)


INPUT_WINDOW_DAYS = CURRENT_WINDOW_DAYS + PAST_WINDOW_DAYS
"""DB 에서 읽어올 기간(일). 탐지가 실제로 보는 범위와 같다.

`build_baseline` 의 과거 윈도우가 **현재 윈도우 직전** 28일이라, 필요한 전부가
`[window_end - 34, window_end]` 다. 값이 `STATE_RETENTION_DAYS` 와 같지만 **사유가
다르다** — 저쪽은 캐시 보관 기간이다. 한쪽을 바꿀 일이 생겼을 때 다른 쪽이 조용히
따라가지 않도록 따로 둔다.
"""

# 분모(원문)와 분자(분류 결과)를 **따로** 읽는다. 확정 문서 §4 는 이 둘을 CTE 로 나눠
# SQL 안에서 집계하는 형태인데, 우리는 집계를 `app/detection/aggregate.py` 가 하므로
# 행을 그대로 가져오는 두 쿼리로 나눈다. 지켜야 할 규칙은 같다 —
#   ① 분모는 원문(voc_document)에서만 센다. classified_item_aspect 자식에서 세면
#      aspect 0개인 문서가 통째로 빠져 부정률이 부풀려진다(§2-6 경고, loader docstring).
#   ② `sentiment = -1` 을 WHERE 로 올리지 않는다. 올리면 LEFT JOIN 이 INNER JOIN 으로
#      퇴화해 같은 버그가 돌아온다. 부정 여부는 읽어온 뒤 파이썬이 센다.
_DOCUMENT_SQL = f"""
    SELECT item_id, source, channel_id, product_group_id, content, occurred_at
    FROM {raw_schema.VOC_DOCUMENT}
"""

# 🔴 **활성 분류기 버전 필터 (2026-08-12).**
#
# 이 배치가 읽는 구간은 35일(현재 7 + 과거 28)이다. 그 사이에 분류 프롬프트를 바꾸면
# **과거 구간은 옛 프롬프트 · 현재 구간은 새 프롬프트** 결과로 한 검정을 돌리게 된다.
# 부정률이 움직인 원인이 "고객이 달라졌다"가 아니라 "우리가 라벨러를 바꿨다"인데,
# Fisher 검정은 그 둘을 구분하지 못한다 — **프롬프트 개선이 그대로 고객 이상 알림으로
# 발화한다.** 컬럼에 버전을 남기고는 있었지만 읽는 쪽이 아무도 안 봐서 조용했다.
#
# 술어와 파라미터 순서는 `raw_schema` 가 정본이다 — 적재(`scripts/classification_worker.py`)와
# 조회(여기)가 각자 적으면 한쪽만 고쳐졌을 때 조회가 0건이 되고, 그건 미탐이라 조용하다.
#
# ⚠️ 거르는 축이 셋(prompt·model·pipeline)인 이유: 프롬프트가 그대로여도 라벨러는 바뀐다.
#    모델을 갈아끼우면 같은 프롬프트로 다른 라벨이 나오고, 후처리·폴백을 손봐도 분포가
#    움직인다(`CLASSIFIER_PIPELINE_VERSION` docstring 의 실측 2.1% 참고).
_ACTIVE_VERSION_PREDICATE = raw_schema.active_version_predicate("ci")

_ASPECT_SQL = f"""
    SELECT ci.item_id AS item_id, a.aspect AS aspect, a.sentiment AS sentiment
    FROM classified_item ci
    JOIN {raw_schema.VOC_DOCUMENT} v ON v.item_id = ci.item_id
    LEFT JOIN classified_item_aspect a ON a.item_id = ci.item_id
    WHERE {_ACTIVE_VERSION_PREDICATE}
"""

# 윈도우 안에서 활성/전체가 각각 몇 행인지. 위 필터가 **얼마나 잘라냈는지**를 세는 쿼리라
# 필터와 같은 조인·같은 비교식을 써야 한다(다르면 세는 대상과 거르는 대상이 어긋난다).
_VERSION_COUNT_SQL = f"""
    SELECT
        SUM(CASE WHEN {_ACTIVE_VERSION_PREDICATE} THEN 1 ELSE 0 END) AS active,
        COUNT(*) AS total
    FROM classified_item ci
    JOIN {raw_schema.VOC_DOCUMENT} v ON v.item_id = ci.item_id
    WHERE v.content IS NOT NULL AND TRIM(v.content) <> ''
"""
"""윈도우 안 활성/전체 행 수. **세우는 판정의 근거라 집합을 정확히 맞춰야 한다.**

🔴 **불변식: 배치를 세우는 집합 ⊆ `--reclassify-stale` 이 고칠 수 있는 집합.**
   본문 조건(`TRIM(content) <> ''`)이 여기 있는 이유가 그것이다. 워커의
   `FETCH_STALE_SQL`·`COUNT_STALE_SQL` 이 같은 조건을 요구하므로, 여기서만 빼면
   **고칠 수단이 없는 상태로 배치가 매일 선다**:

       원문은 있는데 본문이 비어('   ') 있는 stale 행이 1건이라도 있으면
           워커 count_stale()       = 0   ← 재분류 대상 없음
           워커 fetch_stale_batch() = 0 rows
           배치                     = RuntimeError 로 중단
       에러가 시키는 `--reclassify-stale` 은 "재분류할 문서가 없습니다"로 끝나고,
       손으로 SQL 을 치는 것 말고 빠져나갈 길이 없다.

   경고만 하던 때는 무해했고 fail-closed 로 바뀌면서 교착이 됐다.
   (2026-08-12 리뷰 §1 후속, 지인님 실측)

⚠️ 위 필터가 **얼마나 잘라냈는지**를 세는 쿼리이므로 `_ASPECT_SQL` 과 같은 조인·같은
   비교식을 쓴다. 다르면 세는 대상과 거르는 대상이 어긋난다.
"""


def _active_version_params() -> tuple[str, str, str, str]:
    """`_ACTIVE_VERSION_PREDICATE` 의 `?` 에 넣을 값. 워커의 `active_version_params()` 와 짝이다.

    ⚠️ **모델을 설정에서 매번 읽는다.** `LLM_MODEL` 을 바꾸면 그 순간부터 옛 행이 안 읽히고,
       윈도우가 통째로 옛 버전이면 `_check_version_cutover()` 가 배치를 세운다. 오타 하나로
       세워지는 건 거칠지만, 다른 모델이 만든 라벨을 같은 검정에 섞는 것보다 낫다 —
       그쪽은 조용하다.
    """
    return raw_schema.version_params(
        PROMPT_ASPECT_VERSION,
        PROMPT_SENTIMENT_VERSION,
        get_settings().llm_model,
        CLASSIFIER_PIPELINE_VERSION,
    )


def _aspect_window_clause(where: str) -> str:
    """분모용 조건절을 `classified_item` 조인 쿼리에 붙일 형태로 바꾼다.

    두 가지를 고친다:
      - `occurred_at` → `v.occurred_at` (그쪽 쿼리는 뷰에 별칭이 붙어 있다)
      - 첫 `WHERE` → `AND`

    윈도우가 없으면(`where == ""`) 그대로 빈 문자열이다.

    ⚠️ **두 호출부(`_ASPECT_SQL`·`_VERSION_COUNT_SQL`)가 모두 `WHERE` 로 시작해야 한다.**
       그래야 이 절이 `AND` 로 이어붙는다. `WHERE` 가 없는 쿼리에 붙이면 조건이
       **`JOIN ... ON` 뒤로** 들어가는데, INNER JOIN 이면 결과가 같아 통과하지만 나중에
       LEFT JOIN 으로 바꾸는 순간 **조용히 의미가 달라진다** — ON 절의 조건은 행을 안 지우고
       NULL 로 채우기 때문이다. 새 호출부를 만들면 `WHERE` 를 먼저 두거나 이 함수를 쓰지
       말 것. (2026-08-12 리뷰 잔가지)
    """
    return where.replace("occurred_at", "v.occurred_at").replace(" WHERE ", " AND ", 1)


def _to_kst(value: str | datetime) -> datetime:
    """저장된 시각 → KST 시각. 날짜 절단의 유일한 경로다.

    ⚠️ **입력 타입이 백엔드마다 다르다.** sqlite 는 시각을 TEXT 로 들고 있어 ISO 문자열이
       오고, Postgres 는 `TIMESTAMPTZ` 라 psycopg 가 **aware `datetime` 을 그대로** 준다.
       문자열만 받으면 Postgres 에서 `fromisoformat(datetime)` 이 `TypeError` 로 터진다 —
       조회는 성공한 뒤 변환에서 죽는 모양이라 원인이 SQL 쪽으로 보이지 않는다.

    ⚠️ **문서에 넣는 `created_at` 도 이 값이어야 한다.** `build_rows` 가 `.date()` 로
       날짜를 다시 뽑는데, 여기서 거른 날짜와 그쪽이 뽑는 날짜가 다르면 윈도우 경계의
       문서가 "읽히긴 했는데 집계에선 다른 날"이 된다.

    🔴 **오프셋이 없으면 KST 로 간주한다 — 호스트 시간대를 보지 않는다.**
       `.astimezone()` 만 쓰면 naive 값을 **실행 호스트의 로컬 시각**으로 해석한다.
       `2026-08-28T20:00:00` 이 KST 노트북에선 08-28 인데 **UTC 컨테이너에선 08-29**
       가 된다 — §3(KST 경계)을 지키려고 만든 함수가 배포 환경에 따라 §3 을 어기는
       셈이고, 개발 머신이 KST 라 **로컬 테스트로는 영원히 안 잡힌다.**
       규칙은 생성기와 같다(`mock_producer.to_kst_iso()`: "naive 면 KST 로 간주한다").
       **둘은 한 쌍이다** — 저쪽이 오프셋을 붙여 쓰고 이쪽이 그걸 읽어 자른다.
       `KST` 상수도 `core/constants.py` 하나를 공유한다(PR #70).

    ⚠️ 지금 목 파이프라인은 전부 오프셋을 붙여 저장하므로 이 분기는 안 탄다. 다만
       인프라 연동 후에는 **적재하는 쪽이 백엔드로 바뀌므로** 방어가 필요하다.
       (2026-08-11 리뷰 ⑥)
    """
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST)


def load_inputs_from_db(
    window_end: date | None = None,
    *,
    db_path: str | None = None,
    dropped: Counter[str] | None = None,
) -> tuple[list[ClassifiedItem], list[dict]]:
    """(items, documents) 를 원본 DB 에서 읽는다. **기본 입력원.**

    items 는 워커가 분류해 `classified_item` 부모 행으로 남긴 결과, documents 는
    `cs`·`reviews` 원문이다. 둘을 따로 읽는 이유는 **분모가 documents 에서 나와야 하고
    부모 행 존재 여부로 분류 완료를 확인해야 하기 때문**이다. aspect 0개인 정상 리뷰도
    부모 행은 남고 자식 행만 0개라, 미분류 원문과 구분할 수 있다(탐지 분모 산출 방식 §1).

    두 소스의 시각 컬럼명이 다르므로(`cs.inquired_at` / `reviews.created_at`)
    `voc_document` 뷰를 거친다 — 호출부에서 UNION 을 다시 쓰면 시각 컬럼을 잘못 고르는
    실수가 각자 생긴다(raw_schema 모듈 docstring).

    **읽기 전용으로 연다.** AI 노드는 원문 테이블에 읽기 권한만 있고(§5-2), 경로가
    틀렸을 때 sqlite 가 빈 파일을 새로 만들어 "문서 0건" 으로 조용히 통과하는 것도 막는다.

    평가·재현으로 배치를 돌리려면 `scripts/golden_inputs.load_golden_inputs` 를
    주입한다 — 골든을 `app/` 안에서 읽지 않기 위해 밖으로 뺐다(eval/README §232).

    ⚠️ **items 는 활성 분류기로 만든 결과만 담는다**(`_ACTIVE_VERSION_PREDICATE`).
       35일 창에 두 분류기의 결과가 섞이면 라벨러 교체가 고객 이상으로 둔갑하기
       때문이다. 다만 그 필터는 **분자에만** 걸리므로 걸러진 상태로 검정을 돌리면 안 된다 —
       `_check_version_cutover()` 가 옛 버전 행이 **1건이라도 있으면 여기서 세운다**
       (fail-closed). 근거는 그 함수 docstring.

    Args:
        window_end: 현재 윈도우 마지막 날. 주면 `[window_end-34, window_end]` 35일만
            읽는다(현재 7 + 과거 28 — `build_baseline` 의 과거 윈도우가 현재 윈도우
            직전 28일이라 그 합이 필요한 전부다). None 이면 전량을 읽고 호출부가 최신
            날짜로 윈도우를 정한다 — 첫 실행·백필용이고, 매일 배치에서는 반드시 줄 것
            (현재 목 데이터 기준 128,228건 풀스캔이다).
        db_path: raw DB 경로. 기본은 `settings.raw_db_path`(테스트 주입용 인자다).
        dropped: 주면 **사유별 제외 건수**를 여기에 채운다(`_build_inputs`). 안 주면
            지금까지처럼 경고 로그로만 남는다.

            🔴 **반환값을 3-tuple 로 늘리지 말 것 — 그게 자연스러워 보이지만 위험하다.**
            이 함수는 로더 seam 의 한쪽이라 `load_golden_inputs` 와 시그니처가 같아야
            하고, 그쪽을 같이 늘리면 `(items, documents)` 로 언패킹하는 **저장소 밖 호출부
            9곳**(`scripts/detection_experiments/` 7 + `eval/` 1 + 테스트 fake 들)이 전부
            깨진다. 그 실험 스크립트들에는 **테스트가 없어서** 하나를 빠뜨려도 아무 데서도
            안 걸리고 실행 시점에야 터진다 — 2026-08-16 의 *"`py_compile`·ruff 는 import
            깨짐을 못 잡는다"* 와 같은 계열이다.
            수집기로 받으면 공개 계약이 안 바뀌어 그 9곳이 무변경이고, **배치가 부르는
            함수가 남들이 부르는 함수와 여전히 같다** — 별도 private 본체를 두면 그쪽이
            갈려서 테스트가 배치의 실제 경로를 안 타게 된다(#99 게이트 픽스처와 같은 사유).

    Returns:
        items: 분자의 출처 (ClassifiedItem)
        documents: **분모의 출처** (원본 문서)

    Raises:
        FileNotFoundError: sqlite DB 파일이 없을 때. 목 파이프라인은
            `scripts/mock_producer.py` 가 먼저 돌아야 원문이 생긴다.
        RuntimeError: 분류 결과 테이블이 없거나 구버전일 때
            (`_require_classified_tables`), 또는 윈도우 안 분류 결과가 전부 옛 프롬프트
            기준일 때(`_check_version_cutover`).
        psycopg.Error: Postgres 백엔드에서 접속·스키마·권한이 안 될 때. 위 두 타입의
            하위가 아니므로 `main()` 이 `raw_db.connection_error_types()` 를 같이 잡아
            exit 2 로 가른다 — 빠지면 exit 1 + raw traceback 이다.
    """
    where, params = _window_clause(window_end)
    aspect_where = _aspect_window_clause(where)
    conn = connect_readonly(db_path)
    try:
        _require_classified_tables(conn, describe_target(db_path))
        _check_version_cutover(conn, aspect_where, params)
        doc_rows = conn.execute(_DOCUMENT_SQL + where, params).fetchall()
        # 버전 파라미터가 먼저다 — WHERE 절이 윈도우 조건절보다 앞에 있다.
        aspect_rows = conn.execute(
            _ASPECT_SQL + aspect_where, (*_active_version_params(), *params)
        ).fetchall()
    finally:
        conn.close()

    items, documents, drops = _build_inputs(doc_rows, aspect_rows, window_end)
    if dropped is not None:
        dropped.update(drops)
    return items, documents


def _read_inputs(
    loader: Callable[..., tuple[list[ClassifiedItem], list[dict]]],
    window_end: date | None,
) -> tuple[list[ClassifiedItem], list[dict], dict[str, int] | None]:
    """입력을 읽고, **관측할 수 있으면** 사유별 제외 건수도 같이 낸다. 못 보면 `None`.

    🔴 **`None`("이 입력원은 보고하지 않는다")과 `{}`("봤고 0건")은 다른 값이다.**
       `_classifier_versions_for` 가 골든 입력에 `None` 을 싣는 것과 같은 규칙이다 —
       안 가르면 골든으로 돌린 배치가 "제외 0건" 이라고 **주장**하게 되는데, 골든 로더는
       매핑이 없는 행을 세지 않고 그냥 건너뛴다(`scripts/golden_inputs.py`). 즉 0 은
       관측이 아니라 무지다.

    ⚠️ 수집기를 **모든 로더에** 넘기면 안 된다 — 테스트가 주입하는 fake 는
       `lambda window_end: ([], [])` 라 키워드 인자를 안 받는다.
    """
    if loader is not load_inputs_from_db:
        return (*loader(window_end), None)

    dropped: Counter[str] = Counter()
    items, documents = loader(window_end, dropped=dropped)
    return items, documents, dict(dropped)


def _classifier_versions_for(loader: Callable) -> dict | None:
    """이 배치가 발행할 알림에 실을 분류기 신원. 모르면 None(=payload 에 `null`).

    🔴 **입력원이 `load_inputs_from_db` 일 때만 값이 있다.** 실을 수 있는 근거가
       `_ASPECT_SQL` 의 활성 버전 필터뿐이기 때문이다 — 그 필터가 "이 알림에 기여한 모든
       행의 버전 3종이 활성 값"임을 쿼리로 강제한다. 그래서 이건 주장이 아니라 관측이다.

       `--input-source golden` 은 CSV 를 그대로 읽어 그 필터를 안 탄다. 거기서도 같은 값을
       실으면 **검증한 적 없는 것을 검증된 것처럼 보고**하게 된다. 골든은 분류 오차가 0 인
       oracle 입력이라 애초에 분류기를 안 거쳤다 — `null` 이 정확한 답이다.

    ⚠️ 값을 `_active_version_params()` 에서 가져온다 — **필터가 쓴 것과 같은 값**이다.
       여기서 따로 조립하면 필터와 보고가 갈릴 수 있고, 그러면 payload 가 실제로 읽은
       것과 다른 버전을 말하게 된다.
    """
    if loader is not load_inputs_from_db:
        return None
    prompt_cs, prompt_review, model, pipeline = _active_version_params()
    return {
        "prompt_cs": prompt_cs,
        "prompt_review": prompt_review,
        "model": model,
        "pipeline": pipeline,
    }


def _check_version_cutover(conn: RawDbConnection, aspect_where: str, params: tuple) -> None:
    """윈도우 안에 옛 분류기 결과가 **하나라도** 있으면 세운다 (fail-closed).

    🔴 **경고로 넘기면 오탐이 난다 — 그것도 최대 강도로.** 활성 버전 필터는 `_ASPECT_SQL`
       에만, 즉 **분자에만** 걸린다. 분모(`_DOCUMENT_SQL` → documents)는 원문이라 필터를
       안 타므로, 과거 구간이 stale 이면 `past_neg` 만 0 이 되고 `past_total` 은 그대로다.
       기준선이 작아지는 게 아니라 **0 이 된다.**

           진짜 부정률을 양쪽 다 5% 로 고정(변화 없음)하고 리뷰 소스로 실측:
               대조군(전부 활성)   documents 1000 / items 1000  →  알림 0건
               섞임(과거=옛 버전)  documents 1000 / items  200  →  알림 1건 🚨
               (past_rate=0.0000 cur_rate=0.0500 delta=+0.0500 p=8.52e-08)

       같은 데이터를 필터가 없던 시절에 돌리면 0건이다. 즉 **필터가 새로 여는 오탐 경로**라,
       막으려던 병이 뒤집힌 채로 재발한다. (2026-08-12 서영님 리뷰 §1, 실측)

    ⚠️ **CS 는 우연히 안전하고 리뷰만 뚫린다.** CS 는 과거 구간 aspect 가 0 이 되면
       `check_coverage` 가 갭으로 잡아 `unreliable_slots` 로 빠진다. 리뷰는
       `COVERAGE_CHECKED_SOURCES` 가 CS 전용이라 안 잡히고, 리뷰 커버리지는 그 방법으로
       **원리적으로 검증이 안 된다**(`detection/loader.py` docstring). 방어선이 없다.

    **왜 분모에서 같이 빼지 않고 세우는가** (2026-08-12 결정, 2안):
      혼재는 표본이 줄어든 게 아니라 **검정 전제가 깨진 것**이다. Fisher 검정은 같은
      분류기로 완전히 라벨링된 현재 7일과 과거 28일을 비교한다는 전제 위에 서 있다.
      stale 원문을 분모에서도 빼면 그 전제를 복원하는 대신 **비무작위 결측**을 들인다 —
      `FETCH_STALE_SQL` 이 `ORDER BY r.occurred_at` 이라 `--limit` 으로 나눠 backfill 하면
      오래된 것부터 채워져, 남는 분모가 시간순 앞쪽 조각만 된다. 윈도우 안에 추세가 있으면
      교란되고, 무엇보다 **불완전한 윈도우를 정상 검정으로 간주**하게 된다.
      가용성보다 통계적 정합성을 택한다 — 재현 가능하고 설명도 명확하다.
      (한 건 때문에 배치가 서는 비용이 실제로 크다고 확인되면, 그때 측정과 합의를 거쳐
       슬롯 단위 보류를 설계한다. 조용히 우회하지는 않는다.)

    ⚠️ **`LLM_MODEL` 오타도 여기로 온다.** 설정이 틀리면 전량이 stale 로 잡혀 배치가
       선다. 메시지에 활성 3축을 다 찍는 이유가 그것이다 — "backfill 이 필요하다"와
       "설정이 틀렸다"를 사람이 값을 보고 가를 수 있어야 한다.

    Raises:
        RuntimeError: 윈도우 안에 옛 분류기 결과가 1건이라도 있을 때.
    """
    row = conn.execute(
        _VERSION_COUNT_SQL + aspect_where, (*_active_version_params(), *params)
    ).fetchone()

    total = row["total"] or 0
    active = row["active"] or 0
    stale = total - active
    if not stale:
        # total==0(워커를 아직 안 돌림)도 여기로 온다 — 그건 버전 문제가 아니라 커버리지
        # 문제라 `check_coverage` 가 일자별로 잡는다. 여기서 또 세우면 원인이 흐려진다.
        return

    prompt_cs, prompt_review, model, pipeline = _active_version_params()
    raise RuntimeError(
        f"윈도우 안 분류 결과 {total}건 중 {stale}건이 옛 분류기 기준입니다"
        f"(활성 {active}건 / 옛 버전 {stale}건). "
        f"활성: cs={prompt_cs}, review={prompt_review}, model={model}, pipeline={pipeline}\n"
        "  섞인 채로는 돌리지 않습니다 — 필터가 분자에만 걸려서, 과거 구간이 옛 버전이면 "
        "기준선 부정률이 작아지는 게 아니라 0 이 되고 그대로 오탐이 됩니다.\n"
        "  활성 값이 의도한 것인지 먼저 확인하고(LLM_MODEL 오타면 설정을 고치세요), "
        "맞다면 `python scripts/classification_worker.py --reclassify-stale` 로 "
        "backfill 을 **끝까지** 돌린 뒤 다시 실행하세요."
    )


def _require_classified_tables(conn: RawDbConnection, target: str) -> None:
    """분류 결과 테이블이 **쓸 수 있는 상태인지** 먼저 확인한다.

    두 가지를 가른다. 둘 다 원문만 적재된 DB 에서 실제로 나는 상태다:
      - 테이블 자체가 없음 → 워커를 아직 안 돌렸다. 그냥 두면 `no such table` 이
        올라오는데, 원인이 "경로가 틀렸나"인지 "안 돌렸나"인지 안 드러난다.
      - 테이블 모양이 확정본과 다름 → `find_legacy_tables`. `IF NOT EXISTS` 가 이미 있는
        테이블을 그대로 두기 때문에 **조회 단계에 가서야** 터진다(PR #37 에서 워커가 같은
        함정을 맞았다). `data/` 는 gitignore 라 팀원마다 DB 상태가 달라서 남아 있을 수 있다.

        ⚠️ **사유가 둘이고 증상이 다르다 — 메시지가 그 둘을 단정하면 안 된다.**
          · 컬럼이 옛것    → 조회가 `no such column` 으로 **시끄럽게** 죽는다
          · UNIQUE 제약 누락 → 아무것도 안 죽고 재분류가 같은 `(item_id, aspect)` 를
            중복 적재해 **탐지 분자가 부푼다.** 오탐 방향이라 조용하다.
        뒤쪽은 인프라가 낡은 문서로 테이블을 먼저 세워 뒀을 때 나오는 모양이다
        (2026-08-18). 조치는 둘 다 "지우고 다시 만들기" 로 같지만, **사유를 "8/7 이전
        스키마" 로 단정하면 제약이 빠진 사람이 스키마 버전을 뒤지게 된다.**

    조용히 빈 결과로 넘기지 않는 이유: items 가 0건이면 분자가 통째로 비어 **알림이
    한 건도 안 나오는데 배치는 정상 종료**한다. 무동작이 성공으로 보고되는 형태다.
    """
    stale = raw_schema.find_legacy_tables(conn)
    if stale:
        raise RuntimeError(
            f"raw DB 의 분류 결과 테이블이 확정 스키마와 다릅니다({', '.join(stale)}): "
            f"{target} — 컬럼이 옛것이거나 UNIQUE 제약이 빠져 있습니다. 해당 테이블을 지우고 "
            "mock_producer·classification_worker 를 다시 돌리세요"
            "(자세한 안내는 워커가 출력합니다)."
        )
    # ⚠️ **두 테이블을 다 본다.** `classified_item` 만 보면 자식 테이블이 없는 DB 에서
    #    `no such table: classified_item_aspect` 가 조회 단계에서 그대로 올라온다 —
    #    이 가드가 막으려던 바로 그 모양이다. (2026-08-11 리뷰 잔가지)
    #
    # ⚠️ 목록 조회는 `raw_db.existing_tables()` 를 쓴다 — `sqlite_master` 는 sqlite 에만
    #    있어서, 직접 쓰면 Postgres 에서 이 가드가 `UndefinedTable` 로 먼저 죽는다.
    wanted = {"classified_item", "classified_item_aspect"}
    missing = wanted - existing_tables(conn, wanted)
    if missing:
        raise RuntimeError(
            f"분류 결과 테이블이 없습니다({', '.join(sorted(missing))}): {target} — "
            "scripts/classification_worker.py 를 먼저 돌려야 분자(부정 건수)가 생깁니다."
        )


def _window_clause(window_end: date | None) -> tuple[str, tuple]:
    """35일 범위조회 조건절. 인덱스(`cs.inquired_at`·`reviews.created_at`)를 타라고 둔다.

    경계를 하루씩 넓혀 잡고 **정확한 절단은 파이썬이 한다**(`_build_inputs`). 문자열
    비교라 저장된 오프셋이 전부 같아야 정확한데, 그 전제가 깨진 행이 하나 섞여도
    경계에서 조용히 빠지지 않게 하려는 것이다. 넓힌 만큼은 뒤에서 다시 걸러진다.
    """
    if window_end is None:
        return "", ()
    start = date.fromordinal(window_end.toordinal() - INPUT_WINDOW_DAYS + 1)
    return (
        " WHERE occurred_at >= ? AND occurred_at < ?",
        (
            date.fromordinal(start.toordinal() - 1).isoformat(),
            date.fromordinal(window_end.toordinal() + 2).isoformat(),
        ),
    )


def _build_inputs(
    doc_rows: list, aspect_rows: list, window_end: date | None
) -> tuple[list[ClassifiedItem], list[dict], Counter[str]]:
    """조회 결과 → (items, documents, 사유별 제외 건수).

    ⚠️ **버리는 방향은 항상 "분모에서도 뺀다" 이다.** 문서는 남기고 분류 결과만 버리면
       그 슬롯이 분류 커버리지 미달로 잡혀(`check_coverage`) 검정에서 통째로 빠지는데,
       그건 오탐이 아니라 미탐 방향이라 조용하다. 사유별 건수를 로그로 남기는 이유다.
    """
    start = (
        date.fromordinal(window_end.toordinal() - INPUT_WINDOW_DAYS + 1)
        if window_end
        else None
    )

    documents: list[dict] = []
    created_of: dict[str, datetime] = {}
    meta_of: dict[str, dict] = {}
    dropped: Counter[str] = Counter()

    for row in doc_rows:
        product = (row["product_group_id"] or "").strip()
        if not product:
            # 상품매핑이 아직 안 붙은 원문. 어느 상품의 분모인지 모르니 셀 수 없다.
            dropped["상품매핑 없음"] += 1
            continue
        try:
            channel = Channel(row["channel_id"])
            source = Source(row["source"])
            created = _to_kst(row["occurred_at"])
        except ValueError:
            dropped["채널·소스·시각 형식 오류"] += 1
            continue
        if start and not (start <= created.date() <= window_end):
            continue  # 조건절을 하루씩 넓혀 잡은 만큼 (_window_clause)

        item_id = row["item_id"]
        documents.append(
            {
                "id": item_id,
                "product": product,
                "channel": channel.value,
                "source": source.value,
                "created_at": created,
                "text": row["content"],
            }
        )
        created_of[item_id] = created
        meta_of[item_id] = {
            "source": source,
            "channel": channel,
            "product_group_id": product,
            "raw_text": row["content"],
        }

    aspects_of: dict[str, list[dict]] = {}
    for row in aspect_rows:
        item_id = row["item_id"]
        if item_id not in meta_of:
            continue  # 윈도우 밖이거나 위에서 버린 문서
        # LEFT JOIN 이라 aspect 가 NULL 인 행이 온다. 그래도 **키는 만든다** — aspect
        # 0개인 분류 결과도 item 으로 살아남아야 한다(리뷰의 정상 출력).
        entry = aspects_of.setdefault(item_id, [])
        if row["aspect"] is not None:
            entry.append({"aspect": row["aspect"], "sentiment": row["sentiment"]})

    items: list[ClassifiedItem] = []
    for item_id, aspects in aspects_of.items():
        try:
            items.append(
                ClassifiedItem(
                    item_id=item_id,
                    created_at=created_of[item_id],
                    aspects=aspects,
                    **meta_of[item_id],
                )
            )
        except ValidationError:
            # 리뷰에 허용 밖 aspect 가 붙은 경우 등. 분모(문서)는 남고 분자만 빠진다.
            #
            # `check_coverage` 가 부모 item 누락으로 잡아 해당 리뷰 슬롯을 검정 전에
            # 제외하므로 결과를 조용히 오염시키지는 않는다. 그래도 정상 경로는 아니다.
            #    지금은 워커가 `ClassifiedItem` 을 만들 때 이미 걸러서 DB 에 들어올 수
            #    없으므로 **도달 불가**다. 로그로 세는 이유가 그것이다 — 0 이 아니면
            #    워커 쪽 계약이 깨진 것이다. (2026-08-11 리뷰 잔가지)
            dropped["분류 결과 스키마 불일치"] += 1

    if dropped:
        logger.warning(
            "raw DB 입력에서 %d건을 제외했습니다: %s",
            sum(dropped.values()),
            dict(dropped),
        )
    logger.info(
        "raw DB 로드 documents=%d items=%d (window_end=%s)",
        len(documents),
        len(items),
        window_end,
    )
    return items, documents, dropped
