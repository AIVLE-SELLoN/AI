"""실험① 이상탐지 주력 지표 — 발표용 대표 숫자.

무엇을 재나: 탐지율 · 오탐률 · 편중/전역 판정 정확도
어떻게:     golden 카운트를 검정에 **직접 입력**(oracle)하고 golden_anomaly 와 대조
비용:       **$0** — 통계 계산이고 앞단 분류를 안 태운다

⚠️ oracle 인 이유: 저사건 케이스는 분류 오차 1건에 판정이 뒤집혀서, 탐지 로직이
   정상인데도 오답 처리된다. 분류 성능은 실험②·③에서 따로 잰다.

실행:
    python eval/run_detection_eval.py

TODO: 데이터·스키마 확정 후 구현.
  - data/golden/ 로딩 → 케이스별 카운트 추출
  - app.detection 의 순수 함수에 카운트 직접 주입 (분류기 경유 금지)
  - 예측 ↔ golden_anomaly join → 지표 산출
  - 채점 제외 케이스는 지표에서 빼고 "오탐/미탐 성향" 리포트에만 반영
"""


def main() -> None:
    raise NotImplementedError("데이터·스키마 확정 후 구현")


if __name__ == "__main__":
    main()