from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any

from kafka import KafkaConsumer

# 프로젝트 루트 경로 추가 (app 및 scripts 모듈 참조)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.classification.service import ClassificationService
from app.core import constants
from app.core.schemas import ClassifiedItem, Sentiment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ClassificationWorker")


class KafkaClassificationWorker:
    def __init__(self) -> None:
        self.service = ClassificationService()
        self.consumer: KafkaConsumer | None = None
        self.is_running = True

    def start(self) -> None:
        """Kafka Consumer 생성 및 폴링 루프 가동"""
        self.consumer = KafkaConsumer(
            constants.KAFKA_TOPIC_VOC_RAW,
            bootstrap_servers=constants.KAFKA_BOOTSTRAP_SERVERS,
            group_id=constants.KAFKA_GROUP_CLASSIFICATION_WORKER,
            enable_auto_commit=False,  # 수동 오프셋 커밋 (Manual Commit)
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=constants.KAFKA_POLL_TIMEOUT_MS,
        )
        logger.info("[WORKER STARTED] scripts/classification_worker.py (kafka-python 기반) 가동 완료")

        # Graceful Shutdown 시그널 처리
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

        self.poll_loop()

    def poll_loop(self) -> None:
        """Message Polling Loop"""
        buffer: list[dict[str, Any]] = []

        while self.is_running:
            try:
                # consumer.poll()을 통해 배치 데이터 폴링
                msg_pack = self.consumer.poll(
                    timeout_ms=constants.KAFKA_POLL_TIMEOUT_MS,
                    max_records=constants.BATCH_SIZE
                )

                for tp, messages in msg_pack.items():
                    for msg in messages:
                        buffer.append(msg.value)

                # 배치 임계치 도달 또는 폴링 메시지 부재 시 파이프라인 처리
                if len(buffer) >= constants.BATCH_SIZE or (buffer and not msg_pack):
                    self.process_batch(buffer)
                    buffer.clear()

            except Exception as e:
                logger.error(f"[POLL ERROR] {str(e)}", exc_info=True)

        # Graceful Shutdown 시 버퍼 잔여 메시지 처리
        if buffer:
            logger.info(f"[CLEANUP] 잔여 데이터 {len(buffer)}건 최종 처리")
            self.process_batch(buffer)
            buffer.clear()

        if self.consumer:
            self.consumer.close()
            logger.info("[WORKER STOPPED] Kafka Consumer가 안전하게 종료되었습니다.")

    def process_batch(self, batch: list[dict[str, Any]]) -> None:
        """LLM 배치 추론 및 DB 트랜잭션, 오프셋 커밋 수행"""
        if not batch:
            return

        logger.info(f"[BATCH PIPELINE] {len(batch)}건 배치 처리 시작")

        # 1. LLM 비동기 추론 동기 실행
        classified_items: list[ClassifiedItem] = asyncio.run(
            self.service.classify_batch(batch)
        )

        # 2. DB UPDATE/INSERT 트랜잭션 동기 실행
        asyncio.run(self.save_classified_items_to_db(classified_items))

        # 3. Kafka Offset 수동 커밋 (Manual Commit)
        self.consumer.commit()
        logger.info(f"[BATCH COMPLETE] {len(classified_items)}건 처리 및 Offset Commit 완료")

    async def save_classified_items_to_db(self, items: list[ClassifiedItem]) -> None:
        """
        비즈니스 규약:
        1건 문의에 여러 부정 aspect 존재 시 각각 +1 카운트 저장하되,
        총 문의 분모(total_voc_count)는 1건으로 집계
        """
        for item in items:
            # TODO: 분모(total_voc_count) 1회 증가 DB 쿼리 실행
            # await db.increment_total_voc(item.product_group_id, item.channel, item.created_at)

            negative_aspects = [
                asp for asp in item.aspects if asp.sentiment == Sentiment.NEGATIVE
            ]
            for asp in negative_aspects:
                # TODO: aspect별 부정 카운트 UPSERT 수행
                # await db.upsert_aspect_negative_count(
                #     product_group_id=item.product_group_id,
                #     channel=item.channel,
                #     aspect=asp.aspect,
                #     created_at=item.created_at
                # )
                pass

    def request_shutdown(self, signum: int, frame: Any) -> None:
        """종료 시그널 수신 처리"""
        logger.info(f"[SHUTDOWN SIGNAL] 시그널({signum}) 수신 - 워커 종료 절차 개시")
        self.is_running = False


if __name__ == "__main__":
    worker = KafkaClassificationWorker()
    worker.start()