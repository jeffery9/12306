import json
import datetime
import logging
from sqlalchemy import select
from src.app.models import OutboxEvent
from src.app.kafka_client import get_kafka_producer

logger = logging.getLogger(__name__)

# Track active producer start status globally
_producer_started = False

class OutboxPublisher:
    @staticmethod
    async def publish_events(db_session) -> int:
        global _producer_started

        # 1. Grab up to 100 NEW outbox events using high-concurrency FOR UPDATE SKIP LOCKED
        stmt = select(OutboxEvent).where(
            OutboxEvent.status == "NEW"
        ).with_for_update(skip_locked=True).limit(100)

        events = (await db_session.execute(stmt)).scalars().all()
        if not events:
            return 0

        # 2. Lazy start the global Kafka producer safely
        producer = await get_kafka_producer()
        if not _producer_started:
            try:
                await producer.start()
                _producer_started = True
            except Exception as e:
                # Handle connection issues or already started errors gracefully
                logger.warning(f"Producer start bypass or error: {e}")
                _producer_started = True

        published_count = 0

        # 3. Ship events to Kafka topic "ticket_events" with strict partitioning
        for event in events:
            try:
                # Extract partition key: schedule_id from reservation or order payload
                payload = event.payload
                schedule_id = payload.get("schedule_id", 0)
                
                # Serialized partitioning key ensuring in-order delivery per train schedule
                partition_key = str(schedule_id).encode()

                # Build full structural message
                message_body = {
                    "event_id": event.event_id,
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": event.aggregate_id,
                    "event_type": event.event_type,
                    "payload": payload,
                    "created_at": event.created_at.isoformat() if event.created_at else None
                }

                # Publish message and wait for confirmation
                await producer.send_and_wait(
                    topic="ticket_events",
                    key=partition_key,
                    value=json.dumps(message_body).encode("utf-8")
                )

                # Update status in local transaction
                event.status = "PUBLISHED"
                event.published_at = datetime.datetime.now()
                published_count += 1

            except Exception as e:
                logger.error(f"Failed to publish event {event.event_id} to Kafka: {e}")
                # We do not crash the publisher loop; other events can still be sent
                # On transactional flush, only successfully updated ones will go to PUBLISHED

        if published_count > 0:
            await db_session.flush()

        return published_count
