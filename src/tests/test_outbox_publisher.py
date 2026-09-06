import pytest
import uuid
import asyncio
from sqlalchemy import select
from src.app.models import OutboxEvent
from src.app.outbox_publisher import OutboxPublisher
from aiokafka import AIOKafkaConsumer

@pytest.mark.asyncio
async def test_outbox_publishing_to_real_kafka(db_session):
    # 1. Insert a sample HELD event in outbox_event table
    event_id = str(uuid.uuid4())
    res_id = f"RES_{uuid.uuid4().hex[:8].upper()}"
    test_event = OutboxEvent(
        event_id=event_id,
        aggregate_type="RESERVATION",
        aggregate_id=res_id,
        event_type="RESERVATION_HELD",
        payload={
            "reservation_id": res_id,
            "request_id": "REQ_KAFKA_TEST",
            "schedule_id": 999, # serves as partition key
            "seat_id": 888,
            "from_segment": 1,
            "to_segment": 3,
            "mask": 3
        },
        status="NEW"
    )
    db_session.add(test_event)
    await db_session.commit()

    # 2. Run the outbox publisher to read, send, and confirm status
    # This will use the real Kafka container on localhost:9092
    published_count = await OutboxPublisher.publish_events(db_session=db_session)
    assert published_count >= 1
    await db_session.commit()

    # 3. Verify event state in the MySQL database is now "PUBLISHED" and published_at is set
    stmt = select(OutboxEvent).where(OutboxEvent.event_id == event_id)
    updated_ev = (await db_session.execute(stmt)).scalar()
    assert updated_ev.status == "PUBLISHED"
    assert updated_ev.published_at is not None

    # 4. Verify Kafka consumption: pull the message and verify the payload
    consumer = AIOKafkaConsumer(
        "ticket_events",
        bootstrap_servers="localhost:9092",
        group_id="test_consumer_group",
        auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        # Fetch message with timeout
        # We wrap in asyncio.wait_for to prevent hanging if Kafka gets delayed
        msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
        assert msg is not None
        assert msg.key == str(999).encode() # Partitioned by schedule_id (999)
        assert event_id in msg.value.decode()
    finally:
        await consumer.stop()
