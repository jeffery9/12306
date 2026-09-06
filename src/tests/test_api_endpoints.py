import pytest
import datetime
import httpx
import json
import asyncio
import uuid
from sqlalchemy import select
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment
from src.app.redis_client import get_redis
from src.app.outbox_publisher import OutboxPublisher
from aiokafka import AIOKafkaConsumer
from src.app.projector import Projector

@pytest.mark.asyncio
async def test_full_api_workflow(db_session):
    # 1. Seed master data
    train = Train(code="G888")
    db_session.add(train)
    await db_session.flush()

    s1 = Station(train_id=train.id, name="北京", sequence=1)
    s2 = Station(train_id=train.id, name="天津", sequence=2)
    s3 = Station(train_id=train.id, name="上海", sequence=3)
    db_session.add_all([s1, s2, s3])

    schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 1), status="ACTIVE")
    db_session.add(schedule)
    await db_session.flush()

    seat = Seat(schedule_id=schedule.id, carriage_no="04", seat_no="04F", seat_class="BUSINESS")
    db_session.add(seat)
    await db_session.flush()

    seat_id = seat.id
    schedule_id = schedule.id

    seg1 = SeatSegment(schedule_id=schedule_id, seat_id=seat_id, segment_no=1, state="AVAILABLE", version=0)
    seg2 = SeatSegment(schedule_id=schedule_id, seat_id=seat_id, segment_no=2, state="AVAILABLE", version=0)
    db_session.add_all([seg1, seg2])
    await db_session.commit()

    from src.app.main import app

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        # 2. Query availability: cold start (should Miss & auto-calculate, returning 1)
        resp = await ac.get(
            "/api/v1/query",
            params={
                "schedule_id": schedule_id,
                "from_station_seq": 1,
                "to_station_seq": 3,
                "seat_class": "BUSINESS"
            }
        )
        assert resp.status_code == 200
        assert resp.json() == {"available_seats": 1}

        # 3. Reserve seat (北京 -> 天津, Seq 1 -> 2)
        resp = await ac.post(
            "/api/v1/reserve",
            json={
                "request_id": "REQ_API_01",
                "schedule_id": schedule_id,
                "from_station_seq": 1,
                "to_station_seq": 2,
                "seat_class": "BUSINESS"
            }
        )
        assert resp.status_code == 200
        res_data = resp.json()
        res_id = res_data.get("reservation_id")
        assert res_id is not None

        # 4. Create Order
        resp = await ac.post(
            "/api/v1/order",
            json={
                "request_id": "REQ_API_ORD",
                "reservation_id": res_id,
                "amount": 200.00
            }
        )
        assert resp.status_code == 200
        order_data = resp.json()
        order_id = order_data.get("order_id")
        assert order_id is not None

        # 5. Pay Order
        resp = await ac.post(
            "/api/v1/pay",
            json={
                "order_id": order_id
            }
        )
        assert resp.status_code == 200
        assert resp.json() == {"success": True}

        # 6. ASYNC COUPLING: Publish Outbox events & consume from Kafka to trigger Projector updates
        # This simulates the background Event-Driven Consumer loop
        await OutboxPublisher.publish_events(db_session=db_session)
        await db_session.commit()

        # Generate a completely unique group_id for this test run to read freshly from the earliest offset
        consumer = AIOKafkaConsumer(
            "ticket_events",
            bootstrap_servers="localhost:9092",
            group_id=f"api_test_group_{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest"
        )
        await consumer.start()
        try:
            # Sentinel loop: Keep consuming and projecting until the final ORDER_PAID event is successfully projected
            while True:
                msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
                event_data = json.loads(msg.value.decode("utf-8"))
                
                # Start a fresh transaction snapshot to read committed changes from other sessions
                await db_session.rollback()
                db_session.expire_all()
                
                await Projector.process_event(db_session=db_session, event=event_data)
                
                # Commit ProcessedEvent and release transaction locks
                await db_session.commit()
                
                # Check if this is the final ORDER_PAID event of our specific order
                if event_data.get("event_type") == "ORDER_PAID" and event_data.get("aggregate_id") == order_id:
                    break
        finally:
            await consumer.stop()

        # 7. Query availability again: segment 1 is booked, so Beijing to Shanghai (1->3) should be 0
        resp = await ac.get(
            "/api/v1/query",
            params={
                "schedule_id": schedule_id,
                "from_station_seq": 1,
                "to_station_seq": 3,
                "seat_class": "BUSINESS"
            }
        )
        assert resp.status_code == 200
        assert resp.json() == {"available_seats": 0}
