import pytest
import datetime
import json
import uuid
from sqlalchemy import select
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, ProcessedEvent
from src.app.reservation_service import ReservationService
from src.app.outbox_publisher import OutboxPublisher
from src.app.projector import Projector
from src.app.redis_client import get_redis

def test_projection_recalculation_and_idempotency(db_session, event_loop):
    """Verify Projector can cold-start recalculate availability caches and safely process events idempotently."""
    async def _impl():
        # 1. Seed master data
        train = Train(code="G555")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        s3 = Station(train_id=train.id, name="济南", sequence=3)
        db_session.add_all([s1, s2, s3])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        # Create 2 Seats
        seat1 = Seat(schedule_id=schedule.id, carriage_no="03", seat_no="01A", seat_class="BUSINESS")
        seat2 = Seat(schedule_id=schedule.id, carriage_no="03", seat_no="01B", seat_class="BUSINESS")
        db_session.add_all([seat1, seat2])
        await db_session.flush()

        # Capture IDs before commit
        schedule_id = schedule.id
        seat1_id = seat1.id
        seat2_id = seat2.id

        # Create 2 segments per seat
        seg1_1 = SeatSegment(schedule_id=schedule_id, seat_id=seat1_id, segment_no=1, state="AVAILABLE", version=0)
        seg1_2 = SeatSegment(schedule_id=schedule_id, seat_id=seat1_id, segment_no=2, state="AVAILABLE", version=0)
        seg2_1 = SeatSegment(schedule_id=schedule_id, seat_id=seat2_id, segment_no=1, state="AVAILABLE", version=0)
        seg2_2 = SeatSegment(schedule_id=schedule_id, seat_id=seat2_id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1_1, seg1_2, seg2_1, seg2_2])
        await db_session.commit()

        # Run projector cold-start recalculate to initialize Redis query cache
        # This should show 2 seats available for ALL sub-routes: 1->2, 1->3, 2->3
        await Projector.recalculate_and_project(db_session=db_session, schedule_id=schedule_id)
        
        redis_client = get_redis()
        cache_key = f"q:availability:{schedule_id}:BUSINESS"
        
        val_1_2 = await redis_client.hget(cache_key, "1-2")
        val_1_3 = await redis_client.hget(cache_key, "1-3")
        val_2_3 = await redis_client.hget(cache_key, "2-3")
        assert int(val_1_2) == 2
        assert int(val_1_3) == 2
        assert int(val_2_3) == 2

        # 2. Simulate booking Seat 1 for route 1->2 (北京 -> 天津, seq 1->2)
        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_PROJ_01",
            schedule_id=schedule_id,
            from_seq=1,
            to_seq=2,
            seat_class="BUSINESS",
            passenger_ids=["PSG_001"]
        )
        await db_session.commit()

        # Retrieve the RESERVATION_HELD event from Outbox
        published_count = await OutboxPublisher.publish_events(db_session=db_session)
        assert published_count == 1
        await db_session.commit()

        # 3. Simulate consuming and projecting that event via mock payload
        # Let's verify that consumer_name + event_id idempotency is honored
        event_id = str(uuid.uuid4())
        event_body = {
            "event_id": event_id,
            "aggregate_type": "RESERVATION",
            "aggregate_id": res_id,
            "event_type": "RESERVATION_HELD",
            "payload": {
                "reservation_id": res_id,
                "schedule_id": schedule_id,
                "seat_class": "BUSINESS"
            }
        }

        # Consume event first time (recalculates Redis)
        processed_ok = await Projector.process_event(db_session=db_session, event=event_body)
        assert processed_ok is True
        await db_session.commit()

        # Let's inspect Redis query cache now:
        # Seat 1 is booked for 1->2 (Beijing->Tianjin).
        # Seat 2 is fully available (Beijing->Shanghai).
        # For route 1->2: Seat 1 is booked, Seat 2 is available -> count = 1
        # For route 1->3: Seat 1 has Segment 1 booked, Seat 2 is available -> count = 1
        # For route 2->3: Segment 2 is available on both -> count = 2
        val_1_2 = await redis_client.hget(cache_key, "1-2")
        val_1_3 = await redis_client.hget(cache_key, "1-3")
        val_2_3 = await redis_client.hget(cache_key, "2-3")
        assert int(val_1_2) == 1
        assert int(val_1_3) == 1
        assert int(val_2_3) == 2

        # 4. Consume the exact same event ID again to verify Idempotency table
        # This should bypass processing and return False
        processed_again = await Projector.process_event(db_session=db_session, event=event_body)
        assert processed_again is False
        await db_session.commit()

        # Verify ProcessedEvent entry was saved
        stmt = select(ProcessedEvent).where(ProcessedEvent.event_id == event_id)
        pe = (await db_session.execute(stmt)).scalar()
        assert pe is not None

    event_loop.run_until_complete(_impl())
