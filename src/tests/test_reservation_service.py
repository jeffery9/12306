import pytest
import datetime
from sqlalchemy import select
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Reservation, OutboxEvent
from src.app.reservation_service import ReservationService

def test_reservation_success_and_overlap_refusal(db_session, event_loop):
    """Verify ReservationService can lock non-overlapping sub-routes and reject overlapping ones."""
    async def _impl():
        # 1. Seed master data: Train T1, Stations (A=1, B=2, C=3, D=4), Schedule, and 1 Seat (Carriage 1, Seat A1)
        train = Train(code="G123")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        s3 = Station(train_id=train.id, name="济南", sequence=3)
        s4 = Station(train_id=train.id, name="上海", sequence=4)
        db_session.add_all([s1, s2, s3, s4])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        # Create 3 segments: seg 1 (Seq 1->2), seg 2 (Seq 2->3), seg 3 (Seq 3->4)
        seg1 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=2, state="AVAILABLE", version=0)
        seg3 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=3, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2, seg3])
        await db_session.commit()

        # 2. Perform successful Reservation on sub-route 1->3 (北京 -> 济南, mask = 1<<0 | 1<<1 = 3)
        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_001",
            schedule_id=schedule.id,
            from_seq=1,
            to_seq=3,
            seat_class="BUSINESS"
        )
        assert res_id is not None

        # Verify database state for segment 1 & 2 are now HELD
        await db_session.commit() # Re-open a fresh session context to query
        
        # Query segments
        stmt = select(SeatSegment).where(SeatSegment.seat_id == seat.id).order_by(SeatSegment.segment_no)
        segments = (await db_session.execute(stmt)).scalars().all()
        assert segments[0].state == "HELD"
        assert segments[0].reservation_id == res_id
        assert segments[1].state == "HELD"
        assert segments[1].reservation_id == res_id
        assert segments[2].state == "AVAILABLE" # Seg 3 is untouched

        # Verify Outbox event was created
        outbox_stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == res_id)
        outbox_ev = (await db_session.execute(outbox_stmt)).scalar()
        assert outbox_ev is not None
        assert outbox_ev.event_type == "RESERVATION_HELD"
        assert outbox_ev.payload["request_id"] == "REQ_001"

        # 3. Overlapping booking refusal: Attempt booking 2->4 (天津 -> 上海, mask = 1<<1 | 1<<2 = 6)
        # This overlaps on Segment 2, so it must be rejected!
        with pytest.raises(Exception) as excinfo:
            await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id="REQ_002",
                schedule_id=schedule.id,
                from_seq=2,
                to_seq=4,
                seat_class="BUSINESS"
            )
        assert "No seats available" in str(excinfo.value)

    event_loop.run_until_complete(_impl())
