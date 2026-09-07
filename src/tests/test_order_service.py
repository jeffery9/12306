import pytest
import datetime
import asyncio
from sqlalchemy import select
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Reservation, Orders, OutboxEvent
from src.app.reservation_service import ReservationService
from src.app.order_service import OrderService
from src.app.redis_client import get_redis

def test_order_creation_payment_and_timeout_release(db_session, event_loop):
    """Verify order creation state machine, payment logic, and background timeout release."""
    async def _impl():
        # 1. Seed master data
        train = Train(code="G999")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        s3 = Station(train_id=train.id, name="上海", sequence=3)
        db_session.add_all([s1, s2, s3])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="02", seat_no="02F", seat_class="FIRST")
        db_session.add(seat)
        await db_session.flush()

        # Capture IDs before committing to prevent lazy loading MissingGreenlet errors later
        seat_id = seat.id
        schedule_id = schedule.id

        seg1 = SeatSegment(schedule_id=schedule_id, seat_id=seat_id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule_id, seat_id=seat_id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2])
        await db_session.commit()

        # 2. Reserve seat (北京 -> 天津, Seq 1 -> 2)
        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_ORD_01",
            schedule_id=schedule_id,
            from_seq=1,
            to_seq=2,
            seat_class="FIRST"
        )
        assert res_id is not None
        await db_session.commit()

        # 3. Create Order
        order_id = await OrderService.create_order(
            db_session=db_session,
            request_id="REQ_PAY_01",
            reservation_id=res_id,
            amount=150.00
        )
        assert order_id is not None
        await db_session.commit()

        # Verify Order is WAITING_PAYMENT
        stmt = select(Orders).where(Orders.id == order_id)
        order = (await db_session.execute(stmt)).scalar()
        assert order.state == "WAITING_PAYMENT"

        # 4. Pay Order and verify everything goes to CONFIRMED
        pay_ok = await OrderService.pay_order(db_session=db_session, order_id=order_id)
        assert pay_ok is True
        await db_session.commit()

        # Verify state updates
        db_session.expire_all()
        order = (await db_session.execute(select(Orders).where(Orders.id == order_id))).scalar()
        assert order.state == "CONFIRMED"

        res = (await db_session.execute(select(Reservation).where(Reservation.id == res_id))).scalar()
        assert res.state == "CONFIRMED"

        seg = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat_id, SeatSegment.segment_no == 1))).scalar()
        assert seg.state == "CONFIRMED"

        # Verify Paid outbox event
        outbox_ev = (await db_session.execute(select(OutboxEvent).where(OutboxEvent.event_type == "ORDER_PAID", OutboxEvent.aggregate_id == order_id))).scalar()
        assert outbox_ev is not None

        # 5. TEST TIMEOUT RELEASE
        # Reserve a second ticket on the second segment (天津 -> 上海, Seq 2 -> 3)
        res_id2 = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_ORD_02",
            schedule_id=schedule_id,
            from_seq=2,
            to_seq=3,
            seat_class="FIRST"
        )
        assert res_id2 is not None
        await db_session.commit()

        # Manually backdate the expires_at of Reservation 2 to make it look expired
        stmt_update = select(Reservation).where(Reservation.id == res_id2)
        res2 = (await db_session.execute(stmt_update)).scalar()
        res2.expires_at = datetime.datetime.now() - datetime.timedelta(seconds=1)
        await db_session.commit()

        # Run release of expired reservations
        released_count = await OrderService.release_expired_reservations(db_session=db_session)
        assert released_count == 1
        await db_session.commit()

        # Verify Reservation 2 is now RELEASED and segment 2 is AVAILABLE
        res2_check = (await db_session.execute(select(Reservation).where(Reservation.id == res_id2))).scalar()
        assert res2_check.state == "RELEASED"

        seg2_check = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat_id, SeatSegment.segment_no == 2))).scalar()
        assert seg2_check.state == "AVAILABLE"
        assert seg2_check.reservation_id is None

        # Verify Redis is released
        redis_client = get_redis()
        seat_key = f"r:{schedule_id}:seat:{seat_id}"
        occupied = await redis_client.get(seat_key)
        # Beijing -> Tianjin segment 1 is CONFIRMED (mask 1<<0 = 1).
        # Tianjin -> Shanghai segment 2 was RELEASED (mask 1<<1 = 2).
        # So the remaining occupied bitmap in Redis must be exactly 1!
        assert int(occupied) == 1

    event_loop.run_until_complete(_impl())
