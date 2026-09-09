import pytest
import datetime
import asyncio
from sqlalchemy import select
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Reservation, Orders, OutboxEvent, Passenger, Ticket
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
            seat_class="FIRST",
            passenger_ids=["PSG_001"]
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
            seat_class="FIRST",
            passenger_ids=["PSG_002"]
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

def test_order_refund_and_pool_recovery(db_session, event_loop):
    """Verify refund flow releases database segments, restores Redis ticket pool, and updates state."""
    async def _impl():
        # 1. Seed master data
        train = Train(code="G888")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        s3 = Station(train_id=train.id, name="上海", sequence=3)
        db_session.add_all([s1, s2, s3])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 2), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="03", seat_no="03A", seat_class="FIRST")
        db_session.add(seat)
        await db_session.flush()

        seat_id = seat.id
        schedule_id = schedule.id

        seg1 = SeatSegment(schedule_id=schedule_id, seat_id=seat_id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule_id, seat_id=seat_id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2])
        await db_session.commit()

        # 2. Reserve seat (北京 -> 上海, Seq 1 -> 3, full-trip)
        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_REFUND_01",
            schedule_id=schedule_id,
            from_seq=1,
            to_seq=3,
            seat_class="FIRST",
            passenger_ids=["PSG_001"]
        )
        assert res_id is not None

        # 3. Create order
        order_id = await OrderService.create_order(
            db_session=db_session,
            request_id="REQ_REFUND_ORD_01",
            reservation_id=res_id,
            amount=150.0
        )
        assert order_id is not None

        # 4. Pay order (Confirm)
        success = await OrderService.pay_order(db_session=db_session, order_id=order_id)
        assert success is True
        await db_session.commit()

        # Verify initial occupied state in Redis (mask 3)
        redis_client = get_redis()
        seat_key = f"r:{schedule_id}:seat:{seat_id}"
        occupied_before = await redis_client.get(seat_key)
        assert int(occupied_before) == 3

        # 5. Perform Refund
        refund_res = await OrderService.refund_order(db_session=db_session, order_id=order_id)
        assert refund_res["success"] is True
        await db_session.commit()

        # 6. Verify Database states
        order_check = (await db_session.execute(select(Orders).where(Orders.id == order_id))).scalar()
        assert order_check.state == "REFUNDED"

        res_check = (await db_session.execute(select(Reservation).where(Reservation.id == res_id))).scalar()
        assert res_check.state == "RELEASED"

        segs_check = (await db_session.execute(select(SeatSegment).where(SeatSegment.reservation_id == res_id))).scalars().all()
        assert len(segs_check) == 0  # Since reservation_id is set to None on release!

        seg1_check = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat_id, SeatSegment.segment_no == 1))).scalar()
        assert seg1_check.state == "AVAILABLE"
        assert seg1_check.reservation_id is None

        seg2_check = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat_id, SeatSegment.segment_no == 2))).scalar()
        assert seg2_check.state == "AVAILABLE"
        assert seg2_check.reservation_id is None

        # 7. Verify Redis state is fully cleared (0)
        occupied_after = await redis_client.get(seat_key)
        assert int(occupied_after) == 0

    event_loop.run_until_complete(_impl())

def test_order_partial_refund_and_tier_handling_fee(db_session, event_loop):
    """Verify partial refund logic deletes only the targeted passenger's ticket and calculates 5% fee."""
    async def _impl():
        # 1. Seed Train, Stations, and TrainSchedule (Departure 2 days / 48 hours later)
        train = Train(code="G777_PARTIAL")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        s3 = Station(train_id=train.id, name="上海", sequence=3)
        db_session.add_all([s1, s2, s3])
        await db_session.flush()

        dep_date = datetime.date.today() + datetime.timedelta(days=2)
        schedule = TrainSchedule(
            train_id=train.id,
            service_date=dep_date,
            status="ACTIVE"
        )
        db_session.add(schedule)
        await db_session.flush()
        schedule_id = schedule.id

        # 2. Seed 2 adjacent Seats & Segments
        seat1 = Seat(schedule_id=schedule_id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        seat2 = Seat(schedule_id=schedule_id, carriage_no="01", seat_no="01C", seat_class="BUSINESS")
        db_session.add_all([seat1, seat2])
        await db_session.flush()
        seat1_id = seat1.id
        seat2_id = seat2.id

        seg1_1 = SeatSegment(schedule_id=schedule_id, seat_id=seat1.id, segment_no=1, state="AVAILABLE", version=0)
        seg1_2 = SeatSegment(schedule_id=schedule_id, seat_id=seat1.id, segment_no=2, state="AVAILABLE", version=0)
        seg2_1 = SeatSegment(schedule_id=schedule_id, seat_id=seat2.id, segment_no=1, state="AVAILABLE", version=0)
        seg2_2 = SeatSegment(schedule_id=schedule_id, seat_id=seat2.id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1_1, seg1_2, seg2_1, seg2_2])

        # 3. Seed 2 Passengers
        p1 = Passenger(id="PSG_PARTIAL_01", name="退票甲", id_no="110101199012019901", passenger_type="ADULT")
        p2 = Passenger(id="PSG_PARTIAL_02", name="乘车乙", id_no="110101199012019902", passenger_type="ADULT")
        db_session.add_all([p1, p2])
        await db_session.commit()

        # 4. Reserve seats for BOTH passengers (Multi-passenger merge Reservation)
        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_PARTIAL_RES",
            schedule_id=schedule_id,
            from_seq=1,
            to_seq=3,
            seat_class="BUSINESS",
            passenger_ids=["PSG_PARTIAL_01", "PSG_PARTIAL_02"]
        )
        assert res_id is not None

        # 5. Create Order & Complete Payment
        order_id = await OrderService.create_order(
            db_session=db_session,
            request_id="REQ_PARTIAL_ORD",
            reservation_id=res_id,
            amount=200.00
        )
        assert order_id is not None

        pay_success = await OrderService.pay_order(db_session=db_session, order_id=order_id)
        assert pay_success is True
        await db_session.commit()

        # Verify Redis before partial refund (seats 1 and 2 occupied, masks equal 3 each)
        redis_client = get_redis()
        s1_key = f"r:{schedule_id}:seat:{seat1.id}"
        s2_key = f"r:{schedule_id}:seat:{seat2.id}"
        assert int(await redis_client.get(s1_key)) == 3
        assert int(await redis_client.get(s2_key)) == 3

        # 6. Perform Partial Refund of PSG_PARTIAL_01
        # Departure date is in 2 days -> rate is 5% -> Handling Fee: 5.0 yuan, Refund Cash: 95.00 yuan
        refund_res = await OrderService.refund_order(db_session=db_session, order_id=order_id, passenger_id="PSG_PARTIAL_01")
        assert refund_res["success"] is True
        assert refund_res["handling_fee"] == 5.00
        assert refund_res["refund_amount"] == 95.00
        assert refund_res["remaining_order_amount"] == 100.00
        await db_session.commit()

        # 7. Assertions:
        # - Target Passenger 01's ticket is deleted
        # - Target Seat segments are set back to AVAILABLE
        # - Passenger 02's ticket remains CONFIRMED and unchanged
        db_session.expire_all()
        t1_check = (await db_session.execute(select(Ticket).where(Ticket.passenger_id == "PSG_PARTIAL_01"))).scalar()
        assert t1_check is None

        t2_check = (await db_session.execute(select(Ticket).where(Ticket.passenger_id == "PSG_PARTIAL_02"))).scalar()
        assert t2_check is not None
        assert float(t2_check.price) == 100.00

        # Seat 1 should be fully AVAILABLE
        s1_segs = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat1_id))).scalars().all()
        for seg in s1_segs:
            assert seg.state == "AVAILABLE"
            assert seg.reservation_id is None

        # Seat 2 should remain CONFIRMED and associated with original reservation
        s2_segs = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat2_id))).scalars().all()
        for seg in s2_segs:
            assert seg.state == "CONFIRMED"
            assert seg.reservation_id == res_id

        # Redis verification
        assert int(await redis_client.get(s1_key)) == 0  # seat1 fully released
        assert int(await redis_client.get(s2_key)) == 3  # seat2 remains occupied

    event_loop.run_until_complete(_impl())

def test_order_atomic_reschedule_and_savepoint_rollback(db_session, event_loop):
    """Verify atomic rescheduling transitions tickets cleanly and rolls back on failure via Savepoints."""
    async def _impl():
        # 1. Seed Train, Station, Schedule G111 (Old Train)
        t_old = Train(code="G111_OLD")
        db_session.add(t_old)
        await db_session.flush()

        os1 = Station(train_id=t_old.id, name="北京", sequence=1)
        os2 = Station(train_id=t_old.id, name="天津", sequence=2)
        os3 = Station(train_id=t_old.id, name="上海", sequence=3)
        db_session.add_all([os1, os2, os3])
        await db_session.flush()

        sched_old = TrainSchedule(train_id=t_old.id, service_date=datetime.date.today(), status="ACTIVE")
        db_session.add(sched_old)
        await db_session.flush()

        seat_old = Seat(schedule_id=sched_old.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        db_session.add(seat_old)
        await db_session.flush()

        oseg1 = SeatSegment(schedule_id=sched_old.id, seat_id=seat_old.id, segment_no=1, state="AVAILABLE", version=0)
        oseg2 = SeatSegment(schedule_id=sched_old.id, seat_id=seat_old.id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([oseg1, oseg2])

        # 2. Seed Train, Station, Schedule G222 (New Target Train)
        t_new = Train(code="G222_NEW")
        db_session.add(t_new)
        await db_session.flush()

        ns1 = Station(train_id=t_new.id, name="北京", sequence=1)
        ns2 = Station(train_id=t_new.id, name="天津", sequence=2)
        ns3 = Station(train_id=t_new.id, name="上海", sequence=3)
        db_session.add_all([ns1, ns2, ns3])
        await db_session.flush()

        sched_new = TrainSchedule(train_id=t_new.id, service_date=datetime.date.today(), status="ACTIVE")
        db_session.add(sched_new)
        await db_session.flush()

        seat_new = Seat(schedule_id=sched_new.id, carriage_no="01", seat_no="01F", seat_class="BUSINESS")
        db_session.add(seat_new)
        await db_session.flush()

        nseg1 = SeatSegment(schedule_id=sched_new.id, seat_id=seat_new.id, segment_no=1, state="AVAILABLE", version=0)
        nseg2 = SeatSegment(schedule_id=sched_new.id, seat_id=seat_new.id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([nseg1, nseg2])

        # 3. Seed Passenger
        passenger = Passenger(id="PSG_RS_01", name="改签哥", id_no="110101199012019909", passenger_type="ADULT")
        blocker = Passenger(id="PSG_RS_BLOCKER", name="占位帝", id_no="110101199012019999", passenger_type="ADULT")
        db_session.add_all([passenger, blocker])
        await db_session.commit()

        # 4. Reserve G111 & Pay
        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_RS_RES_OLD",
            schedule_id=sched_old.id,
            from_seq=1,
            to_seq=3,
            seat_class="BUSINESS",
            passenger_ids=["PSG_RS_01"]
        )
        assert res_id is not None

        order_id = await OrderService.create_order(
            db_session=db_session,
            request_id="REQ_RS_ORD_OLD",
            reservation_id=res_id,
            amount=150.00
        )
        assert order_id is not None

        pay_success = await OrderService.pay_order(db_session=db_session, order_id=order_id)
        assert pay_success is True
        await db_session.commit()

        # Retrieve the original ticket ID
        t_stmt = select(Ticket).where(Ticket.reservation_id == res_id)
        orig_ticket = (await db_session.execute(t_stmt)).scalar()
        ticket_id = orig_ticket.id

        # Verify G111 Redis is occupied
        redis_client = get_redis()
        old_seat_key = f"r:{sched_old.id}:seat:{seat_old.id}"
        new_seat_key = f"r:{sched_new.id}:seat:{seat_new.id}"
        assert int(await redis_client.get(old_seat_key)) == 3
        assert await redis_client.get(new_seat_key) is None

        # Save seats scalars to avoid lazy loading issues
        seat_old_id = seat_old.id
        seat_new_id = seat_new.id
        sched_old_id = sched_old.id
        sched_new_id = sched_new.id

        # 5. Success Reschedule Case
        res_res = await OrderService.reschedule_ticket(
            db_session=db_session,
            ticket_id=ticket_id,
            new_schedule_id=sched_new.id,
            new_seat_class="BUSINESS"
        )
        assert res_res["success"] is True
        assert res_res["new_ticket_id"] == ticket_id
        assert res_res["new_seat_no"] == "01F"
        await db_session.commit()

        # Verify old G111 is released and G222 is occupied
        db_session.expire_all()
        assert int(await redis_client.get(old_seat_key)) == 0  # old seat is 100% free!
        assert int(await redis_client.get(new_seat_key)) == 3  # new seat is 100% occupied!

        # G111 segments set to AVAILABLE
        old_segs_check = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat_old_id))).scalars().all()
        for seg in old_segs_check:
            assert seg.state == "AVAILABLE"
            assert seg.reservation_id is None

        # G222 segments set to CONFIRMED
        new_segs_check = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat_new_id))).scalars().all()
        for seg in new_segs_check:
            assert seg.state == "CONFIRMED"
            assert seg.reservation_id is not None

        # 6. Failure / Rollback Savepoint Case (Try to reschedule G222 back to G111 but let's make G111 sold out!)
        # Let's lock the only seat on G111 for another passenger to make it unavailable
        res_id_g111_block = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_RS_BLOCK",
            schedule_id=sched_old_id,
            from_seq=1,
            to_seq=3,
            seat_class="BUSINESS",
            passenger_ids=["PSG_RS_BLOCKER"] # This is just to lock G111
        )
        await db_session.commit()

        # Now try to reschedule G222 ticket back to G111! It should FAIL because G111 is sold out.
        # This will verify the nested Savepoint rolls back G222 ticket cleanly!
        with pytest.raises(Exception, match="No seats available"):
            await OrderService.reschedule_ticket(
                db_session=db_session,
                ticket_id=ticket_id,
                new_schedule_id=sched_old_id,
                new_seat_class="BUSINESS"
            )

        # Ensure G222 is STILL occupied and NOT released due to rollback!
        assert int(await redis_client.get(new_seat_key)) == 3

    event_loop.run_until_complete(_impl())

