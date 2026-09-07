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
            seat_class="BUSINESS",
            passenger_ids=["PSG_001"]
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
                seat_class="BUSINESS",
                passenger_ids=["PSG_002"]
            )
        assert "No seats available" in str(excinfo.value)

    event_loop.run_until_complete(_impl())

def test_waitlist_and_auto_fulfillment(db_session, event_loop):
    """Verify that when seats are sold out, users can join the waitlist, and when seats are released, waitlist automatically gets fulfilled."""
    async def _impl():
        # 1. Seed Train, Stations (1 to 4), Schedule, and 1 Seat (Carriage 1, Seat A1)
        train = Train(code="G888")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        s3 = Station(train_id=train.id, name="济南", sequence=3)
        s4 = Station(train_id=train.id, name="上海", sequence=4)
        db_session.add_all([s1, s2, s3, s4])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 11, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        # Create 3 segments (Beijing->Tianjin, Tianjin->Jinan, Jinan->Shanghai)
        seg1 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=2, state="AVAILABLE", version=0)
        seg3 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=3, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2, seg3])
        await db_session.commit()

        # 2. Book the entire route (1->4) so that no seats are left!
        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_LONG_001",
            schedule_id=schedule.id,
            from_seq=1,
            to_seq=4,
            seat_class="BUSINESS",
            passenger_ids=["PSG_001"]
        )
        assert res_id is not None
        await db_session.commit()

        # 3. Attempting another booking (1->2) must fail with Exception (sold out)
        with pytest.raises(Exception):
            await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id="REQ_SHORT_002",
                schedule_id=schedule.id,
                from_seq=1,
                to_seq=2,
                seat_class="BUSINESS",
                passenger_ids=["PSG_002"]
            )

        # 4. Submit a Waitlist request for 1->2 instead!
        wl_id = await ReservationService.submit_waitlist(
            db_session=db_session,
            request_id="REQ_WAIT_001",
            schedule_id=schedule.id,
            from_seq=1,
            to_seq=2,
            seat_class="BUSINESS",
            passenger_ids=["PSG_002"]
        )
        assert wl_id is not None

        # Verify Waitlist record state in DB is QUEUED
        from src.app.models import Waitlist
        wl_stmt = select(Waitlist).where(Waitlist.id == wl_id)
        wl_item = (await db_session.execute(wl_stmt)).scalar()
        assert wl_item is not None
        assert wl_item.state == "QUEUED"

        # 5. Simulate expiration of the reservation by backdating its expires_at
        update_stmt = select(Reservation).where(Reservation.id == res_id)
        res_record = (await db_session.execute(update_stmt)).scalar()
        res_record.expires_at = datetime.datetime.now() - datetime.timedelta(hours=1)
        await db_session.commit()

        # 6. Trigger release of expired reservations! This must release SeatSegment 1->4 AND trigger auto-fulfillment!
        from src.app.order_service import OrderService
        released_count = await OrderService.release_expired_reservations(db_session=db_session)
        assert released_count == 1
        await db_session.commit()

        # 7. Check the Waitlist record status. It must be automatically updated to SUCCESS!
        wl_item_refreshed = (await db_session.execute(wl_stmt)).scalar()
        assert wl_item_refreshed.state == "SUCCESS"

        # 8. Check SeatSegments. Segments 1->2 (which the waitlist wanted) should now be in HELD state!
        segments_stmt = select(SeatSegment).where(SeatSegment.seat_id == seat.id).order_by(SeatSegment.segment_no)
        segments = (await db_session.execute(segments_stmt)).scalars().all()
        assert segments[0].state == "HELD"  # Claimed by Waitlist auto-fulfillment!
        assert segments[1].state == "AVAILABLE" # Rest of route is released and free!
        assert segments[2].state == "AVAILABLE"

    event_loop.run_until_complete(_impl())


def test_collision_guard_blocks_duplicate(db_session, event_loop):
    async def _impl():
        # Setup Master Data (Train, stations, schedule, seat and Passenger)
        from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Passenger
        train = Train(code="G999")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        s3 = Station(train_id=train.id, name="上海", sequence=3)
        db_session.add_all([s1, s2, s3])

        sched = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 12, 1), status="ACTIVE")
        db_session.add(sched)
        await db_session.flush()

        seat = Seat(schedule_id=sched.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        seat2 = Seat(schedule_id=sched.id, carriage_no="01", seat_no="01B", seat_class="BUSINESS")
        db_session.add_all([seat, seat2])
        await db_session.flush()

        seg1_1 = SeatSegment(schedule_id=sched.id, seat_id=seat.id, segment_no=1, state="AVAILABLE")
        seg1_2 = SeatSegment(schedule_id=sched.id, seat_id=seat.id, segment_no=2, state="AVAILABLE")
        seg2_1 = SeatSegment(schedule_id=sched.id, seat_id=seat2.id, segment_no=1, state="AVAILABLE")
        seg2_2 = SeatSegment(schedule_id=sched.id, seat_id=seat2.id, segment_no=2, state="AVAILABLE")
        db_session.add_all([seg1_1, seg1_2, seg2_1, seg2_2])

        p1 = Passenger(id="PSG_CO_01", name="碰撞甲", id_no="110101199012019999", passenger_type="ADULT")
        db_session.add(p1)
        await db_session.commit()

        # Buy 1st ticket for PSG_CO_01
        res1 = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_CO_1",
            schedule_id=sched.id,
            from_seq=1,
            to_seq=3,
            seat_class="BUSINESS",
            passenger_ids=["PSG_CO_01"]
        )
        assert res1 is not None

        # Buy 2nd ticket for PSG_CO_01 on the SAME schedule should raise Collision exception!
        with pytest.raises(Exception) as exc:
            await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id="REQ_CO_2",
                schedule_id=sched.id,
                from_seq=1,
                to_seq=2,
                seat_class="BUSINESS",
                passenger_ids=["PSG_CO_01"]
            )
        assert "conflicting booking" in str(exc.value)

    event_loop.run_until_complete(_impl())


def test_waitlist_realname_flow(db_session, event_loop):
    async def _impl():
        # Setup Train, stations, schedule, seat, Passenger
        from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Passenger, Waitlist
        train = Train(code="G777")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        db_session.add_all([s1, s2])

        sched = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 12, 1), status="ACTIVE")
        db_session.add(sched)
        await db_session.flush()

        seat = Seat(schedule_id=sched.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        seg = SeatSegment(schedule_id=sched.id, seat_id=seat.id, segment_no=1, state="AVAILABLE")
        db_session.add(seg)

        p1 = Passenger(id="PSG_WL_01", name="候补张", id_no="110101199012018888", passenger_type="ADULT")
        db_session.add(p1)
        await db_session.commit()

        # 1. Book the only seat (1->2) to sell out the train
        res1 = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_WL_BUY",
            schedule_id=sched.id,
            from_seq=1,
            to_seq=2,
            seat_class="BUSINESS",
            passenger_ids=["PSG_WL_01"]
        )
        assert res1 is not None
        await db_session.commit()

        # 2. Submit waitlist for same passenger (PSG_WL_01) - should block due to Collision Guard!
        with pytest.raises(Exception) as exc:
            await ReservationService.submit_waitlist(
                db_session=db_session,
                request_id="REQ_WL_SUB_FAIL",
                schedule_id=sched.id,
                from_seq=1,
                to_seq=2,
                seat_class="BUSINESS",
                passenger_ids=["PSG_WL_01"]
            )
        assert "conflicting booking" in str(exc.value)

    event_loop.run_until_complete(_impl())

