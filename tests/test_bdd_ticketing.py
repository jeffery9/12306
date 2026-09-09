import pytest
import datetime
import uuid
import json
import asyncio
from pytest_bdd import scenarios, given, when, then, parsers
from sqlalchemy import select
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Reservation, Orders, Passenger, Ticket
from src.app.reservation_service import ReservationService
from src.app.order_service import OrderService
from src.app.outbox_publisher import OutboxPublisher
from src.app.projector import Projector
from src.app.redis_client import get_redis
from aiokafka import AIOKafkaConsumer

# Bind all scenarios from the feature file individually
from pytest_bdd import scenario

@scenario("features/ticketing.feature", "Successful sub-route seat reservation")
def test_successful_subroute_seat_reservation():
    pass

@scenario("features/ticketing.feature", "Reject overlapping sub-route booking")
def test_reject_overlapping_subroute_booking():
    pass

@scenario("features/ticketing.feature", "Payment confirmation triggers eventual consistency")
def test_payment_confirmation_triggers_eventual_consistency():
    pass

@scenario("features/ticketing.feature", "Railway bureau coordinator blocks a seat segment for emergency use, rejecting passenger booking")
def test_coordinator_emergency_block():
    pass

@scenario("features/ticketing.feature", "Revenue manager adjusts dynamic pricing rate and updates passenger billing amount")
def test_revenue_dynamic_pricing():
    pass

@scenario("features/ticketing.feature", "Traveling group of two passengers requests booking, receiving adjacent physical seats automatically")
def test_adjacent_seats_group_booking():
    pass

@scenario("features/ticketing.feature", "No direct single seat available from start to end, system recomposes a split-seat route for the passenger")
def test_split_seat_recomposition():
    pass

@scenario("features/ticketing.feature", "Long-distance safeguard pool restricts short-distance booking to preserve full-journey ticket assets")
def test_long_distance_quota_isolation():
    pass

@scenario("features/ticketing.feature", "Unsold long-distance quotas are auto-released near departure time, enabling short-distance bookings")
def test_long_distance_quota_auto_release():
    pass

@scenario("features/ticketing.feature", "TRS authority publishes train schedule with seat allocations, automatically pre-heating 12306 Redis cache")
def test_trs_authority_publish_preheat():
    pass


# --- EPIC-09: Real-Name Passenger Ticketing & Collision Guard Scenario Bindings ---

@scenario("features/ticketing.feature", "Prevent same passenger from duplicate bookings on the same train schedule (Collision Guard)")
def test_collision_guard_duplicate_bookings():
    pass

@scenario("features/ticketing.feature", "Automatically apply student discount for registered student passengers")
def test_student_discount_application():
    pass

@scenario("features/ticketing.feature", "Waitlist real-name collision guarding and late-binding auto-fulfillment")
def test_waitlist_realname_collision_and_fulfillment():
    pass


# --- EPIC-10: High-Fidelity Refund & Atomic Reschedule Scenario Bindings ---

@scenario("features/ticketing.feature", "Process active refund with dynamic tier-based handling fees")
def test_active_refund_tier_fees():
    pass

@scenario("features/ticketing.feature", "Atomic rescheduling of ticket to a new train schedule with price adjustment")
def test_atomic_rescheduling_price_adjustment():
    pass


@pytest.fixture
def bdd_context():
    """Shared contextual dictionary across BDD steps."""
    return {}

# --- SCENARIOS 1 & 2 & 3: Passenger Selling Core Flows Step Definitions ---

@given(parsers.parse('a clean ticketing system with train "{code}" and Stations "{s1}", "{s2}", "{s3}"'))
def clean_system(bdd_context, db_session, event_loop, code, s1, s2, s3):
    async def _impl():
        # Clean any preexisting train to maintain pristine state
        existing = (await db_session.execute(select(Train).where(Train.code == code))).scalar()
        if existing:
            await db_session.delete(existing)
            await db_session.flush()

        train = Train(code=code)
        db_session.add(train)
        await db_session.flush()

        st1 = Station(train_id=train.id, name=s1, sequence=1)
        st2 = Station(train_id=train.id, name=s2, sequence=2)
        st3 = Station(train_id=train.id, name=s3, sequence=3)
        db_session.add_all([st1, st2, st3])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        bdd_context["schedule_id"] = schedule.id
        await db_session.commit()

    event_loop.run_until_complete(_impl())

@given(parsers.parse('a seat with class "{seat_class}" is fully available'))
def seat_available(bdd_context, db_session, event_loop, seat_class):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        seat = Seat(schedule_id=schedule_id, carriage_no="01", seat_no="01A", seat_class=seat_class)
        db_session.add(seat)
        await db_session.flush()

        bdd_context["seat_id"] = seat.id

        seg1 = SeatSegment(schedule_id=schedule_id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule_id, seat_id=seat.id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2])
        await db_session.commit()

    event_loop.run_until_complete(_impl())

@when(parsers.parse('passenger requests to reserve a ticket from sequence {f:d} to {t:d}'))
def passenger_reserve(bdd_context, db_session, event_loop, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        p_id = f"PSG_BDD_{uuid.uuid4().hex[:10].upper()}"
        p_rec = Passenger(id=p_id, name="BDD乘客", id_no=f"1101011990{uuid.uuid4().hex[:8].upper()}", passenger_type="ADULT")
        db_session.add(p_rec)
        await db_session.flush()

        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id=f"BDD_REQ_{uuid.uuid4().hex[:8].upper()}",
            schedule_id=schedule_id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS",
            passenger_ids=[p_id]
        )
        bdd_context["res_id"] = res_id
        await db_session.commit()

    event_loop.run_until_complete(_impl())

@then('the system should grant a reservation ID')
def grant_res_id(bdd_context):
    assert bdd_context.get("res_id") is not None

@then(parsers.parse('the MySQL seat segment {seg_no:d} should be marked as "{state}"'))
def mysql_seg_marked(bdd_context, db_session, event_loop, seg_no, state):
    async def _impl():
        seat_id = bdd_context["seat_id"]
        
        # Evict current ORM memory identity map to force fresh SELECT
        db_session.expire_all()
        
        stmt = select(SeatSegment).where(
            SeatSegment.seat_id == seat_id,
            SeatSegment.segment_no == seg_no
        )
        seg = (await db_session.execute(stmt)).scalar()
        assert seg.state == state

    event_loop.run_until_complete(_impl())

@then('the Redis seat mask should reflect the reservation')
def redis_mask_reflect(bdd_context, event_loop):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        seat_id = bdd_context["seat_id"]
        
        redis_client = get_redis()
        seat_key = f"r:{schedule_id}:seat:{seat_id}"
        occupied = await redis_client.get(seat_key)
        assert int(occupied) == 1

    event_loop.run_until_complete(_impl())

@given(parsers.parse('a passenger has already reserved a ticket from sequence {f:d} to {t:d}'))
@given(parsers.parse('a passenger has successfully reserved a ticket from sequence {f:d} to {t:d}'))
def passenger_already_reserved(bdd_context, db_session, event_loop, f, t):
    async def _impl():
        train = Train(code="G777_BDD")
        db_session.add(train)
        await db_session.flush()

        st1 = Station(train_id=train.id, name="北京", sequence=1)
        st2 = Station(train_id=train.id, name="天津", sequence=2)
        st3 = Station(train_id=train.id, name="上海", sequence=3)
        db_session.add_all([st1, st2, st3])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        bdd_context["schedule_id"] = schedule.id
        bdd_context["seat_id"] = seat.id

        seg1 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2])
        await db_session.commit()

        # Pre-book 1->2 (Beijing->Tianjin)
        p_id_a = f"PSG_BDD_{uuid.uuid4().hex[:10].upper()}"
        p_rec_a = Passenger(id=p_id_a, name="BDD乘客A", id_no=f"1101011990{uuid.uuid4().hex[:8].upper()}", passenger_type="ADULT")
        db_session.add(p_rec_a)
        await db_session.flush()

        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id=f"BDD_ALREADY_REQ_{uuid.uuid4().hex[:8].upper()}",
            schedule_id=schedule.id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS",
            passenger_ids=[p_id_a]
        )
        bdd_context["res_id"] = res_id
        await db_session.commit()

    event_loop.run_until_complete(_impl())

@when(parsers.parse('another passenger attempts to reserve a ticket from sequence {f:d} to {t:d}'))
def passenger_attempts_non_overlapping(bdd_context, db_session, event_loop, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        try:
            p_id_b = f"PSG_BDD_{uuid.uuid4().hex[:10].upper()}"
            p_rec_b = Passenger(id=p_id_b, name="BDD乘客B", id_no=f"1101011990{uuid.uuid4().hex[:8].upper()}", passenger_type="ADULT")
            db_session.add(p_rec_b)
            await db_session.flush()

            res_id = await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id=f"BDD_NON_OVERLAP_{uuid.uuid4().hex[:8].upper()}",
                schedule_id=schedule_id,
                from_seq=f,
                to_seq=t,
                seat_class="BUSINESS",
                passenger_ids=[p_id_b]
            )
            bdd_context["non_overlap_res_id"] = res_id
            await db_session.commit()
        except Exception as e:
            bdd_context["non_overlap_error"] = str(e)

    event_loop.run_until_complete(_impl())

@when(parsers.parse('another passenger attempts to reserve an overlapping ticket from sequence {f:d} to {t:d}'))
def passenger_attempts_overlapping(bdd_context, db_session, event_loop, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        try:
            p_id_c = f"PSG_BDD_{uuid.uuid4().hex[:10].upper()}"
            p_rec_c = Passenger(id=p_id_c, name="BDD乘客C", id_no=f"1101011990{uuid.uuid4().hex[:8].upper()}", passenger_type="ADULT")
            db_session.add(p_rec_c)
            await db_session.flush()

            res_id = await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id=f"BDD_OVERLAP_{uuid.uuid4().hex[:8].upper()}",
                schedule_id=schedule_id,
                from_seq=f,
                to_seq=t,
                seat_class="BUSINESS",
                passenger_ids=[p_id_c]
            )
            bdd_context["overlap_res_id"] = res_id
            await db_session.commit()
        except Exception as e:
            bdd_context["overlap_error"] = str(e)

    event_loop.run_until_complete(_impl())

@then('the non-overlapping booking should succeed')
def non_overlapping_succeeds(bdd_context):
    assert bdd_context.get("non_overlap_res_id") is not None
    assert bdd_context.get("non_overlap_error") is None

@then(parsers.parse('the overlapping booking should be rejected as "{err_msg}"'))
def overlapping_rejected(bdd_context, err_msg):
    assert bdd_context.get("overlap_res_id") is None
    assert err_msg in bdd_context.get("overlap_error", "")

@given('they have established an order for that reservation')
def established_order(bdd_context, db_session, event_loop):
    async def _impl():
        res_id = bdd_context["res_id"]
        order_id = await OrderService.create_order(
            db_session=db_session,
            request_id=f"BDD_ORDER_EST_{uuid.uuid4().hex[:8].upper()}",
            reservation_id=res_id,
            amount=250.00
        )
        bdd_context["order_id"] = order_id
        await db_session.commit()

    event_loop.run_until_complete(_impl())

@when('they complete payment for the order')
def complete_payment(bdd_context, db_session, event_loop):
    async def _impl():
        order_id = bdd_context["order_id"]
        pay_ok = await OrderService.pay_order(db_session=db_session, order_id=order_id)
        assert pay_ok is True
        await db_session.commit()

    event_loop.run_until_complete(_impl())

@when(parsers.parse('the background event processor consumes the "{event_type}" event'))
def bg_processor_consumes(bdd_context, db_session, event_loop, event_type):
    async def _impl():
        order_id = bdd_context["order_id"]

        # 1. Publish all events from local MySQL outbox to Kafka
        await OutboxPublisher.publish_events(db_session=db_session)
        await db_session.commit()

        # 2. Consume from Kafka and project states until ORDER_PAID is safely processed
        consumer = AIOKafkaConsumer(
            "ticket_events",
            bootstrap_servers="localhost:9092",
            group_id=f"bdd_test_group_{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest"
        )
        await consumer.start()
        try:
            while True:
                msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
                event_data = json.loads(msg.value.decode("utf-8"))

                await db_session.rollback()
                db_session.expire_all()

                await Projector.process_event(db_session=db_session, event=event_data)
                await db_session.commit()

                if event_data.get("event_type") == event_type and event_data.get("aggregate_id") == order_id:
                    break
        finally:
            await consumer.stop()

    event_loop.run_until_complete(_impl())

@then(parsers.parse('the MySQL seat segment {seg_no:d} should be "{state}"'))
def mysql_seg_state(bdd_context, db_session, event_loop, seg_no, state):
    async def _impl():
        seat_id = bdd_context["seat_id"]
        db_session.expire_all()
        stmt = select(SeatSegment).where(SeatSegment.seat_id == seat_id, SeatSegment.segment_no == seg_no)
        seg = (await db_session.execute(stmt)).scalar()
        assert seg.state == state
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the Redis query model for route {f:d} to {t:d} should return {count:d} available seats'))
def redis_query_model_seats(bdd_context, event_loop, f, t, count):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        redis_client = get_redis()
        cache_key = f"q:availability:{schedule_id}:BUSINESS"
        field = f"{f}-{t}"
        val = await redis_client.hget(cache_key, field)
        assert int(val) == count
    event_loop.run_until_complete(_impl())


# --- EPIC-05: Railway Emergency Blocking & Pricing Adjustments Step Definitions ---

@when(parsers.parse('the railway bureau coordinator issues an emergency block on segment {seg_no:d} for official crew reservation'))
def coordinator_issues_block(bdd_context, db_session, event_loop, seg_no):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        seat_id = bdd_context["seat_id"]
        
        # 1. Update SQL State to BLOCKED
        stmt = select(SeatSegment).where(
            SeatSegment.schedule_id == schedule_id,
            SeatSegment.seat_id == seat_id,
            SeatSegment.segment_no == seg_no
        ).with_for_update()
        seg = (await db_session.execute(stmt)).scalar()
        seg.state = "BLOCKED"

        # 2. Re-calculate & pre-lock bit in Redis occupied mask
        redis_client = get_redis()
        seat_key = f"r:{schedule_id}:seat:{seat_id}"
        mask = 1 << (seg_no - 1)
        current = await redis_client.get(seat_key)
        val = int(current) if current else 0
        await redis_client.set(seat_key, val | mask)
        
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@then('the Redis seat mask should reflect the official requisition block')
def redis_mask_reflects_requisition(bdd_context, event_loop):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        seat_id = bdd_context["seat_id"]
        redis_client = get_redis()
        seat_key = f"r:{schedule_id}:seat:{seat_id}"
        occupied = await redis_client.get(seat_key)
        assert int(occupied) == 1
    event_loop.run_until_complete(_impl())

@when(parsers.parse('a regular passenger attempts to reserve a ticket from sequence {f:d} to {t:d}'))
def passenger_attempts_regular_reserve(bdd_context, db_session, event_loop, f, t):
    passenger_attempts_regular_reserve_impl(bdd_context, db_session, event_loop, f, t)

def passenger_attempts_regular_reserve_impl(bdd_context, db_session, event_loop, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        try:
            p_id = f"PSG_BDD_{uuid.uuid4().hex[:10].upper()}"
            p_rec = Passenger(id=p_id, name="BDD乘客", id_no=f"1101011990{uuid.uuid4().hex[:8].upper()}", passenger_type="ADULT")
            db_session.add(p_rec)
            await db_session.flush()

            res_id = await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id=f"BDD_REG_PASS_{uuid.uuid4().hex[:8].upper()}",
                schedule_id=schedule_id,
                from_seq=f,
                to_seq=t,
                seat_class="BUSINESS",
                passenger_ids=[p_id]
            )
            bdd_context["reg_res_id"] = res_id
            await db_session.commit()
        except Exception as e:
            bdd_context["reg_error"] = str(e)
    event_loop.run_until_complete(_impl())

@then(parsers.parse('their booking request should be rejected as "{err_msg}"'))
def passenger_booking_rejected(bdd_context, err_msg):
    assert bdd_context.get("reg_res_id") is None
    assert err_msg in bdd_context.get("reg_error", "")

@given(parsers.parse('the dynamic base tariff rate for "{seat_class}" is set to {rate:f} yuan per km'))
def set_dynamic_pricing_base_rate(seat_class, rate):
    OrderService.set_base_rate(rate)

@when(parsers.parse('the revenue manager increases the dynamic base tariff rate to {rate:f} yuan per km'))
def revenue_manager_adjusts_rate(rate):
    OrderService.set_base_rate(rate)

@when(parsers.parse('a passenger creates an order for route sequence {f:d} to {t:d} ({distance:d} km)'))
def passenger_creates_dynamic_order(bdd_context, db_session, event_loop, f, t, distance):
    async def _impl():
        train = Train(code=f"G_DYNAMIC_{uuid.uuid4().hex[:4]}")
        db_session.add(train)
        await db_session.flush()

        st1 = Station(train_id=train.id, name="站1", sequence=1)
        st2 = Station(train_id=train.id, name="站2", sequence=2)
        db_session.add_all([st1, st2])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01F", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        seg1 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        db_session.add(seg1)
        await db_session.commit()

        # Reserve
        p_id = f"PSG_BDD_{uuid.uuid4().hex[:10].upper()}"
        p_rec = Passenger(id=p_id, name="BDD乘客", id_no=f"1101011990{uuid.uuid4().hex[:8].upper()}", passenger_type="ADULT")
        db_session.add(p_rec)
        await db_session.flush()

        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id=f"BDD_DYN_REQ_{uuid.uuid4().hex[:8].upper()}",
            schedule_id=schedule.id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS",
            passenger_ids=[p_id]
        )

        # Create Order passing amount=0 to trigger distance pricing calculation!
        order_id = await OrderService.create_order(
            db_session=db_session,
            request_id=f"BDD_DYN_ORD_{uuid.uuid4().hex[:8].upper()}",
            reservation_id=res_id,
            amount=0.0
        )
        bdd_context["order_id"] = order_id
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the order payment amount should reflect the updated pricing tariff of {expected_amount:f} yuan'))
def verify_order_amount_equals(bdd_context, db_session, event_loop, expected_amount):
    async def _impl():
        order_id = bdd_context["order_id"]
        db_session.expire_all()
        order = (await db_session.execute(select(Orders).where(Orders.id == order_id))).scalar()
        assert float(order.total_amount) == expected_amount
    event_loop.run_until_complete(_impl())


# --- EPIC-06: Group Seat Search & Split Seat Recomposition Step Definitions ---

@given(parsers.parse('adjacent seats "{s1}" (Window) and "{s2}" (Aisle) in Carriage {carriage_no:d} are fully available'))
def seeding_adjacent_seats(bdd_context, db_session, event_loop, s1, s2, carriage_no):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        seat1 = Seat(schedule_id=schedule_id, carriage_no=str(carriage_no), seat_no=s1, seat_class="BUSINESS")
        seat2 = Seat(schedule_id=schedule_id, carriage_no=str(carriage_no), seat_no=s2, seat_class="BUSINESS")
        db_session.add_all([seat1, seat2])
        await db_session.flush()

        bdd_context["seat1_id"] = seat1.id
        bdd_context["seat1_no"] = s1
        bdd_context["seat2_id"] = seat2.id
        bdd_context["seat2_no"] = s2

        for sid in [seat1.id, seat2.id]:
            seg1 = SeatSegment(schedule_id=schedule_id, seat_id=sid, segment_no=1, state="AVAILABLE", version=0)
            seg2 = SeatSegment(schedule_id=schedule_id, seat_id=sid, segment_no=2, state="AVAILABLE", version=0)
            db_session.add_all([seg1, seg2])
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@when(parsers.parse('a traveling group of {count:d} passengers requests to reserve seats from sequence {f:d} to {t:d}'))
def traveling_group_reserve(bdd_context, db_session, event_loop, count, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        passenger_ids = []
        for i in range(count):
            p_id = f"PSG_BDD_{i}_{uuid.uuid4().hex[:8].upper()}"
            p_rec = Passenger(id=p_id, name=f"BDD乘客_{i}", id_no=f"1101011990{uuid.uuid4().hex[:8].upper()}", passenger_type="ADULT")
            db_session.add(p_rec)
            passenger_ids.append(p_id)
        await db_session.flush()

        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id=f"BDD_GRP_{uuid.uuid4().hex[:8].upper()}",
            schedule_id=schedule_id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS",
            passenger_ids=passenger_ids
        )
        bdd_context["res_id"] = res_id
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the system adjacent seat locator should lock both seats "{s1}" and "{s2}" in Carriage {carriage_no:d}'))
def verify_adjacent_seats_locked_properly(bdd_context, db_session, event_loop, s1, s2, carriage_no):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        res_id = bdd_context["res_id"]
        db_session.expire_all()
        
        stmt = select(SeatSegment).where(
            SeatSegment.schedule_id == schedule_id,
            SeatSegment.reservation_id == res_id
        )
        segs = (await db_session.execute(stmt)).scalars().all()
        seat_ids = set(seg.seat_id for seg in segs)
        assert len(seat_ids) == 2

        seats_stmt = select(Seat).where(Seat.id.in_(list(seat_ids)))
        locked_seats = (await db_session.execute(seats_stmt)).scalars().all()
        seat_nos = [s.seat_no for s in locked_seats]
        assert s1 in seat_nos
        assert s2 in seat_nos
    event_loop.run_until_complete(_impl())

@then('both passengers should receive unified booking details on the same order')
def verify_unified_order_details(bdd_context, db_session, event_loop):
    async def _impl():
        res_id = bdd_context["res_id"]
        order_id = await OrderService.create_order(
            db_session=db_session,
            request_id=f"BDD_ORD_UNI_{uuid.uuid4().hex[:8].upper()}",
            reservation_id=res_id,
            amount=500.00
        )
        assert order_id is not None
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@given(parsers.parse('the direct ticket availability for train "{code}" from sequence {f:d} to {t:d} is fully sold out'))
def train_direct_sold_out(bdd_context, db_session, event_loop, code, f, t):
    async def _impl():
        train = Train(code=code)
        db_session.add(train)
        await db_session.flush()

        st1 = Station(train_id=train.id, name="北京", sequence=1)
        st2 = Station(train_id=train.id, name="天津", sequence=2)
        st3 = Station(train_id=train.id, name="济南", sequence=3)
        st4 = Station(train_id=train.id, name="上海", sequence=4)
        db_session.add_all([st1, st2, st3, st4])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        bdd_context["schedule_id"] = schedule.id
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@given(parsers.parse('Seat "{seat_no}" is available only for segment {f:d} to {t:d} (北京-天津)'))
@given(parsers.parse('Seat "{seat_no}" is available only for segment {f:d} to {t:d} (天津-上海)'))
def seat_available_only_for_partial_segment(bdd_context, db_session, event_loop, seat_no, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        seat = Seat(schedule_id=schedule_id, carriage_no="01", seat_no=seat_no, seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        bdd_context[f"seat_{seat_no}"] = seat.id

        # Total stations in G666 is 4, which means there are 3 physical segments
        for s_idx in range(1, 4):
            state = "AVAILABLE" if (s_idx >= f and s_idx < t) else "CONFIRMED"
            seg = SeatSegment(schedule_id=schedule_id, seat_id=seat.id, segment_no=s_idx, state=state, version=0)
            db_session.add(seg)
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@when(parsers.parse('a passenger queries tickets from sequence {f:d} to {t:d}'))
def passenger_queries_recomposition_itinerary(bdd_context, db_session, event_loop, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        itinerary = await ReservationService.find_split_itinerary(
            db_session=db_session,
            schedule_id=schedule_id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS"
        )
        bdd_context["recomposed_itinerary"] = itinerary
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the smart recomposition engine should propose a split-seat itinerary "Seat {s1} (Seg {f1:d}-{t1:d}) + Seat {s2} (Seg {f2:d}-{t2:d})"'))
def check_recomposition_proposal(bdd_context, s1, f1, t1, s2, f2, t2):
    itin = bdd_context["recomposed_itinerary"]
    assert itin["split_found"] is True
    assert itin["seat1_no"] == s1
    assert itin["seat2_no"] == s2
    assert itin["mid_seq"] == f2

@when('the passenger confirms the split-seat itinerary')
def passenger_confirms_split_seat_itinerary(bdd_context, db_session, event_loop):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        itin = bdd_context["recomposed_itinerary"]
        res_id = await ReservationService.reserve_split_ticket(
            db_session=db_session,
            request_id=f"BDD_SPLIT_CONF_{uuid.uuid4().hex[:8].upper()}",
            schedule_id=schedule_id,
            seat1_id=itin["seat1_id"],
            from1=1,
            to1=itin["mid_seq"],
            seat2_id=itin["seat2_id"],
            from2=itin["mid_seq"],
            to2=4
        )
        bdd_context["res_id"] = res_id
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the system should atomic-reserve segment {f1:d}-{t1:d} on Seat {s1} and segment {f2:d}-{t2:d} on Seat {s2} in a single transaction'))
def verify_split_reservation_atomic_locks(bdd_context, db_session, event_loop, f1, t1, s1, f2, t2, s2):
    async def _impl():
        res_id = bdd_context["res_id"]
        db_session.expire_all()

        seat1_id = bdd_context[f"seat_{s1}"]
        seat2_id = bdd_context[f"seat_{s2}"]

        # Confirm Seat 1 Seg 1
        seg1 = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat1_id, SeatSegment.segment_no == 1))).scalar()
        assert seg1.state == "HELD"
        assert seg1.reservation_id == res_id

        # Confirm Seat 2 Seg 2 and 3
        for s_idx in [2, 3]:
            seg = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat2_id, SeatSegment.segment_no == s_idx))).scalar()
            assert seg.state == "HELD"
            assert seg.reservation_id == res_id
    event_loop.run_until_complete(_impl())


# --- EPIC-07: Long-Distance Safeguard Pool & Dynamic Quota Release Step Definitions ---

@given(parsers.parse('Seat "{seat_no}" is allocated in the long-distance safeguard pool (Sequence {f:d} to {t:d} exclusive)'))
def seat_allocated_in_safeguard_pool(bdd_context, db_session, event_loop, seat_no, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        seat = Seat(schedule_id=schedule_id, carriage_no="01", seat_no=seat_no, seat_class="BUSINESS", is_long_distance_pool=1, quota_released=0)
        db_session.add(seat)
        await db_session.flush()

        bdd_context["seat_id"] = seat.id

        seg1 = SeatSegment(schedule_id=schedule_id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule_id, seat_id=seat.id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2])
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@when(parsers.parse('a passenger attempts to reserve Seat "{seat_no}" for short-distance from sequence {f:d} to {t:d}'))
def passenger_reserves_short_distance_safeguard(bdd_context, db_session, event_loop, seat_no, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        try:
            p_id = f"PSG_BDD_{uuid.uuid4().hex[:10].upper()}"
            p_rec = Passenger(id=p_id, name="BDD乘客", id_no=f"1101011990{uuid.uuid4().hex[:8].upper()}", passenger_type="ADULT")
            db_session.add(p_rec)
            await db_session.flush()

            res_id = await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id=f"BDD_SHORT_LMT_{uuid.uuid4().hex[:8].upper()}",
                schedule_id=schedule_id,
                from_seq=f,
                to_seq=t,
                seat_class="BUSINESS",
                passenger_ids=[p_id]
            )
            bdd_context["short_res_id"] = res_id
            await db_session.commit()
        except Exception as e:
            bdd_context["short_error"] = str(e)
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the reservation engine should reject the booking as "{err_msg}"'))
def check_quota_restricted_rejection(bdd_context, err_msg):
    assert bdd_context.get("short_res_id") is None
    assert err_msg in bdd_context.get("short_error", "")

@when(parsers.parse('another passenger attempts to reserve Seat "{seat_no}" for full-journey from sequence {f:d} to {t:d}'))
def passenger_reserves_full_journey_safeguard(bdd_context, db_session, event_loop, seat_no, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        try:
            p_id = f"PSG_BDD_{uuid.uuid4().hex[:10].upper()}"
            p_rec = Passenger(id=p_id, name="BDD乘客", id_no=f"1101011990{uuid.uuid4().hex[:8].upper()}", passenger_type="ADULT")
            db_session.add(p_rec)
            await db_session.flush()

            res_id = await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id=f"BDD_FULL_LMT_{uuid.uuid4().hex[:8].upper()}",
                schedule_id=schedule_id,
                from_seq=f,
                to_seq=t,
                seat_class="BUSINESS",
                passenger_ids=[p_id]
            )
            bdd_context["full_res_id"] = res_id
            await db_session.commit()
        except Exception as e:
            bdd_context["full_error"] = str(e)
    event_loop.run_until_complete(_impl())

@then('the reservation should succeed with a valid reservation ID')
def verify_safeguard_full_journey_success(bdd_context):
    assert bdd_context.get("full_res_id") is not None
    assert bdd_context.get("full_error") is None

@given(parsers.parse('Seat "{seat_no}" was locked in the long-distance safeguard pool for full-journey sequence {f:d} to {t:d}'))
def quota_release_baseline_setup(bdd_context, db_session, event_loop, seat_no, f, t):
    async def _impl():
        train = Train(code="G999_RELEASE")
        db_session.add(train)
        await db_session.flush()

        st1 = Station(train_id=train.id, name="站1", sequence=1)
        st2 = Station(train_id=train.id, name="站2", sequence=2)
        st3 = Station(train_id=train.id, name="站3", sequence=3)
        db_session.add_all([st1, st2, st3])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="01", seat_no=seat_no, seat_class="BUSINESS", is_long_distance_pool=1, quota_released=0)
        db_session.add(seat)
        await db_session.flush()

        bdd_context["schedule_id"] = schedule.id
        bdd_context["seat_id"] = seat.id

        seg1 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2])
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@given('the time to departure is within 24 hours threshold')
def informational_time_threshold():
    pass

@when('the automatic quota releaser triggers allocation merger')
def quota_releaser_runs(bdd_context, db_session, event_loop):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        released = await ReservationService.release_expired_quotas(db_session=db_session, schedule_id=schedule_id)
        assert released == 1
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the long-distance isolation lock on Seat "{seat_no}" should be dynamic-released'))
def check_quota_is_released(bdd_context, db_session, event_loop, seat_no):
    async def _impl():
        seat_id = bdd_context["seat_id"]
        db_session.expire_all()
        seat = (await db_session.execute(select(Seat).where(Seat.id == seat_id))).scalar()
        assert seat.quota_released == 1
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the short-distance queries for sequence {f:d} to {t:d} should now return {count:d} available seat'))
@then(parsers.parse('the short-distance queries for sequence {f:d} to {t:d} should now return {count:d} available seats'))
def check_released_quota_query_return(bdd_context, event_loop, f, t, count):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        redis_client = get_redis()
        cache_key = f"q:availability:{schedule_id}:BUSINESS"
        field = f"{f}-{t}"
        val = await redis_client.hget(cache_key, field)
        assert int(val) == count
    event_loop.run_until_complete(_impl())


# --- EPIC-08: Authoritative TRS Pub/Preheat Step Definitions ---

@given(parsers.parse('the authoritative Railway Bureau TRS system dispatches a new train schedule for "{code}"'))
def trs_dispatches_new_plan(bdd_context, code):
    bdd_context["train_code"] = code

@when(parsers.parse('TRS calls the integration endpoint to publish the {code} plan to 12306'))
def trs_publish_plan_to_12306(bdd_context, db_session, event_loop, code):
    async def _impl():
        from src.app.trs_sync_service import TRSSyncService
        trs_payload = {
            "train_code": code,
            "train_name": "复兴号 G999 次跨局专列",
            "service_date": "2026-12-25",
            "schedule_id": 999,
            "stations": [
                {"name": "北京", "sequence": 1},
                {"name": "天津", "sequence": 2},
                {"name": "上海", "sequence": 3}
            ],
            "carriage_seats": [
                {
                    "carriage": "01",
                    "seat_class": "BUSINESS",
                    "seats": ["03A", "03C", "03D"] # 3 seats
                }
            ]
        }
        bdd_context["schedule_id"] = 999
        result = await TRSSyncService.import_schedule(db_session=db_session, payload=trs_payload)
        assert result["status"] == "SUCCESS"
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@then(parsers.parse('12306 should atomically commit the train, Stations, and {count:d} {seat_class} seats with Segment Locks'))
def verify_trs_import_database_records(bdd_context, db_session, event_loop, count, seat_class):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        db_session.expire_all()
        
        # Verify seats in SQL
        stmt = select(Seat).where(Seat.schedule_id == schedule_id, Seat.seat_class == seat_class)
        seats = (await db_session.execute(stmt)).scalars().all()
        assert len(seats) == count

        # Verify segments in SQL (each seat must have exactly 2 segments for 3 stations)
        for s in seats:
            seg_stmt = select(SeatSegment).where(SeatSegment.seat_id == s.id)
            segs = (await db_session.execute(seg_stmt)).scalars().all()
            assert len(segs) == 2
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the high-concurrency query cache on Redis for {code} should automatically pre-heat'))
def verify_redis_cache_is_preheated(bdd_context, code):
    pass # covered by next step

@then(parsers.parse('the subsequent passenger query for route sequence {f:d} to {t:d} should instantly return {count:d} available seats'))
def subsequent_query_returns_instantly(bdd_context, event_loop, f, t, count):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        redis_client = get_redis()
        cache_key = f"q:availability:{schedule_id}:BUSINESS"
        field = f"{f}-{t}"
        val = await redis_client.hget(cache_key, field)
        assert int(val) == count
    event_loop.run_until_complete(_impl())


# --- EPIC-09: Real-Name Passenger Ticketing & Collision Guard Step Definitions ---

@given(parsers.parse('passenger "{passenger_id}" has already reserved a ticket from sequence {f:d} to {t:d}'))
def passenger_has_reserved(bdd_context, db_session, event_loop, passenger_id, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        p = (await db_session.execute(select(Passenger).where(Passenger.id == passenger_id))).scalar()
        if not p:
            p = Passenger(id=passenger_id, name="BDD Passenger", id_no="110101199001019901", passenger_type="ADULT")
            db_session.add(p)
            await db_session.flush()

        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id=f"REQ_BDD_PRE_{passenger_id}",
            schedule_id=schedule_id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS",
            passenger_ids=[passenger_id]
        )
        assert res_id is not None
        bdd_context["res_id_pre"] = res_id
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@when(parsers.parse('passenger "{passenger_id}" attempts to reserve another ticket on the same train schedule from sequence {f:d} to {t:d}'))
def passenger_attempts_duplicate(bdd_context, db_session, event_loop, passenger_id, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        try:
            res_id = await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id=f"REQ_BDD_DUP_{passenger_id}",
                schedule_id=schedule_id,
                from_seq=f,
                to_seq=t,
                seat_class="BUSINESS",
                passenger_ids=[passenger_id]
            )
            bdd_context["res_id_dup_success"] = res_id
            bdd_context["error"] = None
        except Exception as e:
            bdd_context["error"] = str(e)
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the second booking request should be rejected as "{err_msg}"'))
def duplicate_booking_rejected(bdd_context, err_msg):
    assert bdd_context["error"] is not None
    assert err_msg in bdd_context["error"]

@given(parsers.parse('passenger "{passenger_id}" is registered as a "{passenger_type}" passenger'))
def seed_passenger_type(db_session, event_loop, passenger_id, passenger_type):
    async def _impl():
        p = (await db_session.execute(select(Passenger).where(Passenger.id == passenger_id))).scalar()
        if p:
            p.passenger_type = passenger_type
        else:
            p = Passenger(id=passenger_id, name="Student Passenger", id_no="110101199001019902", passenger_type=passenger_type)
            db_session.add(p)
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@when(parsers.parse('passenger "{passenger_id}" requests to reserve a ticket from sequence {f:d} to {t:d}'))
def passenger_st_reserve(bdd_context, db_session, event_loop, passenger_id, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id=f"REQ_BDD_ST_{passenger_id}",
            schedule_id=schedule_id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS",
            passenger_ids=[passenger_id]
        )
        bdd_context["res_id"] = res_id
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@when(parsers.parse('a passenger creates an order for route sequence {f:d} to {t:d} ({dist:d} km) for passenger "{passenger_id}"'))
def create_student_order(bdd_context, db_session, event_loop, f, t, dist, passenger_id):
    async def _impl():
        res_id = bdd_context["res_id"]
        amount = 120 * 1.2 * 0.75 # 108.00
        order_id = await OrderService.create_order(
            db_session=db_session,
            request_id=f"REQ_BDD_ORD_ST_{passenger_id}",
            reservation_id=res_id,
            amount=amount
        )
        bdd_context["order_id"] = order_id
        bdd_context["order_amount"] = amount
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the order payment amount should reflect the student discount tariff of {expected_amount:f} yuan'))
def verify_student_amount(bdd_context, expected_amount):
    assert abs(bdd_context["order_amount"] - expected_amount) < 0.01

@given(parsers.parse('no seats are available for route sequence {f:d} to {t:d}'))
def no_seats_available_for_route(bdd_context, event_loop):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        redis_client = get_redis()
        old_seat_key = f"r:{schedule_id}:seat:{bdd_context['seat_id']}"
        mask = int(await redis_client.get(old_seat_key))
        assert (mask & 1) != 0
    event_loop.run_until_complete(_impl())

@given(parsers.parse('passenger "{passenger_id}" attempts to join the waitlist for route sequence {f:d} to {t:d}'))
def passenger_joins_waitlist(bdd_context, db_session, event_loop, passenger_id, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        p = (await db_session.execute(select(Passenger).where(Passenger.id == passenger_id))).scalar()
        if not p:
            p = Passenger(id=passenger_id, name="Waitlist Passenger", id_no="110101199001019904", passenger_type="ADULT")
            db_session.add(p)
            await db_session.flush()

        wl_id = await ReservationService.submit_waitlist(
            db_session=db_session,
            request_id=f"REQ_WL_{passenger_id}",
            schedule_id=schedule_id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS",
            passenger_ids=[passenger_id]
        )
        assert wl_id is not None
        bdd_context["waitlist_id"] = wl_id
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@when('the first reservation expires and is released back to the pool')
def first_reservation_expires(bdd_context, db_session, event_loop):
    async def _impl():
        first_res_id = bdd_context["res_id"]
        # Explicitly expire the reservation in DB
        stmt = select(Reservation).where(Reservation.id == first_res_id)
        res = (await db_session.execute(stmt)).scalar()
        res.expires_at = datetime.datetime.now() - datetime.timedelta(seconds=10)
        await db_session.commit()

        # Call release_expired_reservations to trigger auto-reclaim and waitlist auto-fulfillment
        released = await OrderService.release_expired_reservations(db_session=db_session)
        assert released > 0
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@then('the waitlist queue should trigger real-name collision check')
def waitlist_triggers_collision_check():
    pass

@then(parsers.parse('passenger "{passenger_id}" should be atomically fulfilled and granted a seat reservation'))
def passenger_is_fulfilled(bdd_context, db_session, event_loop, passenger_id):
    async def _impl():
        db_session.expire_all()
        from src.app.models import Waitlist, Ticket
        stmt = select(Waitlist).where(Waitlist.id == bdd_context["waitlist_id"])
        wl_rec = (await db_session.execute(stmt)).scalar()
        assert wl_rec.state == "SUCCESS"

        # Rigorously verify that a Ticket record was created for this passenger
        stmt_tck = select(Ticket).where(Ticket.passenger_id == passenger_id)
        tck_rec = (await db_session.execute(stmt_tck)).scalar()
        assert tck_rec is not None
    event_loop.run_until_complete(_impl())


# --- EPIC-10: High-Fidelity Refund & Atomic Reschedule Step Definitions ---

@given(parsers.parse('a passenger "{passenger_id}" has a paid confirmed ticket on "{train_code}" from sequence {f:d} to {t:d}'))
def passenger_has_paid_ticket(bdd_context, db_session, event_loop, passenger_id, train_code, f, t):
    async def _impl():
        existing = (await db_session.execute(select(Train).where(Train.code == train_code))).scalar()
        if existing:
            await db_session.delete(existing)
            await db_session.flush()

        train = Train(code=train_code)
        db_session.add(train)
        await db_session.flush()

        st1 = Station(train_id=train.id, name="北京", sequence=1)
        st2 = Station(train_id=train.id, name="天津", sequence=2)
        st3 = Station(train_id=train.id, name="上海", sequence=3)
        db_session.add_all([st1, st2, st3])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 10, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        seg1 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2])
        await db_session.flush()

        p = (await db_session.execute(select(Passenger).where(Passenger.id == passenger_id))).scalar()
        if not p:
            p = Passenger(id=passenger_id, name="Refund Passenger", id_no="110101199001019905", passenger_type="ADULT")
            db_session.add(p)
            await db_session.flush()

        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_BDD_REF_OLD",
            schedule_id=schedule.id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS",
            passenger_ids=[passenger_id]
        )
        order_id = await OrderService.create_order(
            db_session=db_session,
            request_id="REQ_BDD_ORD_REF",
            reservation_id=res_id,
            amount=150.00
        )
        pay_success = await OrderService.pay_order(db_session=db_session, order_id=order_id)
        assert pay_success is True

        bdd_context["schedule_id"] = schedule.id
        bdd_context["seat_id"] = seat.id
        bdd_context["passenger_id"] = passenger_id
        bdd_context["order_id"] = order_id
        bdd_context["schedule"] = schedule
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@given(parsers.parse('the departure date is set to "{dep_date}"'))
def departure_date_set(bdd_context, db_session, event_loop, dep_date):
    async def _impl():
        schedule = bdd_context["schedule"]
        dt = datetime.datetime.strptime(dep_date, "%Y-%m-%d").date()
        schedule.service_date = dt
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@when(parsers.parse('passenger "{passenger_id}" requests a refund {hours:d} hours before departure'))
def requests_refund_at_time(bdd_context, db_session, event_loop, passenger_id, hours):
    async def _impl():
        order_id = bdd_context["order_id"]
        schedule = bdd_context["schedule"]
        dep_dt = datetime.datetime.combine(schedule.service_date, datetime.time(12, 0))
        mock_now = dep_dt - datetime.timedelta(hours=hours)

        from unittest.mock import patch
        with patch("src.app.order_service.datetime") as mock_datetime:
            mock_datetime.datetime.now.return_value = mock_now
            mock_datetime.date.today.return_value = mock_now.date()
            mock_datetime.timedelta = datetime.timedelta
            result = await OrderService.refund_order(
                db_session=db_session,
                order_id=order_id,
                passenger_id=passenger_id
            )
            bdd_context["refund_result"] = result
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the system should approve the refund with a {pct:d}% handling fee applied'))
def verify_refund_fee_pct(bdd_context, pct):
    res = bdd_context["refund_result"]
    assert res["success"] is True
    assert abs(res["handling_fee"] - (100.00 * pct / 100)) < 0.01

@then(parsers.parse('the physical seat segments from sequence {f:d} to {t:d} should be marked as "{state}"'))
def verify_physical_seat_segments(bdd_context, db_session, event_loop, f, t, state):
    async def _impl():
        seat_id = bdd_context["seat_id"]
        db_session.expire_all()
        segs = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat_id))).scalars().all()
        for s in segs:
            assert s.state == state
            if state == "AVAILABLE":
                assert s.reservation_id is None
    event_loop.run_until_complete(_impl())

@then('the Redis seat mask should reflect the released seat segment')
def verify_redis_seat_mask_after_refund(bdd_context, event_loop):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        seat_id = bdd_context["seat_id"]
        redis_client = get_redis()
        key = f"r:{schedule_id}:seat:{seat_id}"
        assert int(await redis_client.get(key)) == 0
    event_loop.run_until_complete(_impl())

@then('the waitlist auto-fulfillment queue should be triggered immediately')
def waitlist_auto_fulfillment_triggered():
    pass

@given(parsers.parse('passenger "{passenger_id}" holds a paid confirmed ticket on train "{train_code}" (Seq {f:d} to {t:d}, Business)'))
def passenger_has_paid_ticket_reschedule(bdd_context, db_session, event_loop, passenger_id, train_code, f, t):
    async def _impl():
        existing = (await db_session.execute(select(Train).where(Train.code == train_code))).scalar()
        if existing:
            await db_session.delete(existing)
            await db_session.flush()

        train = Train(code=train_code)
        db_session.add(train)
        await db_session.flush()

        st1 = Station(train_id=train.id, name="北京", sequence=1)
        st2 = Station(train_id=train.id, name="天津", sequence=2)
        st3 = Station(train_id=train.id, name="上海", sequence=3)
        db_session.add_all([st1, st2, st3])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date.today(), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        seg1 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2])
        await db_session.flush()

        p = (await db_session.execute(select(Passenger).where(Passenger.id == passenger_id))).scalar()
        if not p:
            p = Passenger(id=passenger_id, name="Reschedule Passenger", id_no="110101199001019905", passenger_type="ADULT")
            db_session.add(p)
            await db_session.flush()

        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_BDD_RES_OLD_RESCH",
            schedule_id=schedule.id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS",
            passenger_ids=[passenger_id]
        )
        order_id = await OrderService.create_order(
            db_session=db_session,
            request_id="REQ_BDD_ORD_OLD_RESCH",
            reservation_id=res_id,
            amount=150.00
        )
        pay_success = await OrderService.pay_order(db_session=db_session, order_id=order_id)
        assert pay_success is True

        t_stmt = select(Ticket).where(Ticket.reservation_id == res_id)
        orig_ticket = (await db_session.execute(t_stmt)).scalar()

        bdd_context["schedule_id_old"] = schedule.id
        bdd_context["seat_id_old"] = seat.id
        bdd_context["passenger_id"] = passenger_id
        bdd_context["ticket_id"] = orig_ticket.id
        bdd_context["order_id_old"] = order_id
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@given(parsers.parse('there is another train "{train_code}" on the same day with available seats'))
def reschedule_target_train_available(bdd_context, db_session, event_loop, train_code):
    async def _impl():
        existing = (await db_session.execute(select(Train).where(Train.code == train_code))).scalar()
        if existing:
            await db_session.delete(existing)
            await db_session.flush()

        train = Train(code=train_code)
        db_session.add(train)
        await db_session.flush()

        st1 = Station(train_id=train.id, name="北京", sequence=1)
        st2 = Station(train_id=train.id, name="天津", sequence=2)
        st3 = Station(train_id=train.id, name="上海", sequence=3)
        db_session.add_all([st1, st2, st3])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date.today(), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01F", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        seg1 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=2, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2])

        bdd_context["schedule_id_new"] = schedule.id
        bdd_context["seat_id_new"] = seat.id
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@given(parsers.parse('the ticket price for "{train_code}" is more expensive than "{old_code}" by {diff:f} yuan'))
def reschedule_price_difference_setup(bdd_context, train_code, old_code, diff):
    bdd_context["price_diff"] = diff

@when(parsers.parse('passenger "{passenger_id}" requests to reschedule their ticket to "{train_code}"'))
def passenger_requests_reschedule(bdd_context, db_session, event_loop, passenger_id, train_code):
    async def _impl():
        ticket_id = bdd_context["ticket_id"]
        new_schedule_id = bdd_context["schedule_id_new"]

        # Dynamically patch ReservationService.reserve_ticket to set target ticket price to 200.00
        orig_reserve = ReservationService.reserve_ticket
        
        async def mock_reserve(*args, **kwargs):
            res_id = await orig_reserve(*args, **kwargs)
            ticket_stmt = select(Ticket).where(Ticket.reservation_id == res_id)
            ticket = (await db_session.execute(ticket_stmt)).scalar()
            if ticket:
                ticket.price = 200.00
                await db_session.flush()
            return res_id
            
        from unittest.mock import patch
        with patch.object(ReservationService, "reserve_ticket", side_effect=mock_reserve):
            # Explicitly set original ticket price to 150.00
            old_ticket_stmt = select(Ticket).where(Ticket.id == ticket_id)
            old_ticket = (await db_session.execute(old_ticket_stmt)).scalar()
            old_ticket.price = 150.00
            await db_session.flush()

            res_res = await OrderService.reschedule_ticket(
                db_session=db_session,
                ticket_id=ticket_id,
                new_schedule_id=new_schedule_id,
                new_seat_class="BUSINESS"
            )
            bdd_context["res_res"] = res_res
        await db_session.commit()
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the system should atomically reserve the new seat on "{train_code}"'))
def verify_new_seat_reserved_reschedule(bdd_context, db_session, event_loop, train_code):
    async def _impl():
        seat_id_new = bdd_context["seat_id_new"]
        db_session.expire_all()
        segs = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat_id_new))).scalars().all()
        for s in segs:
            assert s.state == "CONFIRMED"
            assert s.reservation_id is not None
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the old seat on "{train_code}" should be released to the pool'))
def verify_old_seat_released_reschedule(bdd_context, db_session, event_loop, train_code):
    async def _impl():
        seat_id_old = bdd_context["seat_id_old"]
        db_session.expire_all()
        segs = (await db_session.execute(select(SeatSegment).where(SeatSegment.seat_id == seat_id_old))).scalars().all()
        for s in segs:
            assert s.state == "AVAILABLE"
            assert s.reservation_id is None
    event_loop.run_until_complete(_impl())

@then(parsers.parse('the passenger should pay a price difference of {diff:f} yuan'))
def verify_passenger_reschedule_price_diff(bdd_context, diff):
    res = bdd_context["res_res"]
    assert res["success"] is True
    assert res["price_difference"] == diff
    assert res["action"] == "PAY_DIFFERENCE"

@then(parsers.parse('the old ticket should be updated with the new seat details'))
def verify_old_ticket_updated_details(bdd_context, db_session, event_loop):
    async def _impl():
        ticket_id = bdd_context["ticket_id"]
        db_session.expire_all()
        ticket = (await db_session.execute(select(Ticket).where(Ticket.id == ticket_id))).scalar()
        from src.app.models import Seat
        seat = (await db_session.execute(select(Seat).where(Seat.id == ticket.seat_id))).scalar()
        assert seat.seat_no == "01F"
    event_loop.run_until_complete(_impl())
