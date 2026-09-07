import pytest
import datetime
import uuid
import json
import asyncio
from pytest_bdd import scenarios, given, when, then, parsers
from sqlalchemy import select
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment
from src.app.reservation_service import ReservationService
from src.app.order_service import OrderService
from src.app.outbox_publisher import OutboxPublisher
from src.app.projector import Projector
from src.app.redis_client import get_redis
from aiokafka import AIOKafkaConsumer

# Bind scenarios individually to allow advanced specs to reside in the feature file as specifications
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

@pytest.fixture
def bdd_context():
    """Shared contextual dictionary across BDD steps."""
    return {}

# --- SCENARIO 1: Successful sub-route seat reservation ---

@given(parsers.parse('a clean ticketing system with train "{code}" and Stations "{s1}", "{s2}", "{s3}"'))
def clean_system(bdd_context, db_session, event_loop, code, s1, s2, s3):
    async def _impl():
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
        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id=f"BDD_REQ_{uuid.uuid4().hex[:8].upper()}",
            schedule_id=schedule_id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS"
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


# --- SCENARIO 2: Reject overlapping sub-route booking ---

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
        res_id = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id=f"BDD_ALREADY_REQ_{uuid.uuid4().hex[:8].upper()}",
            schedule_id=schedule.id,
            from_seq=f,
            to_seq=t,
            seat_class="BUSINESS"
        )
        bdd_context["res_id"] = res_id
        await db_session.commit()

    event_loop.run_until_complete(_impl())

@when(parsers.parse('another passenger attempts to reserve a ticket from sequence {f:d} to {t:d}'))
def passenger_attempts_non_overlapping(bdd_context, db_session, event_loop, f, t):
    async def _impl():
        schedule_id = bdd_context["schedule_id"]
        try:
            res_id = await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id=f"BDD_NON_OVERLAP_{uuid.uuid4().hex[:8].upper()}",
                schedule_id=schedule_id,
                from_seq=f,
                to_seq=t,
                seat_class="BUSINESS"
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
            res_id = await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id=f"BDD_OVERLAP_{uuid.uuid4().hex[:8].upper()}",
                schedule_id=schedule_id,
                from_seq=f,
                to_seq=t,
                seat_class="BUSINESS"
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


# --- SCENARIO 3: Payment confirmation triggers eventual consistency ---

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
    mysql_seg_marked(bdd_context, db_session, event_loop, seg_no, state)

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
