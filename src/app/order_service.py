import datetime
import uuid
import logging
from collections import defaultdict
from sqlalchemy import select
from src.app.models import Reservation, Orders, SeatSegment, OutboxEvent
from src.app.redis_client import get_redis, release_seat_lua

logger = logging.getLogger(__name__)

class OrderService:
    base_rate = 1.2  # Base dynamic tariff rate in yuan per km

    @classmethod
    def set_base_rate(cls, rate: float):
        cls.base_rate = rate

    @classmethod
    async def create_order(
        cls,
        db_session,
        request_id: str,
        reservation_id: str,
        amount: float
    ) -> str:
        # 1. Fetch reservation
        stmt = select(Reservation).where(Reservation.id == reservation_id)
        res = (await db_session.execute(stmt)).scalar()
        
        if not res:
            raise Exception("Reservation not found")
        if res.state != "HELD":
            raise Exception(f"Reservation is in invalid state: {res.state}")
        if res.expires_at < datetime.datetime.now():
            raise Exception("Reservation has already expired")

        # Dynamic Pricing calculation if amount is passed as 0
        if amount == 0:
            distance = (res.to_segment - res.from_segment) * 120
            amount = round(cls.base_rate * distance, 2)

        # 2. Create Order
        order_id = f"ORD_{uuid.uuid4().hex[:16].upper()}"
        order = Orders(
            id=order_id,
            request_id=request_id,
            reservation_id=reservation_id,
            state="WAITING_PAYMENT",
            total_amount=amount,
            expires_at=res.expires_at
        )
        db_session.add(order)

        # 3. Create Outbox Event
        outbox_event = OutboxEvent(
            event_id=str(uuid.uuid4()),
            aggregate_type="ORDER",
            aggregate_id=order_id,
            event_type="ORDER_CREATED",
            payload={
                "order_id": order_id,
                "reservation_id": reservation_id,
                "request_id": request_id,
                "total_amount": str(amount),
                "expires_at": res.expires_at.isoformat()
            },
            status="NEW"
        )
        db_session.add(outbox_event)
        
        await db_session.flush()
        return order_id

    @staticmethod
    async def pay_order(db_session, order_id: str) -> bool:
        # 1. Lock the order with FOR UPDATE
        stmt = select(Orders).where(Orders.id == order_id).with_for_update()
        order = (await db_session.execute(stmt)).scalar()

        if not order:
            raise Exception("Order not found")
        if order.state != "WAITING_PAYMENT":
            raise Exception(f"Order is in invalid state: {order.state}")
        if order.expires_at < datetime.datetime.now():
            # Mark expired
            order.state = "EXPIRED"
            await db_session.flush()
            raise Exception("Order has already expired")

        # 2. Update order state to CONFIRMED
        order.state = "CONFIRMED"

        # 3. Fetch and Lock reservation
        res_stmt = select(Reservation).where(Reservation.id == order.reservation_id).with_for_update()
        res = (await db_session.execute(res_stmt)).scalar()
        if not res or res.state != "HELD":
            raise Exception("Reservation was already released or invalidated")
        res.state = "CONFIRMED"

        # 4. Fetch and Lock ALL seat segments for this reservation ID (handles group bookings cleanly)
        seg_stmt = select(SeatSegment).where(
            SeatSegment.schedule_id == res.schedule_id,
            SeatSegment.reservation_id == res.id
        ).with_for_update().order_by(SeatSegment.seat_id.asc(), SeatSegment.segment_no.asc())
        
        segments = (await db_session.execute(seg_stmt)).scalars().all()
        for seg in segments:
            seg.state = "CONFIRMED"
            seg.version += 1

        # 5. Insert outbox event
        outbox_event = OutboxEvent(
            event_id=str(uuid.uuid4()),
            aggregate_type="ORDER",
            aggregate_id=order_id,
            event_type="ORDER_PAID",
            payload={
                "order_id": order_id,
                "reservation_id": res.id,
                "schedule_id": res.schedule_id,
                "from_segment": res.from_segment,
                "to_segment": res.to_segment
            },
            status="NEW"
        )
        db_session.add(outbox_event)

        await db_session.flush()
        return True

    @staticmethod
    async def release_expired_reservations(db_session) -> int:
        now = datetime.datetime.now()
        
        # 1. Fetch expired reservations that are still HELD
        stmt = select(Reservation).where(
            Reservation.state == "HELD",
            Reservation.expires_at < now
        )
        expired_reservations = (await db_session.execute(stmt)).scalars().all()

        redis_client = get_redis()
        released_count = 0
        released_schedule_ids = set()

        for res in expired_reservations:
            # 2. Lock the Reservation row
            lock_res_stmt = select(Reservation).where(Reservation.id == res.id).with_for_update()
            locked_res = (await db_session.execute(lock_res_stmt)).scalar()
            
            # Double check if state is still HELD (concurrency guard)
            if not locked_res or locked_res.state != "HELD":
                continue

            # 3. Lock all associated SeatSegments for this reservation ID ASC
            seg_stmt = select(SeatSegment).where(
                SeatSegment.schedule_id == locked_res.schedule_id,
                SeatSegment.reservation_id == locked_res.id
            ).with_for_update().order_by(SeatSegment.seat_id.asc(), SeatSegment.segment_no.asc())
            
            segments = (await db_session.execute(seg_stmt)).scalars().all()

            # 4. Release database segment and reservation records
            for seg in segments:
                seg.state = "AVAILABLE"
                seg.reservation_id = None
                seg.version += 1

            locked_res.state = "RELEASED"

            # 5. Cancel any matching Orders that are still WAITING_PAYMENT
            order_stmt = select(Orders).where(
                Orders.reservation_id == locked_res.id,
                Orders.state == "WAITING_PAYMENT"
            ).with_for_update()
            associated_order = (await db_session.execute(order_stmt)).scalar()
            if associated_order:
                associated_order.state = "EXPIRED"

            # 6. Group released segments by seat_id and release Redis masks via Lua
            seat_masks = defaultdict(int)
            for seg in segments:
                seat_masks[seg.seat_id] |= (1 << (seg.segment_no - 1))

            for s_id, mask in seat_masks.items():
                seat_key = f"r:{locked_res.schedule_id}:seat:{s_id}"
                res_key = f"r:{locked_res.schedule_id}:reservation:{locked_res.id}"

                await release_seat_lua(
                    redis_conn=redis_client,
                    seat_key=seat_key,
                    res_key=res_key,
                    mask=mask,
                    res_id=locked_res.id
                )

            # 7. Add Outbox released event
            outbox_event = OutboxEvent(
                event_id=str(uuid.uuid4()),
                aggregate_type="RESERVATION",
                aggregate_id=locked_res.id,
                event_type="RESERVATION_RELEASED",
                payload={
                    "reservation_id": locked_res.id,
                    "schedule_id": locked_res.schedule_id,
                    "from_segment": locked_res.from_segment,
                    "to_segment": locked_res.to_segment,
                    "seat_masks": dict(seat_masks)
                },
                status="NEW"
            )
            db_session.add(outbox_event)

            released_schedule_ids.add(locked_res.schedule_id)
            released_count += 1

        if released_count > 0:
            await db_session.flush()
            # Trigger waitlist auto-fulfillment for each impacted train schedule
            from src.app.reservation_service import ReservationService
            for sched_id in released_schedule_ids:
                await ReservationService.auto_fulfill_waitlist(db_session, sched_id)

        return released_count
