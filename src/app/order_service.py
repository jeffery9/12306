import datetime
import uuid
import logging
from sqlalchemy import select
from src.app.models import Reservation, Orders, SeatSegment, OutboxEvent
from src.app.redis_client import get_redis, release_seat_lua

logger = logging.getLogger(__name__)

class OrderService:
    @staticmethod
    async def create_order(
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

        # 4. Fetch and Lock seat segments in deterministic ASC order to prevent deadlocks
        seg_stmt = select(SeatSegment).where(
            SeatSegment.schedule_id == res.schedule_id,
            SeatSegment.seat_id == res.seat_id,
            SeatSegment.segment_no >= res.from_segment,
            SeatSegment.segment_no < res.to_segment
        ).with_for_update().order_by(SeatSegment.segment_no.asc())
        
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
                "seat_id": res.seat_id,
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

        for res in expired_reservations:
            # 2. Lock the Reservation row
            lock_res_stmt = select(Reservation).where(Reservation.id == res.id).with_for_update()
            locked_res = (await db_session.execute(lock_res_stmt)).scalar()
            
            # Double check if state is still HELD (concurrency guard)
            if not locked_res or locked_res.state != "HELD":
                continue

            # 3. Lock corresponding SeatSegments in deterministic ASC order to prevent deadlocks
            seg_stmt = select(SeatSegment).where(
                SeatSegment.schedule_id == locked_res.schedule_id,
                SeatSegment.seat_id == locked_res.seat_id,
                SeatSegment.segment_no >= locked_res.from_segment,
                SeatSegment.segment_no < locked_res.to_segment
            ).with_for_update().order_by(SeatSegment.segment_no.asc())
            
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

            # 6. Reclaim/Release occupied bitmap segments in Redis cache via Lua
            # Compute bitmask
            mask = 0
            for seg_idx in range(locked_res.from_segment, locked_res.to_segment):
                mask |= (1 << (seg_idx - 1))

            seat_key = f"r:{locked_res.schedule_id}:seat:{locked_res.seat_id}"
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
                    "seat_id": locked_res.seat_id,
                    "from_segment": locked_res.from_segment,
                    "to_segment": locked_res.to_segment,
                    "mask": mask
                },
                status="NEW"
            )
            db_session.add(outbox_event)

            released_count += 1

        if released_count > 0:
            await db_session.flush()

        return released_count
