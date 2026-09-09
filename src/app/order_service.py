import datetime
import uuid
import logging
from collections import defaultdict
from typing import Union, Dict, Any, Optional
from sqlalchemy import select
from src.app.models import Reservation, Orders, SeatSegment, OutboxEvent, Ticket, TrainSchedule
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
    async def refund_order(db_session, order_id: str, passenger_id: str = None) -> Dict[str, Any]:
        # 1. Fetch and Lock Order (Pessimistic lock)
        order_stmt = select(Orders).where(Orders.id == order_id).with_for_update()
        order = (await db_session.execute(order_stmt)).scalar()
        if not order:
            raise Exception("Order not found")
        if order.state != "CONFIRMED":
            raise Exception("Only paid orders in CONFIRMED state can be refunded")

        # 2. Fetch and Lock associated reservation
        res_stmt = select(Reservation).where(Reservation.id == order.reservation_id).with_for_update()
        res = (await db_session.execute(res_stmt)).scalar()
        if not res or res.state != "CONFIRMED":
            raise Exception("Associated reservation is in an invalid state")

        # Fetch train schedule to calculate dynamic handling fees
        sched_stmt = select(TrainSchedule).where(TrainSchedule.id == res.schedule_id)
        sched = (await db_session.execute(sched_stmt)).scalar()
        if not sched:
            raise Exception("Train schedule not found")

        # Calculate fee rate based on days to departure (TrainSchedule.service_date vs today)
        today = datetime.date.today()
        service_date = sched.service_date
        if isinstance(service_date, str):
            service_date = datetime.datetime.strptime(service_date, "%Y-%m-%d").date()
        elif hasattr(service_date, "hour"):
            service_date = service_date.date()
            
        diff_days = (service_date - today).days

        if diff_days >= 15:
            rate = 0.0
        elif diff_days >= 2:
            rate = 0.05
        elif diff_days >= 1:
            rate = 0.10
        else:
            rate = 0.20

        # Retrieve tickets
        t_stmt = select(Ticket).where(Ticket.reservation_id == res.id)
        tickets = (await db_session.execute(t_stmt)).scalars().all()
        if not tickets:
            raise Exception("No tickets associated with this reservation")

        redis_client = get_redis()
        seat_masks = defaultdict(int)

        if passenger_id:
            # --- PARTIAL REFUND FLOW ---
            # 1. Locate the specific passenger's ticket
            target_ticket = next((t for t in tickets if t.passenger_id == passenger_id), None)
            if not target_ticket:
                raise Exception(f"Passenger {passenger_id} has no ticket in this order")

            # 2. Lock and release specific seat segments for this ticket only
            seg_stmt = select(SeatSegment).where(
                SeatSegment.schedule_id == res.schedule_id,
                SeatSegment.seat_id == target_ticket.seat_id,
                SeatSegment.segment_no >= res.from_segment,
                SeatSegment.segment_no < res.to_segment
            ).with_for_update().order_by(SeatSegment.seat_id.asc(), SeatSegment.segment_no.asc())
            segments_to_release = (await db_session.execute(seg_stmt)).scalars().all()

            for seg in segments_to_release:
                seg.state = "AVAILABLE"
                seg.reservation_id = None
                seg.version += 1
                seat_masks[seg.seat_id] |= (1 << (seg.segment_no - 1))

            # Atomic release on Redis via Lua Script
            for s_id, mask in seat_masks.items():
                seat_key = f"r:{res.schedule_id}:seat:{s_id}"
                res_key = f"r:{res.schedule_id}:reservation:{res.id}"
                await release_seat_lua(
                    redis_conn=redis_client,
                    seat_key=seat_key,
                    res_key=res_key,
                    mask=mask,
                    res_id=res.id
                )

            # 3. Apply fee and pricing calculations
            ticket_price = float(target_ticket.price)
            handling_fee = round(ticket_price * rate, 2)
            refund_amount = round(ticket_price - handling_fee, 2)

            # 4. Delete the physical Ticket from DB
            await db_session.delete(target_ticket)

            # 5. Decrement order total_amount
            new_amount = max(0.0, float(order.total_amount) - ticket_price)
            order.total_amount = round(new_amount, 2)

            # Check if there are any remaining tickets for this reservation
            remaining_tickets = [t for t in tickets if t.passenger_id != passenger_id]
            if not remaining_tickets:
                order.state = "REFUNDED"
                res.state = "RELEASED"
            
            # Write TRANSACTIONAL ORDER_REFUNDED outbox event
            outbox_event = OutboxEvent(
                event_id=str(uuid.uuid4()),
                aggregate_type="ORDER",
                aggregate_id=order_id,
                event_type="ORDER_REFUNDED",
                payload={
                    "order_id": order_id,
                    "reservation_id": res.id,
                    "schedule_id": res.schedule_id,
                    "from_segment": res.from_segment,
                    "to_segment": res.to_segment,
                    "passenger_id": passenger_id,
                    "refund_amount": refund_amount,
                    "handling_fee": handling_fee,
                    "seat_masks": dict(seat_masks)
                },
                status="NEW"
            )
            db_session.add(outbox_event)
            await db_session.flush()

            # Trigger CQRS projection and waitlist matching
            from src.app.projector import Projector
            await Projector.recalculate_and_project(db_session=db_session, schedule_id=res.schedule_id)

            from src.app.reservation_service import ReservationService
            await ReservationService.auto_fulfill_waitlist(db_session=db_session, schedule_id=res.schedule_id)

            return {
                "success": True,
                "refund_amount": refund_amount,
                "handling_fee": handling_fee,
                "remaining_order_amount": float(order.total_amount)
            }
        else:
            # --- FULL REFUND FLOW ---
            seg_stmt = select(SeatSegment).where(
                SeatSegment.schedule_id == res.schedule_id,
                SeatSegment.reservation_id == res.id
            ).with_for_update().order_by(SeatSegment.seat_id.asc(), SeatSegment.segment_no.asc())
            segments = (await db_session.execute(seg_stmt)).scalars().all()

            for seg in segments:
                seg.state = "AVAILABLE"
                seg.reservation_id = None
                seg.version += 1
                seat_masks[seg.seat_id] |= (1 << (seg.segment_no - 1))

            for s_id, mask in seat_masks.items():
                seat_key = f"r:{res.schedule_id}:seat:{s_id}"
                res_key = f"r:{res.schedule_id}:reservation:{res.id}"
                await release_seat_lua(
                    redis_conn=redis_client,
                    seat_key=seat_key,
                    res_key=res_key,
                    mask=mask,
                    res_id=res.id
                )

            # Calculate total handling fee for all tickets
            total_handling_fee = 0.0
            total_refund_amount = 0.0
            for t in tickets:
                ticket_price = float(t.price)
                fee = round(ticket_price * rate, 2)
                refund = round(ticket_price - fee, 2)
                total_handling_fee += fee
                total_refund_amount += refund
                await db_session.delete(t)

            order.state = "REFUNDED"
            order.total_amount = 0.0
            res.state = "RELEASED"

            outbox_event = OutboxEvent(
                event_id=str(uuid.uuid4()),
                aggregate_type="ORDER",
                aggregate_id=order_id,
                event_type="ORDER_REFUNDED",
                payload={
                    "order_id": order_id,
                    "reservation_id": res.id,
                    "schedule_id": res.schedule_id,
                    "from_segment": res.from_segment,
                    "to_segment": res.to_segment,
                    "refund_amount": total_refund_amount,
                    "handling_fee": total_handling_fee,
                    "seat_masks": dict(seat_masks)
                },
                status="NEW"
            )
            db_session.add(outbox_event)
            await db_session.flush()

            from src.app.projector import Projector
            await Projector.recalculate_and_project(db_session=db_session, schedule_id=res.schedule_id)

            from src.app.reservation_service import ReservationService
            await ReservationService.auto_fulfill_waitlist(db_session=db_session, schedule_id=res.schedule_id)

            return {
                "success": True,
                "refund_amount": total_refund_amount,
                "handling_fee": total_handling_fee,
                "remaining_order_amount": 0.0
            }

    @staticmethod
    async def reschedule_ticket(
        db_session,
        ticket_id: str,
        new_schedule_id: int,
        new_seat_class: str
    ) -> Dict[str, Any]:
        # 1. Fetch Ticket & Pessimistically lock it
        ticket_stmt = select(Ticket).where(Ticket.id == ticket_id).with_for_update()
        ticket = (await db_session.execute(ticket_stmt)).scalar()
        if not ticket:
            raise Exception("Original ticket not found")

        # 2. Fetch associated Reservation & Orders and lock them
        res_stmt = select(Reservation).where(Reservation.id == ticket.reservation_id).with_for_update()
        res = (await db_session.execute(res_stmt)).scalar()
        if not res or res.state != "CONFIRMED":
            raise Exception("Associated reservation is not confirmed")

        order_stmt = select(Orders).where(Orders.reservation_id == res.id).with_for_update()
        order = (await db_session.execute(order_stmt)).scalar()
        if not order or order.state != "CONFIRMED":
            raise Exception("Associated order is not paid")

        # 3. Check Real-name collision on the target train schedule
        collision_stmt = select(Ticket).join(
            Reservation, Ticket.reservation_id == Reservation.id
        ).where(
            Reservation.schedule_id == new_schedule_id,
            Ticket.passenger_id == ticket.passenger_id,
            Reservation.state.in_(["HELD", "CONFIRMED"])
        )
        collision = (await db_session.execute(collision_stmt)).first()
        if collision:
            raise Exception("Passenger has conflicting booking on target train schedule")

        old_schedule_id = res.schedule_id
        old_seat_id = ticket.seat_id
        old_ticket_price = float(ticket.price)

        # 4. Establish Nested Savepoint Transaction for ATOMIC rollback
        async with db_session.begin_nested():
            # Generate temporary request ID for the new reservation
            req_id = f"REQ_RESCHEDULE_{ticket_id}_{uuid.uuid4().hex[:8].upper()}"
            
            # Request Reservation on the new train schedule
            from src.app.reservation_service import ReservationService
            new_res_id = await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id=req_id,
                schedule_id=new_schedule_id,
                from_seq=res.from_segment,
                to_seq=res.to_segment,
                seat_class=new_seat_class,
                passenger_ids=[ticket.passenger_id]
            )

            # Retrieve new Reservation and new Ticket created under it
            new_res = (await db_session.execute(select(Reservation).where(Reservation.id == new_res_id).with_for_update())).scalar()
            new_ticket = (await db_session.execute(select(Ticket).where(Ticket.reservation_id == new_res_id).with_for_update())).scalar()

            # Release old seat segments in database
            old_seg_stmt = select(SeatSegment).where(
                SeatSegment.schedule_id == old_schedule_id,
                SeatSegment.seat_id == old_seat_id,
                SeatSegment.segment_no >= res.from_segment,
                SeatSegment.segment_no < res.to_segment
            ).with_for_update().order_by(SeatSegment.seat_id.asc(), SeatSegment.segment_no.asc())
            old_segments = (await db_session.execute(old_seg_stmt)).scalars().all()
            
            old_seat_masks = defaultdict(int)
            for seg in old_segments:
                seg.state = "AVAILABLE"
                seg.reservation_id = None
                seg.version += 1
                old_seat_masks[seg.seat_id] |= (1 << (seg.segment_no - 1))

            # Atomic release on Redis via Lua Script
            redis_client = get_redis()
            for s_id, mask in old_seat_masks.items():
                old_seat_key = f"r:{old_schedule_id}:seat:{s_id}"
                old_res_key = f"r:{old_schedule_id}:reservation:{res.id}"
                await release_seat_lua(
                    redis_conn=redis_client,
                    seat_key=old_seat_key,
                    res_key=old_res_key,
                    mask=mask,
                    res_id=res.id
                )

            # Settle pricing differences
            price_diff = round(float(new_ticket.price) - old_ticket_price, 2)
            
            # Update original Ticket to point to the new seat & reservation
            ticket.seat_id = new_ticket.seat_id
            ticket.reservation_id = new_res_id
            ticket.price = new_ticket.price

            # Delete the temporary new ticket record
            await db_session.delete(new_ticket)

            # Complete the new reservation (confirm lock)
            new_res.state = "CONFIRMED"
            new_seg_stmt = select(SeatSegment).where(
                SeatSegment.schedule_id == new_schedule_id,
                SeatSegment.reservation_id == new_res_id
            ).with_for_update().order_by(SeatSegment.seat_id.asc(), SeatSegment.segment_no.asc())
            new_segments = (await db_session.execute(new_seg_stmt)).scalars().all()
            for seg in new_segments:
                seg.state = "CONFIRMED"
                seg.version += 1

            # Check if there are other tickets left on the old reservation
            remaining_tickets = (await db_session.execute(select(Ticket).where(Ticket.reservation_id == res.id))).scalars().all()
            
            if not remaining_tickets:
                res.state = "RELEASED"
                order.reservation_id = new_res_id
                new_order_amount = max(0.0, float(order.total_amount) + price_diff)
                order.total_amount = round(new_order_amount, 2)
            else:
                # Multi-passenger order splitting
                new_order_id = f"ORD_RS_{uuid.uuid4().hex[:12].upper()}"
                new_order = Orders(
                    id=new_order_id,
                    request_id=f"REQ_ORD_RS_{new_res_id}",
                    reservation_id=new_res_id,
                    state="CONFIRMED",
                    total_amount=ticket.price,
                    expires_at=order.expires_at
                )
                db_session.add(new_order)
                order.total_amount = round(max(0.0, float(order.total_amount) - old_ticket_price), 2)

            # 5. Insert Transaction Event Outbox
            outbox_event = OutboxEvent(
                event_id=str(uuid.uuid4()),
                aggregate_type="TICKET",
                aggregate_id=ticket.id,
                event_type="TICKET_RESCHEDULED",
                payload={
                    "ticket_id": ticket.id,
                    "old_schedule_id": old_schedule_id,
                    "new_schedule_id": new_schedule_id,
                    "old_reservation_id": res.id,
                    "new_reservation_id": new_res_id,
                    "price_difference": price_diff,
                    "seat_masks": dict(old_seat_masks)
                },
                status="NEW"
            )
            db_session.add(outbox_event)
            await db_session.flush()

        # 6. Trigger CQRS Projection Recalculations immediately (Both old and new trains)
        from src.app.projector import Projector
        await Projector.recalculate_and_project(db_session=db_session, schedule_id=old_schedule_id)
        await Projector.recalculate_and_project(db_session=db_session, schedule_id=new_schedule_id)

        # 7. Trigger Waitlist Auto-Fulfillment to immediately match waiting users for both schedules
        await ReservationService.auto_fulfill_waitlist(db_session=db_session, schedule_id=old_schedule_id)
        await ReservationService.auto_fulfill_waitlist(db_session=db_session, schedule_id=new_schedule_id)

        action = "COMPLETED"
        if price_diff > 0:
            action = "PAY_DIFFERENCE"
        elif price_diff < 0:
            action = "REFUND_DIFFERENCE"

        # Determine returned seat number to align with Vue frontend expectations
        from src.app.models import Seat
        new_seat = (await db_session.execute(select(Seat).where(Seat.id == ticket.seat_id))).scalar()
        new_seat_no = new_seat.seat_no if new_seat else "N/A"

        return {
            "success": True,
            "new_ticket_id": ticket.id,
            "new_seat_no": new_seat_no,
            "price_difference": price_diff,
            "action": action
        }

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
