import datetime
import uuid
import logging
from sqlalchemy import select
from src.app.config import settings
from src.app.models import Seat, SeatSegment, Reservation, OutboxEvent
from src.app.redis_client import get_redis, reserve_seat_lua, release_seat_lua

logger = logging.getLogger(__name__)

class ReservationService:
    @staticmethod
    async def reserve_ticket(
        db_session,
        request_id: str,
        schedule_id: int,
        from_seq: int,
        to_seq: int,
        seat_class: str
    ) -> str:
        # 1. Compute the bitmask for the requested interval segments
        # Segment numbers occupied are [from_seq, to_seq - 1]
        # In bitmap, bit 0 represents segment_no 1, bit 1 represents segment_no 2, etc.
        mask = 0
        for seg in range(from_seq, to_seq):
            mask |= (1 << (seg - 1))

        # Generate unique reservation ID and Redis key
        reservation_id = f"RES_{uuid.uuid4().hex[:16].upper()}"
        res_key = f"r:{schedule_id}:reservation:{reservation_id}"

        # 2. Query candidates from database to match the requested seat class and schedule
        stmt = select(Seat).where(
            Seat.schedule_id == schedule_id,
            Seat.seat_class == seat_class
        )
        seats = (await db_session.execute(stmt)).scalars().all()

        redis_client = get_redis()
        success_reserved = False
        reserved_seat_id = None
        reserved_seat_key = None

        # 3. Iterate through candidates and attempt "Redis First" reservation via Lua
        for seat in seats:
            seat_key = f"r:{schedule_id}:seat:{seat.id}"
            
            # Auto-healing cache: if the seat key does not exist, build it from the active database
            exists = await redis_client.exists(seat_key)
            if not exists:
                segment_stmt = select(SeatSegment).where(
                    SeatSegment.seat_id == seat.id,
                    SeatSegment.state.in_(["HELD", "CONFIRMED"])
                )
                booked_segments = (await db_session.execute(segment_stmt)).scalars().all()
                db_mask = 0
                for s in booked_segments:
                    db_mask |= (1 << (s.segment_no - 1))
                await redis_client.set(seat_key, db_mask)

            # Attempt atomic reservation with TTL via Lua
            res = await reserve_seat_lua(
                redis_conn=redis_client,
                seat_key=seat_key,
                res_key=res_key,
                mask=mask,
                res_id=reservation_id,
                ttl=settings.RESERVE_TTL
            )
            if res == 1:
                success_reserved = True
                reserved_seat_id = seat.id
                reserved_seat_key = seat_key
                break

        if not success_reserved:
            raise Exception("No seats available (Redis filtered)")

        # 4. Proceed to MySQL transaction layer with row-level locks (FOR UPDATE)
        try:
            # Lock segments in deterministic ASC order to prevent deadlocks
            segment_stmt = select(SeatSegment).where(
                SeatSegment.schedule_id == schedule_id,
                SeatSegment.seat_id == reserved_seat_id,
                SeatSegment.segment_no >= from_seq,
                SeatSegment.segment_no < to_seq
            ).with_for_update().order_by(SeatSegment.segment_no.asc())

            locked_segments = (await db_session.execute(segment_stmt)).scalars().all()

            expected_num_segments = to_seq - from_seq
            if len(locked_segments) != expected_num_segments:
                raise Exception("Missing seat segments in database")

            # Double check all segment states are AVAILABLE
            for segment in locked_segments:
                if segment.state != "AVAILABLE":
                    raise Exception("Seat segment already locked by another transaction (Database conflict)")

            # 5. Apply database state changes
            for segment in locked_segments:
                segment.state = "HELD"
                segment.reservation_id = reservation_id
                segment.version += 1

            # Insert Reservation Record
            expires_at = datetime.datetime.now() + datetime.timedelta(seconds=settings.RESERVE_TTL)
            res_record = Reservation(
                id=reservation_id,
                request_id=request_id,
                schedule_id=schedule_id,
                seat_id=reserved_seat_id,
                from_segment=from_seq,
                to_segment=to_seq,
                state="HELD",
                expires_at=expires_at
            )
            db_session.add(res_record)

            # Insert Transaction Outbox Event
            outbox_event = OutboxEvent(
                event_id=str(uuid.uuid4()),
                aggregate_type="RESERVATION",
                aggregate_id=reservation_id,
                event_type="RESERVATION_HELD",
                payload={
                    "reservation_id": reservation_id,
                    "request_id": request_id,
                    "schedule_id": schedule_id,
                    "seat_id": reserved_seat_id,
                    "from_segment": from_seq,
                    "to_segment": to_seq,
                    "mask": mask
                },
                status="NEW"
            )
            db_session.add(outbox_event)

            # Commit the local MySQL transaction
            await db_session.flush()

            return reservation_id

        except Exception as e:
            # On database transaction failure, roll back MySQL session AND revert Redis bitmap state
            logger.error(f"MySQL transaction failed: {e}. Reverting Redis lock...")
            await db_session.rollback()
            await release_seat_lua(
                redis_conn=redis_client,
                seat_key=reserved_seat_key,
                res_key=res_key,
                mask=mask,
                res_id=reservation_id
            )
            raise Exception(f"No seats available ({str(e)})")
