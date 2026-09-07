import datetime
import uuid
import logging
from typing import List, Union, Dict, Any
from sqlalchemy import select
from src.app.config import settings
from src.app.models import Seat, SeatSegment, Reservation, OutboxEvent, TrainSchedule, Station
from src.app.redis_client import get_redis, reserve_seat_lua, release_seat_lua

logger = logging.getLogger(__name__)

def are_adjacent(seat1: Seat, seat2: Seat) -> bool:
    if seat1.carriage_no != seat2.carriage_no:
        return False
    # Extract row number from seat_no (e.g., "01A" -> "01", "12C" -> "12")
    row1 = "".join([c for c in seat1.seat_no if c.isdigit()])
    row2 = "".join([c for c in seat2.seat_no if c.isdigit()])
    return row1 == row2 and row1 != ""

class ReservationService:
    @staticmethod
    async def reserve_ticket(
        db_session,
        request_id: str,
        schedule_id: int,
        from_seq: int,
        to_seq: int,
        seat_class: str,
        passenger_count: int = 1
    ) -> str:
        # 1. Compute the bitmask for the requested interval segments
        mask = 0
        for seg in range(from_seq, to_seq):
            mask |= (1 << (seg - 1))

        reservation_id = f"RES_{uuid.uuid4().hex[:16].upper()}"
        res_key = f"r:{schedule_id}:reservation:{reservation_id}"

        # Get total stations to verify long-distance quota rules
        schedule_stmt = select(TrainSchedule).where(TrainSchedule.id == schedule_id)
        sched = (await db_session.execute(schedule_stmt)).scalar()
        total_stations = 3 # fallback default
        if sched:
            station_stmt = select(Station).where(Station.train_id == sched.train_id)
            stations_list = (await db_session.execute(station_stmt)).scalars().all()
            if stations_list:
                total_stations = len(stations_list)

        # 2. Query candidates from database to match the requested seat class and schedule
        stmt = select(Seat).where(
            Seat.schedule_id == schedule_id,
            Seat.seat_class == seat_class
        )
        seats = (await db_session.execute(stmt)).scalars().all()

        redis_client = get_redis()
        success_reserved = False
        reserved_seat_ids = []
        quota_restricted = False

        # --- SECTION FOR ADJACENT SEATS ALLOCATION (GROUP BOOKING) ---
        if passenger_count == 2:
            adjacent_pair = None
            for i in range(len(seats)):
                for j in range(i + 1, len(seats)):
                    s1 = seats[i]
                    s2 = seats[j]
                    if are_adjacent(s1, s2):
                        # Verify neither is quota restricted
                        if s1.is_long_distance_pool == 1 and s1.quota_released == 0:
                            if not (from_seq == 1 and to_seq == total_stations):
                                continue
                        if s2.is_long_distance_pool == 1 and s2.quota_released == 0:
                            if not (from_seq == 1 and to_seq == total_stations):
                                continue
                        adjacent_pair = (s1, s2)
                        break
                if adjacent_pair:
                    break

            if adjacent_pair:
                seat1, seat2 = adjacent_pair
                # Try to reserve both in Redis
                s1_key = f"r:{schedule_id}:seat:{seat1.id}"
                s2_key = f"r:{schedule_id}:seat:{seat2.id}"
                
                # Check existences
                for s_obj, s_key in [(seat1, s1_key), (seat2, s2_key)]:
                    if not await redis_client.exists(s_key):
                        seg_stmt = select(SeatSegment).where(
                            SeatSegment.seat_id == s_obj.id,
                            SeatSegment.state.in_(["HELD", "CONFIRMED", "BLOCKED"])
                        )
                        booked = (await db_session.execute(seg_stmt)).scalars().all()
                        db_mask = 0
                        for s_seg in booked:
                            db_mask |= (1 << (s_seg.segment_no - 1))
                        await redis_client.set(s_key, db_mask)

                res1 = await reserve_seat_lua(redis_client, s1_key, res_key, mask, reservation_id, settings.RESERVE_TTL)
                if res1 == 1:
                    res2 = await reserve_seat_lua(redis_client, s2_key, res_key, mask, reservation_id, settings.RESERVE_TTL)
                    if res2 == 1:
                        success_reserved = True
                        reserved_seat_ids = [seat1.id, seat2.id]
                    else:
                        # Revert first
                        await release_seat_lua(redis_client, s1_key, res_key, mask, reservation_id)

        # --- SECTION FOR SINGLE SEAT ALLOCATION ---
        else:
            for seat in seats:
                # Quota Restriction verification
                if seat.is_long_distance_pool == 1 and seat.quota_released == 0:
                    if not (from_seq == 1 and to_seq == total_stations):
                        quota_restricted = True
                        continue

                seat_key = f"r:{schedule_id}:seat:{seat.id}"
                
                # Auto-healing cache
                exists = await redis_client.exists(seat_key)
                if not exists:
                    segment_stmt = select(SeatSegment).where(
                        SeatSegment.seat_id == seat.id,
                        SeatSegment.state.in_(["HELD", "CONFIRMED", "BLOCKED"])
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
                    reserved_seat_ids = [seat.id]
                    break

        if not success_reserved:
            if quota_restricted:
                raise Exception("Quota restricted")
            # Query if there are any blocked segments in this schedule & seat class to return accurate detail
            blocked_stmt = select(SeatSegment).join(Seat, SeatSegment.seat_id == Seat.id).where(
                SeatSegment.schedule_id == schedule_id,
                SeatSegment.state == "BLOCKED",
                Seat.seat_class == seat_class
            )
            blocked_seg = (await db_session.execute(blocked_stmt)).scalar()
            if blocked_seg:
                raise Exception("Seat segment blocked for official use")
            raise Exception("No seats available (Redis filtered)")

        # 4. Proceed to MySQL transaction layer with row-level locks (FOR UPDATE)
        try:
            # Lock segments in deterministic ASC order to prevent deadlocks
            segment_stmt = select(SeatSegment).where(
                SeatSegment.schedule_id == schedule_id,
                SeatSegment.seat_id.in_(reserved_seat_ids),
                SeatSegment.segment_no >= from_seq,
                SeatSegment.segment_no < to_seq
            ).with_for_update().order_by(SeatSegment.seat_id.asc(), SeatSegment.segment_no.asc())

            locked_segments = (await db_session.execute(segment_stmt)).scalars().all()

            expected_num_segments = (to_seq - from_seq) * len(reserved_seat_ids)
            if len(locked_segments) != expected_num_segments:
                raise Exception("Missing seat segments in database")

            # Check segment states and trigger EPIC block/conflict detections
            for segment in locked_segments:
                if segment.state == "BLOCKED":
                    raise Exception("Seat segment blocked for official use")
                elif segment.state != "AVAILABLE":
                    raise Exception("Seat segment already locked by another transaction (Database conflict)")

            # 5. Apply database state changes
            for segment in locked_segments:
                segment.state = "HELD"
                segment.reservation_id = reservation_id
                segment.version += 1

            # Insert Reservation Record pointing to primary seat
            expires_at = datetime.datetime.now() + datetime.timedelta(seconds=settings.RESERVE_TTL)
            res_record = Reservation(
                id=reservation_id,
                request_id=request_id,
                schedule_id=schedule_id,
                seat_id=reserved_seat_ids[0],
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
                    "seat_ids": reserved_seat_ids,
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
            for r_id in reserved_seat_ids:
                s_key = f"r:{schedule_id}:seat:{r_id}"
                await release_seat_lua(
                    redis_conn=redis_client,
                    seat_key=s_key,
                    res_key=res_key,
                    mask=mask,
                    res_id=reservation_id
                )
            raise Exception(f"No seats available ({str(e)})")

    @staticmethod
    async def find_split_itinerary(db_session, schedule_id: int, from_seq: int, to_seq: int, seat_class: str) -> Dict[str, Any]:
        """EPIC-06: Find a split-seat recomposition (断配拼座) itinerary when direct tickets are sold out."""
        # Find a middle station sequence 'mid' where from_seq < mid < to_seq
        for mid in range(from_seq + 1, to_seq):
            # Find an available seat for Leg 1 (from_seq to mid)
            stmt1 = select(Seat).where(Seat.schedule_id == schedule_id, Seat.seat_class == seat_class)
            seats1 = (await db_session.execute(stmt1)).scalars().all()
            
            seat1_id = None
            seat1_no = None
            for s in seats1:
                # Query segments to verify Leg 1 is AVAILABLE
                seg_stmt = select(SeatSegment).where(
                    SeatSegment.schedule_id == schedule_id,
                    SeatSegment.seat_id == s.id,
                    SeatSegment.segment_no >= from_seq,
                    SeatSegment.segment_no < mid
                )
                segs = (await db_session.execute(seg_stmt)).scalars().all()
                if len(segs) == (mid - from_seq) and all(seg.state == "AVAILABLE" for seg in segs):
                    seat1_id = s.id
                    seat1_no = s.seat_no
                    break

            # Find an available seat for Leg 2 (mid to to_seq)
            stmt2 = select(Seat).where(Seat.schedule_id == schedule_id, Seat.seat_class == seat_class)
            seats2 = (await db_session.execute(stmt2)).scalars().all()
            
            seat2_id = None
            seat2_no = None
            for s in seats2:
                seg_stmt = select(SeatSegment).where(
                    SeatSegment.schedule_id == schedule_id,
                    SeatSegment.seat_id == s.id,
                    SeatSegment.segment_no >= mid,
                    SeatSegment.segment_no < to_seq
                )
                segs = (await db_session.execute(seg_stmt)).scalars().all()
                if len(segs) == (to_seq - mid) and all(seg.state == "AVAILABLE" for seg in segs):
                    seat2_id = s.id
                    seat2_no = s.seat_no
                    break

            if seat1_id and seat2_id:
                # Return the recommended split-seat itinerary
                return {
                    "split_found": True,
                    "mid_seq": mid,
                    "seat1_id": seat1_id,
                    "seat1_no": seat1_no,
                    "seat2_id": seat2_id,
                    "seat2_no": seat2_no,
                    "description": f"Seat {seat1_no} (Seg {from_seq}-{mid}) + Seat {seat2_no} (Seg {mid}-{to_seq})"
                }
        return {"split_found": False}

    @staticmethod
    async def reserve_split_ticket(
        db_session,
        request_id: str,
        schedule_id: int,
        seat1_id: int,
        from1: int,
        to1: int,
        seat2_id: int,
        from2: int,
        to2: int
    ) -> str:
        """EPIC-06: Atomically reserve a split-seat recomposition itinerary in a single transaction."""
        reservation_id = f"RES_SPLIT_{uuid.uuid4().hex[:12].upper()}"
        res_key = f"r:{schedule_id}:reservation:{reservation_id}"
        redis_client = get_redis()

        # Compute masks
        mask1 = 0
        for seg in range(from1, to1):
            mask1 |= (1 << (seg - 1))
        
        mask2 = 0
        for seg in range(from2, to2):
            mask2 |= (1 << (seg - 1))

        # Redis Reservation for both seats
        s1_key = f"r:{schedule_id}:seat:{seat1_id}"
        s2_key = f"r:{schedule_id}:seat:{seat2_id}"

        # Initialize caches if needed
        for s_id, s_key in [(seat1_id, s1_key), (seat2_id, s2_key)]:
            if not await redis_client.exists(s_key):
                seg_stmt = select(SeatSegment).where(
                    SeatSegment.seat_id == s_id,
                    SeatSegment.state.in_(["HELD", "CONFIRMED", "BLOCKED"])
                )
                booked = (await db_session.execute(seg_stmt)).scalars().all()
                db_mask = 0
                for s_seg in booked:
                    db_mask |= (1 << (s_seg.segment_no - 1))
                await redis_client.set(s_key, db_mask)

        # Lua Reserve
        res1 = await reserve_seat_lua(redis_client, s1_key, res_key, mask1, reservation_id, settings.RESERVE_TTL)
        if res1 != 1:
            raise Exception("No seats available (Redis Leg 1 filtered)")
        
        res2 = await reserve_seat_lua(redis_client, s2_key, res_key, mask2, reservation_id, settings.RESERVE_TTL)
        if res2 != 1:
            # Revert leg 1
            await release_seat_lua(redis_client, s1_key, res_key, mask1, reservation_id)
            raise Exception("No seats available (Redis Leg 2 filtered)")

        try:
            # Lock database segments
            seg_stmt = select(SeatSegment).where(
                SeatSegment.schedule_id == schedule_id,
                (
                    ((SeatSegment.seat_id == seat1_id) & (SeatSegment.segment_no >= from1) & (SeatSegment.segment_no < to1)) |
                    ((SeatSegment.seat_id == seat2_id) & (SeatSegment.segment_no >= from2) & (SeatSegment.segment_no < to2))
                )
            ).with_for_update().order_by(SeatSegment.seat_id.asc(), SeatSegment.segment_no.asc())

            locked_segments = (await db_session.execute(seg_stmt)).scalars().all()
            expected_segs = (to1 - from1) + (to2 - from2)
            if len(locked_segments) != expected_segs:
                raise Exception("Missing seat segments in database")

            for seg in locked_segments:
                if seg.state != "AVAILABLE":
                    raise Exception("Seat segment already locked by another transaction (Database conflict)")

            # Set HELD
            for seg in locked_segments:
                seg.state = "HELD"
                seg.reservation_id = reservation_id
                seg.version += 1

            # Insert Reservation pointing to seat1
            expires_at = datetime.datetime.now() + datetime.timedelta(seconds=settings.RESERVE_TTL)
            res_record = Reservation(
                id=reservation_id,
                request_id=request_id,
                schedule_id=schedule_id,
                seat_id=seat1_id,
                from_segment=from1,
                to_segment=to2, # Spans the entire request
                state="HELD",
                expires_at=expires_at
            )
            db_session.add(res_record)

            # Insert Outbox
            outbox_event = OutboxEvent(
                event_id=str(uuid.uuid4()),
                aggregate_type="RESERVATION",
                aggregate_id=reservation_id,
                event_type="RESERVATION_HELD",
                payload={
                    "reservation_id": reservation_id,
                    "request_id": request_id,
                    "schedule_id": schedule_id,
                    "split_reservation": True,
                    "seat1_id": seat1_id,
                    "from1": from1,
                    "to1": to1,
                    "seat2_id": seat2_id,
                    "from2": from2,
                    "to2": to2
                },
                status="NEW"
            )
            db_session.add(outbox_event)

            await db_session.flush()
            return reservation_id

        except Exception as e:
            logger.error(f"MySQL split transaction failed: {e}. Reverting Redis locks...")
            await db_session.rollback()
            await release_seat_lua(redis_client, s1_key, res_key, mask1, reservation_id)
            await release_seat_lua(redis_client, s2_key, res_key, mask2, reservation_id)
            raise Exception(f"No seats available ({str(e)})")

    @staticmethod
    async def release_expired_quotas(db_session, schedule_id: int) -> int:
        """EPIC-07: Automatically release unsold long-distance quotas, enabling short-distance bookings."""
        stmt = select(Seat).where(
            Seat.schedule_id == schedule_id,
            Seat.is_long_distance_pool == 1,
            Seat.quota_released == 0
        ).with_for_update()
        
        seats = (await db_session.execute(stmt)).scalars().all()
        released_count = 0
        for seat in seats:
            seat.quota_released = 1
            released_count += 1

        if released_count > 0:
            # Trigger immediate Query Projection recalculation to heal/pre-heat read caches
            from src.app.projector import Projector
            await db_session.flush()
            await Projector.recalculate_and_project(db_session=db_session, schedule_id=schedule_id)
            
            # TRS Trigger: Attempt automatic waitlist fulfillment
            await ReservationService.auto_fulfill_waitlist(db_session, schedule_id)
            
        return released_count

    @staticmethod
    async def submit_waitlist(
        db_session,
        request_id: str,
        schedule_id: int,
        from_seq: int,
        to_seq: int,
        seat_class: str,
        passenger_count: int = 1
    ) -> str:
        """TRS: Accept a waitlist submission when seats are sold out, and write audit trail in Outbox."""
        from src.app.models import Waitlist
        
        # 1. Check idempotency to prevent duplicate submissions
        stmt = select(Waitlist).where(Waitlist.request_id == request_id)
        existing = (await db_session.execute(stmt)).scalar()
        if existing:
            return existing.id

        # 2. Validation
        if from_seq >= to_seq:
            raise Exception("Invalid station sequence: from_seq must be less than to_seq")

        waitlist_id = "WL_" + str(uuid.uuid4().hex[:12].upper())
        wl_item = Waitlist(
            id=waitlist_id,
            request_id=request_id,
            schedule_id=schedule_id,
            from_segment=from_seq,
            to_segment=to_seq,
            seat_class=seat_class,
            passenger_count=passenger_count,
            state="QUEUED"
        )
        db_session.add(wl_item)
        await db_session.flush()

        # 3. Write Outbox audit trail
        outbox_event = OutboxEvent(
            event_id=str(uuid.uuid4()),
            aggregate_type="WAITLIST",
            aggregate_id=waitlist_id,
            event_type="WAITLIST_SUBMITTED",
            payload={
                "waitlist_id": waitlist_id,
                "request_id": request_id,
                "schedule_id": schedule_id,
                "from_segment": from_seq,
                "to_segment": to_seq,
                "seat_class": seat_class,
                "passenger_count": passenger_count
            },
            status="NEW"
        )
        db_session.add(outbox_event)
        await db_session.flush()

        return waitlist_id

    @staticmethod
    async def auto_fulfill_waitlist(db_session, schedule_id: int) -> int:
        """TRS: Attempt to automatically fulfill active waitlist requests when seats become available."""
        from src.app.models import Waitlist
        
        # 1. Fetch all active QUEUED waitlists for this schedule_id, oldest first
        wl_stmt = select(Waitlist).where(
            Waitlist.schedule_id == schedule_id,
            Waitlist.state == "QUEUED"
        ).order_by(Waitlist.created_at.asc()).with_for_update()
        
        queued_items = (await db_session.execute(wl_stmt)).scalars().all()
        fulfilled_count = 0

        for wl in queued_items:
            # 2. Attempt Reservation utilizing nested SAVEPOINT
            try:
                async with db_session.begin_nested():
                    # Generate a unique request_id for the reservation (prefixing waitlist's id)
                    res_req_id = f"REQ_AUTO_{wl.id}_{wl.request_id[:20]}"
                    
                    # Reuse existing seat-allocation logic 
                    res_id = await ReservationService.reserve_ticket(
                        db_session=db_session,
                        request_id=res_req_id,
                        schedule_id=wl.schedule_id,
                        from_seq=wl.from_segment,
                        to_seq=wl.to_segment,
                        seat_class=wl.seat_class,
                        passenger_count=wl.passenger_count
                    )
                    
                    # Succeeded! Update waitlist record state
                    wl.state = "SUCCESS"
                    
                    # Insert notification outbox event
                    outbox_event = OutboxEvent(
                        event_id=str(uuid.uuid4()),
                        aggregate_type="WAITLIST",
                        aggregate_id=wl.id,
                        event_type="WAITLIST_FULFILLED",
                        payload={
                            "waitlist_id": wl.id,
                            "reservation_id": res_id,
                            "schedule_id": wl.schedule_id,
                            "from_segment": wl.from_segment,
                            "to_segment": wl.to_segment,
                            "seat_class": wl.seat_class,
                            "passenger_count": wl.passenger_count
                        },
                        status="NEW"
                    )
                    db_session.add(outbox_event)
                
                # Commit nested transaction and increment count
                fulfilled_count += 1
                logger.info(f"[Waitlist-Fulfill] Successfully auto-fulfilled waitlist item {wl.id} with reservation {res_id}")
                
            except Exception as e:
                # Failed nested transaction: seat not available or adjacent failed. Roll back to savepoint and continue
                logger.debug(f"[Waitlist-Fulfill-Skip] Waitlist {wl.id} cannot be fulfilled yet: {e}")
                continue

        if fulfilled_count > 0:
            # Trigger Projection recalculation to reflect auto-reservations in Redis
            from src.app.projector import Projector
            await db_session.flush()
            await Projector.recalculate_and_project(db_session=db_session, schedule_id=schedule_id)

        return fulfilled_count

