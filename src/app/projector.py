import json
import logging
from sqlalchemy import select
from src.app.models import TrainSchedule, Station, Seat, SeatSegment, ProcessedEvent
from src.app.redis_client import get_redis

logger = logging.getLogger(__name__)

class Projector:
    @staticmethod
    async def process_event(db_session, event: dict) -> bool:
        event_id = event.get("event_id")
        if not event_id:
            logger.warning("Event missing event_id, skipping.")
            return False

        # 1. Idempotency Check: try to write a ProcessedEvent inside a local transaction
        stmt = select(ProcessedEvent).where(
            ProcessedEvent.consumer_name == "Projector",
            ProcessedEvent.event_id == event_id
        )
        existing = (await db_session.execute(stmt)).scalar()
        if existing:
            logger.info(f"Event {event_id} already processed by Projector, skipping.")
            return False

        # Insert ProcessedEvent record
        pe = ProcessedEvent(consumer_name="Projector", event_id=event_id)
        db_session.add(pe)

        # 2. Extract payload and recalculate affected train schedule availability query cache
        payload = event.get("payload", {})
        schedule_id = payload.get("schedule_id")
        if not schedule_id:
            logger.warning(f"Event {event_id} missing schedule_id in payload, skipping.")
            return False

        await Projector.recalculate_and_project(db_session=db_session, schedule_id=schedule_id)
        return True

    @staticmethod
    async def recalculate_and_project(db_session, schedule_id: int):
        # 1. Retrieve the train_id associated with this schedule
        schedule_stmt = select(TrainSchedule).where(TrainSchedule.id == schedule_id)
        schedule = (await db_session.execute(schedule_stmt)).scalar()
        if not schedule:
            logger.error(f"Train schedule {schedule_id} not found during projection.")
            return

        # 2. Retrieve all stations for this train to find the maximum station sequence sequence (N)
        station_stmt = select(Station).where(
            Station.train_id == schedule.train_id
        ).order_by(Station.sequence.asc())
        stations = (await db_session.execute(station_stmt)).scalars().all()
        if len(stations) < 2:
            logger.warning(f"Train {schedule.train_id} has fewer than 2 stations. Skipping projection.")
            return
        
        max_seq = stations[-1].sequence

        # 3. Retrieve all seats for this schedule
        seat_stmt = select(Seat).where(Seat.schedule_id == schedule_id)
        seats = (await db_session.execute(seat_stmt)).scalars().all()

        # 4. Retrieve all seat segments for this schedule
        segment_stmt = select(SeatSegment).where(SeatSegment.schedule_id == schedule_id)
        segments = (await db_session.execute(segment_stmt)).scalars().all()

        # 5. Build bitwise occupancy bitmaps per seat
        # seat_bitmaps: { seat_id: bitmask_int }
        seat_bitmaps = {seat.id: 0 for seat in seats}
        for seg in segments:
            if seg.seat_id in seat_bitmaps:
                if seg.state != "AVAILABLE":
                    # segment_no 1 -> bit 0, segment_no 2 -> bit 1, etc.
                    seat_bitmaps[seg.seat_id] |= (1 << (seg.segment_no - 1))

        # Group seat entities by seat_class for scoped querying
        # seats_by_class: { seat_class: [seat_id, ...] }
        seats_by_class = {}
        for seat in seats:
            seats_by_class.setdefault(seat.seat_class, []).append(seat.id)

        redis_client = get_redis()

        # 6. Generate all sub-route intervals (from_seq, to_seq) where 1 <= from_seq < to_seq <= N
        intervals = []
        for f in range(1, max_seq):
            for t in range(f + 1, max_seq + 1):
                intervals.append((f, t))

        # 7. For each seat class, calculate available seat counts per route and batch write via Redis Pipeline
        async with redis_client.pipeline() as pipe:
            for seat_class, seat_ids in seats_by_class.items():
                cache_key = f"q:availability:{schedule_id}:{seat_class}"
                counts = {}

                for f, t in intervals:
                    # Compute route interval bitmask
                    route_mask = 0
                    for seg_idx in range(f, t):
                        route_mask |= (1 << (seg_idx - 1))

                    # Count how many seats in this class are free for this interval
                    available_count = 0
                    for seat_id in seat_ids:
                        occupied_mask = seat_bitmaps.get(seat_id, 0)
                        if (occupied_mask & route_mask) == 0:
                            available_count += 1

                    counts[f"{f}-{t}"] = str(available_count)

                # Atomically overwrite the Redis hash map
                pipe.delete(cache_key)
                if counts:
                    pipe.hset(cache_key, mapping=counts)

            await pipe.execute()
