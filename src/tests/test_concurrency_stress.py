import pytest
import datetime
import uuid
import asyncio
from sqlalchemy import select
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Reservation
from src.app.reservation_service import ReservationService
from src.app.redis_client import get_redis

def test_high_concurrency_ticket_clash(db_session, event_loop):
    """
    Simulate extreme ticketing clash on G999 (4 stations: Seq 1 -> 2 -> 3 -> 4).
    We seed 1 BUSINESS seat (3 segments: Seg 1, Seg 2, Seg 3).
    We spawn 50 concurrent requests simultaneously:
      - 20 requests: leg 1->2 (Beijing -> Tianjin)
      - 20 requests: leg 2->4 (Tianjin -> Shanghai)
      - 10 requests: leg 1->4 (Beijing -> Shanghai)
    """
    async def _impl():
        # 1. Seed Train & 4 Stations
        train = Train(code="G999")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        s3 = Station(train_id=train.id, name="济南", sequence=3)
        s4 = Station(train_id=train.id, name="上海", sequence=4)
        db_session.add_all([s1, s2, s3, s4])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 12, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        seat_id = seat.id
        schedule_id = schedule.id

        # 3 Segments: Seg 1 (Seq 1->2), Seg 2 (Seq 2->3), Seg 3 (Seq 3->4)
        seg1 = SeatSegment(schedule_id=schedule_id, seat_id=seat_id, segment_no=1, state="AVAILABLE", version=0)
        seg2 = SeatSegment(schedule_id=schedule_id, seat_id=seat_id, segment_no=2, state="AVAILABLE", version=0)
        seg3 = SeatSegment(schedule_id=schedule_id, seat_id=seat_id, segment_no=3, state="AVAILABLE", version=0)
        db_session.add_all([seg1, seg2, seg3])
        await db_session.commit()

        # Define tasks
        tasks = []

        # Helper function to invoke reservation in a separate db session context
        async def try_reserve(req_id, f_seq, t_seq):
            from src.app.database import async_session
            async with async_session() as session:
                try:
                    res_id = await ReservationService.reserve_ticket(
                        db_session=session,
                        request_id=req_id,
                        schedule_id=schedule_id,
                        from_seq=f_seq,
                        to_seq=t_seq,
                        seat_class="BUSINESS"
                    )
                    await session.commit()
                    return {"success": True, "res_id": res_id, "error": None}
                except Exception as e:
                    await session.rollback()
                    return {"success": False, "res_id": None, "error": str(e)}

        # Leg 1->2 (Beijing -> Tianjin, Seq 1 -> 2) : 20 tasks
        for i in range(20):
            tasks.append(try_reserve(f"REQ_CLASH_A_{i}", 1, 2))

        # Leg 2->4 (Tianjin -> Shanghai, Seq 2 -> 4) : 20 tasks
        for i in range(20):
            tasks.append(try_reserve(f"REQ_CLASH_B_{i}", 2, 4))

        # Leg 1->4 (Beijing -> Shanghai, Seq 1 -> 4) : 10 tasks
        for i in range(10):
            tasks.append(try_reserve(f"REQ_CLASH_C_{i}", 1, 4))

        # Execute all 50 requests in parallel
        results = await asyncio.gather(*tasks)

        # 3. Analyze clash outcomes
        success_1_2 = []
        success_2_4 = []
        success_1_4 = []
        
        deadlocks = 0
        unique_violations = 0

        for idx, r in enumerate(results):
            err = r["error"]
            if err:
                if "deadlock" in err.lower():
                    deadlocks += 1
                if "duplicate" in err.lower() or "unique" in err.lower():
                    unique_violations += 1

            if r["success"]:
                if idx < 20:
                    success_1_2.append(r["res_id"])
                elif idx < 40:
                    success_2_4.append(r["res_id"])
                else:
                    success_1_4.append(r["res_id"])

        # 4. Assert mathematical invariants of interval ticketing
        assert deadlocks == 0, f"Encountered {deadlocks} deadlock errors during concurrent clash!"
        assert unique_violations == 0, f"Encountered {unique_violations} unique constraint violations!"

        num_1_2 = len(success_1_2)
        num_2_4 = len(success_2_4)
        num_1_4 = len(success_1_4)

        print(f"\nCONCURRENCY CLASH RESULTS:")
        print(f"Leg 1->2 (Beijing -> Tianjin) Successful bookings: {num_1_2} {success_1_2}")
        print(f"Leg 2->4 (Tianjin -> Shanghai) Successful bookings: {num_2_4} {success_2_4}")
        print(f"Leg 1->4 (Beijing -> Shanghai) Successful bookings: {num_1_4} {success_1_4}")

        # Case A: If G999 whole path (1->4) won
        if num_1_4 == 1:
            assert num_1_2 == 0, "Leg 1->2 should have been blocked because 1->4 took the seat!"
            assert num_2_4 == 0, "Leg 2->4 should have been blocked because 1->4 took the seat!"
        # Case B: If partial legs won
        else:
            # Since 1->2 and 2->4 don't overlap, they can BOTH succeed concurrently on G999 seat 1!
            assert num_1_2 <= 1, "Leg 1->2 booked more than the physical limit of 1 seat!"
            assert num_2_4 <= 1, "Leg 2->4 booked more than the physical limit of 1 seat!"
            
        # Final global constraint asserting no leg double-booking with long-distance
        assert num_1_2 + num_1_4 <= 1, "Interval 1-2 exceeded seat booking capacity!"
        assert num_2_4 + num_1_4 <= 1, "Interval 2-4 exceeded seat booking capacity!"
        
        # 5. Verify database consistency
        db_session.expire_all()
        stmt = select(SeatSegment).where(SeatSegment.seat_id == seat_id).order_by(SeatSegment.segment_no.asc())
        db_segments = (await db_session.execute(stmt)).scalars().all()
        
        # Segment 1 (Seq 1->2)
        if num_1_2 == 1 or num_1_4 == 1:
            assert db_segments[0].state == "HELD"
        else:
            assert db_segments[0].state == "AVAILABLE"

        # Segment 2 & 3 (Seq 2->3, 3->4)
        if num_2_4 == 1 or num_1_4 == 1:
            assert db_segments[1].state == "HELD"
            assert db_segments[2].state == "HELD"
        else:
            assert db_segments[1].state == "AVAILABLE"
            assert db_segments[2].state == "AVAILABLE"

    event_loop.run_until_complete(_impl())
