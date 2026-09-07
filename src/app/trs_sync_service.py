from sqlalchemy.future import select
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment
from src.app.projector import Projector

class TRSSyncService:
    """Authority TRS (Railway Core System) Integration Service for 12306 Ticket Publishing."""
    
    @staticmethod
    async def import_schedule(db_session, payload: dict) -> dict:
        """
        Parses authority dispatch schedule publishing from TRS, establishes the ticket segments 
        in 12306 SQL master db, and immediately pre-heats the Redis CQRS read model.
        """
        # 1. Extract payload variables
        train_code = payload["train_code"]
        train_name = payload["train_name"]
        service_date = payload["service_date"]
        schedule_id = payload["schedule_id"]
        stations_data = payload["stations"]
        carriage_seats = payload["carriage_seats"]

        # 2. Check and establish physical Train entity
        stmt = select(Train).where(Train.code == train_code)
        train = (await db_session.execute(stmt)).scalar_one_or_none()
        if not train:
            train = Train(code=train_code)
            db_session.add(train)
            await db_session.flush()  # Capture train.id

        # 3. Check and establish authoritative TrainSchedule
        stmt = select(TrainSchedule).where(TrainSchedule.id == schedule_id)
        schedule = (await db_session.execute(stmt)).scalar_one_or_none()
        if not schedule:
            schedule = TrainSchedule(
                id=schedule_id, 
                train_id=train.id, 
                service_date=service_date, 
                status="ACTIVE"
            )
            db_session.add(schedule)
            await db_session.flush()

        # 4. Check and establish train經停站序列 (Stations)
        stmt = select(Station).where(Station.train_id == train.id)
        existing_stations = (await db_session.execute(stmt)).scalars().all()
        if not existing_stations:
            for s in stations_data:
                station_row = Station(
                    train_id=train.id, 
                    name=s["name"], 
                    sequence=s["sequence"]
                )
                db_session.add(station_row)
            await db_session.flush()

        # 5. Iteratively populate seats and sub-route SeatSegments
        seats_created = 0
        segments_created = 0
        num_segments = len(stations_data) - 1

        for cs in carriage_seats:
            carriage = cs["carriage"]
            seat_class = cs["seat_class"]
            seat_numbers = cs["seats"]

            for seat_no in seat_numbers:
                # Idempotency check: Ensure the seat doesn't already exist in this schedule
                stmt = select(Seat).where(
                    Seat.schedule_id == schedule_id,
                    Seat.carriage_no == carriage,
                    Seat.seat_no == seat_no
                )
                existing_seat = (await db_session.execute(stmt)).scalar_one_or_none()
                if not existing_seat:
                    seat_row = Seat(
                        schedule_id=schedule_id, 
                        carriage_no=carriage, 
                        seat_no=seat_no, 
                        seat_class=seat_class
                    )
                    db_session.add(seat_row)
                    await db_session.flush()  # Capture seat_row.id
                    
                    # Create empty available physical lock segments between经停站
                    for seg_no in range(1, num_segments + 1):
                        segment_row = SeatSegment(
                            schedule_id=schedule_id,
                            seat_id=seat_row.id,
                            segment_no=seg_no,
                            state="AVAILABLE"
                        )
                        db_session.add(segment_row)
                        segments_created += 1
                    seats_created += 1

        # 6. Commit triggers outward but first, pre-heat/recompute 12306 read models in Redis
        # Ensure DDL inserts are flushed before calling the projector recalculator
        await db_session.flush()
        await Projector.recalculate_and_project(db_session, schedule_id)

        return {
            "status": "SUCCESS",
            "schedule_id": schedule_id,
            "seats_created": seats_created,
            "segments_created": segments_created
        }
