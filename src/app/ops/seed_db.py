import asyncio
import sys
import os

# Append project root to sys.path to resolve imports correctly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from src.app.database import async_session, engine
from src.app.models import Base, Train, Station, TrainSchedule, Seat, SeatSegment
from src.app.redis_client import get_redis
from src.app.projector import Projector

async def seed_system():
    print("==================================================")
    print("🌌 12306 Next-Gen Ops Tool: Systems Auto-Seeder  ")
    print("==================================================")
    
    # Step 1: Clean and recreate DDL schemas
    print("[Ops] 正在重置 MySQL 数据库物理表 DDL Schema...")
    async with engine.begin() as conn:
        # We drop all and recreate to ensure a clean slate
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    print("[Ops] DDL 重构完成 (MySQL Schema initialized).")

    # Step 2: Flush Redis Cache
    print("[Ops] 正在擦除 Redis 高速读缓存视图...")
    redis_client = get_redis()
    await redis_client.flushdb()
    print("[Ops] Redis 清刷完成 (Redis memory storage cleared).")

    # Step 3: Populate database with standard mock schedules
    async with async_session() as session:
        async with session.begin():
            print("[Ops] 正在写入基线列车主数据 Train [G888]...")
            train = Train(code="G888", name="复兴号智能动车组 G888 次")
            session.add(train)
            await session.flush()  # Capture auto-incremented ID

            print("[Ops] 正在写入 4 个黄金经停站序列 Station (北京 -> 天津 -> 济南 -> 上海)...")
            stations = [
                Station(train_id=train.id, name="北京", sequence=1),
                Station(train_id=train.id, name="天津", sequence=2),
                Station(train_id=train.id, name="济南", sequence=3),
                Station(train_id=train.id, name="上海", sequence=4),
            ]
            session.add_all(stations)

            print("[Ops] 正在写入发车时刻计划 TrainSchedule (Schedule ID: 1, service_date=2026-10-01)...")
            schedule = TrainSchedule(id=1, train_id=train.id, service_date="2026-10-01", status="ACTIVE")
            session.add(schedule)

            # Insert 5 luxury business class seats in Carriage 01
            print("[Ops] 正在配席 5 个物理席位 Seat (Carriage 01, Seats: 01A, 01C, 01D, 01F, 02A)...")
            seat_labels = ["01A", "01C", "01D", "01F", "02A"]
            seats = []
            for label in seat_labels:
                s = Seat(schedule_id=schedule.id, carriage="01", seat_no=label, seat_class="BUSINESS")
                session.add(s)
                seats.append(s)
            await session.flush()  # Capture Seat IDs

            print("[Ops] 正在生成 3 段区间物理锁段 SeatSegment [北京(1)-天津(2), 天津(2)-济南(3), 济南(3)-上海(4)]...")
            for seat in seats:
                for segment_no in [1, 2, 3]:
                    seg = SeatSegment(
                        schedule_id=schedule.id,
                        seat_id=seat.id,
                        segment_no=segment_no,
                        state="AVAILABLE"
                    )
                    session.add(seg)

    print("[Ops] 数据库基础配席写入成功 (Master transactions committed).")

    # Step 4: Rebuild Redis read projections (Query cache pre-heating)
    print("[Ops] 正在调度 Projector 异步重算位掩码并预热 Redis 余票视图...")
    async with async_session() as session:
        await Projector.recalculate_and_project(db_session=session, schedule_id=1)
    
    print("[Ops] 系统热身成功！读侧已同步可用商务票源。")
    print("==================================================")
    print("🎉 DevOps: System Auto-Seeding Completed SUCCESS!")
    print("==================================================")

if __name__ == "__main__":
    asyncio.run(seed_system())
