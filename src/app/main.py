from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from src.app.database import async_session
from src.app.redis_client import get_redis
from src.app.reservation_service import ReservationService
from src.app.order_service import OrderService

app = FastAPI(title="12306 High-Concurrency Ticketing MVP", version="1.0.0")

# Enable Cross-Origin Resource Sharing (CORS) for independent Frontend Web Apps
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to allowed origins (e.g. localhost:8080)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. Database Dependency Generator
async def get_db():
    async with async_session() as session:
        yield session

from typing import List

# 2. Pydantic Schemas for JSON Request Bodies
class StationImportModel(BaseModel):
    name: str
    sequence: int

class CarriageSeatsImportModel(BaseModel):
    carriage: str
    seat_class: str
    seats: List[str]

class TRSImportScheduleRequest(BaseModel):
    train_code: str
    train_name: str
    service_date: str
    schedule_id: int
    stations: List[StationImportModel]
    carriage_seats: List[CarriageSeatsImportModel]

class ReserveRequest(BaseModel):
    request_id: str
    schedule_id: int
    from_station_seq: int
    to_station_seq: int
    seat_class: str

class OrderRequest(BaseModel):
    request_id: str
    reservation_id: str
    amount: float

class PayRequest(BaseModel):
    order_id: str

# 3. HTTP API Endpoints

@app.get("/api/v1/query")
async def query_availability(
    schedule_id: int,
    from_station_seq: int,
    to_station_seq: int,
    seat_class: str,
    db=Depends(get_db)
):
    redis_client = get_redis()
    cache_key = f"q:availability:{schedule_id}:{seat_class}"
    field = f"{from_station_seq}-{to_station_seq}"
    
    # Check memory read model first
    val = await redis_client.hget(cache_key, field)
    if val is None:
        # Cache Miss: trigger localized projection recalculation to heal the read model
        from src.app.projector import Projector
        await Projector.recalculate_and_project(db_session=db, schedule_id=schedule_id)
        val = await redis_client.hget(cache_key, field)
        
    count = int(val) if val is not None else 0
    return {"available_seats": count}

@app.post("/api/v1/reserve")
async def reserve_ticket(req: ReserveRequest, db=Depends(get_db)):
    try:
        reservation_id = await ReservationService.reserve_ticket(
            db_session=db,
            request_id=req.request_id,
            schedule_id=req.schedule_id,
            from_seq=req.from_station_seq,
            to_seq=req.to_station_seq,
            seat_class=req.seat_class
        )
        await db.commit()
        return {"reservation_id": reservation_id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/order")
async def create_order(req: OrderRequest, db=Depends(get_db)):
    try:
        order_id = await OrderService.create_order(
            db_session=db,
            request_id=req.request_id,
            reservation_id=req.reservation_id,
            amount=req.amount
        )
        await db.commit()
        return {"order_id": order_id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/pay")
async def pay_order(req: PayRequest, db=Depends(get_db)):
    try:
        success = await OrderService.pay_order(
            db_session=db,
            order_id=req.order_id
        )
        await db.commit()
        return {"success": success}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/cron/release")
async def cron_release(db=Depends(get_db)):
    try:
        count = await OrderService.release_expired_reservations(db_session=db)
        await db.commit()
        return {"released_count": count}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/ops/health")
async def ops_health(db=Depends(get_db)):
    """Liveness & Readiness health check probe for DevOps/SRE orchestration."""
    from sqlalchemy import text
    mysql_status = "UNKNOWN"
    redis_status = "UNKNOWN"
    
    # 1. Probe MySQL Core Engine
    try:
        await db.execute(text("SELECT 1"))
        mysql_status = "OK"
    except Exception as e:
        mysql_status = f"ERROR: {str(e)}"
        
    # 2. Probe Redis Pre-lock Layer
    try:
        redis_client = get_redis()
        await redis_client.ping()
        redis_status = "OK"
    except Exception as e:
        redis_status = f"ERROR: {str(e)}"
        
    overall_status = "UP" if (mysql_status == "OK" and redis_status == "OK") else "DOWN"
    
    return {
        "status": overall_status,
        "components": {
            "mysql": mysql_status,
            "redis": redis_status
        }
    }

@app.post("/api/v1/ops/trs/import-schedule")
async def trs_import_schedule(req: TRSImportScheduleRequest, db=Depends(get_db)):
    """Authoritative API endpoint for TRS (Railway Core System) to publish/sync train schedules to 12306."""
    from src.app.trs_sync_service import TRSSyncService
    try:
        # Convert Pydantic request model to dictionary for the integration service
        payload = req.model_dump()
        result = await TRSSyncService.import_schedule(db_session=db, payload=payload)
        await db.commit()
        return result
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
