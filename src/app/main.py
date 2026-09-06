from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from src.app.database import async_session
from src.app.redis_client import get_redis
from src.app.reservation_service import ReservationService
from src.app.order_service import OrderService

app = FastAPI(title="12306 High-Concurrency Ticketing MVP", version="1.0.0")

# 1. Database Dependency Generator
async def get_db():
    async with async_session() as session:
        yield session

# 2. Pydantic Schemas for JSON Request Bodies
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
