from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import uuid
import logging
from src.app.database import async_session
from src.app.redis_client import get_redis
from src.app.reservation_service import ReservationService
from src.app.order_service import OrderService
from src.app.telemetry import setup_telemetry_logging, trace_id_var, traceparent_var

# Initialize SRE structured telemetry logging carrying distributed trace_ids
setup_telemetry_logging()

logger = logging.getLogger(__name__)

app = FastAPI(title="12306 High-Concurrency Ticketing MVP", version="1.0.0")

# Enable Cross-Origin Resource Sharing (CORS) for independent Frontend Web Apps
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# W3C traceparent context propagation middleware for distributed tracing compliance
@app.middleware("http")
async def trace_context_propagation_middleware(request: Request, call_next):
    traceparent = request.headers.get("traceparent")
    
    if traceparent:
        parts = traceparent.split("-")
        if len(parts) == 4:
            trace_id = parts[1]
            span_id = parts[2]
        else:
            trace_id = uuid.uuid4().hex
            span_id = uuid.uuid4().hex[16:]
    else:
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[16:]
        
    # Standard W3C Traceparent: 00-trace_id-span_id-trace_flags
    new_traceparent = f"00-{trace_id}-{span_id}-01"
    
    # Store trace_id in request state for debugging or logs
    request.state.trace_id = trace_id
    
    # Set thread/async-task-safe ContextVar tokens to inject trace_id automatically to all logs
    token_id = trace_id_var.set(trace_id)
    token_parent = traceparent_var.set(new_traceparent)
    
    try:
        response = await call_next(request)
    finally:
        # Prevent any potential memory leak by safely resetting contextvar tokens
        trace_id_var.reset(token_id)
        traceparent_var.reset(token_parent)
    
    # Propagate trace context to response headers and inject multi-language identifier
    response.headers["traceparent"] = new_traceparent
    response.headers["X-12306-Engine"] = "python-fastapi"
    response.headers["X-Trace-ID"] = trace_id
    return response

# 1. Database Dependency Generator
async def get_db():
    async with async_session() as session:
        yield session

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
    passenger_ids: List[str]

class SplitReserveRequest(BaseModel):
    request_id: str
    schedule_id: int
    seat1_id: int
    from1: int
    to1: int
    seat2_id: int
    from2: int
    to2: int

class QuotaReleaseRequest(BaseModel):
    schedule_id: int

class GraphQLRequest(BaseModel):
    query: str
    variables: Optional[dict] = None

class OrderRequest(BaseModel):
    request_id: str
    reservation_id: str
    amount: float

class PayRequest(BaseModel):
    order_id: str

class RefundRequest(BaseModel):
    order_id: str
    passenger_id: Optional[str] = None

class RescheduleRequest(BaseModel):
    ticket_id: str
    new_schedule_id: int
    new_seat_class: str

class WaitlistRequest(BaseModel):
    request_id: str
    schedule_id: int
    from_station_seq: int
    to_station_seq: int
    seat_class: str
    passenger_ids: List[str]

# 3. HTTP API Endpoints

@app.get("/api/v1/query")
async def query_availability(
    schedule_id: int,
    from_station_seq: int,
    to_station_seq: int,
    seat_class: str,
    db=Depends(get_db)
):
    cache_key = f"q:availability:{schedule_id}:{seat_class}"
    field = f"{from_station_seq}-{to_station_seq}"
    
    redis_available = True
    try:
        redis_client = get_redis()
        val = await redis_client.hget(cache_key, field)
    except Exception as redis_err:
        logger.warning(f"SRE: Redis cache unavailable ({str(redis_err)}). Gracefully degrading to database on-the-fly calculation.")
        redis_available = False
        val = None

    if val is None:
        if not redis_available:
            # Redis SPOF Fallback: perform pure database-driven bitmask calculation on-the-fly!
            from src.app.models import Seat, SeatSegment
            from sqlalchemy import select
            
            # Fetch all seats of this class and schedule
            seat_stmt = select(Seat).where(Seat.schedule_id == schedule_id, Seat.seat_class == seat_class)
            seats = (await db.execute(seat_stmt)).scalars().all()
            seat_ids = [seat.id for seat in seats]
            
            if not seat_ids:
                return {"available_seats": 0}
                
            # Fetch all seat segments for these seats
            segment_stmt = select(SeatSegment).where(SeatSegment.schedule_id == schedule_id, SeatSegment.seat_id.in_(seat_ids))
            segments = (await db.execute(segment_stmt)).scalars().all()
            
            # Build occupied bitmaps
            seat_bitmaps = {sid: 0 for sid in seat_ids}
            for seg in segments:
                if seg.state != "AVAILABLE":
                    seat_bitmaps[seg.seat_id] |= (1 << (seg.segment_no - 1))
                    
            # Compute route interval bitmask
            route_mask = 0
            for seg_idx in range(from_station_seq, to_station_seq):
                route_mask |= (1 << (seg_idx - 1))
                
            available_count = 0
            for seat_id in seat_ids:
                occupied_mask = seat_bitmaps.get(seat_id, 0)
                if (occupied_mask & route_mask) == 0:
                    available_count += 1
            return {"available_seats": available_count}
            
        # Standard Cache Miss path: trigger localized projection recalculation to heal the read model
        from src.app.projector import Projector
        try:
            await Projector.recalculate_and_project(db_session=db, schedule_id=schedule_id)
            val = await redis_client.hget(cache_key, field)
        except Exception as proj_err:
            logger.error(f"SRE: Recalculate projection failed ({str(proj_err)}). Falling back to direct database query.")
            # Fallback path if projector save fails due to Redis timeout/issues
            from src.app.models import Seat, SeatSegment
            from sqlalchemy import select
            seat_stmt = select(Seat).where(Seat.schedule_id == schedule_id, Seat.seat_class == seat_class)
            seats = (await db.execute(seat_stmt)).scalars().all()
            seat_ids = [seat.id for seat in seats]
            if not seat_ids:
                return {"available_seats": 0}
            segment_stmt = select(SeatSegment).where(SeatSegment.schedule_id == schedule_id, SeatSegment.seat_id.in_(seat_ids))
            segments = (await db.execute(segment_stmt)).scalars().all()
            seat_bitmaps = {sid: 0 for sid in seat_ids}
            for seg in segments:
                if seg.state != "AVAILABLE":
                    seat_bitmaps[seg.seat_id] |= (1 << (seg.segment_no - 1))
            route_mask = 0
            for seg_idx in range(from_station_seq, to_station_seq):
                route_mask |= (1 << (seg_idx - 1))
            available_count = 0
            for seat_id in seat_ids:
                occupied_mask = seat_bitmaps.get(seat_id, 0)
                if (occupied_mask & route_mask) == 0:
                    available_count += 1
            return {"available_seats": available_count}
        
    count = int(val) if val is not None else 0
    return {"available_seats": count}

@app.get("/api/v1/query/recompose")
async def query_recompose_itinerary(
    schedule_id: int,
    from_station_seq: int,
    to_station_seq: int,
    seat_class: str,
    db=Depends(get_db)
):
    """GET endpoint to fetch smart split-seat recomposition recommendations when direct tickets are sold out."""
    result = await ReservationService.find_split_itinerary(
        db_session=db,
        schedule_id=schedule_id,
        from_seq=from_station_seq,
        to_seq=to_station_seq,
        seat_class=seat_class
    )
    return result

@app.post("/api/v1/reserve")
async def reserve_ticket(req: ReserveRequest, db=Depends(get_db)):
    try:
        reservation_id = await ReservationService.reserve_ticket(
            db_session=db,
            request_id=req.request_id,
            schedule_id=req.schedule_id,
            from_seq=req.from_station_seq,
            to_seq=req.to_station_seq,
            seat_class=req.seat_class,
            passenger_ids=req.passenger_ids
        )
        await db.commit()
        return {"reservation_id": reservation_id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/reserve/split")
async def reserve_split_ticket(req: SplitReserveRequest, db=Depends(get_db)):
    """POST endpoint to confirm and atomically reserve a recommended split-seat itinerary."""
    try:
        reservation_id = await ReservationService.reserve_split_ticket(
            db_session=db,
            request_id=req.request_id,
            schedule_id=req.schedule_id,
            seat1_id=req.seat1_id,
            from1=req.from1,
            to1=req.to1,
            seat2_id=req.seat2_id,
            from2=req.from2,
            to2=req.to2
        )
        await db.commit()
        return {"reservation_id": reservation_id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/waitlist")
async def submit_waitlist(req: WaitlistRequest, db=Depends(get_db)):
    """POST endpoint to submit a waitlist reservation request when tickets are sold out."""
    try:
        waitlist_id = await ReservationService.submit_waitlist(
            db_session=db,
            request_id=req.request_id,
            schedule_id=req.schedule_id,
            from_seq=req.from_station_seq,
            to_seq=req.to_station_seq,
            seat_class=req.seat_class,
            passenger_ids=req.passenger_ids
        )
        await db.commit()
        return {"waitlist_id": waitlist_id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/waitlist/status")
async def get_waitlist_status(waitlist_id: str, db=Depends(get_db)):
    """GET endpoint to fetch the current status of a waitlist request."""
    from src.app.models import Waitlist
    from sqlalchemy import select
    try:
        stmt = select(Waitlist).where(Waitlist.id == waitlist_id)
        wl = (await db.execute(stmt)).scalar()
        if not wl:
            raise HTTPException(status_code=404, detail="Waitlist request not found")
        return {
            "waitlist_id": wl.id,
            "request_id": wl.request_id,
            "schedule_id": wl.schedule_id,
            "from_segment": wl.from_segment,
            "to_segment": wl.to_segment,
            "seat_class": wl.seat_class,
            "passenger_count": wl.passenger_count,
            "state": wl.state,
            "created_at": wl.created_at.isoformat() if wl.created_at else None
        }
    except HTTPException:
        raise
    except Exception as e:
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

@app.post("/api/v1/refund")
async def refund_order(req: RefundRequest, db=Depends(get_db)):
    try:
        result = await OrderService.refund_order(
            db_session=db,
            order_id=req.order_id,
            passenger_id=req.passenger_id
        )
        await db.commit()
        return result
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/reschedule")
async def reschedule_ticket(req: RescheduleRequest, db=Depends(get_db)):
    try:
        result = await OrderService.reschedule_ticket(
            db_session=db,
            ticket_id=req.ticket_id,
            new_schedule_id=req.new_schedule_id,
            new_seat_class=req.new_seat_class
        )
        await db.commit()
        return result
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
        payload = req.model_dump()
        result = await TRSSyncService.import_schedule(db_session=db, payload=payload)
        await db.commit()
        return result
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/ops/quota/release")
async def trs_quota_release(req: QuotaReleaseRequest, db=Depends(get_db)):
    """Authoritative operational API to trigger long-distance safeguard pool dynamic release."""
    try:
        count = await ReservationService.release_expired_quotas(db_session=db, schedule_id=req.schedule_id)
        await db.commit()
        return {"status": "SUCCESS", "released_count": count}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/graphql")
async def graphql_endpoint(req: GraphQLRequest, db=Depends(get_db)):
    """Unified POST endpoint executing custom high-concurrency GraphQL queries and mutations."""
    from src.app.graphql_engine import resolve_graphql_query
    return await resolve_graphql_query(db_session=db, query_str=req.query)

@app.get("/graphql")
async def graphql_schema():
    """Unified GET endpoint rendering the authoritative GraphQL SDL Schema."""
    from src.app.graphql_engine import GRAPHQL_SCHEMA_SDL
    return {"schema": GRAPHQL_SCHEMA_SDL}
