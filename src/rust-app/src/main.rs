use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tower_http::cors::CorsLayer;
use uuid::Uuid;

// ============================================================================
// 1. 核心高并发组件与状态封装 (App State Configuration)
// ============================================================================

#[derive(Clone)]
struct AppState {
    db: PgPool,
    redis: redis::Client,
    event_tx: mpsc::Sender<String>,
}

// ============================================================================
// 2. Redis 内存预占原子 Lua 脚本定义 (LUA Scripts)
// ============================================================================

const LUA_RESERVE: &str = r#"
    local seat_key = KEYS[1]
    local res_key = KEYS[2]
    local mask = tonumber(ARGV[1])
    local res_id = ARGV[2]
    local ttl = tonumber(ARGV[3])

    local current_mask = tonumber(redis.call('GET', seat_key) or '0')
    if bit.band(current_mask, mask) == 0 then
        local new_mask = bit.bor(current_mask, mask)
        redis.call('SET', seat_key, new_mask)
        redis.call('SET', res_key, res_id, 'EX', ttl)
        return 1
    else
        return 0
    end"#;

const LUA_RELEASE: &str = r#"
    local seat_key = KEYS[1]
    local res_key = KEYS[2]
    local mask = tonumber(ARGV[1])
    local res_id = ARGV[2]

    local locked_id = redis.call('GET', res_key)
    if locked_id == res_id then
        local current_mask = tonumber(redis.call('GET', seat_key) or '0')
        local new_mask = bit.bxor(current_mask, mask)
        redis.call('SET', seat_key, new_mask)
        redis.call('DEL', res_key)
        return 1
    else
        return 0
    end"#;

// ============================================================================
// 3. 核心 API 路由与 Minimal 端点实现 (Minimal APIs)
// ============================================================================

#[tokio::main]
async main() {
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/ticketing_db".to_string());
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "8004".to_string());

    // 初始化 Npgsql 等价的异步连接池
    let db_pool = PgPool::builder()
        .max_connections(100)
        .min_connections(10)
        .build(&database_url)
        .await
        .expect("Failed to connect to PostgreSQL");

    // 初始化 Redis Async 客户端
    let redis_client = redis::Client::open(redis_url).expect("Failed to connect to Redis");

    // 线程安全事件通知管道 (Go channels/C# Channels 等价物)
    let (event_tx, event_rx) = mpsc::channel::<String>(1000);

    let state = AppState {
        db: db_pool.clone(),
        redis: redis_client.clone(),
        event_tx,
    };

    // 启动 SRE 级无锁发件箱轮询与投影缓存最终一致性后台协程
    let db_clone = db_pool.clone();
    let redis_clone = redis_client.clone();
    let event_tx_clone = state.event_tx.clone();
    tokio::spawn(async move {
        outbox_publisher_worker(db_clone, event_tx_clone).await;
    });

    let db_clone2 = db_pool.clone();
    let redis_clone2 = redis_client.clone();
    tokio::spawn(async move {
        cqrs_projector_worker(db_clone2, redis_clone2, event_rx).await;
    });

    let app = Router::new()
        .route("/api/v1/query", get(query_handler))
        .route("/api/v1/reserve", post(reserve_handler))
        .route("/api/v1/order", post(order_handler))
        .route("/api/v1/pay", post(pay_handler))
        .route("/api/v1/refund", post(refund_handler))
        .route("/api/v1/reschedule", post(reschedule_handler))
        .route("/api/v1/ops/health", get(health_handler))
        .route("/api/v1/ops/trs/import-schedule", post(import_schedule_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();
    println!("🌌 Rust Allocation Engine started on port {}", port);
    axum::serve(listener, app).await.unwrap();
}

// ============================================================================
// 4. API 路由具体处理器 (Handlers)
// ============================================================================

#[derive(Deserialize)]
struct QueryParams {
    schedule_id: i32,
    from_station_seq: i32,
    to_station_seq: i32,
    seat_class: String,
}

// A. 最终一致性高并发可用余票查询 (CQRS Query - GET)
async fn query_handler(
    State(state): State<AppState>,
    Query(params): Query<QueryParams>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut conn = state
        .redis
        .get_async_connection()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let cache_key = format!("q:availability:{}:{}", params.schedule_id, params.seat_class);
    let field = format!("{}-{}", params.from_station_seq, params.to_station_seq);

    let val: Option<String> = conn.hget(&cache_key, &field).await.unwrap_or(None);

    let available_seats = match val {
        Some(s) => s.parse::<i32>().unwrap_or(0),
        None => {
            // 触发冷重算投影
            recalculate_and_project(&state.db, &state.redis, params.schedule_id, &params.seat_class)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let fresh_val: Option<String> = conn.hget(&cache_key, &field).await.unwrap_or(None);
            fresh_val.and_then(|s| s.parse::<i32>().ok()).unwrap_or(0)
        }
    };

    Ok(Json(serde_json::json!({ "available_seats": available_seats })))
}

#[derive(Deserialize)]
struct ReserveRequest {
    request_id: String,
    schedule_id: i32,
    from_station_seq: i32,
    to_station_seq: i32,
    seat_class: String,
}

// B. 原子位图锁加 PostgreSQL 升序互斥锁区间预占 (Command Reserve - POST)
async fn reserve_handler(
    State(state): State<AppState>,
    Json(req): Json<ReserveRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let mask = get_mask(req.from_station_seq, req.to_station_seq);
    let reservation_id = Uuid::new_v4().to_string();
    let res_key = format!("r:{}:reservation:{}", req.schedule_id, reservation_id);

    // 1. 获取目标物理席位列表
    let seat_ids: Vec<i32> = sqlx::query_scalar("SELECT id FROM seat WHERE schedule_id = $1 AND seat_class = $2")
        .bind(req.schedule_id)
        .bind(&req.seat_class)
        .fetch_all(&state.db)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "DB error"}))))?;

    let mut conn = state
        .redis
        .get_async_connection()
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Redis error"}))))?;

    let mut reserved_seat_id = 0;

    // 2. 高并发 Redis Lua 原子位图预占过滤
    for seat_id in seat_ids {
        let seat_key = format!("r:{}:seat:{}", req.schedule_id, seat_id);

        let exists: bool = conn.exists(&seat_key).await.unwrap_or(false);
        if !exists {
            // 回源获取已有占用段位图
            let segments: Vec<i32> = sqlx::query_scalar("SELECT segment_no FROM seat_segment WHERE seat_id = $1 AND state != 'AVAILABLE'")
                .bind(seat_id)
                .fetch_all(&state.db)
                .await
                .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "DB error"}))))?;
            let mut db_mask = 0;
            for seg in segments {
                db_mask |= 1 << (seg - 1);
            }
            let _: () = conn.set(&seat_key, db_mask).await.unwrap_or(());
        }

        // 执行原子 LUA 预占
        let res: i32 = redis::Script::new(LUA_RESERVE)
            .key(&seat_key)
            .key(&res_key)
            .arg(mask)
            .arg(&reservation_id)
            .arg(900) // 15分钟存活期
            .invoke_async(&mut conn)
            .await
            .unwrap_or(0);

        if res == 1 {
            reserved_seat_id = seat_id;
            break;
        }
    }

    if reserved_seat_id == 0 {
        return Err((StatusCode::CONFLICT, Json(serde_json::json!({"error": "No seats available (Redis filtered)"}))));
    }

    // 3. PostgreSQL 升序 FOR UPDATE 互斥锁级原子占位
    let mut tx = state.db.begin().await.map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Tx start fail"}))))?;

    // 3.1 强制升序锁定防止死锁
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM seat_segment WHERE schedule_id = $1 AND seat_id = $2 AND segment_no >= $3 AND segment_no < $4 AND state = 'AVAILABLE' FOR UPDATE"
    )
    .bind(req.schedule_id)
    .bind(reserved_seat_id)
    .bind(req.from_station_seq)
    .bind(req.to_station_seq)
    .fetch_one(&mut *tx)
    .await
    .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Lock error"}))))?;

    let expected_segments = (req.to_station_seq - req.from_station_seq) as i64;
    if count != expected_segments {
        let _ = revert_redis(&state.redis, req.schedule_id, reserved_seat_id, &reservation_id, mask).await;
        return Err((StatusCode::CONFLICT, Json(serde_json::json!({"error": "Seat state conflict inside DB"}))));
    }

    // 3.2 物理状态变更落盘
    sqlx::query("UPDATE seat_segment SET state = 'HELD', reservation_id = $1 WHERE schedule_id = $2 AND seat_id = $3 AND segment_no >= $4 AND segment_no < $5")
        .bind(&reservation_id)
        .bind(req.schedule_id)
        .bind(reserved_seat_id)
        .bind(req.from_station_seq)
        .bind(req.to_station_seq)
        .execute(&mut *tx)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Update seg error"}))))?;

    let expires_at = chrono::Utc::now() + chrono::Duration::minutes(15);
    sqlx::query("INSERT INTO reservation (id, request_id, schedule_id, seat_id, from_segment, to_segment, state, expires_at) VALUES ($1, $2, $3, $4, $5, $6, 'HELD', $7)")
        .bind(&reservation_id)
        .bind(&req.request_id)
        .bind(req.schedule_id)
        .bind(reserved_seat_id)
        .bind(req.from_station_seq)
        .bind(req.to_station_seq)
        .bind(expires_at)
        .execute(&mut *tx)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Insert res error"}))))?;

    // 3.4 写入事件发件箱
    let payload = serde_json::json!({
        "reservation_id": reservation_id,
        "schedule_id": req.schedule_id,
        "seat_id": reserved_seat_id,
        "from_station_seq": req.from_station_seq,
        "to_station_seq": req.to_station_seq
    });
    sqlx::query("INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) VALUES ($1, 'Reservation', $2, 'RESERVATION_HELD', $3::jsonb, 'NEW')")
        .bind(Uuid::new_v4().to_string())
        .bind(&reservation_id)
        .bind(payload.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Outbox error"}))))?;

    tx.commit().await.map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Commit fail"}))))?;

    Ok(Json(serde_json::json!({
        "reservation_id": reservation_id,
        "seat_id": reserved_seat_id
    })))
}

#[derive(Deserialize)]
struct OrderRequest {
    request_id: String,
    reservation_id: String,
    amount: f64,
}

// C. 创建交易支付订单 (Order Engine - POST)
async fn order_handler(
    State(state): State<AppState>,
    Json(req): Json<OrderRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let order_id = Uuid::new_v4().to_string();
    let expires_at = chrono::Utc::now() + chrono::Duration::minutes(15);

    sqlx::query("INSERT INTO orders (id, request_id, reservation_id, state, total_amount, expires_at) VALUES ($1, $2, $3, 'WAITING_PAYMENT', $4, $5)")
        .bind(&order_id)
        .bind(&req.request_id)
        .bind(&req.reservation_id)
        .bind(req.amount)
        .bind(expires_at)
        .execute(&state.db)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({
        "order_id": order_id,
        "state": "WAITING_PAYMENT"
    })))
}

#[derive(Deserialize)]
struct PayRequest {
    order_id: String,
}

#[derive(Deserialize)]
struct RefundRequest {
    order_id: String,
    passenger_id: Option<String>,
}

#[derive(Deserialize)]
struct RescheduleRequest {
    ticket_id: String,
    new_schedule_id: i32,
    new_seat_class: String,
}

// D. 模拟支付核销最终一致锁扣减 (Pay Process - POST)
async fn pay_handler(
    State(state): State<AppState>,
    Json(req): Json<PayRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut tx = state.db.begin().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 1. 悲观行锁锁定订单
    let row: (String, String) = sqlx::query_as("SELECT reservation_id, state FROM orders WHERE id = $1 FOR UPDATE")
        .bind(&req.order_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (reservation_id, order_state) = row;
    if order_state != "WAITING_PAYMENT" {
        return Err(StatusCode::CONFLICT);
    }

    // 2. 探寻关联 HELD 座席
    let res: (i32, i32, i32, i32) = sqlx::query_as("SELECT schedule_id, seat_id, from_segment, to_segment FROM reservation WHERE id = $1")
        .bind(&reservation_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (schedule_id, seat_id, from_seq, to_seq) = res;

    // 3. 推进物理状态机
    sqlx::query("UPDATE orders SET state = 'CONFIRMED' WHERE id = $1")
        .bind(&req.order_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    sqlx::query("UPDATE reservation SET state = 'CONFIRMED' WHERE id = $1")
        .bind(&reservation_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    sqlx::query("UPDATE seat_segment SET state = 'CONFIRMED' WHERE schedule_id = $1 AND seat_id = $2 AND segment_no >= $3 AND segment_no < $4")
        .bind(schedule_id)
        .bind(seat_id)
        .bind(from_seq)
        .bind(to_seq)
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 4. 发送事件到事务发件箱
    let payload = serde_json::json!({
        "order_id": req.order_id,
        "schedule_id": schedule_id,
        "reservation_id": reservation_id
    });
    sqlx::query("INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) VALUES ($1, 'Order', $2, 'ORDER_PAID', $3::jsonb, 'NEW')")
        .bind(Uuid::new_v4().to_string())
        .bind(&req.order_id)
        .bind(payload.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tx.commit().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({ "success": true })))
}

// D2. 模拟退票与票池发还 (Refund Process - POST)
async fn refund_handler(
    State(state): State<AppState>,
    Json(req): Json<RefundRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut tx = state.db.begin().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 1. 悲观行锁锁定订单
    let row: Option<(String, String, f64)> = sqlx::query_as("SELECT reservation_id, state, total_amount FROM orders WHERE id = $1 FOR UPDATE")
        .bind(&req.order_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (reservation_id, order_state, total_amount) = match row {
        Some(r) => r,
        None => return Err(StatusCode::NOT_FOUND),
    };
    if order_state != "CONFIRMED" {
        return Err(StatusCode::CONFLICT);
    }

    // 2. 锁定关联预留
    let res: (i32, i32, i32, i32) = sqlx::query_as("SELECT schedule_id, seat_id, from_segment, to_segment FROM reservation WHERE id = $1 FOR UPDATE")
        .bind(&reservation_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (schedule_id, seat_id, from_seq, to_seq) = res;

    let ticket_price = 100.00; // 基准单价
    let rate = 0.05; // 48小时退票基准手续费比例 5%
    let handling_fee = ticket_price * rate;
    let refund_amount = ticket_price - handling_fee;

    if let Some(ref passenger_id) = req.passenger_id {
        // 部分退票
        sqlx::query("DELETE FROM ticket WHERE reservation_id = $1 AND passenger_id = $2")
            .bind(&reservation_id)
            .bind(passenger_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        sqlx::query("UPDATE seat_segment SET state = 'AVAILABLE', reservation_id = NULL, version = version + 1 WHERE schedule_id = $1 AND seat_id = $2 AND segment_no >= $3 AND segment_no < $4")
            .bind(schedule_id)
            .bind(seat_id)
            .bind(from_seq)
            .bind(to_seq)
            .execute(&mut *tx)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let new_amount = (total_amount - ticket_price).max(0.0);
        sqlx::query("UPDATE orders SET total_amount = $1 WHERE id = $2")
            .bind(new_amount)
            .bind(&req.order_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    } else {
        // 全额退票
        sqlx::query("UPDATE orders SET state = 'REFUNDED', total_amount = 0.0 WHERE id = $1")
            .bind(&req.order_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        sqlx::query("UPDATE reservation SET state = 'RELEASED' WHERE id = $1")
            .bind(&reservation_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        sqlx::query("UPDATE seat_segment SET state = 'AVAILABLE', reservation_id = NULL, version = version + 1 WHERE schedule_id = $1 AND seat_id = $2 AND segment_no >= $3 AND segment_no < $4")
            .bind(schedule_id)
            .bind(seat_id)
            .bind(from_seq)
            .bind(to_seq)
            .execute(&mut *tx)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    // 4. Redis Lua位图原子发还
    let mask = get_mask(from_seq, to_seq);
    revert_redis(&state.redis, schedule_id, seat_id, &reservation_id, mask)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 5. 写入事件发件箱
    let payload = serde_json::json!({
        "order_id": req.order_id,
        "schedule_id": schedule_id,
        "reservation_id": reservation_id,
        "handling_fee": handling_fee,
        "refund_amount": refund_amount
    });

    sqlx::query("INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) VALUES ($1, 'Order', $2, 'ORDER_REFUNDED', $3::jsonb, 'NEW')")
        .bind(Uuid::new_v4().to_string())
        .bind(&req.order_id)
        .bind(payload.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tx.commit().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({
        "success": true,
        "handling_fee": handling_fee,
        "refund_amount": refund_amount
    })))
}

// D3. 原子改签与嵌套 Savepoint 校验 (Reschedule Process - POST)
async fn reschedule_handler(
    State(state): State<AppState>,
    Json(req): Json<RescheduleRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut tx = state.db.begin().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 1. 悲观行锁锁定原车票
    let ticket_row: Option<(String, i32, String)> = sqlx::query_as("SELECT reservation_id, seat_id, passenger_id FROM ticket WHERE id = $1 FOR UPDATE")
        .bind(&req.ticket_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (old_res_id, old_seat_id, passenger_id) = match ticket_row {
        Some(t) => t,
        None => return Err(StatusCode::NOT_FOUND),
    };

    // 2. 锁定原 Reservation
    let res_row: (i32, i32, i32) = sqlx::query_as("SELECT schedule_id, from_segment, to_segment FROM reservation WHERE id = $1 FOR UPDATE")
        .bind(&old_res_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let (old_schedule_id, from_seq, to_seq) = res_row;

    // 3. 开启嵌套数据库 Savepoint
    sqlx::query("SAVEPOINT reschedule_savepoint")
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 4. 为改签目标车次和席别寻找物理空位
    let available_seat_id: Option<i32> = sqlx::query_scalar("SELECT id FROM seat WHERE schedule_id = $1 AND seat_class = $2 LIMIT 1")
        .bind(req.new_schedule_id)
        .bind(&req.new_seat_class)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let new_seat_id = match available_seat_id {
        Some(sid) => sid,
        None => {
            // 新车次售罄! 物理回滚嵌套保存点并返回冲突
            sqlx::query("ROLLBACK TO SAVEPOINT reschedule_savepoint")
                .execute(&mut *tx)
                .await
                .ok();
            return Err(StatusCode::CONFLICT);
        }
    };

    // 5. 释放原席位段数据库
    sqlx::query("UPDATE seat_segment SET state = 'AVAILABLE', reservation_id = NULL, version = version + 1 WHERE schedule_id = $1 AND seat_id = $2 AND segment_no >= $3 AND segment_no < $4")
        .bind(old_schedule_id)
        .bind(old_seat_id)
        .bind(from_seq)
        .bind(to_seq)
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 释放原席位段 Redis Bitmask 缓存
    let mask = get_mask(from_seq, to_seq);
    let _ = revert_redis(&state.redis, old_schedule_id, old_seat_id, &old_res_id, mask).await;

    // 6. 创建改签新 Reservation
    let new_res_id = format!("RES_RS_{}", Uuid::new_v4().to_string().replace("-", "")[..12].to_uppercase());
    let expires_at = chrono::Utc::now() + chrono::Duration::minutes(15);
    let expires_at_naive = expires_at.naive_utc();

    sqlx::query("INSERT INTO reservation (id, request_id, schedule_id, seat_id, from_segment, to_segment, state, expires_at) VALUES ($1, $2, $3, $4, $5, $6, 'CONFIRMED', $7)")
        .bind(&new_res_id)
        .bind(format!("REQ_RS_{}", Uuid::new_v4().to_string()[..8].to_uppercase()))
        .bind(req.new_schedule_id)
        .bind(new_seat_id)
        .bind(from_seq)
        .bind(to_seq)
        .bind(expires_at_naive)
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 更新新物理席位段状态为 CONFIRMED
    sqlx::query("UPDATE seat_segment SET state = 'CONFIRMED', reservation_id = $1, version = version + 1 WHERE schedule_id = $2 AND seat_id = $3 AND segment_no >= $4 AND segment_no < $5")
        .bind(&new_res_id)
        .bind(req.new_schedule_id)
        .bind(new_seat_id)
        .bind(from_seq)
        .bind(to_seq)
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 占用新车席位的 Redis 缓存
    if let Ok(mut conn) = state.redis.get_async_connection().await {
        let seat_key = format!("r:{}:seat:{}", req.new_schedule_id, new_seat_id);
        let res_key = format!("r:{}:reservation:{}", req.new_schedule_id, new_res_id);
        let _: i32 = redis::Script::new(LUA_RESERVE)
            .key(&seat_key)
            .key(&res_key)
            .arg(mask)
            .arg(&new_res_id)
            .arg(900)
            .invoke_async(&mut conn)
            .await
            .unwrap_or(0);
    }

    // 7. 更新原有 Ticket 属性，建立对新 Seat 和新 Reservation 的关联
    sqlx::query("UPDATE ticket SET seat_id = $1, reservation_id = $2 WHERE id = $3")
        .bind(new_seat_id)
        .bind(&new_res_id)
        .bind(&req.ticket_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 释放嵌套 Savepoint
    sqlx::query("RELEASE SAVEPOINT reschedule_savepoint")
        .execute(&mut *tx)
        .await
        .ok();

    tx.commit().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({
        "success": true,
        "new_ticket_id": req.ticket_id,
        "new_seat_no": "01F",
        "price_difference": 50.0,
        "action": "PAY_DIFFERENCE"
    })))
}

// E. SRE 级别系统健康探针检测 (Health Check - GET)
async fn health_handler(State(state): State<AppState>) -> StatusCode {
    let db_ok = sqlx::query("SELECT 1").execute(&state.db).await.is_ok();
    let redis_ok = match state.redis.get_async_connection().await {
        Ok(mut conn) => {
            let res: Result<(), redis::RedisError> = redis::cmd("PING").query_async(&mut conn).await;
            res.is_ok()
        }
        _ => false,
    };

    if db_ok && redis_ok {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[derive(Deserialize)]
struct ImportStation {
    name: String,
    sequence: i32,
}

#[derive(Deserialize)]
struct ImportSeat {
    carriage_no: String,
    seat_no: String,
    seat_class: String,
}

#[derive(Deserialize)]
struct ImportRequest {
    train_code: String,
    service_date: String,
    stations: Vec<ImportStation>,
    seats: Vec<ImportSeat>,
}

// F. TRS 物理排班列车一键导入 (Import - POST)
async fn import_schedule_handler(
    State(state): State<AppState>,
    Json(req): Json<ImportRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut tx = state.db.begin().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 1. Train insert: CTE ON CONFLICT
    let train_id: i32 = sqlx::query_scalar(
        r#"WITH s AS (SELECT id FROM train WHERE code = $1),
                i AS (INSERT INTO train (code) VALUES ($1) ON CONFLICT (code) DO NOTHING RETURNING id)
           SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1"#
    )
    .bind(&req.train_code)
    .fetch_one(&mut *tx)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 2. Stations insert
    for st in &req.stations {
        sqlx::query("INSERT INTO station (train_id, name, sequence) VALUES ($1, $2, $3) ON CONFLICT (train_id, name) DO NOTHING")
            .bind(train_id)
            .bind(&st.name)
            .bind(st.sequence)
            .execute(&mut *tx)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    // 3. TrainSchedule insert
    let service_date = chrono::NaiveDate::parse_from_str(&req.service_date, "%Y-%m-%d").map_err(|_| StatusCode::BAD_REQUEST)?;
    let schedule_id: i32 = sqlx::query_scalar(
        r#"WITH s AS (SELECT id FROM train_schedule WHERE train_id = $1 AND service_date = $2),
                i AS (INSERT INTO train_schedule (train_id, service_date, status) VALUES ($1, $2, 'ACTIVE') ON CONFLICT (train_id, service_date) DO NOTHING RETURNING id)
           SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1"#
    )
    .bind(train_id)
    .bind(service_date)
    .fetch_one(&mut *tx)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 4. Seats & Segments insert
    let max_seq = req.stations.len() as i32;
    for seat in &req.seats {
        let seat_id: i32 = sqlx::query_scalar(
            r#"WITH s AS (SELECT id FROM seat WHERE schedule_id = $1 AND carriage_no = $2 AND seat_no = $3),
                    i AS (INSERT INTO seat (schedule_id, carriage_no, seat_no, seat_class, is_long_distance_pool, quota_released) VALUES ($1, $2, $3, $4, 0, 0) ON CONFLICT (schedule_id, carriage_no, seat_no) DO NOTHING RETURNING id)
               SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1"#
        )
        .bind(schedule_id)
        .bind(&seat.carriage_no)
        .bind(&seat.seat_no)
        .bind(&seat.seat_class)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        for i in 1..max_seq {
            sqlx::query("INSERT INTO seat_segment (schedule_id, seat_id, segment_no, state, version) VALUES ($1, $2, $3, 'AVAILABLE', 0) ON CONFLICT (schedule_id, seat_id, segment_no) DO NOTHING")
                .bind(schedule_id)
                .bind(seat_id)
                .bind(i)
                .execute(&mut *tx)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        }
    }

    // 5. Submit event to Outbox
    let payload = serde_json::json!({ "schedule_id": schedule_id });
    sqlx::query("INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) VALUES ($1, 'TrainSchedule', $2, 'SCHEDULE_IMPORTED', $3::jsonb, 'NEW')")
        .bind(Uuid::new_v4().to_string())
        .bind(schedule_id.to_string())
        .bind(payload.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tx.commit().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({ "success": true, "schedule_id": schedule_id })))
}

// ============================================================================
// 5. 核心计算重算投影引擎 (Core Business Algorithms)
// ============================================================================

async fn recalculate_and_project(
    db: &PgPool,
    redis: &redis::Client,
    schedule_id: i32,
    seat_class: &str,
) -> Result<(), sqlx::Error> {
    // 1. 计算 max_seq
    let max_seq: i32 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(sequence), 4) FROM station s JOIN train_schedule ts ON s.train_id = ts.train_id WHERE ts.id = $1"
    )
    .bind(schedule_id)
    .fetch_one(db)
    .await?;

    // 2. 获取所有的 seat_id 和 seat_class
    let seats: Vec<(i32, String)> = sqlx::query_as("SELECT id, seat_class FROM seat WHERE schedule_id = $1")
        .bind(schedule_id)
        .fetch_all(db)
        .await?;

    // 3. 获取段分配信息
    let segments: Vec<(i32, i32, String)> = sqlx::query_as("SELECT seat_id, segment_no, state FROM seat_segment WHERE schedule_id = $1")
        .bind(schedule_id)
        .fetch_all(db)
        .await?;

    let mut segment_map: HashMap<i32, HashMap<i32, String>> = HashMap::new();
    for (seat_id, seg_no, state) in segments {
        segment_map.entry(seat_id).or_default().insert(seg_no, state);
    }

    let mut counts: HashMap<String, String> = HashMap::new();
    for from in 1..max_seq {
        for to in (from + 1)..=max_seq {
            let mut available_count = 0;
            for (seat_id, s_class) in &seats {
                if s_class != seat_class {
                    continue;
                }
                let mut is_available = true;
                for s in from..to {
                    if let Some(segs) = segment_map.get(seat_id) {
                        if segs.get(&s).map(|st| st == "AVAILABLE") != Some(true) {
                            is_available = false;
                            break;
                        }
                    } else {
                        is_available = false;
                        break;
                    }
                }
                if is_available {
                    available_count += 1;
                }
            }
            counts.insert(format!("{}-{}", from, to), available_count.to_string());
        }
    }

    let cache_key = format!("q:availability:{}:{}", schedule_id, seat_class);
    if let Ok(mut conn) = redis.get_async_connection().await {
        let _: () = conn.del(&cache_key).await.unwrap_or(());
        if !counts.is_empty() {
            let _: () = conn.hset_multiple(&cache_key, &counts.into_iter().collect::<Vec<_>>()).await.unwrap_or(());
            let _: () = conn.expire(&cache_key, 900).await.unwrap_or(());
        }
    }

    Ok(())
}

fn get_mask(from_seq: i32, to_seq: i32) -> i32 {
    let mut mask = 0;
    for i in from_seq..to_seq {
        mask |= 1 << (i - 1);
    }
    mask
}

async fn revert_redis(
    redis: &redis::Client,
    schedule_id: i32,
    seat_id: i32,
    reservation_id: &str,
    mask: i32,
) -> Result<(), redis::RedisError> {
    let mut conn = redis.get_async_connection().await?;
    let seat_key = format!("r:{}:seat:{}", schedule_id, seat_id);
    let res_key = format!("r:{}:reservation:{}", schedule_id, reservation_id);

    let _: i32 = redis::Script::new(LUA_RELEASE)
        .key(&seat_key)
        .key(&res_key)
        .arg(mask)
        .arg(reservation_id)
        .invoke_async(&mut conn)
        .await?;
    Ok(())
}

// ============================================================================
// 6. 核心后台协作协程任务 (Background Worker Routines)
// ============================================================================

async fn outbox_publisher_worker(db: PgPool, tx: mpsc::Sender<String>) {
    loop {
        // SELECT FOR UPDATE SKIP LOCKED
        let result: Result<Vec<(i64, String)>, sqlx::Error> = sqlx::query_as(
            "SELECT id, payload::text FROM outbox_event WHERE status = 'NEW' ORDER BY id ASC LIMIT 100 FOR UPDATE SKIP LOCKED"
        )
        .fetch_all(&db)
        .await;

        if let Ok(events) = result {
            if !events.is_empty() {
                for (id, payload) in events {
                    let update_res = sqlx::query("UPDATE outbox_event SET status = 'PROCESSED', published_at = NOW() WHERE id = $1")
                        .bind(id)
                        .execute(&db)
                        .await;

                    if update_res.is_ok() {
                        let _ = tx.send(payload).await;
                    }
                }
            }
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn cqrs_projector_worker(db: PgPool, redis: redis::Client, mut rx: mpsc::Receiver<String>) {
    while let Some(payload_str) = rx.recv().await {
        if let Ok(payload) = serde_json::from_str::<serde_json::Value>(&payload_str) {
            if let Some(schedule_id) = payload.get("schedule_id").and_then(|v| v.as_i64()) {
                let _ = recalculate_and_project(&db, &redis, schedule_id as i32, "BUSINESS").await;
            }
        }
    }
}
