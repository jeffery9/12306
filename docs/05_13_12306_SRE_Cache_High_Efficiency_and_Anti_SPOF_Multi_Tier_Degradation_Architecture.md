# 🌌 12306 高并发票务分配系统 — 极速缓存效率优化与 Redis 单点故障（SPOF）高可用平稳降级设计白皮书

在大高并发、超低时延的 12306 区间分配系统下，**缓存（Redis）** 既是高并发查询（CQRS 读模型）的效率脊梁，又是高并发区间锁定（Lua 原子预占）的写模型第一道盾牌。这意味着：**Redis 已经成为系统核心链路中的最大单点故障源（SPOF, Single Point of Failure）**。

一旦 Redis 整体宕机或发生长时网络抖动，系统不仅将损失全网 $O(1)$ 的高速查询能力，更会导致高并发写的 Lua 预占机制瘫痪，全部抢票洪峰将瞬间硬着陆到数据库行锁上，直接引发系统全面瘫痪。

本白皮书针对该问题，设计了 **L1 本地内存 + L2 缓存分布式多级缓存架构**，并确立了 **Redis SPOF 故障下的高可用柔性无损降级回退协议**，实现金融级系统的持续可用性。

---

## 1. 缓存效能极致优化：L1/L2 双层多级缓存架构 (Multi-Tier Cache)

为了彻底释放分布式 Redis 的网络带宽与 CPU 时延开销，读大屏路径采用 **L1 本地内存（Microsecond Level） + L2 Redis（Millisecond Level）** 的双层多级缓存模型：

```text
========================================================================================================
                               L1/L2 MULTI-TIER CACHE ARCHITECTURE WITH PUB/SUB
========================================================================================================

                  GET /api/v1/query
                          │
                          ▼
             ┌─────────────────────────┐
             │ L1 Local Memory Cache   │ (Process Local: e.g., Cache-Aside / LRU)
             └────────────┬────────────┘
                          │
                          ├────────(Hit - Microseconds)────────► [Return Stale-Safe Ticket Count]
                          │ (Miss)
                          ▼
             ┌─────────────────────────┐
             │ L2 Distributed Redis    │ (Port 6379 / Cluster Sharded)
             └────────────┬────────────┘
                          │
                          ├────────(Hit - Milliseconds)────────► Cache Fill to L1 ──► [Return]
                          │ (Miss)
                          ▼
             ┌─────────────────────────┐
             │ PostgreSQL DB Recalculate│
             └────────────┬────────────┘
                          │
                          ▼ (Projector Async Broadcasts Eviction)
             ┌─────────────────────────┐
             │ Redis Pub/Sub Broadcast ├──────────────┐
             └─────────────────────────┘              │
                                                      ▼ (Notify All App Instances)
                                         ┌─────────────────────────┐
                                         │  Invalidate L1 Cache    │ (Keep eventual consistency)
                                         └─────────────────────────┘
========================================================================================================
```

### A. 读屏 L1 缓存极速卡位
*   在各语言微服务进程内，开辟极速、零网络 IO 的本地内存哈希表（如 Go `sync.Map`、Rust `DashMap`、C# `ConcurrentDictionary`）。
*   **本地生存期**：设置极短的本地生存期（如 1s 至 3s）。由于本地读取时耗在 **微秒（μs）级**，这能将同一个车次的重复查询在应用层就地拦截过滤，免去对 Redis 建立网络连接与协议编解码的开销，Redis QPS 压力可骤降 **80%+**。

### B. 基于 Redis 发布订阅（Pub/Sub）的主动失效机制
由于 L1 缓存是分布在不同微服务应用进程内的本地内存，一旦出票成功（状态变更），L1 缓存必须能够以毫秒级时效获得刷新。
*   **发布机制**：当 Projector 协程计算出最新的区间余票，写入 Redis（L2）后，统一向 Redis 广播通道 **`channel:cache_evict`** 发布一个失效事件。
*   **订阅与驱逐**：全网所有微服务应用实例启动时均作为订阅者监听 **`channel:cache_evict`**。收到特定车次 `schedule_id` 的驱逐通知后，**瞬间异步清空其本地 L1 缓存**，实现读写模型的毫秒级最终一致，彻底防止用户看到“幽灵票额”。

---

## 2. 物理无单点高可用堆叠：Redis 生产集群拓扑 (Redis Cluster)

为在物理上根除 Redis 单点故障风险，12306 生产环境必须杜绝任何 Standalone 单机模式，采用高能物理堆叠：

```text
========================================================================================================
                           REDIS HIGH-AVAILABILITY CLUSTER TOPOLOGY
========================================================================================================

                             [ Client Requests / Write & Read ]
                                             │
                       ┌─────────────────────┴─────────────────────┐
                       ▼ (Lua Writes)                              ▼ (Massive Reads)
             ┌───────────────────┐                       ┌───────────────────┐
             │  Master Nodes     │                       │  Replica Nodes    │
             │  (Shard 1 - 128)  │                       │  (Shard 1 - 128)  │
             └─────────┬─────────┘                       └─────────┬─────────┘
                       │                                           │
                       │ (Asynchronous Mirroring)                  │
                       └───────────────────►◄──────────────────────┘
                                             │
                                   ┌─────────┴─────────┐
                                   │  Sentinel Array   │ (3-Node Quorum Failover)
                                   └───────────────────┘
========================================================================================================
```

### A. 128 分片集群 (Cluster Sharding)
*   余票与物理席位 Key 以 `schedule_id` 作为 Hash Tag（例如 `{schedule:123}:availability` ），强制将同一车次的所有相关 Key 绑定、分配到同一个 Redis 分片（Node）。
*   通过集群哈希环物理横向扩展，最大支持 128 对主从主物理集群，分摊整体并发压力。

### B. 读写分离路由 (Read-Write Splitting)
*   **写锁端点**：`/api/v1/reserve` 的 Redis Lua 预占锁执行，必须强制路由至 **Redis 主节点（Master Nodes）**，保证数据的绝对原子性和强一致。
*   **读屏端点**：`/api/v1/query` 的 HGET 余票读取，完全路由至 **Redis 从节点（Replica Nodes）**。这保证了数以百万计的背景查询流量绝不消耗主节点的 CPU 算力，为主节点 Lua 锁座管道留出充裕的黄金运算带宽。

---

## 3. SPOF 触发时的柔性无损降级回退协议 (Graceful Fallback)

如果发生了极低概率的“Redis 集群半数以上物理节点崩溃、导致 Redis 服务整体失联/瘫痪”这一地狱级灾难，系统必须能够**不假死、不崩溃、持续安全售票**。

为此，我们在核心端点上内置了**柔性高可用熔断回退控制器（Circuit Breaker & Fallback Controller）**：

### A. 核心锁票端点 `/api/v1/reserve` 的无缓存熔断降级
1.  **异常捕获与熔断**：
    当请求进入锁票路由时，应用检测到 Redis 客户端抛出连接异常或超时。控制器在 50ms 内快速中断 Redis 访问，宣告“Redis 熔断隔离区生效”。
2.  **优雅回退至 PostgreSQL 行级锁保障（Direct DB Lock Fallback）**：
    系统跳过 Redis Lua 预占，**直接将完整流量导流至底层 PostgreSQL 事务中**。由于数据库事务内严格采用了针对目标车次座位物理升序排位的 `SELECT ... FOR UPDATE` 互斥悲观锁机制，**即便完全没有 Redis，底层数据库也能 100% 严丝合缝、防超卖、防重票地安全出票**！
3.  **柔性限流保底**：
    由于失去了 Redis Lua 缓存的阻断保护，PostgreSQL 所能承载的写并发从万级回退到千级。降级机制会立刻联动网关，对该车次执行 **SRE 柔性限流（50% 抛弃或排队）**。虽然购票成功率有所降低，但系统在没有 Redis 保护下**依然持续运转、出票准确性完美保持 100%**。

### B. 核心查询端点 `/api/v1/query` 的降级读大屏
1.  **静态本地兜底**：
    若 Redis 无法提供 HGET 服务，查询处理器直接返回应用实例本地 **L1 缓存中的最后一次有效票额**，并强制附带前端提示 *“大屏刷新延迟”*。
2.  **旁路副本回源**：
    对于穿透 L1 的核心查询，不回源 Master 数据库，而是将极少量的重算流路由至 PostgreSQL 的 **只读副本（Read Replicas）**。写库（Master DB）受到物理隔离保护，安全系数极高。

---

## 4. SPOF 自愈降级控制器伪代码实现 (Fallback Controller Implementation)

以下是在我们高并发引擎写锁路由（`Reserve`）中内置的高可用熔断降级控制骨架实现（以 **Go / Rust 核心原语**为例）：

```rust
// 🌌 写锁端点柔性熔断降级核心逻辑 (Rust Rust-App Built-in Protection)
async fn reserve_handler_fallback_safe(
    State(state): State<AppState>,
    Json(req): Json<ReserveRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let mask = get_mask(req.from_station_seq, req.to_station_seq);
    let reservation_id = Uuid::new_v4().to_string();
    let seat_key = format!("r:{}:seat", req.schedule_id); // 简化 Hash-Tag
    let res_key = format!("r:{{{}}}:reservation:{}", req.schedule_id, reservation_id);

    // ───────────────────────────────────────────────────────────────────────
    // 【第 1 关】尝试 Redis Lua 预占阻断 (以 20ms 极短超时保护，防止 Redis SPOF 挂死系统)
    // ───────────────────────────────────────────────────────────────────────
    let mut redis_available = false;
    let mut reserved_seat_id = 0;

    let redis_conn_result = tokio::time::timeout(
        Duration::from_millis(20),
        state.redis.get_async_connection()
    ).await;

    if let Ok(Ok(mut conn)) = redis_conn_result {
        redis_available = true;
        
        // 尝试获取物理座位并执行原子 Lua 锁
        if let Ok(seat_ids) = fetch_seat_list_cached(&state.db, req.schedule_id, &req.seat_class).await {
            for seat_id in seat_ids {
                let s_key = format!("r:{{{}}}:seat:{}", req.schedule_id, seat_id);
                let lua_res: Result<i32, _> = redis::Script::new(LUA_RESERVE)
                    .key(&s_key)
                    .key(&res_key)
                    .arg(mask)
                    .arg(&reservation_id)
                    .arg(900)
                    .invoke_async(&mut conn)
                    .await;

                if let Ok(1) = lua_res {
                    reserved_seat_id = seat_id;
                    break;
                }
            }
        }
    }

    // ───────────────────────────────────────────────────────────────────────
    // 【第 2 关】SPOF 触发判断：若 Redis 宕机或锁票无可用，进入降级自愈分支
    // ───────────────────────────────────────────────────────────────────────
    if !redis_available {
        // [SRE NOTICE] Redis 单点故障已触发！系统无缝开启 PostgreSQL 强一致行锁保底机制！
        // 触发限流，阻断 50% 流量，防止数据库发生死锁爆表
        if rand::random::<u32>() % 100 < 50 {
            return Err((StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
                "error": "系统繁忙，正在柔性排队中，请重试 (SPOF Soft Rate Limited)"
            }))));
        }

        // 强行穿透 Redis，直接进入 PostgreSQL 物理座位 FOR UPDATE 悲观互斥抢票链路
        return db_only_reserve_fallback(&state.db, req, reservation_id).await;
    }

    if reserved_seat_id == 0 {
        return Err((StatusCode::CONFLICT, Json(serde_json::json!({"error": "No seats available (Redis filtered)"}))));
    }

    // ───────────────────────────────────────────────────────────────────────
    // 【第 3 关】Redis 正常，执行常规 PostgreSQL 强一致性本地事务落盘 (与 Redis 状态对齐)
    // ───────────────────────────────────────────────────────────────────────
    match execute_db_transaction(&state.db, req, reserved_seat_id, &reservation_id).await {
        Ok(_) => Ok(Json(serde_json::json!({
            "reservation_id": reservation_id,
            "seat_id": reserved_seat_id,
            "channel": "Standard (Redis + DB Double Shield)"
        }))),
        Err(_) => {
            // 事务回退 Redis 预占状态
            let _ = revert_redis(&state.redis, req.schedule_id, reserved_seat_id, &reservation_id, mask).await;
            Err((StatusCode::CONFLICT, Json(serde_json::json!({"error": "State mismatch, booking failed"}))))
        }
    }
}
```

---

## 5. 总结 (Summary)

把缓存当成不可动摇的黄金后台是不智的。通过引入 **L1/L2 级主动失效多级缓存**，我们将分布式 Redis 的网关交互时延最大化压缩，实现了微秒级的高能卡位；同时，内置的 **Redis SPOF 高可用熔断降级控制器**，将 Redis 单点故障下的崩溃风险直接消解为“有序限流下的无损 PostgreSQL 悲观行锁直接抢票”。该设计实现了**即使 Redis 整体物理粉碎崩溃，全国春运售票依然能安全、准确、稳健、持续开展**的数字高可用长城！
