# 🌌 12306 高并发票务分配系统 — 生产级 Prometheus & Grafana 监控与智能告警架构设计方案

本规范定义了 12306 跨多语言（Python, Go, C#, Java, Rust）共享的高并发区间票务分配系统在生产环境（Production）下的 **Prometheus 监控指标清册、Grafana 核心监控看板设计面板** 以及基于 **PromQL 与 Alertmanager 的故障自愈告警规则集**。

---

## 1. 全景监控拓扑与多语言指标收集模型 (Metrics Collection Model)

系统采用 **Prometheus Pull 模型** 异步拉取各节点指标。对于五套物理等价的后端，其内置的 Prometheus SDK 暴露完全一致的指标命名规范（Metric Naming Conventions）和 `/metrics`（或 `/actuator/prometheus`）端点：

```text
========================================================================================================
                                     METRICS COLLECTION TOPOLOGY
========================================================================================================

  ┌───────────────────┐    ┌─────────────────┐    ┌──────────────────┐
  │   Python Engine   │    │    Go Engine    │    │   .NET Engine    │
  │   (Port 8000)     │    │   (Port 8001)   │    │   (Port 8002)    │
  └─────────┬─────────┘    └────────┬────────┘    └────────┬─────────┘
            │ /metrics              │ /metrics             │ /metrics
            ▼                       ▼                      ▼
    ┌──────────────┐        ┌──────────────┐       ┌──────────────┐
    │ Java Engine  │◄───────┤  PROMETHEUS  ├──────►│ Rust Engine  │
    │ (Port 8003)  │        │  (Pull Core) │       │ (Port 8004)  │
    └──────────────┘        └───────┬──────┘       └──────────────┘
      /actuator/prometheus          │                /metrics
                                    ▼
                            ┌──────────────┐
                            │   GRAFANA    │ (Visual Dashboards)
                            └──────────────┘
```

### 多语言指标收集技术选型：
*   **Python (FastAPI)**: 使用 `prometheus_client` 库，在多进程模式（Gunicorn/Uvicorn multiprocess）下采用共享内存目录方式，统一暴露系统级度量。
*   **Go**: 采用 `github.com/prometheus/client_golang/prometheus` 库，注册高并发原生 Collector，拉取极致平快。
*   **C# (.NET)**: 引入 `prometheus-net.AspNetCore` 中间件，无缝集成到 Minimal API 的 WebHost 管道。
*   **Java (Spring Boot)**: 启用 `io.micrometer:micrometer-registry-prometheus` 与 Spring Boot Actuator，通过 `/actuator/prometheus` 极速映射。
*   **Rust (Axum)**: 使用 `metrics-exporter-prometheus` 库，作为异步 Middleware 拦截 Tokio 任务时间分布。

---

## 2. 谷歌 SRE“四大黄金信号”监控大图 (The 4 Golden Signals)

我们将业界公认的 **谷歌 SRE 四大黄金信号（Latency, Traffic, Errors, Saturation）**，针对 12306 位图及 CQRS 读写分离引擎进行深度场景化定制：

### A. 响应时延 (Latency)
*   **指标定义**：系统处理一次 HTTP/RPC 请求所需的耗时（单位：秒）。
*   **核心监控端点**：`/api/v1/reserve` (核心锁票) 与 `/api/v1/query` (高速余票查询)。
*   **Prometheus 指标**：
    *   `http_request_duration_seconds_bucket{method="POST", path="/api/v1/reserve"}`
    *   `http_request_duration_seconds_bucket{method="GET", path="/api/v1/query"}`
*   **指标看板**：P99 / P95 / P90 / Mean 响应时间分布柱状图。

### B. 吞吐流量 (Traffic)
*   **指标定义**：系统正在承受的瞬时访问压力，以 QPS（Queries Per Second）或 TPS（Transactions Per Second）度量。
*   **Prometheus 指标**：
    *   `rate(http_requests_total[1m])`
*   **指标看板**：写通道（Reserve / Order / Pay）QPS 趋势线，读通道（Query）QPS 趋势线。

### C. 错误率 (Errors)
*   **指标定义**：请求处理失败的比例，包括 HTTP 5xx 状态码、事务回滚率、Lua 碰撞打回率。
*   **Prometheus 指标**：
    *   `rate(http_requests_total{status=~"5.."}[1m])`
    *   `rate(ticketing_reserve_collisions_total[1m])` (Lua 位图过滤未通过的总次数)
*   **指标看板**：5xx 错误率占比饼图、Lua 预占锁碰撞阻断比率（锁拒绝/锁通过）。

### D. 饱和度 (Saturation)
*   **指标定义**：系统最受限的资源水位，用以评估服务何时会“被撑爆”（瓶颈卡点）。
*   **Prometheus 指标**：
    *   `db_pool_active_connections` (PostgreSQL 活跃物理连接数占比)
    *   `redis_memory_used_bytes` / `redis_memory_max_bytes` (Redis 内存使用饱和度)
    *   `process_cpu_seconds_total` / `process_resident_memory_bytes` (微服务节点 CPU/内存饱和度)
*   **指标看板**：数据库连接池水位百分比、Redis 内存配额仪表盘、Tokio 协程数 / JVM 堆内存饱和水位线。

---

## 3. CQRS & EDA 深度监控指标规范 (CQRS & EDA Deep Inspection)

除了常规信号外，本系统特有的 CQRS 读写分离架构与事件驱动机制，必须增加以下**分布式最终一致性核心指标**，防止大促期间产生消息堆积（Lag）或读模型失真。

### A. 事务发件箱事件堆积延迟 (Transactional Outbox Event Lag)
*   **定义**：在 PostgreSQL 中处于 `NEW` 状态尚未被 Poller 协程捞取并发布的 Outbox 记录条数。
*   **计算公式**：
    $$[Outbox\ Lag] = \text{MAX}(id) - \text{MIN}(id) \quad \text{WHERE status = 'NEW'}$$
*   **Prometheus 指标**：
    *   `ticketing_outbox_lag_events` (由 Outbox 轮询协程在捞取时动态 Gauge 输出)
*   **指标看板**：代表写模型 EDA 发送通道的“消化通畅度”。**如果 Lag 呈单调上升，表明底层 Event Bus 或 Poller 协程消费卡死。**

### B. 读模型投影时延 (CQRS Projection Delay)
*   **定义**：一个购票成功的 Reservation 状态落盘，到其最新的可用票额数据成功在 Redis Hash（`q:availability:...`）中投影更新所消耗的物理时间差（最终一致性滞后窗口）。
*   **Prometheus 指标**：
    *   `ticketing_projection_delay_seconds_bucket` (记录从 Outbox Event 产生到 Projector 覆盖 Redis 成功的全生命周期时耗)
*   **指标看板**：投影延时直方图。**目标：P99 投影延时必须限制在 200ms 以内，防止用户看到过期的假票数（Ghost Tickets）。**

### C. 锁池竞争率 (Lock Contention Rate)
*   **定义**：在 PostgreSQL 底层对 `seat_segment` 执行 `FOR UPDATE` 悲观互斥锁排队时所消耗的排队等待时延与死锁规避发生率。
*   **Prometheus 指标**：
    *   `ticketing_db_lock_wait_time_seconds`
*   **指标看板**：行级锁持锁时长与等待深度。

---

## 4. Grafana 看板布局与面板规格设计 (Grafana Dashboard Panels)

推荐的 Grafana 监控大盘应划分成三大视觉排版区域（Three-Tier Visual Hierarchy）：

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              12306 REAL-TIME MONITORS (DASHBOARD)                      │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 【ROW 1：业务交易核心大盘】                                                             │
│  ┌──────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────┐  │
│  │   Reservations (QPS)     │  │   Avg Latency (Reserve)  │  │  Lua Collision Rate  │  │
│  │   Value: 4.8k /s  [📈]   │  │   Value: 24ms      [🟢]  │  │  Value: 88.2%  [🔥]  │  │
│  └──────────────────────────┘  └──────────────────────────┘  └──────────────────────┘  │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 【ROW 2：CQRS 与 EDA 一致性流水线（SRE 最关心的核心指标）】                              │
│  ┌──────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────┐  │
│  │   Outbox Event Lag       │  │   Projection Delay P99   │  │  Channel Queue Depth │  │
│  │   Value: 48 (Normal)     │  │   Value: 84ms      [🟢]  │  │  Value: 2/1000       │  │
│  └──────────────────────────┘  └──────────────────────────┘  └──────────────────────┘  │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 【ROW 3：基础设施物理饱和度】                                                           │
│  ┌──────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────┐  │
│  │  Postgres ConnPool Water │  │  Redis Mem Saturation    │  │  Tokio/JVM ThreadCnt │  │
│  │  Value: 34% (Safe)       │  │  Value: 41% (Safe)       │  │  Value: 48 (Safe)    │  │
│  └──────────────────────────┘  └──────────────────────────┘  └──────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Alertmanager 智能故障自愈告警规则集 (PromQL Alerts)

以下是运行于生产环境 Prometheus 内的 **标准 PromQL 告警规则定义文件（`alerting_rules.yml`）**，用于针对各种核心故障（如位图锁失效、发件箱阻塞、数据库过载）执行秒级警报发现与自动化弹性自愈触发：

```yaml
groups:
  - name: 12306_core_engine_alerts
    rules:

      # -----------------------------------------------------------------------
      # 1. 核心锁票时延报警（系统降级与扩容前兆）
      # -----------------------------------------------------------------------
      - alert: TicketingReserveLatencyTooHigh
        expr: histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{path="/api/v1/reserve"}[1m])) by (le)) > 0.5
        for: 15s
        labels:
          severity: critical
          tier: api
        annotations:
          summary: "12306 核心锁票端点 P99 时延突破 500ms！"
          description: "当前锁票 P99 平均耗时为 {{ $value }} 秒（已持续超过 15s），可能存在数据库锁激烈竞争、锁排队（FOR UPDATE Contention）或 Redis Lua 阻塞！"

      # -----------------------------------------------------------------------
      # 2. EDA 发件箱积压严重（分布式最终一致性破坏报警）
      # -----------------------------------------------------------------------
      - alert: EDAOutboxEventLagSpike
        expr: ticketing_outbox_lag_events > 5000
        for: 1m
        labels:
          severity: warning
          tier: eda
        annotations:
          summary: "EDA Outbox 发件箱事件积压数突破 5000 条！"
          description: "当前 NEW 状态未处理的 Outbox 消息积压量为 {{ $value }} 条，表明事件分发组件（Event Publisher）或本地管道产生性能瓶颈或阻塞，读模型更新极度滞后！"

      # -----------------------------------------------------------------------
      # 3. 读大屏查询高延迟报警
      # -----------------------------------------------------------------------
      - alert: CQRSQueryLatencyTooHigh
        expr: histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{path="/api/v1/query"}[1m])) by (le)) > 0.1
        for: 30s
        labels:
          severity: warning
          tier: api
        annotations:
          summary: "余票查询 GET /api/v1/query P95 时延突破 100ms！"
          description: "当前余票查询 P95 耗时为 {{ $value }} 秒，严重影响读大屏幕实时刷新率。请立刻核查 Redis 宿主机 CPU、哈希缓存热 Miss 重算比率（Thundering Herd Risk）。"

      # -----------------------------------------------------------------------
      # 4. 购票核心错误率飙升
      # -----------------------------------------------------------------------
      - alert: TicketingErrorRateSpike
        expr: (sum(rate(http_requests_total{status=~"5.."}[1m])) / sum(rate(http_requests_total[1m]))) * 100 > 1.0
        for: 10s
        labels:
          severity: critical
          tier: api
        annotations:
          summary: "购票核心接口系统 5xx 错误率突破 1%！"
          description: "当前购票全系统接口的 5xx 异常错误率占比已达 {{ $value }}%（已持续 10s），表明有大批购买流事务发生 Rollback 或报错，请立刻核查错误日志！"

      # -----------------------------------------------------------------------
      # 5. 基础设施：数据库连接池枯竭
      # -----------------------------------------------------------------------
      - alert: PostgresConnectionPoolExhausted
        expr: (db_pool_active_connections / db_pool_max_connections) * 100 > 90
        for: 30s
        labels:
          severity: critical
          tier: db
        annotations:
          summary: "PostgreSQL 物理连接池可用配额不足 10%！"
          description: "当前连接池饱和度已达 {{ $value }}%，面临物理连接资源枯竭风险。请立刻核查数据库慢查询、未及时释放的持久连接或长事务，并准备通过 HPA 自动横向扩容微服务副本。"
