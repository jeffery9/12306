# 🌌 12306-CQRS-Python: AI Agent 赋能的 12306 高并发区间票务分配系统 MVP

[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/tests-11%20%2F%2011%20Passed-brightgreen.svg)](#-5-自动化测试与高并发撞击压测)

> **解构中国铁路级难题**：12306 作为全球并发写峰值最高、售票区间逻辑最复杂的票务系统之一，民间存在诸多关于“海量锁冲突、库存超卖、长途腿抢占短途腿、数据库瞬间瘫痪”的传说与技术猜想。
> 
> 本项目以 **CQRS（读写分离）** 与 **EDA（事件驱动）** 为核心架构思想，在 **AI Agent（Gemini CLI 极致去幻觉协议 + ChatGPT 首席架构师）** 的深度协作下，使用纯粹而精纯的 **Python 3.9 + FastAPI + SQLAlchemy + Redis Lua + Kafka** 核心生态，实现了针对“一车多站、区间座位复用、防死锁、最终一致性”这一 L3/L4 级别核心难题的工业级可运行垂直切片 MVP。

---

# 1. 12306 难题拆解与核心设计猜想 (The Speculative Architecture)

### ❓ 核心痛点：区间座位复用 (Segment Seat-Reuse)
设有一趟列车经停：**北京 ──► 天津 ──► 济南 ──► 上海**。
- 如果旅客 A 购买了【北京 ──► 天津】段，此座位在【天津 ──► 上海】段依然可以再次售卖给旅客 B。
- 如果旅客 C 购买了全线贯通的【北京 ──► 上海】票，则该座位的 3 个子区间段将被全部买断，不再对 A 和 B 开放。

这导致：
1. **传统的数据库行级锁（如直接 `SELECT ... FOR UPDATE`）会导致全车次行级锁冲突极度严重**，高并发下导致事务排队闪退或数据库死锁。
2. **长短途库存争抢问题**：长途票会吃掉全部短途腿，短途腿又会碎片化长途空间。

### 💡 我们的破局猜想：双防御、读写分离、内存位掩码（CQRS & Bitmask）
本系统抛弃了传统的“票仓数量减 1”的设计，采用 **“区间段位图（Bitmap）预占 + 数据库排序段级行锁”** 的物理方案：

```text
  站点序列 (Station Sequence):   [北京] (1) ───► [天津] (2) ───► [济南] (3) ───► [上海] (4)
  物理区间 (Physical Segments):             [段 1]          [段 2]          [段 3]
  二进制对应位 (Binary Bits):                bit 0           bit 1           bit 2
  二进制位图空间 (Bitmap Layout):             bit 2           bit 1           bit 0
  位图表示 (Binary Segment Mask):          [ 段 3 ]        [ 段 2 ]        [ 段 1 ]
```

- **第一防御线（Redis 极速预占）**：在 Redis 缓存中使用一个 Integer 值的二进制位来代表一个座位的占用状态。购票动作等价于一次原子的位运算 `AND` 与 `OR`。
- **第二防御线（MySQL 物理事务）**：当 Redis 扣减成功后，异步提交到 MySQL 事务中。MySQL 中不保存整趟车的“库存数”，只维护原子分段 `SeatSegment`。加锁时，强制按照 `seat_id` 和 `segment_no` **升序（ASC）**加锁，从物理上 100% 杜绝并发写事务产生的死锁。
- **CQRS 最终一致性**：写请求绝不在事务中等待 Kafka 发送、API 扣款或缓存重建。MySQL 产生的 `ORDER_PAID` 事件通过本地 **Transactional Outbox 发件箱表** 归档，由后台 Worker 采用 `SKIP LOCKED` 毫秒级捞取并异步发布到 Kafka，最终由 **Projector 投影器** 异步幂等重建 Redis 读缓存。

---

# 2. 系统物理架构拓扑 (System Topology)

本系统严格实现读写模型完全物理隔离：

```text
                         ┌─────────────────────┐
                         │   Client (HTTP)     │
                         └──────────┬──────────┘
                                    │
                         ┌──────────▼──────────┐
                         │   FastAPI Web App   │
                         └──────┬──────────┬───┘
                                │          │
           ┌────────────────────┘          └────────────────────┐
           ▼ (Command Side)                                     ▼ (Query Side)
    [ Command Service ]                                  [ Query Service ]
   (Reserve / Order / Pay)                                (GET /api/v1/query)
           │                                                    │
     (Redis First Filter)                                  (Cache Read)
           ├──────────────► [ Redis Cache ] ◄───────────────────┤
           ▼           ( r:{sched}:seat:{id} )                  │
     (DB Transaction)  ( q:availability:{id} )                  │
           │                                                    │
     [ MySQL Shard ]                                            │
   ( SeatSegment Lock )                                         │
   ( OutboxEvent Write )                                        │
           │                                                    │
           ▼ (Polling FOR UPDATE SKIP LOCKED)                   │
   [ Outbox Publisher ]                                         │
           │                                                    │
           ▼ (Publish)                                          │
    [ Kafka Topic ] ────────────────────────────────────────────┘
    (ticket_events)                 (Process Event & Update Cache)
                                                  │
                                                  ▼
                                          [ Projector ]
```

---

# 3. 技术栈与技术亮点 (Technology Highlights)

- **开发语言**：Python 3.9+ （利用 `asyncio` 提供极高并发处理吞吐，摒弃繁重的多线程切换）。
- **Web 框架**：FastAPI （原生支持高并发协程、自动生成 OpenAPI 文档）。
- **ORM 与数据库**：SQLAlchemy Async + aiomysql + MySQL 8.0（全面拥抱协程驱动、数据库连接池，行级锁隔离设计）。
- **缓存引擎**：Redis 7.0 （通过加载自研位掩码 Lua 脚本保证预占锁座的高性能、原子性）。
- **消息队列**：Apache Kafka 3.7 + ZooKeeper（用于解耦写服务与投影器，根据车次 `schedule_id` 自动进行分区，保障单车次事件绝对保序消费）。
- **测试框架**：Pytest-BDD（行为驱动） + 协程同步桥接器。

---

# 4. 项目目录结构 (Directory Structure)

```text
/Users/jeffery/Downloads/12306/
├── src/
│   ├── app/
│   │   ├── main.py                # FastAPI Web 路由层与自愈式读写隔离控制器
│   │   ├── database.py            # SQLAlchemy AsyncEngine 及异步会话生命周期
│   │   ├── models.py              # 高一致性物理表设计（Outbox、Idempotency 幂等表等）
│   │   ├── redis_client.py        # Redis 连接池及核心预占/释放 Lua 脚本库
│   │   ├── reservation_service.py # 分布式双防御锁座服务
│   │   ├── order_service.py       # 订单状态机与超时未支付背景回收机制
│   │   ├── outbox_publisher.py    # SKIP LOCKED 本地事务发件箱高性能发送 Worker
│   │   └── projector.py           # Kafka 最终一致性事件投影处理器
│   └── tests/
│       ├── features/
│       │   └── ticketing.feature  # BDD 用户故事描述（标准 Gherkin 语法）
│       ├── conftest.py            # 会话级事件循环、一键表重构、Redis/Kafka 干净 reset 夹具
│       ├── test_database.py       # 数据库物理 DDL 联通性测试
│       ├── test_redis_lua.py      # 区间重合/非重合原子 Lua 检票测试
│       ├── test_reservation_service.py # 核心订票拦截测试
│       ├── test_order_service.py  # 订单状态回滚与 Redis 状态倒带测试
│       ├── test_outbox_publisher.py # Outbox 模式 Kafka 分区保序消费测试
│       ├── test_projector.py      # 缓存一致性重算投影测试
│       ├── test_api_endpoints.py  # Web 全链路端到端 HTTP 测试
│       ├── test_bdd_ticketing.py  # Gherkin 行为驱动真实场景验收测试
│       └── test_concurrency_stress.py # 50路极端高并发撞击与死锁检测压力测试
├── pytest.ini                     # Pytest-Asyncio 全局会话作用域配置文件
└── docker-compose.yml             # MySQL + Redis + Kafka 分布式物理环境一键编排
```

---

# 5. 自动化测试与高并发撞击压测 (Test & Benchmark)

本项目在开发全流程中严格贯彻 **TDD（测试驱动开发）** 与 **BDD（行为驱动开发）**。

### 🧪 运行全量测试套件
```bash
# 激活本地虚拟环境并执行 pytest
source venv/bin/activate
python -m pytest src/tests/ -v
```

### 📈 50 路并发压力碰撞压测验证
在 `test_concurrency_stress.py` 中，我们对 BUSINESS 等级的单列车、单座席注入了 **50 个并发写协程同时抢夺经停区间**：
- **北京 ──► 天津（段 1）**：20 路抢票
- **天津 ──► 上海（段 2 & 3）**：20 路抢票
- **北京 ──► 上海（全程）**：10 路抢票

**压测验证结果（100% 正确性）**：
1. **区间复用极致体现**：北京->天津的一张票、天津->上海的一张票在毫秒间被同时并发抢订成功，一物理座席实现 100% 运力翻倍！
2. **长短途冲突完美拦截**：10 个试图全线通铺【北京->上海】的长途抢票由于区间已被抢占，安全遭遇 `0 预占`，系统在极端并发写碰撞下：
   - **0 超卖**
   - **0 脏数据**
   - **0 数据库死锁 / 0 唯一主键约束冲突**。

---

# 6. 本地开发与现场冒烟调试 (Get Started & Curl Tests)

### 🐳 1. 开启分布式基础设施
```bash
# 一键拉起 MySQL、Redis、Kafka 镜像（确保本地 Docker 已运行）
docker-compose up -d
```

### 🐍 2. 初始化 Python 虚拟环境与依赖
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 🔌 3. 启动 FastAPI API 实例
```bash
python -m uvicorn src.app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 🚀 4. 发起 curl 撞击调试流
请参照最新的 [12306_Python_MVP_技术实现手册.md](./docs/12306_Python_MVP_技术实现手册.md) 中的 **第 4 节 (API 端点现场调用与冒烟调试指南)**，通过 6 个原子的 `curl` 请求对以下流程执行手动调试验证：
- 余票冷查询回源重算重建缓存 ──► 并发抢票预占 ──► 待支付订单创建 ──► 模拟支付扣款 ──► 触发后台 Outbox 消息循环 ──► 读写最终一致性检验。

---

# 🤖 7. 关于 AI Agent 协同研发的故事 (The Agentic Story)

本项目的成功合龙是 **ChatGPT（首席架构师）** 与 **Gemini CLI（开发执行官 - YOLO 自动驾驶模式）** 协同作战的结晶：
- ** ChatGPT **：担任 Principal Architect，敲定了 12306 高并发系统的 CQRS 及 Event-Driven 顶层演进路线，保证了架构在分布式层级的优雅降级与正确性。
- ** Gemini CLI **：担任 Execution Engine，运行在 YOLO 绝对去冗余和去 AI 废话模式下。在多轮开发和多 Loop 错位的冲突调试中，Gemini 严格履行 Andrej Karpathy 总结的“谋定后动”、“手术刀式精准修改”与“强目标验证交付”准则，自主排查死锁与 Kafka 消费队列污染，最终完成了整套测试的完美一统。

*AI 与人类工程师在 12306 这一千古难题上的这次极简交锋，证明了高度自律的微内核架构与精准测试套件的结合，完全可以让软件开发效率实现十倍级的降维打击。*
