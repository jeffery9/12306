# 🌌 12306 高并发票务分配系统 — 全景文档索引与五阶段导航地图

本导航手册作为 12306 系统设计与 SRE 运维全景文档库的主索引（Master Index）。它按研发与生产治理的五个核心生命周期（业务需求、核心算法、逻辑架构、对称开发、SRE 生产级高可用），组织并串联了全网 **21 册独立技术白皮书与设计方案**。

---

## 🗺️ 1. 全景文档系统五阶段索引地图 (Consensus Documentation Map)

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                 12306 高并发票务分配系统 — 全景文档五阶段索引地图               │
│                                                                             │
│  [01 业务与需求篇]                                                            │
│   ├── 01_01 PO 手册 ────────► 商业价值、周期预估与人月核算                     │
│   └── 01_02 BDD 验收规约 ───► EPIC 1-10 核心用例与 Gherkin 场景描述            │
│                                                                             │
│  [02 核心算法篇]                                                              │
│   └── 02_01 区间段位图预占 ─► O(1) 并发锁座位移掩码与段位合并算法               │
│                                                                             │
│  [03 技术架构篇]                                                              │
│   ├── 03_01 技术架构设计 ───► CQRS、内存预分配与 PostgreSQL 最终一致落盘       │
│   ├── 03_02 事件驱动 EDA ───► Transactional Outbox 锁安全日志、Kafka 顺序重放   │
│   └── 03_03 现代客流调度 ───► 长途优先、公用段动态席位滑窗机制                 │
│                                                                             │
│  [04 跨语言实现篇]                                                            │
│   ├── 04_01 技术全景概述 ───► Python/Go/Rust/C#/Java 对称控制、Savepoint 改签  │
│   └── 04_02 Python 开发 ────► OrderService/ReservationService & Redis LUA 细节  │
│                                                                             │
│  [05 生产高可用与 SRE 运维篇]                                                  │
│   ├── 05_01 IDC 物理拓扑 ───► Spine-Leaf HA、3-Stage Clos、BGP 交换机端口限额  │
│   ├── 05_02 K8s 集群编排 ───► 生产级容器编排、多维 Limits、资源配额限制       │
│   ├── 05_03 弹性伸缩 ───────► K8s HPA、KEDA 基于 Kafka 堆积自动扩缩          │
│   ├── 05_04 安全约束 ───────► WAF 防刷、DDoS 缓解、API Gateway 限流          │
│   ├── 05_05 容量规划 ───────► 峰值 QPS 线程池推导、位图内存大小、高并发布线     │
│   ├── 05_06 财务预算 ───────► 本地 IDC 建设与云端 AWS CapEx/OpEx 财务对比公式   │
│   ├── 05_07 多活容灾 ───────► 异地多活 SOS 静态分片、武汉 etcd 秒级写自锁机制   │
│   ├── 05_08 限流自愈 ───────► SRE Funnel 限流、漏桶防刷、动态虚拟排队          │
│   ├── 05_09 SLA 估算证明 ───► 并/串行可用度、马尔可夫状态转移学术级公式证明    │
│   ├── 05_10 运维响应 ───────► SRE On-Call 班表、故障等级 Severity 划分       │
│   ├── 05_11 监控报警 ───────► Prometheus 硬件指标、PDU、QPS 告警 PromQL 规则  │
│   ├── 05_12 缓存自愈 ───────► 雪崩、击穿、穿透三大模型、随机 TTL、DCL          │
│   ├── 05_13 Caching 降级 ───► 内存预占多级存储降级规则、抗 SPOF 软降级方案     │
│   ├── 05_14 PG 部署 ────────► Patroni 共识主备、流复制巡检、ASC Lexicographical  │
│   └── 05_15 Go/PG 网络 ─────► 硬件光纤直连、Go 数据库连接池 TCP 参数对齐      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📂 2. 五阶段文档目录与点击导航 (Specification Directory & Links)

### 2.1 第一阶段：产品业务与验收规范 (Product & BDD Specifications)
* 🔗 **[01_01 产品负责人 PO 核心研发手册](01_01_12306_PO_Product_Owner_Manual.md)**
  * *业务概要*：定义了 12306 系统的真实商业价值。包含 8 人团队在 5-6 个月生命周期内的成本预算、MVP 工作量分配和各开发引擎的技术难点排期评估。
* 🔗 **[01_02 核心 Epic 用户故事与 BDD 验收规约](01_02_12306_Epic_User_Stories_BDD_Specifications.md)**
  * *业务概要*：罗列了 `EPIC-01` 至 `EPIC-10` 的十个核心业务功能。提供在 `pytest-bdd` 框架下可执行的 Gherkin 测试用例，严格规范了并发防撞（Collision Guard）与退改签嵌套事务。

### 2.2 第二阶段：核心算法设计 (Core Algorithms Design)
* 🔗 **[02_01 车次区间段位图（Segment Bitmap）内存预占算法设计](02_01_12306_Segment_Bitmap_Reservation_Algorithm_Design.md)**
  * *业务概要*：阐述了高并发扣减的核心基石。采用二进制位移位掩码（Bitmask Shift）表达座位所有区间段的状态，将复杂的区间购票锁座扣减和余票合并操作折叠为高效的 O(1) 内存位运算。

### 2.3 第三阶段：逻辑技术架构 (Technical Logical Architecture)
* 🔗 **[03_01 高并发票务系统技术架构设计方案](03_01_12306_High_Concurrency_Ticketing_Technical_Architecture.md)**
  * *业务概要*：系统核心技术架构设计。提出基于 CQRS 读写分离的模型，即“Redis 极速内存扣减 + Transactional Outbox + 数据库延时落盘一致性合并”机制。
* 🔗 **[03_02 12306 事件驱动架构（EDA）设计方案](03_02_12306_Event_Driven_Architecture_EDA_Design.md)**
  * *业务概要*：详述跨微服务的高通量事件流同步。设计了基于 PostgreSQL 物理事务安全的 `outbox_event` 本地日志表和 Kafka 高频生产者，保障已支付订单的 eventual consistency。
* 🔗 **[03_03 现代票务与客流调度综合解决方案](03_03_12306_Modern_Ticketing_and_Passenger_Traffic_Scheduling.md)**
  * *业务概要*：从铁路营运角度解析售票策略。剖析了长途客流优先保障方案、短途公用段动态席位滑窗机制，解决车次沿途座位“利用率最大化”痛点。

### 2.4 第四阶段：多开发语言落地 (Cross-Engine Development Guide)
* 🔗 **[04_01 跨多语言对称实现概述与改签事务流指南](04_01_12306_Technical_Implementation_Overview.md)**
  * *业务概要*：定义了核心引擎在 Go、Rust、C#、Java、Python 里的对称性实现指标。深度阐述了改签业务下基于 PostgreSQL 嵌套 Savepoint 的“先退旧后买新”子事务状态一致性控制。
* 🔗 **[04_02 Python 核心开发指南与源码细节](04_02_12306_Python_Implementation_and_Development_Guide.md)**
  * *业务概要*：Python 后端服务的物理架构细节。提供 `OrderService` 滑动费率退款逻辑、`ReservationService` 锁票和 Redis Lua 状态原子扣减的底层落地细节。

### 2.5 第五阶段：生产级高可用与 SRE 运维指南 (SRE High Availability Playbook)
* 🔗 **[05_01 本地 IDC Spine-Leaf 网络架构设计](05_01_12306_On_Premises_IDC_High_Availability_Spine_Leaf_Architecture.md)**
  * *业务概要*：物理 IDC 的网络层基石。设计了基于 3-Stage Clos 拓扑的非阻塞线速转发，计算 Leaf 与 Spine 的多线互联冗余配额和 BGP EVPN 控制面规约。
* 🔗 **[05_02 生产级多环境 K8s 集群编排指南](05_02_12306_Production_Multi_Environment_Deployment_and_K8s_Orchestration.md)**
  * *业务概要*：定义 12306 的多环境容器标准。规范了在 K8s 生产命名空间下各 Pod 的多维 CPU/Memory Limits 与 Requests 资源配额。
* 🔗 **[05_03 云原生弹性伸缩（HPA / KEDA）方案](05_03_12306_Cloud_Native_Autoscaling_K8s_HPA_KEDA_Specifications.md)**
  * *业务概要*：应对极端客流高峰的动态算力扩容。定义了基于 Kafka 消费组消息堆积指标与 Pod 系统负荷的 KEDA HPA 扩缩容灵敏度及磁滞（Hysteresis）平滑算法。
* 🔗 **[05_04 非功能性需求与系统安全约束规范](05_04_12306_Non_Functional_Requirements_and_Security_Constraints.md)**
  * *业务概要*：保护国家级关键基础设施。设计了由边界 Web 应用防火墙（WAF）层层拦截、DDoS 流量清洗及 API 密钥验签高频限流组成的安全防刷防爬屏障。
* 🔗 **[05_05 亿级超高并发容量规划与硬件选型白皮书](05_05_12306_High_Concurrency_Capacity_Planning_and_Hardware_Sizing.md)**
  * *业务概要*：通过严密的数学与并发公式进行物理容量测算。推导了高峰期并发线程数、席位位图内存吞吐限制、网络出口带宽和磁盘 NVMe IOPS 需求。
* 🔗 **[05_06 系统运行成本估算与财务预算白皮书](05_06_12306_High_Concurrency_System_Cost_Modeling_and_Financial_Budget.md)**
  * *业务概要*：IT 架构师的财务概算红线。对比了本地自建 IDC（CapEx）与租用 AWS 公有云资源（OpEx）在计算、存储、网络层面的 5 年期 TCO 财务对比公式。
* 🔗 **[05_07 两地三中心异地多活容灾架构](05_07_12306_Multi_Region_Active_Active_Disaster_Recovery_Architecture.md)**
  * *业务概要*：国家级重大灾难自愈。详细阐明了车次发车局 SOS 分片、**武汉东西湖 etcd 租约自锁（Fail-Closed）秒级防脑裂接管机制**与 Tier-IV 电力冗余。
* 🔗 **[05_08 SRE 多级限流熔断与动态虚拟排队 playbook](05_08_12306_SRE_Rate_Limiting_Circuit_Breaker_and_Dynamic_Queuing_Playbook.md)**
  * *业务概要*：防止高峰期后端雪崩。部署基于客户端、Anycast 边界和网关层的漏桶、令牌桶多向漏斗形限流，建立动态排队平滑处理算法。
* 🔗 **[05_09 SLA 可用性指标数学推导证明白皮书](05_09_12306_High_Availability_SLA_Estimation_and_Mathematical_Proof.md)**
  * *业务概要*：学术级高可用理论。利用马尔可夫过程（Markov Chain）状态转移方程和串并联系统组合可用度，推导证明 12306 满足年度 99.999% 的五九可用性指标。
* 🔗 **[05_10 SRE On-Call 与生产故障等级事件管理运维手册](05_10_12306_SRE_Team_Building_and_OnCall_Operations_Manual.md)**
  * *业务概要*：运维红线与故障应急流程。规范了 P1-P4 级别的系统故障、On-Call 24/7 班表响应时限（SLA）与应急群防机制。
* 🔗 **[05_11 SRE Prometheus 与 Grafana 监控报警拓扑设计](05_11_12306_SRE_Prometheus_Grafana_Monitoring_and_Alerting_Architecture.md)**
  * *业务概要*：全景物理监控指标。提供采集 CPU、JVM、DB Conn、HTTP SLA 等关键指标的 PromQL 表达式，规划了 PDU/Sentry 联动告警推送网格。
* 🔗 **[05_12 SRE 缓存穿透、击穿、雪崩三大模型物理防御白皮书](05_12_12306_SRE_Cache_Breakdown_Penetration_Avalanche_Mitigation_Whitepaper.md)**
  * *业务概要*：防止数据库底座被瞬时高并发击垮。提出了引入随机时间抖动（Jitter TTL）、Singleflight/DCL 双重检查和 Redis LUA 逻辑限流三大物理应对屏障。
* 🔗 **[05_13 SRE Caching 多级高可用抗 SPOF 故障降级架构白皮书](05_13_12306_SRE_Cache_High_Efficiency_and_Anti_SPOF_Multi_Tier_Degradation_Architecture.md)**
  * *业务概要*：设计了多级高吞吐缓存抗单点故障软件降级流程。阐述了内存预占与物理 Redis 抖动、断开连带时，系统无缝且平稳向本地冷温介质过渡的降级标准。
* 🔗 **[05_14 PostgreSQL 物理集群 Patroni 高可用高并发部署指南](05_14_12306_DB_PostgreSQL_Cluster_Architecture_Deployment_and_Programming_Guide.md)**
  * *业务概要*：关系型存储底座的部署高标准。利用 Patroni 和 etcd 提供 Primary 选举，规划了 `pg_stat_replication` 的巡检语句、死锁 ASC 字母序自解。
* 🔗 **[05_15 生产 Go 与 PostgreSQL HA 融合及网络控制白皮书](05_15_12306_Production_Go_Postgres_HA_and_Spine_Leaf_Consolidation_Whitepaper.md)**
  * *业务概要*：在物理 Spine-Leaf 暗光纤直拉下，优化 Go 语言原生客户端数据库连接池 TCP 状态。保障高通量读写在网络边缘极速传输、无重试积压。
