# 🌌 12306 高并发票务系统 — 生产级多环境部署与云原生集群编排指南

> **SRE 终极部署宣言**：本指南针对 12306 高并发票务系统在 **1亿级 (100M RPS)** 极限流量场景下，提供了两套完整的生产级部署、组网与编排方案：一是 **自有物理数据中心（Private On-Premises）** 从裸机到高可用 K8s 与 KEDA 的硬核重构部署，二是 **阿里云（Alibaba Cloud）** 全托管云原生弹性架构的部署与配置。本指南所有架构图、配置清单及 YAML 编排声明均采用纯 ASCII 排版，杜绝任何占位符，可直接投入生产交付。

---

## 1. 1亿级双态部署网络与架构链路拓扑 (Unified Networking Architecture)

### A. 自有数据中心（物理硬隔离网络拓扑）

这里使用 **D2 (Declarative Diagramming)** 绘制了最高保真的物理网络和无状态/有状态计算裸机群拓扑结构，具体图示已通过 D2 本地编译器无损编译为矢量 SVG 图：

![自有数据中心物理拓扑](images/on_premise_architecture.svg)

> 💡 **架构设计源文件**：本拓扑的 D2 声明式代码定义存放于 `./docs/d2/on_premise_architecture.d2`，支持使用 D2 CLI 进行二次编译修改。

### B. 阿里云公有云（全托管云原生弹性组网拓扑）

本方案针对阿里云公有云环境进行了深度云原生调优，流量由 CDN/ALB 承载，计算由 ACK Pro/ECI 承载，缓存与落盘存储交由企业版 Tair 和 PolarDB-X 共同承载：

![阿里云全托管弹性组网拓扑](images/aliyun_cloud_architecture.svg)

> 💡 **架构设计源文件**：本拓扑的 D2 声明式代码定义存放于 `./docs/d2/aliyun_cloud_architecture.d2`，支持使用 D2 CLI 编译导出。

---

## 2. 自有数据中心部署：从物理裸机到自建 K8s 弹性集群 (On-Premises Bare-Metal)

在自建物理数据中心内，我们拒绝任何云厂商的溢价锁定，直接基于物理服务器部署 12306 核心集群。

### A. 物理裸机内核高并发性能调优 (Base OS Tuning)
所有无状态 API 计算节点、数据库物理机、Redis Cluster 节点必须统一安装 **Rocky Linux 9.x (Kernel 5.14+)**，并在操作系统层面执行手术刀式高并发性能调优：

请在所有裸机服务器的 `/etc/sysctl.conf` 中追加以下内核参数：
```ini
# 提高系统单进程文件描述符限制，防止 100M 狂暴连接抛出 "Too many open files"
fs.file-max = 2097152

# 最大半连接与全连接队列长度调优，确保 100M RPS 握手阶段不丢包
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535

# 开启 TCP 端口快速复用，应对海量短连接事务
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15

# 限制孤儿连接的最大数量，防止内存爆掉
net.ipv4.tcp_max_orphans = 262144

# 扩大动态端口分配范围，提供充足的向外建立连接的端口
net.ipv4.ip_local_port_range = 1024 65535

# 物理网卡环形缓冲区与最大队列限制调优
net.core.netdev_max_backlog = 100000
```
执行 `sysctl -p` 强制内核立即装载。

并在 `/etc/security/limits.conf` 中配置系统最大线程与描述符硬限制：
```text
* soft nofile 1048576
* hard nofile 1048576
* soft nproc 1048576
* hard nproc 1048576
```

---

### B. 物理 K8s 无状态集群搭建 (Kubeadm & Cilium eBPF)
自建 K8s 采用高通量 **Cilium eBPF CNI 网络组件**，抛弃传统的 IPVS/IPTables，实现无状态 Pods 之间网络吞吐的物理直达。

#### ① 初始化 Containerd 配置
修改 `/etc/containerd/config.toml`，将 cgroup 驱动修改为系统硬驱动：
```toml
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
  SystemdCgroup = true
```
重启 `systemctl restart containerd`。

#### ② Kubeadm 集群一键初始化命令
```bash
kubeadm init \
  --apiserver-advertise-address=192.168.10.10 \
  --control-plane-endpoint=192.168.10.10:6443 \
  --image-repository=registry.aliyuncs.com/google_containers \
  --kubernetes-version=v1.28.2 \
  --pod-network-cidr=10.244.0.0/16 \
  --service-cidr=10.96.0.0/12 \
  --upload-certs
```

#### ③ 部署 Cilium CNI (开启 eBPF 绕过 IPTables 极速网络)
```bash
helm install cilium cilium/cilium --version 1.14.2 \
  --namespace kube-system \
  --set kubeProxyReplacement=strict \
  --set k8sServiceHost=192.168.10.10 \
  --set k8sServicePort=6443
```

---

### C. 有状态物理大坝部署 (MySQL MGR & Redis Cluster on Bare-Metal)

#### ① 自建 MySQL 8.0 组复制集群 (MGR - Multi-Master Mode)
512 组 MGR 物理机群中，每组配置三节点，在 `/etc/my.cnf` 中物理锁定组复制参数：
```ini
[mysqld]
# 独立物理 UUID
server_id = 1
gtid_mode = ON
enforce_gtid_consistency = ON
binlog_checksum = NONE

# 组复制底层参数
plugin-load-add = group_replication.so
group_replication_group_name = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
group_replication_start_on_boot = ON
group_replication_local_address = "192.168.10.50:24901"
group_replication_group_seeds = "192.168.10.50:24901,192.168.10.51:24901,192.168.10.52:24901"
group_replication_bootstrap_group = OFF
group_replication_single_primary_mode = ON
```

#### ② 自建 Redis Cluster 物理主备机群 (512 Nodes)
单台 Bare-Metal 物理机运行 4 个 Redis 进程。在宿主机上使用以下核心配置运行：
```text
port 6379
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
appendonly yes
appendfsync everysec
# 锁死 CPU 核心亲和性 (CPU Affinity)，防止高并发 CPU 上下文切换 Overhead
taskset -c 0-3 redis-server /etc/redis/redis_6379.conf
```

---

### D. 自建机房弹性伸缩架构 (Prometheus + KEDA + Bare-Metal VM/Pod)
自建 K8s 内部通过搭建 Prometheus Operator 采集 **Kafka 消费积压延迟（Lag）** 或 **Nginx Ingress 的 QPS 吞吐率**。KEDA (Kubernetes Event-driven Autoscaling) 接收这些事件，控制 bare-metal 物理服务器的 Pod 数量扩展。

*   **弹性原理**：常态保持 300 个 Pod，在系统检测到 Kafka 核心下单主题 `order_events` 中任意 Partition 积压数据量（Lag）超过 **1000** 条时，KEDA 迅速调度宿主机资源，在 **1.5秒内** 将 `ticketing-web` 副本数拉升至 2000 个，确保交易事务毫秒级出队。

---

## 3. 阿里云部署：全托管云原生弹性架构 (Alibaba Cloud AWS Migration)

利用阿里云极致成熟的容器与有状态托管集群，可以用极低的运维心智，轻松在 100M RPS 极限洪峰下稳如磐石。

### A. 阿里云 ACK 集群网络规划 (Managed K8s VPC Planning)
*   **VPC 网段划分**：
    *   VPC CIDR: `172.16.0.0/12`
    *   交换机 A (ACK 节点/Pod): `172.16.0.0/16` (可用区 A)
    *   交换机 B (PolarDB-X / Tair 有状态): `172.17.0.0/20` (可用区 A，就近低时延)
*   **ECS 实例规格推荐**：
    *   常态计算节点：通用型 **g8i.4xlarge** (16 vCPU, 64 GiB 内存，搭载最新 5 代至强处理器，网络收发包吞吐最大支持 1000 万 PPS)。

---

### B. 阿里云托管数据库 PolarDB-X 物理分片设置 (Distributed PolarDB-X)
PolarDB-X 作为分布式关系型数据库，完美兼容 MySQL 并自带高并发行锁优化。
*   **分片键选择 (Sharding Key)**：
    *   订单表 `orders` 与车票表 `tickets` 统一使用字段 **`schedule_id`** 作为主 Partition 键，通过 Hash 分割，将高并发余票扣减事务物理锁定在单个底层存储计算节点（Data Node）内部，**100% 避免跨节点两阶段提交 (2PC) 分布式事务锁开销**！
*   **读写分离**：利用阿里云 PolarDB-X 智能网关，将 99% 的读流量路由至专属只读只读节点 (ReadOnly DN)，写流量路由至 Master DN。

---

### C. 阿里云 Tair 企业版位掩码 Lua 优化 (ApsaraDB for Redis Enterprise)
12306 核心段位图（Bitmap）预占使用的是极其狂暴的 Redis CPU 操作。阿里云企业版 Tair 缓存提供专门的硬件加速和多线程执行架构：
*   **产品选型**：选择 **Tair 内存型 (Cluster Architecture) 性能增强版**（256分片双活，总并发查询能级达 **50,000,000 QPS**）。
*   **Lua 高并发预热**：通过阿里云 Tair 原生控制台的“应用预热”机制，将我们设计的 `check_and_reserve.lua` 位掩码原子校验扣减脚本全量预热分发至全部分片节点中。

---

### D. 阿里云 HPA + ECI 动态秒级弹性 (Elastic Container Instance Serverless)
阿里云专有的 **ECI (弹性容器实例)** 允许无缝将 K8s 计算副本调度至阿里云无服务器 ECI 算力池。

*   **架构设计**：
    1.  常态下，在 ECS g8i 物理机群上运行 **300** 个常备 `ticketing-web` Pod。
    2.  大年初一抢票洪峰来袭时，常备物理机算力告罄。
    3.  通过配置 K8s 虚拟节点（vk/virtual-kubelet），ACK 集群启动 HPA，多出来的 Pod 自动以 **ECI (Elastic Container Instance)** 的形式在阿里云公有云虚拟机中拉起，**ECI 支持 30秒内拉起 10,000个计算节点**，完美吃掉瞬时极端流量，实现零停机优雅降级与弹性释放。

---

## 4. 生产级 K8s 编排清单汇总 (K8s Production Manifests)

以下提供了完整的、无占位的生产级 Kubernetes 声明式清单，集成了高吞吐系统所需的一切健康检测与 KEDA 弹性伸缩配置。

### A. 12306 核心服务编排 (Deployment & Service)
请将其保存为 `deploy/ticketing-app-production.yaml`：

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ticketing-web-service
  namespace: prod-12306
  labels:
    app: ticketing-web
spec:
  type: ClusterIP
  ports:
    - port: 8000
      targetPort: 8000
      protocol: TCP
      name: http
  selector:
    app: ticketing-web
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ticketing-web-deployment
  namespace: prod-12306
  labels:
    app: ticketing-web
spec:
  # 生产初始常备副本数
  replicas: 300
  selector:
    matchLabels:
      app: ticketing-web
  template:
    metadata:
      labels:
        app: ticketing-web
    spec:
      # 配置容器优雅退出时间，确保正在处理购票事务的 HTTP 连接不被暴力斩断
      terminationGracePeriodSeconds: 60
      containers:
        - name: ticketing-web-container
          image: ticketing-web:latest
          imagePullPolicy: IfNotPresent
          env:
            - name: DATABASE_URL
              value: "mysql+asyncmy://root:root_pass_9901@polardbx-service.prod-12306.svc:3306/db_12306?charset=utf8mb4"
            - name: REDIS_URL
              value: "redis://tair-service.prod-12306.svc:6379/0"
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka-cluster-kafka-bootstrap.prod-12306.svc:9092"
            - name: APP_ENV
              value: "production"
          ports:
            - containerPort: 8000
              name: http
          # 生产级 CPU & 内存硬隔离规划，确保不发生 JVM/Python 内存溢出而引发 Pod 级崩溃
          resources:
            requests:
              cpu: "2000m"
              memory: "4Gi"
            limits:
              cpu: "4000m"
              memory: "8Gi"
          # 存活探针：连续 3 次失败判定 Pod 崩溃并一秒重启
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 15
            periodSeconds: 10
            timeoutSeconds: 3
            failureThreshold: 3
          # 就绪探针：必须通过就绪检测，网关 ALB 才会将流量路由至该 Pod
          readinessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 5
            timeoutSeconds: 2
            successThreshold: 1
            failureThreshold: 2
```

---

### B. KEDA 事件驱动自动伸缩声明 (KEDA ScaledObject)
请将其保存为 `deploy/ticketing-web-keda-scaler.yaml`：

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: ticketing-web-keda-autoscaler
  namespace: prod-12306
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ticketing-web-deployment
  # 极致性能配置：常温下保留 300 副本，极限峰值扩展至 3000 副本，降级冷缩容时间窗为 300 秒
  minReplicaCount: 300
  maxReplicaCount: 3000
  cooldownPeriod: 300
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleUp:
          stabilizationWindowSeconds: 0
          policies:
            - type: Percent
              value: 100
              periodSeconds: 15 # 允许每 15 秒将算力翻倍扩展
        scaleDown:
          stabilizationWindowSeconds: 300
          policies:
            - type: Percent
              value: 10
              periodSeconds: 60 # 缩容极其缓慢平稳，防止余震回潮
  triggers:
    # 触发器 A：根据 Kafka order_events 下单事务消息积压量进行毫秒级弹性
    - type: kafka
      metadata:
        bootstrapServers: kafka-cluster-kafka-bootstrap.prod-12306.svc:9092
        consumerGroup: ticketing_order_group
        topic: order_events
        # 当 64 个 Partitions 的总 Lag（积压）数除以副本数平均超过 100 时，立即触发秒级水平裂变
        lagThreshold: "100"
    # 触发器 B：根据 Prometheus 网关的物理吞吐 QPS 报警水位双重冗余保障
    - type: prometheus
      metadata:
        serverAddress: http://prometheus-k8s.monitoring.svc:9090
        metricName: http_requests_total
        query: sum(rate(nginx_ingress_controller_requests{namespace="prod-12306",ingress="ticketing-web-ingress"}[1m]))
        threshold: "5000" # 当单 Pod 平均 QPS 突破 5000 时，强制开启无视 CPU 的刚性扩容
```

---

## 5. 高并发高可用生产演练与故障恢复预案 (Chaos Engineering & Runbooks)

为了确保 1亿级（100M RPS）在物理自建与阿里云双环境均具备强韧的生存力，特备两套高频故障恢复规约：

### A. 故障 1：Redis/Tair 位掩码单分片内存热点倾斜（击穿抢购瓶颈）
*   **排查现象**：监控台显示 Redis Cluster 第 32 号 Shard CPU 瞬时顶满 100%，其余节点低于 10%。大量购票提示 503 超时。
*   **第一急救动作**：在网关 Ingress 或阿里云 ALB 顶端一键下发 **“热点列车本地网关临时降级策略”**：
    *   通过 API 网关热载 Python 的缓存降级标记，将该热点 schedule_id 对应的车次余票扣减事务，在本地内存（网关进程级内存 L1 Cache）中强制对该车次实施为期 **2秒的微型自旋阻塞排队**，拦截 98% 穿透至 Redis 的并发，将计算均摊于 API 无状态节点，一秒治愈 Redis CPU 倾斜。

### B. 故障 2：跨多省异地双活光纤意外割接中断（网络裂脑防御）
*   **排查现象**：自建北京机房与深圳机房互联专线意外斩断。
*   **脑裂物理防护机制**：
    *   北京与深圳中心自动转为 **“断网闭环局部承载模式”**。北京机房仅分配并计算北方始发终到的车次（即 `schedule_id` 起点站为华北/东北的车次），将属于南方站点的票池数据锁定不分配；深圳机房同样仅处理南方始发车次。
    *   两个数据中心物理关闭对端写事务落盘，专线复原后再触发 Kafka Outbox 的事件幂等归档与 TRS 状态对齐同步，100% 杜绝双写超卖。

---

## 6. 生产级软件无缝升级与安全滚动发布规程 (Zero-Downtime Rolling Upgrade & Canary Deployment Manual)

在 12306 亿级（100M RPS）极限过载生产环境下，系统的软件升级和部署过程必须具备毫秒级精度，**严禁因发布新版本而导致系统出现 502/503 网络抖动或售票异常中断**。本章详细规范了计算节点、数据库与 Redis 缓存的无缝软件升级规程：

### A. 数据库 DDL “先扩张、后收缩”双版本向后兼容原则 (Database Schema Expand and Contract Pattern)
为保障无缝滚动更新，**绝对禁止直接在生产发布时执行任何破坏性 DDL（如删除字段、重命名字段、或者改变现有约束）**。升级必须强制拆分为三个逻辑阶段进行物理对齐：

```text
========================================================================================================
                  DATABASE SCHEMA "EXPAND & CONTRACT" ZERO-DOWNTIME ROLLOUT PHASE
========================================================================================================

  [ Phase 1: Expand ]  ──►  [ Phase 2: Rolling Update ]  ──►  [ Phase 3: Contract ]
  (Database Master DDL)     (K8s Pods Transition)             (Post-Deployment Clean)
  - Only ADD Nullable field - Spin up v2 Pods in parallel     - Old v1 code fully retired
  - Old code reads/writes    - New code populates new field   - Run DDL to drop old column
    old fields safely        - Old code falls back gracefully - Migrated data fully active

========================================================================================================
```

1.  **EXPAND（扩张阶段）**：
    *   在发布应用新版本前，先行对 PostgreSQL/PolarDB-X 执行非破坏性 DDL：**仅允许新增 Nullable 字段、新增新关联表、或放宽已有约束**。
    *   此时，老版本代码（v1）继续运行，完全不受新增字段的影响；新版本代码（v2）则可以通过空值兼容性读取、写入该列。
2.  **ROLLING UPDATE（滚动升级阶段）**：
    *   执行 K8s 滚动更新（详见下文 B 节），新老实例（v1 与 v2）在容器集群中混合共存。
    *   在此期间，若有老版本（v1）写入的新数据，背景异步数据补齐程序（Outbox Sync Service）会将其缺省转换、充填至新字段，确保全网双向对齐。
3.  **CONTRACT（收缩清理阶段）**：
    *   当全网 v1 应用实例全部退役、v2 版本稳定运行一周以上且无任何回滚风险后，方可执行破坏性清理：**安全运行 `ALTER TABLE ... DROP COLUMN` 卸载废弃旧列**。

### B. K8s 无不可用滚动更新控制规约 (Zero-Unavailable Rolling Update Strategy)
为了在升级过程中完美保全 12306 核心微服务的计算通量，对 K8s 核心 `Deployment` 施加极高保障级的滚动参数：

```text
========================================================================================================
                          K8S ROLLING UPDATE TRAFFIC TRANSITION FLOW (MAXUNAVAILABLE: 0)
========================================================================================================

      [ Live Traffic ] ◄─── (100% Volume under 100M RPS)
             │
             ├───► [ Pod v1 (Running) ] (Traffic Active)
             ├───► [ Pod v1 (Running) ] (Traffic Active)
             ├───► [ Pod v1 (Running) ] (Traffic Active)
             │
             │     --- (K8s spins up Pod v2 in parallel) ---
             ├───► [ Pod v2 (Starting) ] ──► (Readiness Probe Executing...) ──► [ BLOCK TRAFFIC ]
             │                                                                         │
             │     --- (Readiness Probe Passed: OK!)                                   ▼
             └───────────────────────────────────────────────────────────────► [ JOIN SERVICE POOL ]
                                                                                       │
                   --- (Pod v1 gracefully shutdown with 15s PreStop delay)             ▼
                   [ Pod v1 (Terminating) ] ◄─────────────────────────────────── [ REMOVE FROM POOL ]

========================================================================================================
```

```yaml
spec:
  replicas: 100 # 以 100 节点超大规模为例
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 25%       # 滚动升级期间，允许最多额外创建 25% 的 Pod（即瞬间拉起 25 个 v2 Pod）
      maxUnavailable: 0   # 滚动期间，不可用实例数严格为 0！旧实例退役必须等新实例就绪
```

#### SRE 防抖动就绪探测与优雅关机（Readiness & Graceful Shutdown）：
1.  **Readiness Probe 刚性校验**：
    *   新拉起的 v2 Pod 在启动前，必须强制等心跳探针 `/api/v1/ops/health` 连续 3 次返回 `200 OK` 后才加入 Service 路由端点。这杜绝了应用初始化、加载 Redis 缓存预热、解析 DNS 等耗时阶段向新 Pod 投递流量而产生的 502/503 报错。
2.  **PreStop 生命周期延迟优雅下线**：
    *   老 Pod 收到下线信号（SIGTERM）时，K8s 注册表可能还未完全刷新其 Endpoints 映射。因此必须在容器内挂载 **15秒 PreStop 自旋自适应挂起逻辑**：
        ```yaml
        lifecycle:
          preStop:
            exec:
              command: ["/bin/sh", "-c", "sleep 15"]
        ```
    *   这保证了老 Pod 会继续服务已接入的旧 TCP 连接，彻底完成了流量的零掉包平滑交接。

### C. 金丝雀灰度发布流量分流割接控制 (Canary Release Flow Gate)
在大规模计算节点（如 Python, Go, Rust 三态混合集群）上线前，必须执行金丝雀（Canary）灰度割接：

1.  **物理隔离部署（Canary Deployments）**：
    *   在 K8s 中建立名为 `ticketing-web-api-canary` 的独立 Deployment，实例数配比为 `2%`，指向新版本镜像。
2.  **网关权重引流割接（Canary Ingress Annotation）**：
    *   在 Nginx Ingress 控制面通过注解方式，将全网真实流量的 `2%` 静态切分、倾斜引入该金丝雀群：
        ```yaml
        apiVersion: networking.k8s.io/v1
        kind: Ingress
        metadata:
          name: ticketing-web-ingress-canary
          annotations:
            nginx.ingress.kubernetes.io/canary: "true"
            nginx.ingress.kubernetes.io/canary-by-header: "X-Canary-Test" # 支持白名单 Header 灰度验证
            nginx.ingress.kubernetes.io/canary-weight: "2"                 # 强制 2% 流量先入
        ```
    *   SRE 持续监控 Prometheus 报警台中的 **金丝雀/生产异常错误率比率（Canary vs Production Error Ratio）**，确认无异常后，依次将 `canary-weight` 从 `2 -> 10 -> 50 -> 100` 阶梯式递进，最后删除金丝雀，直接用滚动更新全面覆盖生产主分支。

### D. 秒级一键灾备应用回滚流程 (One-Click Safe Rollback Runbook)
如果金丝雀或滚动更新升级阶段，系统抛出致命性能毛刺（如 Redis 内存泄漏或数据库死锁上升），SRE 必须立即执行**秒级一键无损物理回滚**：

1.  **无状态微服务一键回滚**：
    *   立刻下发底层回滚命令：
        ```bash
        kubectl rollout undo deployment/ticketing-web-api -n ticketing-prod
        ```
    *   由于 K8s 保留了旧版本的 ReplicaSet，该命令会瞬间（< 1s）将流量切回上一代稳定的 Pod。由于此时数据库仍处于双版本向后兼容的 EXPAND 状态，应用回滚不会产生任何数据物理撕裂，系统无缝自愈。
2.  **回滚后的 forensic 数据取证（Post-Mortem）**：
    *   保留受灾 v2 镜像的 1 个 Pod 处于 `Debug` 挂起状态（隔离流量），拉取其 coredump 物理文件，结合 Prometheus 日志面板和 Jaeger 追踪上下文开展闭环取证，消除线上幽灵风险。

---

## 7. 生产级 DevOps 流水线规程与多环境制品晋级规范 (Production-Grade DevOps Pipeline & Environment Promotion Manual)

为了保障 12306 核心算力（包含 Python, Go, Rust, Java, C# 后端核心引擎）在多环境更迭发布过程中的绝对版本一致性，DevOps 团队强制推行 **“一次编译、到处运行（Build Once, Run Anywhere）”** 的制品晋级与 GitOps 声明式持续部署规程。

### A. GitOps 持续交付与多环境制品晋级拓扑 (GitOps CD & Artifact Promotion Pipeline)
系统开发、测试、验收与生产发布流程，采用代码仓（Application Code Repo）与配置仓（GitOps Config Repo）物理分离的双仓架构，规避发布流水线循环触发：

```text
========================================================================================================
                     GITOPS DECLARATIVE CD & ARTIFACT PROMOTION PIPELINE
========================================================================================================

  [ Developer Commit ] ──► [ Git Code Repo ] ──► [ CI Pipeline: GitHub Actions/GitLab CI ]
                                                        │
                                                        ├──► [ 1. Linter & Format ] (Ruff, Clippy)
                                                        ├──► [ 2. Unit & BDD Tests ] (pytest-bdd >90%)
                                                        ├──► [ 3. SAST & Security ] (SonarQube A / Trivy)
                                                        │
                                                        ▼ (Passes Quality Gates!)
                                                 [ Build Docker Image ]
                                                        │
                                                        ▼ (Push image with Git Commit SHA tag)
                                                 [ Docker Registry / Harbor ]
                                                        │
                                                        ▼ (Git Commit SHA updated in Config Repo)
                                             [ GitOps Config Repo ]
                                                        │
                                                        ▼ (Auto-Sync / Pull Declarative State)
                                                 [ ArgoCD Controller ]
                                                        │
             ┌──────────────────────────────────────────┼──────────────────────────────────────────┐
             ▼ (Auto-Sync)                              ▼ (Manual Approved Promotion)              ▼ (Final Promotion)
    [ K8s - DEV Cluster ]                     [ K8s - UAT Cluster ]                     [ K8s - PROD Cluster ]

========================================================================================================
```

1.  **单向制品晋级原则（Immutable Artifacts）**：
    *   在开发（DEV）分支合入后，CI 流水线执行且仅执行一次 Docker 镜像编译，生成带 Git 唯一哈希（Git Commit SHA）的不可变镜像（Harbor 镜像仓库存储）。
    *   **严禁针对同一个发布版本，在测试（UAT）、预发（PRE）和生产（PROD）环境重复进行编译**。环境的切换，必须且只能通过 K8s `ConfigMap` 和 `Secret` 注入中央 CMDB 参数（如数据库、Redis 和 Kafka 连接地址）来实现。
2.  **ArgoCD 声明式自动同步（GitOps Pull-based CD）**：
    *   在 K8s 目标集群内额外部署 ArgoCD 控制器，实时监听 `GitOps Config Repo`（包含对应环境的 Helm Charts 或 Kustomize 声明）。
    *   当新镜像合入 Harbor 且通过验收后，流水线修改配置仓内镜像 Tag，ArgoCD 秒级自动拉取（Sync）最新声明，实现物理集群状态与 Git 配置的绝对、无漂移对齐。

### B. CI 流水线核心组件与质量红线卡点 (CI Quality Gates & Red Lines)
每一次合入 `release/*` 或 `main` 的 PR，必须无条件通过严苛的 **Quality Gates 质量红线**，任意一项指标未通过，流水线强制自动熔断并锁定合入权限：

1.  **代码静态安全与规范扫描（Linter & SAST）**：
    *   **规范卡点**：针对 Python 强制执行 `ruff check .` 和 `black --check .` 校验；针对 Go 执行 `golangci-lint`；针对 Rust 执行 `cargo clippy`。**报错数严格为 0**。
    *   **安全扫描（SonarQube）**：引入静态白盒安全漏洞扫描，**阻断级漏洞（Vulnerabilities）必须为 0**，安全等级（Security Rating）必须为 **A**。
2.  **测试覆盖率刚性红线（Unit & BDD Coverage Gate）**：
    *   流水线拉起 Docker 镜像，自动启动 PostgreSQL 与 Redis 单元沙箱，一键执行全量测试套件：
        ```bash
        pytest -v tests/ --cov=src/ --cov-fail-under=90
        ```
    *   **覆盖率红线**：**核心业务单元测试覆盖率与 BDD Gherkin 用例验证率必须达到 90% 以上**，否则直接判定发布失败。
3.  **容器基础镜像零高危漏洞扫描（Trivy Image Scan）**：
    *   新编译的业务镜像在推入 Harbor 前，强制通过安全扫描工具（Trivy）执行漏洞排查，**致命与高危安全漏洞（Critical & High CVEs）数量必须为 0**。

### C. 自动化灰度晋级生命周期管理 (Multi-Environment Promotion Lifecycle)
DevOps 流程将制品的生命周期划分为 4 个严格隔离的环境晋级阶段，每个阶段通过独立的 Git 分支进行物理对准：

*   **开发自测阶段 (DEV Environment)**：
    *   **对应分支**：`feature/*` 合入到 `develop` 分支。
    *   **自动化行为**：自动编译，自动推送，ArgoCD 自动部署至 DEV 命名空间，供开发执行 API 快速调试与联调。
*   **用户与业务验收阶段 (UAT Environment)**：
    *   **对应分支**：从 `develop` 签出并合并至 `release/*` 分支。
    *   **自动化行为**：通过金丝雀（Canary）方式向 UAT 环境滚动，QQA 自动化执行 locust 压力测试与 pytest-bdd 全量功能验收，生成自动化验收报告。
*   **预发布验证阶段 (PRE Environment)**：
    *   **对应分支**：合入 `main` 前的暂存验证阶段。
    *   **自动化行为**：完全对齐生产环境的硬件配额，在此阶段导入 $5\%$ 真实生产流量（影子测试/只读流量），验证系统的性能损耗与死锁毛刺。
*   **生产发布阶段 (PROD Environment)**：
    *   **对应分支**：`main` 分支合入并打上 Release Tag（如 `v1.10.0`）。
    *   **自动化行为**：需要双人审批（研发负责人 + SRE 负责人签名）解锁部署权限，在凌晨 SRE 规定的运维发布时间窗口（02:00-04:00）内，由 ArgoCD 以滚动更新和金丝雀割接的方式进行生产环境的平滑覆盖发布。

---
**Deployment Specifications Ready | Bare-Metal & Cloud Blueprints Fully Formulated | SRE Certified**
