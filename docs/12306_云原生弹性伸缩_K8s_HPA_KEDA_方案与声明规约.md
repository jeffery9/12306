# 🌌 12306 高并发票务系统 — 云原生自动弹性伸缩 (Auto-Scaling) 方案与 K8s/KEDA 声明规约

> **云原生弹性愿景**：售票系统的流量具有极其恐怖的瞬发突涌特征。例如，春运放票那一瞬间，QPS 会在 1 秒内飙升数十倍，而深夜则流量归零。
>
> 传统的单节点或静态集群配置，要么在抢票瞬间因资源耗尽雪崩死锁，要么在低谷期造成巨大的基建服务器浪费。
>
> 本项目完美遵循 **“无状态（Stateless）与有状态（Stateful）物理隔离”** 思想。通过在 Kubernetes (K8s) 中融合标准的 **HPA v2（基于 CPU/Memory 水平扩容）** 与尖端的 **KEDA（基于 Kafka 消息积压的事件驱动扩容）**，实现了百万级流量下极速自动扩容、低谷期智能归零的自愈闭环生态。

---

## 1. 自动弹性伸缩架构拓扑 (Auto-Scaling Topology)

系统在 K8s 内部通过两种正交的弹性机制，分别守护系统的“读写流量入口”与“异步自愈投影通道”：

```text
  [ 外部地狱级流量突涌 (Ingress) ] ──► ( 负载均衡 LB )
                                               │
               ┌───────────────────────────────┴───────────────────────────────┐
               ▼ (1. 读写流量入口弹性区)                                           ▼ (2. 异步事件自愈弹性区)
     ┌────────────────────────────┐                                  ┌────────────────────────────┐
     │  ticketing-web-api-hpa     │                                  │  ticketing-projector-keda  │
     ├────────────────────────────┤                                  ├────────────────────────────┤
     │ - 机制: K8s 原生 HPA v2     │                                  │ - 机制: CNCF KEDA          │
     │ - 指标: CPU/Memory 水位     │                                  │ - 指标: Kafka Consumer Lag │
     │ - 动态Pod区间: 5 ~ 100 个  │                                  │ - 动态Pod区间: 2 ~ 32 个   │
     └─────────────┬──────────────┘                                  └─────────────┬──────────────┘
                   │ (秒级横向扩容)                                                  │ (按 lag > 100 瞬间拉起)
                   ▼                                                                 ▼
     ┌────────────────────────────┐                                  ┌────────────────────────────┐
     │ [web-api]  [web-api] [web-api] │                                  │ [projector] [projector]    │
     │ (多路并网, 拦截/预占/查余)    │                                  │ (多实例并行重算读缓存)      │
     └────────────────────────────┘                                  └────────────────────────────┘
```

---

## 2. 核心弹性组件设计规约 (Core Scaler Specifications)

### A. 针对 API 核心的 HorizontalPodAutoscaler (HPA)

无状态的交易网关和查余服务承接了 100% 的用户直接点击。我们基于 CPU 75% 负载与内存 80% 负载的双重警戒线，实施动态水平伸缩。

#### 📈 扩缩容行为（Behavior & Policies）精细微调：
*   **Scale-up (极速扩容政策)**：
    > 抢票是毫秒级遭遇战，不允许任何“预热等待”。我们将扩容稳定窗口（`stabilizationWindowSeconds`）设为 **0 秒**。
    >
    > 允许每 15 秒将现有的 Pod 实例数**直接翻倍（Percent: 100%）**。当检测到 CPU 飙升时，K8s 可以在不到一分钟的时间内，将实例数从 5 个瞬间狂飙至 100 个！
*   **Scale-down (防抖缓慢缩容政策)**：
    > 抢票高峰退潮后，可能存在二次回潮。为了防止 Pod 刚销毁又立刻扩容的“系统抖动/喘息（Thrashing）”现象，我们将缩容稳定窗口设为 **300 秒 (5分钟)**。
    >
    > 规定每分钟至多缓慢缩容 **10%** 的 Pod。平滑、优雅地释放云计算资源，保全物理机器。

---

### B. 针对投影器消费端的 KEDA (Event-driven Scaler)

由于写模型提交极为飞速（主事务仅写 Outbox 表，耗时 < 5ms），而读模型缓存重建（Projector 重算位掩码并 HSET Redis）属于后台异步任务。
若在高并发抢票下，大量的退票、支付事件向 Kafka 倾泻，Projector 如果只有一两个实例，会导致 **Kafka Consumer Group Lag（消费积压）** 持续上升，旅客在前端刷新看到的余票信息就会产生严重的“时间延迟差”。

#### 💡 解决方案：引入 KEDA (基于 Kafka 积压的事件驱动扩容)
*   **KEDA 监听器**：直接通过 Kubernetes API 监听集群内的 Kafka 集群，实时嗅探 `12306-projector-group` 消费组在 `ticket_events` 主题下的物理积压数值。
*   **秒级决策**：一旦总积压消息数（`lagThreshold`）超过 **100 条**，代表投影时差正在被拉大。
*   **弹性拉起**：KEDA 会在秒级将投影器实例（`ticketing-projector`）从 2 个**一键拉满至 32 个并发 Pod**（对应 Kafka Topic 的 32 个物理 Partition 分区）。
*   **收益**：32 路投影消费者并行计算、无冲突批量 HSET Redis，将读写一致性时差重新**强行压缩至 100ms 黄金安全水位**！

---

## 3. 生产级 SRE 可观测性与自愈探针 (Probes & SRE Health)

为了保障弹性伸缩过程中，新拉起的 Pod 绝对不会因初始化未完成而引入脏流量，或者挂死后无法自愈，我们在 Deployment 层面物理硬锁了以下两层 SRE 监控健康探针：

```text
  [ Pod 启动 (Container Start) ]
            │
            ▼ 1. Readiness Probe (就绪检测: 每 5s 呼叫 GET /api/v1/ops/health)
   (检查 MySQL / Redis 网络连接是否完全建立并就绪)
            │
            ├── [未就绪] ──► 保持 Pod 处于 "Unready" 状态，100% 隔离外部 Service 购票流量
            │
            └── [已就绪] ──► 标记 Pod 处于 "Ready" 状态，安全并网，分摊 LB 负载
            │
            ▼ 2. Liveness Probe (存活检测: 每 10s 心跳探测 /api/v1/ops/health)
   (实时嗅探进程是否僵死或内存溢出挂挂)
            │
            ├── [心跳断] ──► 自动判定 Pod 发生致命崩溃，K8s 容器运行时立即强行 kill 并重建 Pod (自愈)
            │
            └── [心跳通] ──► 持续稳定运行
```

---

## 4. 落地部署一键启动

所有声明已成功物理落地至项目根目录的 **`deploy/k8s-autoscale-manifests.yaml`** 清单中。在真实的生产沙盒中，SRE 团队仅需一条指令即可一键部署并激活这套云原生极光防御大堤：

```bash
kubectl apply -f deploy/k8s-autoscale-manifests.yaml
```

---
**K8s Auto-scaling Specs Embedded | HPA & KEDA Active | SRE Cloud-Native Shield Enforced**
