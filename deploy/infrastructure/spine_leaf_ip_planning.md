# 🌌 12306 本地数据中心 3阶段 Clos Spine-Leaf 物理拓扑 IP 规划与 BGP 自治域（ASN）设计白皮书

本规范承载着 12306 本地绿色数据中心内，万级物理裸金属服务器与 K8s HPA/KEDA 弹性容器集群之间的超大规模、无阻塞、低时延网络底座（Fabric）。

网络拓扑采用经典的 **3-Stage Clos（Spine-Leaf）** 非阻塞架构，Underlay 控制平面采用 **eBGP 动态选路协议**，Overlay 数据平面采用 **EVPN-VXLAN**。本规范针对本网 **4 台核心 Spine 交换机 与 48 台 Leaf 交换机** 的超大规模架构，制定物理链路 P2P、Loopback 虚接口、VTEP 隧道及 BGP ASN 自治域的 LLD 像素级地址规划。

---

## 1. 物理 Clos 拓扑与数据流 ASCII 架构大图

```text
========================================================================================================
                                 12306 DC CLOS FABRIC PHYSICAL TOPOLOGY
========================================================================================================

                         [ Core Router / WAN Ingress ]
                                      │
                 ┌────────────────────┴────────────────────┐ (North-South Traffic)
                 ▼                                         ▼
         ┌───────────────┐                         ┌───────────────┐
         │  Border Leaf1 │                         │  Border Leaf2 │ (AS 65100)
         └───────┬───────┘                         └───────┬───────┘
                 │ (100G Uplinks)                          │
                 ├─────────────────────────┐               │
                 │                         │               │
                 ▼                         ▼               ▼
         ┌───────────────┐         ┌───────────────┐┌───────────────┐
         │    Spine 1    │         │    Spine 2    ││    Spine 3    │ ... [Spine 4]
         │  (AS 65000)   │         │  (AS 65000)   ││  (AS 65000)   │
         └───────┬───────┘         └───────┬───────┘└───────┬───────┘
                 │ (100G)                  │                │
                 ├─────────────────────────┼────────────────┘
                 │ (Full-Mesh Clos)        │
                 ▼                         ▼
         ┌───────────────┐         ┌───────────────┐
         │    Leaf 1     │         │    Leaf 2     │ ... [Leaf 48]
         │  (AS 65001)   │         │  (AS 65002)   │     (AS 65048)
         └───────┬───────┘         └───────┬───────┘
                 │                         │
         ┌───────┴───────┐         ┌───────┴───────┐ (10G/25G LACP)
         ▼ (Bare-metal)  ▼ (K8s)   ▼ (Database)    ▼ (Redis)
     [App Node]     [Pod Node]   [Postgres HA]   [Cache cluster]
========================================================================================================
```

---

## 2. IPAM 核心网段池切片规约 (IPAM Pools)

为主网络 `10.0.0.0/16` 分配如下子网段，以确保物理链路、Loopback、管理网与租户网络的完美物理硬隔离：

| 专有网络网段池 (Subnet Pools) | CIDR 划分范围 (Pools Allocation) | 物理用途说明 (Rationale) |
| :--- | :--- | :--- |
| **P2P Link Pool** | `10.0.0.0/19` | 48 Leaf x 4 Spines = 192 条物理点对点互联链路。分配 `/31` 互联子网，极大压平路由表。 |
| **Loopback 0 Pool** | `10.255.255.0/24` | 宿主设备 ID Pool。每台交换机分配 `/32` 主机路由，用于 Underlay 路由协议。 |
| **Loopback 1 Pool** | `10.250.255.0/24` | EVPN VXLAN VTEP 虚端点隧道源 IP 地址池。每台 Leaf 分配 `/32`。 |
| **Overlay VNI Pool** | `10.100.0.0/16` | 覆盖网络租户（数据库、应用、缓存等）的网关与物理 IP 分布区。 |
| **OOB Management Pool**| `192.168.100.0/24`| 带外管理网（Out-of-Band），所有设备的 Console/Management 口在此网中独立寻址。 |

---

## 3. BGP ASN 自治域系统号规划 (BGP Autonomous System Numbers)

Clos 拓扑的 Underlay 选路强制选用 **eBGP 协议**，以便天然隔离环路并启用多路径负载均衡（ECMP）：

*   **Spine Layer (AS 65000)**：
    所有的 Spine 交换机划入同一个自治域 **`AS 65000`**。
*   **Leaf Layer (AS 65001 ~ 65048)**：
    为了使 Leaf 节点之间无需维持 IBGP 全连接，采用 **“One Leaf per AS”** 律令，即每对 Leaf 分配独立的 Private ASN（`65001` 到 `65048`）。
*   **Border Leaf Layer (AS 65100)**：
    出口边界 Leaf 自治域划定为 **`AS 65100`**。

---

## 4. Loopback 虚接口像素级 IP 规约 (Loopback 0 & VTEP Loopback 1)

### A. Spine 核心节点 Loopback 0 规划
| 设备名称 (Hostname) | 物理角色 (Role) | BGP ASN | Loopback 0 (Underlay ID) |
| :--- | :--- | :--- | :--- |
| **Spine-1** | Underlay Transit Core | AS 65000 | `10.255.255.251/32` |
| **Spine-2** | Underlay Transit Core | AS 65000 | `10.255.255.252/32` |
| **Spine-3** | Underlay Transit Core | AS 65000 | `10.255.255.253/32` |
| **Spine-4** | Underlay Transit Core | AS 65000 | `10.255.255.254/32` |

### B. Leaf 接入节点 Loopback 规划
| 节点代号 (Leaf ID) | Loopback 0 (Router-ID) | Loopback 1 (VTEP IP) | BGP ASN (eBGP Private) |
| :--- | :--- | :--- | :--- |
| **Leaf-1** | `10.255.255.1/32` | `10.250.255.1/32` | AS 65001 |
| **Leaf-2** | `10.255.255.2/32` | `10.250.255.2/32` | AS 65002 |
| **Leaf-3** | `10.255.255.3/32` | `10.250.255.3/32` | AS 65003 |
| **Leaf-4** | `10.255.255.4/32` | `10.250.255.4/32` | AS 65004 |
| **...** | ... | ... | ... |
| **Leaf-47** | `10.255.255.47/32` | `10.250.255.47/32` | AS 65047 |
| **Leaf-48** | `10.255.255.48/32` | `10.250.255.48/32` | AS 65048 |

---

## 5. 物理链路 P2P /31 互联地址矩阵 (P2P Interface Interconnect)

每台 Leaf 必须满速率全连接（Full-mesh）至 4 台 Spine。使用 `/31` 掩码以节省 50% 地址空间：

### Leaf-1 ~ Leaf-4 物理链路矩阵示例：
*   **Leaf-1 物理互联链路**：
    *   `Leaf-1 [Eth1/1]` (10.0.0.1/31) ◄──► `Spine-1 [Eth1/1]` (10.0.0.0/31)
    *   `Leaf-1 [Eth1/2]` (10.0.0.3/31) ◄──► `Spine-2 [Eth1/1]` (10.0.0.2/31)
    *   `Leaf-1 [Eth1/3]` (10.0.0.5/31) ◄──► `Spine-3 [Eth1/1]` (10.0.0.4/31)
    *   `Leaf-1 [Eth1/4]` (10.0.0.7/31) ◄──► `Spine-4 [Eth1/1]` (10.0.0.6/31)
*   **Leaf-2 物理互联链路**：
    *   `Leaf-2 [Eth1/1]` (10.0.0.9/31) ◄──► `Spine-1 [Eth1/2]` (10.0.0.8/31)
    *   `Leaf-2 [Eth1/2]` (10.0.0.11/31) ◄──► `Spine-2 [Eth1/2]` (10.0.0.10/31)
    *   `Leaf-2 [Eth1/3]` (10.0.0.13/31) ◄──► `Spine-3 [Eth1/2]` (10.0.0.12/31)
    *   `Leaf-2 [Eth1/4]` (10.0.0.15/31) ◄──► `Spine-4 [Eth1/2]` (10.0.0.14/31)
*   **Leaf-3 物理互联链路**：
    *   `Leaf-3 [Eth1/1]` (10.0.0.17/31) ◄──► `Spine-1 [Eth1/3]` (10.0.0.16/31)
    *   `Leaf-3 [Eth1/2]` (10.0.0.19/31) ◄──► `Spine-2 [Eth1/3]` (10.0.0.18/31)
    *   `Leaf-3 [Eth1/3]` (10.0.0.21/31) ◄──► `Spine-3 [Eth1/3]` (10.0.0.20/31)
    *   `Leaf-3 [Eth1/4]` (10.0.0.23/31) ◄──► `Spine-4 [Eth1/3]` (10.0.0.22/31)
*   **Leaf-4 物理互联链路**：
    *   `Leaf-4 [Eth1/1]` (10.0.0.25/31) ◄──► `Spine-1 [Eth1/4]` (10.0.0.24/31)
    *   `Leaf-4 [Eth1/2]` (10.0.0.27/31) ◄──► `Spine-2 [Eth1/4]` (10.0.0.26/31)
    *   `Leaf-4 [Eth1/3]` (10.0.0.29/31) ◄──► `Spine-3 [Eth1/4]` (10.0.0.28/31)
    *   `Leaf-4 [Eth1/4]` (10.0.0.31/31) ◄──► `Spine-4 [Eth1/4]` (10.0.0.30/31)

*(说明：此后 Leaf-5 到 Leaf-48 的 P2P 互联 IP 呈线性等差推导。Leaf N 连接 Spine M 的 P2P 链路网关计算公式为：`10.0.((N-1)*8 + (M-1)*2)/31`。例如 Leaf-48 连接 Spine-4 对应地址块：`((48-1)*8 + (4-1)*2) = 376 + 6 = 382`，对应网段即 `10.0.1.126/31`，计算精确无误)*。

---

## 6. EVPN VXLAN Overlay 租户子网与 VLAN 映射规约

数据中心内，不同的微服务业务集群划归不同的 VRF，以保持云原生网络的多租户隔离：

| 业务租户名称 (VRF Tenant) | L3 VNI | 分配的 VLAN ID | 网关段 (Overlay Gateway Subnet) |
| :--- | :--- | :--- | :--- |
| **`VRF-CORE-APP`** | `50001` | VLAN `10` | `10.100.10.0/24` (Anycast Gateway: `10.100.10.254`) |
| **`VRF-DATABASE`** | `50002` | VLAN `20` | `10.100.20.0/24` (Anycast Gateway: `10.100.20.254`) |
| **`VRF-REDIS-CACHE`**| `50003` | VLAN `30` | `10.100.30.0/24` (Anycast Gateway: `10.100.30.254`) |
| **`VRF-K8S-PODS`** | `50004` | VLAN `40` | `10.100.40.0/22` (K8s BGP Peering Pool) |

*   **Anycast Gateway 律令**：为了支持物理机和 VM 在不同机架（Leaf）之间进行毫秒级无缝漫游漂移，所有 Leaf 下连接业务的 VLAN 网关 IP（如 `10.100.10.254`）和 MAC 地址均在全网 48 台 Leaf 交换机上保持**绝对相同（Anycast Active-Active）**，彻底避免跨网关漂移时的发夹弯（Hair-pinning）时延损耗。

---

## 7. 部署与自动化验证操作 snippet (Arista EOS OS 示例)

以下是 SRE 在物理 Leaf 交换机（以 Leaf-1 为例）上拉起 Underlay eBGP 互联和 Anycast 网关的核心配置模板：

```text
! 🌌 Leaf-1 Underlay & IPAM LLD Config Snippet
hostname Leaf-1
!
ip routing
!
interface Loopback0
   description Router-ID for eBGP
   ip address 10.255.255.1/32
!
interface Loopback1
   description VTEP Endpoint
   ip address 10.250.255.1/32
!
interface Ethernet1
   description Link to Spine-1
   no switchport
   ip address 10.0.0.1/31
!
interface Ethernet2
   description Link to Spine-2
   no switchport
   ip address 10.0.0.3/31
!
interface Ethernet3
   description Link to Spine-3
   no switchport
   ip address 10.0.0.5/31
!
interface Ethernet4
   description Link to Spine-4
   no switchport
   ip address 10.0.0.7/31
!
! ─────────── eBGP Routing ───────────
router bgp 65001
   router-id 10.255.255.1
   maximum-paths 4
   neighbor SPINE-PEERS peer-group
   neighbor SPINE-PEERS remote-as 65000
   neighbor SPINE-PEERS fall-over bfd     ! BFD 毫秒级链路快速闪断检测
   neighbor 10.0.0.0 peer-group SPINE-PEERS
   neighbor 10.0.0.2 peer-group SPINE-PEERS
   neighbor 10.0.0.4 peer-group SPINE-PEERS
   neighbor 10.0.0.6 peer-group SPINE-PEERS
   !
   redistribute connected route-map RM-CONN-TO-BGP
!
route-map RM-CONN-TO-BGP permit 10
   match interface Loopback0 Loopback1
```

---

## 总结 (Summary)

通过推行本套 **IPAM 像素级等差矩阵划分方案**，我们将本地物理数据中心内数万条 P2P 互联物理接口的 IP 维护成本直接压缩到了“单一算术等差公式”中，消除了人工逐一写配置引发的录入错误；同时，高能 eBGP 自治域、Anycast 统一虚 MAC 漂移网关以及 L3 VNI 隔离网段的设计，最大化保障了 12306 系统在经历双十一/春运流量爆发时，网络底座具备 **ECMP 4等分流控吞吐** 及 **亚秒级物理收敛自愈（BFD）** 的极速、无单点（Anti-SPOF）网络连通性！
