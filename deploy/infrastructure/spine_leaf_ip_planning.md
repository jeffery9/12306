# 🌌 12306 本地数据中心 3阶段 Clos Spine-Leaf 物理拓扑 IP 规划与 BGP 自治域（ASN）设计白皮书

本规范承载着 12306 本地绿色数据中心内，万级物理裸金属服务器与 K8s HPA/KEDA 弹性容器集群之间的超大规模、无阻塞、低时延网络底座（Fabric）。

网络拓扑采用经典的 **3-Stage Clos（Spine-Leaf）** 非阻塞架构，Underlay 控制平面采用 **eBGP 动态选路协议**，Overlay 数据平面采用 **EVPN-VXLAN**。为了在不同部署决策下获得最高性价比与硬件效能，本白皮书提供 **“双轨弹性物理寻址规格”**：

1.  **🚀 生产环境高性能标准（8-Leaf / 2-Spine）**：强制适配 **Go 原生引擎 + PostgreSQL HA** 极限精简选型。服务器数量收缩 75%，网络端口数骤减 80%，节省 100+ 万美元 CapEx 开销，并伴随每年 18 万美元电费能耗削减。
2.  **📋 备用大规模扩展规格（48-Leaf / 4-Spine）**：用于承载传统的 Java JVM 堆内存高开销微服务等历史保留扩展，供超大规模云物理节点并发拓展参考。

---

## 1. 物理 Clos 拓扑与弹性收缩 ASCII 架构图

```text
========================================================================================================
                          CLOS FABRIC ELASTIC CONSOLIDATION DUAL-PROFILE MAP
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
         ┌───────────────┐         ┌───────────────┐ [Spine 3 & 4 - Conserved]
         │    Spine 1    │         │    Spine 2    │ (Only deployed in 
         │  (AS 65000)   │         │  (AS 65000)   │  48-Leaf scaling profile)
         └───────┬───────┘         └───────┬───────┘
                 │ (100G Full-Mesh Clos Fabric)    
                 ├─────────────────────────┼────────────────┐
                 ▼                         ▼                ▼
         ┌───────────────┐         ┌───────────────┐┌───────────────┐
         │  Leaf 1-2     │         │  Leaf 3-4     ││  Leaf 5-8     │ ... [Leaf 9-48 - Conserved]
         │ (AS 65001-02) │         │ (AS 65003-04) ││ (AS 65007-08) │ (Only deployed in
         └───────┬───────┘         └───────┬───────┘└───────┬───────┘  48-Leaf scaling profile)
                 │                         │                │
         ┌───────┴───────┐         ┌───────┴───────┐        └───────────────┐
         ▼ (Bare-metal)  ▼ (K8s)   ▼ (Database)    ▼ (Redis)                ▼ (Storage)
       [Go App]      [Pod Node]  [Postgres HA]   [Cache cluster]          [Backup Store]
========================================================================================================
```

---

## 2. IPAM 核心网段池弹性分配 CIDR 表

为主网络 `10.0.0.0/16` 分配如下子网段，以确保物理链路、Loopback、管理网与租户网络的完美物理隔离：

| 专有网络网段池 (Subnet Pools) | CIDR 划分范围 (Pools Allocation) | 物理用途说明 (Rationale) |
| :--- | :--- | :--- |
| **P2P Link Pool** | `10.0.0.0/19` | 互联链路网段。/31 互联子网。8-Leaf 占用前 16 个网段；48-Leaf 完全占满。 |
| **Loopback 0 Pool** | `10.255.255.0/24` | 交换机 Router-ID 地址。每台分配 `/32` 主机路由。 |
| **Loopback 1 Pool** | `10.250.255.0/24` | EVPN VXLAN VTEP 虚端点隧道源 IP 地址池。每台 Leaf 分配 `/32`。 |
| **Overlay VNI Pool** | `10.100.0.0/16` | 覆盖网络租户（数据库、应用、缓存等）的网关与物理 IP 分布区。 |
| **OOB Management Pool**| `192.168.100.0/24`| 带外管理网（Out-of-Band），所有设备的 Console/Management 口在此网中独立寻址。 |

---

## 3. BGP ASN 自治域号配置规约 (BGP ASN)

Clos 拓扑的 Underlay 选路强制选用 **eBGP 协议**，以便天然隔离环路并启用多路径负载均衡（ECMP）：

*   **Spine Layer (AS 65000)**：所有的 Spine 交换机划入同一个自治域 **`AS 65000`**。
*   **Leaf Layer (AS 65001 ~ AS 65048)**：采用 **“One Leaf per AS”** 律令，即每对 Leaf 分配独立的 Private ASN：
    *   **🚀 生产环境（8-Leaf）**：仅需宣告并占用 `AS 65001` 到 `AS 65008`。
    *   **📋 扩展环境（48-Leaf）**：延伸占用 `AS 65001` 到 `AS 65048`。
*   **Border Leaf Layer (AS 65100)**：出口边界 Leaf 自治域划定为 **`AS 65100`**。

---

## 4. Loopback 虚接口像素级 IP 规约 (Loopback 0 & VTEP Loopback 1)

### A. Spine 核心节点 Loopback 0 规划
| 设备名称 (Hostname) | 物理角色 (Role) | BGP ASN | Loopback 0 (Underlay ID) | 生产激活状态 (Active Profile) |
| :--- | :--- | :--- | :--- | :--- |
| **Spine-1** | Underlay Core | AS 65000 | `10.255.255.251/32` | **🚀 生产环境默认启用** |
| **Spine-2** | Underlay Core | AS 65000 | `10.255.255.252/32` | **🚀 生产环境默认启用** |
| **Spine-3** | Underlay Core | AS 65000 | `10.255.255.253/32` | 📋 48-Leaf 模式下追加启用 |
| **Spine-4** | Underlay Core | AS 65000 | `10.255.255.254/32` | 📋 48-Leaf 模式下追加启用 |

### B. Leaf 接入节点 Loopback 规划
| 节点代号 (Leaf ID) | Loopback 0 (Router-ID) | Loopback 1 (VTEP IP) | BGP ASN | 生产激活状态 (Active Profile) |
| :--- | :--- | :--- | :--- | :--- |
| **Leaf-1** | `10.255.255.1/32` | `10.250.255.1/32` | AS 65001 | **🚀 生产环境默认启用** |
| **Leaf-2** | `10.255.255.2/32` | `10.250.255.2/32` | AS 65002 | **🚀 生产环境默认启用** |
| **Leaf-3** | `10.255.255.3/32` | `10.250.255.3/32` | AS 65003 | **🚀 生产环境默认启用** |
| **...** | ... | ... | ... | ... |
| **Leaf-8** | `10.255.255.8/32` | `10.250.255.8/32` | AS 65008 | **🚀 生产环境默认启用** |
| **Leaf-9** | `10.255.255.9/32` | `10.250.255.9/32` | AS 65009 | 📋 48-Leaf 模式下追加启用 |
| **...** | ... | ... | ... | ... |
| **Leaf-48**| `10.255.255.48/32` | `10.250.255.48/32` | AS 65048 | 📋 48-Leaf 模式下追加启用 |

---

## 5. 物理链路 P2P /31 互联地址矩阵与算术等差推导 (P2P Map)

### 🚀 A. 生产高性能标准（8-Leaf / 2-Spine）P2P 互联表
每台 Leaf 物理全连接至 2 台 Spine。使用 `/31` 掩码：
*   **Leaf-1 物理互联链路**：
    *   `Leaf-1 [Eth1/1]` (10.0.0.1/31) ◄──► `Spine-1 [Eth1/1]` (10.0.0.0/31)
    *   `Leaf-1 [Eth1/2]` (10.0.0.3/31) ◄──► `Spine-2 [Eth1/1]` (10.0.0.2/31)
*   **Leaf-2 物理互联链路**：
    *   `Leaf-2 [Eth1/1]` (10.0.0.5/31) ◄──► `Spine-1 [Eth1/2]` (10.0.0.4/31)
    *   `Leaf-2 [Eth1/2]` (10.0.0.7/31) ◄──► `Spine-2 [Eth1/2]` (10.0.0.6/31)
*   **Leaf-8 物理互联链路**：
    *   `Leaf-8 [Eth1/1]` (10.0.0.29/31) ◄──► `Spine-1 [Eth1/8]` (10.0.0.28/31)
    *   `Leaf-8 [Eth1/2]` (10.0.0.31/31) ◄──► `Spine-2 [Eth1/8]` (10.0.0.30/31)

*(精简拓扑下，Leaf N 连接 Spine M 的 P2P 互联地址网关计算公式为：`10.0.0.((N-1)*4 + (M-1)*2)/31`。全网只需要 16 个 /31 极小网段)*。

### 📋 B. 扩展环境标准（48-Leaf / 4-Spine）P2P 互联表
每台 Leaf 全连接至 4 台 Spine：
*   **Leaf-1 物理链路**：
    *   `Leaf-1 [Eth1/1]` (10.0.0.1/31) ◄──► `Spine-1 [Eth1/1]` (10.0.0.0/31)
    *   `Leaf-1 [Eth1/2]` (10.0.0.3/31) ◄──► `Spine-2 [Eth1/1]` (10.0.0.2/31)
    *   `Leaf-1 [Eth1/3]` (10.0.0.5/31) ◄──► `Spine-3 [Eth1/1]` (10.0.0.4/31)
    *   `Leaf-1 [Eth1/4]` (10.0.0.7/31) ◄──► `Spine-4 [Eth1/1]` (10.0.0.6/31)
*   **Leaf-48 物理链路**：
    *   `Leaf-48 [Eth1/1]` (10.0.1.121/31) ◄──► `Spine-1 [Eth1/48]` (10.0.1.120/31)
    *   `Leaf-48 [Eth1/2]` (10.0.1.123/31) ◄──► `Spine-2 [Eth1/48]` (10.0.1.122/31)
    *   `Leaf-48 [Eth1/3]` (10.0.1.125/31) ◄──► `Spine-3 [Eth1/48]` (10.0.1.124/31)
    *   `Leaf-48 [Eth1/4]` (10.0.1.127/31) ◄──► `Spine-4 [Eth1/48]` (10.0.1.126/31)

*(扩展拓扑下，Leaf N 连接 Spine M 的 P2P 互联网关计算公式为：`10.0.((N-1)*8 + (M-1)*2)/31`)*。

---

## 6. EVPN VXLAN Overlay 租户子网与 VLAN 映射规约

不同的微服务业务集群划归不同的 VRF，以保持云原生网络的多租户物理硬隔离：

| 业务租户名称 (VRF Tenant) | L3 VNI | 分配的 VLAN ID | 网关段 (Overlay Gateway Subnet) |
| :--- | :--- | :--- | :--- |
| **`VRF-CORE-APP`** | `50001` | VLAN `10` | `10.100.10.0/24` (Anycast Gateway: `10.100.10.254`) |
| **`VRF-DATABASE`** | `50002` | VLAN `20` | `10.100.20.0/24` (Anycast Gateway: `10.100.20.254`) |
| **`VRF-REDIS-CACHE`**| `50003` | VLAN `30` | `10.100.30.0/24` (Anycast Gateway: `10.100.30.254`) |
| **`VRF-K8S-PODS`** | `50004` | VLAN `40` | `10.100.40.0/22` (K8s BGP Peering Pool) |

*   **Anycast Gateway**：全网 Leaf 对应的 VLAN 虚拟关 IP 保持绝对相同。

---

## 7. 部署与自动化验证操作 snippet (Arista EOS OS 生产示例)

以下是 SRE 在物理 Leaf 交换机（以 Leaf-1 为例）上配置 eBGP Underlay 和 Anycast 网关的生产配置模板：

```text
! 🌌 Leaf-1 Underlay & IPAM LLD Config (Consolidated Profile)
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
! ─────────── eBGP Routing ───────────
router bgp 65001
   router-id 10.255.255.1
   maximum-paths 2               ! 精简环境下只需双 ECMP 路径
   neighbor SPINE-PEERS peer-group
   neighbor SPINE-PEERS remote-as 65000
   neighbor SPINE-PEERS fall-over bfd     ! BFD 毫秒级链路快速闪断检测
   neighbor 10.0.0.0 peer-group SPINE-PEERS
   neighbor 10.0.0.2 peer-group SPINE-PEERS
   !
   redistribute connected route-map RM-CONN-TO-BGP
!
route-map RM-CONN-TO-BGP permit 10
   match interface Loopback0 Loopback1
```

---

## 总结 (Summary)

本套 **IPAM 与 BGP ASN 像素级寻址双轨制规划**，兼顾了**极限生产高性能（Go 引擎，8-Leaf）** 与 **超大规模扩展性（Java JVM 备用扩展，48-Leaf）**。双轨方案将地址推导压缩为数学等差公式。这为 12306 系统在经历春运流量考验时提供了网络底座极速、高弹性、非阻塞和亚秒级收敛自愈（BFD）的安全网络连通性能！
