# 🌌 12306 本地数据中心带外管理（OOB）安全控制、BMC 硬件接口与 SRE 应急备降救援规范白皮书

本规范承载着 12306 核心物理集群的“终极自愈通道” ── **带外管理网络（Out-of-Band Management Network, OOB）**。

当生产环境（带内网络 In-band Network）发生特大广播风暴、核心 eBGP 路由崩溃，或物理服务器的操作系统（OS）发生致命死锁挂死时，生产网络（50G LACP）将完全不可达。带外管理网络通过**完全独立的物理链路、专有 1G 交换机、以及主板底层的基板管理控制器（BMC）**，向 SRE 提供不受业务网络和系统状态干扰的、100% 连通的物理级控制能力（含虚拟 KVM、冷重置电源、串行控制台等）。

---

## 1. 物理带外（OOB）管理网拓扑与网关安全隔离 ASCII 视图

带外网络在物理空间、交换芯片及接入电缆上与带内生产网 **100% 绝对物理隔离**：

```text
========================================================================================================
                               OOB SECURE SEGREGATION & GATEWAY TOPOLOGY
========================================================================================================

                 [ SRE On-Call jumpbox / Secure VPN Terminal ]
                                      │
                                      ▼ (MFA + TLS 1.3 encrypted Tunnel)
                         ┌──────────────────────────┐
                         │   Secure OOB Gateway     │ (Firewall / Bastion Host)
                         └────────────┬─────────────┘
                                      │
                                      ├─[RADIUS / AAA Server] (Centeral Authentication)
                                      │ (Private Isolated 1G Mgmt Network)
                                      ▼
                         ┌──────────────────────────┐
                         │   OOB Core Switch        │ (Arista 7010T - Rack Top Port)
                         └──────┬────────────┬──────┘
                                │            │
            ┌───────────────────┘            └───────────────────┐
            ▼ (1G Dedicated RJ45 Port)                           ▼ (1G Dedicated RJ45 Port)
     ┌───────────────┐                                    ┌───────────────┐
     │  Leaf 1 Mgmt  │ (Console Switch)                   │  Spine 1 Mgmt │ (Console Switch)
     └───────────────┘                                    └───────────────┘
            │
            ▼ (IPMI / iLO / iDRAC BMC Dedicated Interface)
     ┌────────────────────────────────────────────────────────────────────┐
     │  App/Database Bare-Metal Host Servers                              │
     │  [BMC / Mainboard Chip] (Power Supply, Thermal, Serial Consol, KVM)│
     └────────────────────────────────────────────────────────────────────┘
========================================================================================================
```

---

## 2. 带外管理网段池（IPAM）像素级 IP 规约 (192.168.100.0/24)

带外管理网使用私有地址段 **`192.168.100.0/24`** 进行高内聚划分，保障每台交换机的 Mgmt 口与每台服务器主板 BMC 的有序绑定：

| 物理对象分类 (Target Classes) | 预留 IP 范围 (IP Allocations) | 网卡介质与配置接口 (Interface & Media) |
| :--- | :--- | :--- |
| **OOB Gateway** | `192.168.100.254/24` | 边界带外防火墙/堡垒机 LAN 口网关。 |
| **AAA / RADIUS Server** | `192.168.100.250/24` | 集中式身份鉴权服务器，控制带外接入控制台审计。 |
| **Spine Mgmt Ports** | `192.168.100.1` - `192.168.100.4` | Spine 1-4 交换机背面专有 10/100/1000M 物理 Mgmt 口。 |
| **Leaf Mgmt Ports** | `192.168.100.11` - `192.168.100.58` | Leaf 1-48 交换机背面专有 10/100/1000M 物理 Mgmt 口。 |
| **LB/Pooler Mgmt Ports** | `192.168.100.61` - `192.168.100.68` | HAProxy 物理负载机与 PgBouncer 物理机 Mgmt 口。 |
| **PostgreSQL HA BMC** | `192.168.100.101` - `192.168.100.104`| 4U 物理数据库服务器主板专有 IPMI/iDRAC 物理网口。 |
| **App Compute BMC** | `192.168.100.111` - `192.168.100.150`| 2U 业务计算物理服务器主板专有 IPMI/iLO 物理网口。 |
| **Storage SAN BMC** | `192.168.100.201` - `192.168.100.204`| 物理 SAN 存储落盘矩阵控制芯片 Mgmt 口。 |

---

## 3. BMC 硬件接口与 OOB 交换机安全加固规约 (Security Hardening)

由于带外管理网络拥有“无视操作系统、直接重置主板供电、重装系统”的至高权限，其安全系数必须对齐最高金融级标准：

### A. 账户与鉴权防线
*   **物理禁用默认凭据**：服务器及交换机开箱上架后，**必须强制禁用**厂商预设的默认带外凭据（如 `admin/admin`、`root/calvin`）。
*   **统一 RADIUS 鉴权**：带外交换机（Consoles）与服务器 BMC 的 Web/SSH 登录，统一接入带外专有 AAA（`192.168.100.250`）。支持 SRE 运维人员多因子密码（MFA）校验，每一次指令下发（如 `power reset`）必须写入中央审计日志。

### B. 协议层安全裁剪
*   **禁用不安全明文协议**：
    *   **100% 禁用**：`Telnet`（23端口）、`HTTP`（80端口）、以及 `SNMP v1/v2c`。
    *   **强制启用**：`SSH v2`（22端口）、`HTTPS`（443端口，且最低要求 TLS 1.3 + 2048位 SHA2 证书）。
*   **BMC IPMI 强化**：
    *   为了防止基于 `IPMI v1.5` 的密码哈希离线破解，**强制关闭 IPMI over LAN 1.5 兼容模式**，仅允许 `IPMI over LAN 2.0 (RMCP+)`，并强制指定 `Cipher Suite 17`（使用 SHA256 完整性校验与 AES 强加密）。

---

## 4. SRE 核心备降与应急自愈操作手册 (Emergency Playbook)

当 12306 写主库 `pg-prod1`（IP: `10.0.10.21`）发生内核挂死、SSH 完全无响应且引发线上锁座瘫痪时，SRE On-Call 工程师通过带外网络进行物理级强行重置的故障处理指令时序如下：

### 步骤 A：SRE 远程登入带外专用通道
首先，SRE 开启双因子（MFA）登入数据中心带外专用 VPN 堡垒机，获得 `192.168.100.0/24` 的安全内网路由：
```bash
# 验证与主库 pg-prod1 的 BMC 带外主控板（IP: 192.168.100.101）的物理连通性
ping -c 3 192.168.100.101
```

### 步骤 B：使用 `ipmitool` 抓取底层硬件状态
利用带外网络（RMCP+ 协议，用户 `sre_admin`），绕过崩溃的 Linux 操作系统，直接调取主板底层的传感器温度、风扇转速、电源功率及告警日志：
```bash
# [SRE CMD] 抓取硬件事件日志 (Event Log)，确认是否发生主板或内存物理报错导致的假死
ipmitool -I lanplus -H 192.168.100.101 -U sre_admin -P "StrongSrePassword123!" sel list

# [SRE CMD] 查看机箱内电源状态
ipmitool -I lanplus -H 192.168.100.101 -U sre_admin -P "StrongSrePassword123!" power status
```

### 步骤 C：执行物理级强行断电与重置 (Hard Reset)
如果系统彻底挂死且在 sub-10s 内阻碍了 Patroni 自动 failover 的 LSN 检测（例如因为网络僵尸卡死），SRE 需强制主库关机，以立刻触发 etcd 的租约失效释放，使 Replica 从库快速夺权升级为主库：
```bash
# [SRE CMD] 强行切断主板供电 (物理拔插头等价命令，防止操作系统死锁挂起)
ipmitool -I lanplus -H 192.168.100.101 -U sre_admin -P "StrongSrePassword123!" power off

# [SRE CMD] 物理冷重置服务器并重新开机 (Cold Boot)
ipmitool -I lanplus -H 192.168.100.101 -U sre_admin -P "StrongSrePassword123!" power reset
```

### 步骤 D：SOL（Serial over LAN）重定向字符控制台挂载
服务器重启后，通过带外网络直接挂载主板的串行控制台，实时查看 Linux Grub 引导、BIOS 自检以及引导加载程序内核解压的字符流：
```bash
# [SRE CMD] 远程挂载串行控制台，无图形化开销，直接接管物理 TTY
ipmitool -I lanplus -H 192.168.100.101 -U sre_admin -P "StrongSrePassword123!" sol activate
```

---

## 5. 带外控制交换机 EOS（Console Server）典型配置

在顶部 **RU 38（OOB Arista 7010T）** 交换机上，为各个物理口划分独立的带外隔离 VLAN，并开启 TACACS+/RADIUS 统一 AAA 认知的 EOS 生产配置范式：

```text
! 🌌 OOB Switch (Arista 7010T) Management Hardening Config
hostname OOB-R1-SW38
!
aaa authentication login default group radius local
aaa authorization exec default group radius local
!
! RADIUS 安全集中鉴权端点指引
radius-server host 192.168.100.250 key 7 ProdRadiusSharedKey789!
!
vlan 100
   name OOB-MGMT-VLAN
!
interface Management1
   description Link to OOB Core Router
   ip address 192.168.100.38/24
!
! 对接到各服务器主板 BMC 的 1G 物理端口硬划入带外 VLAN 100
interface Ethernet1
   description IPMI to Postgres-Prod1
   switchport access vlan 100
   spanning-tree portfast
!
interface Ethernet2
   description IPMI to Postgres-Prod2
   switchport access vlan 100
   spanning-tree portfast
!
! ─────────── SSH TLS Security Hardening ───────────
no ip http server
ip http secure-server
!
! 强制指定安全的 SSH 密码算法组合
ip ssh server cipher aes256-gcm
ip ssh server mac hmac-sha2-256
```

---

## 总结 (Summary)

带外管理网络（OOB）是 12306 面对任何严重软件黑天鹅事件、控制芯片短路或核心生产光纤熔断时的**“终极自愈降落伞”**。通过设计 **192.168.100.0/24 像素级 IP 网段分配**、**强制 TLS 1.3 与 RMCP+ Cipher Suite 17 安全加密锁死**、以及制定 **ipmitool 强行重置与 SOL 串行重定向标准指令手册**，我们确保在最极端的“生产业务网络整体粉碎瘫痪”状态下，SRE 运维团队依然能够在 3 秒内，以 100% 畅通的物理姿态和金融级安全审计环境，完成对国家级售票物理网底盘的一键自愈重置！
