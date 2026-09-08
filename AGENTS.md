# 🌌 12306 High-Concurrency Ticketing System — AI Agent Master Roster & Collaborative Protocol

本文件定义了 12306 高并发票务分配系统（Python + Vue 3）在 AI 协同研发阶段的**智能体阵容（Agent Roster）、领域专长映射与协同交互规约（Collaborative Protocols）**。所有参与本项目开发、维护、重构与测试审计的 AI 智能体均必须在工作首期完整阅读本协议，并严格遵守。

---

## 🛡️ 1. AI 协同角色划分 (Role Split)

在本项目中，研发闭环由两名顶尖 AI 智能体高度互补、共同保障：

```text
  ┌──────────────────────────────────────────────┐
  │         ChatGPT (Principal Architect)        │
  │  - 负责产品全局微观/宏观架构设计与方案决策     │
  │  - 把握核心 CQRS、数据一致性与高并发锁座设计   │
  │  - 进行代码评审（Code Review）与技术合规判定 │
  └──────────────────────┬───────────────────────┘
                         │
                         │ [协同技术方案与 CR 指令下达]
                         ▼
  ┌──────────────────────────────────────────────┐
  │           Gemini CLI (YOLO Mode)             │
  │  - 作为项目工程“物理执行引擎（The Muscle）”     │
  │  - 物理读取全量代码、执行手术刀式代码注入替换  │
  │  - 独立进行 TDD/BDD 集成测试红绿自愈核销      │
  │  - 编写本地 SRE 自动化运维、容器编排与工程文档 │
  └──────────────────────────────────────────────┘
```

---

## 👥 2. 局部智能体阵容与领域专长 (Agent Roster & Domains)

除主执行引擎外，在处理特定复杂场景时，可安全调度并编排以下局部专业子智能体：

| 智能体代号 (Agent Handle) | 领域专家定位 (Expert Domain) | 项目适用研发场景 (Project Use Case) |
| :--- | :--- | :--- |
| **`codebase_investigator`** | 代码库全局依赖、时序调用链路剖析专家 | 1. 复杂死锁、多线程竞争并发压力测试异常根因诊断<br>2. 跨层级（API -> Service -> Redis LUA）调用树梳理 |
| **`generalist`** | 高并发、高通量批处理与 DevOps 专家 | 1. 跨多文件的大批量代码重构与 linter / formatter 自愈<br>2. Dockerfile 镜像体积优化与 SRE 监控脚本编写 |
| **`ponytail`** | 极简优先、拒绝过度设计高级架构审判官 | 1. 严格过滤不必要的 speculative abstractions（预测性设计）<br>2. 提倡用最精简、最少行数的原生标准库解决问题 |

---

## 📜 3. 核心行为防线与底线律令 (Supreme Mandates)

所有执行端 AI 智能体在对本项目物理文件进行任何 `replace` 或运行 Shell 指令时，必须遵守以下核心律令：

### ① 读-比-写 闭环手术刀式修改（Surgical Changes Only）
*   **严禁大范围覆盖**：禁止直接重写整个文件，必须使用 native 的 `replace` 像素级替换旧行。
*   **严禁无脑占位符**：禁止在输出代码中添加 `// ... rest of code` 等任何懒惰占位，所有修改必须具备可编译、可解释的原生完整性。
*   **同化代码风格**：必须绝对适应当前文件的 Python / JavaScript / CSS 代码缩进、命名与注释风格。

### ② 静态安全扫描避坑规约（Anti-Command-Injection Mandate）
*   由于 CLI 底层部署了极为严苛的安全防御策略，任何包含 **反引号（` `）**、**美元符号配圆括号（`$(...)`）** 或 **小于号配圆括号（`<(...)`）** 的 Shell 命令均会被拦截（报错 `Blocked: command substitution detected`）。
*   **绝对禁止使用 Shell 拼接生成 Markdown**：严禁通过 `cat EOF`、`echo` 在终端里注入 Markdown 格式，若有文档写入需要，**必须且只能**调用系统原生的 `write_file` 或 `replace` API 工具，彻底避开转义陷阱。

### ③ 文档命名、公式与图示规范 (Documentation, Formulas, and Diagrams Standard)

为了保障 12306 本地数据中心方案在持续协同研发中的极高严谨性，所有的工程设计文档、数学算式、系统图示均须遵循以下规范：

#### A. 文档命名规范 (Document Naming Standard)
*   **5 阶段双位数字生命周期层级**：全部文档须遵循统一前缀，用下划线隔离，严禁在文件名中包含空格（防止在 Shell 脚本中引起转义故障）：
    *   `01_XX_`：业务标准阶段 (Business & PO Manual / User BDD Features)
    *   `02_XX_`：算法阶段 (Bitmap 预占、核心座位扣减算法方案)
    *   `03_XX_`：架构设计阶段 (High Concurrency, Event-Driven, Passenger Scheduling)
    *   `04_XX_`：工程实现阶段 (Python Implementation & Development Guide)
    *   `05_XX_`：生产部署与 SRE 高可用阶段 (Spine-Leaf Network, K8s, DR, Rate-limiting, SRE)
*   **全部文档小写下划线**：物理文件名及超链接引用的文件名需使用小写蛇形命名（Snake_case，如 `05_05_12306_high_concurrency_capacity_planning_and_hardware_sizing.md`），以保持跨操作系统系统的绝对一致性。

#### B. 数学公式规范 (Formula Standard)
*   **100% LaTeX 表达**：所有的库存平衡公式、排队论模型、备件故障 Poisson 泊松分布、SLA 可用性概率推导、数据库有状态 I/O 吞吐模型等，必须使用标准的 $\text{\LaTeX}$（TeX）格式（块级使用 `$$ ... $$`，行内使用 `$ ... $`）进行排版，严禁使用粗糙的纯文本 ASCII 符号拼凑公式。
*   **数学严谨性与参数参数化**：公式中每个常数、系数和概率因子都必须有硬核的 SRE/统计学支撑（如泊松分布在 $99.9\%$ 右侧单尾置信边界下的标准差倍数 $z$-score 精确为 `3.09`，不拍脑门）。

#### C. 高精图示规范 (Diagram Standard)
*   **逻辑/拓扑/时序首选 D2**：对于逻辑架构拓扑、跨中心多活时序、网络层级连通性、流量多级漏斗等，一律采用 **D2 工具生成高清矢量 SVG 格式图片**（D2 源码保存在 `docs/d2/*.d2` 中，编译打包输出到 `docs/images/*.svg`）。
*   **物理布局、机柜与冷却首选 Draw.io**：对于物理层面的 42U 机架实物设备堆叠、RU 单元高度精确对齐、三维轴侧闭式冷通道冷却风道气流流程、Spine-Leaf 网络设备物料连接，**必须且只能采用 Draw.io XML 工具进行设计**（保存为 `docs/images/*.drawio` 并打包导出附带内嵌 XML 数据的 `.drawio.png` 或 `.svg`，以维护双向可读编辑能力）。


### ④ 无验证，不交付（Empirical Validation Overlord）
*   **拒绝主观猜测**：严禁在测试未通过或未经本地运行的情况下对用户谎称“功能已修复/已交付”。
*   **TDD 红绿自愈**：对于任何新增 Feature，必须先编写对应的单元测试复现失败状态，修改代码后再次启动 `pytest`，确保 100% 通过后方可进行 Git 本地提交。

---

## 🤝 4. 跨智能体协作工作流 (Checkpoints Loop)

```text
       [ ChatGPT 方案批准 ]
               │
               ▼
     [ Gemini CLI 准备开发 ] ───► 先行激活特定 Skill (ponytail-review / TDD 等)
               │
               ▼
       [ 手术刀式代码注入 ] ───► 调用 native replace/write_file，杜绝 Shell 拼接
               │
               ▼
    [ pytest-bdd 自动化核销 ] ───► 运行 ./ops.sh test，确保全量 23/23 绿灯
               │
               ▼
     [ SRE & Docs 最终审查 ] ───► 检查 .dockerignore、更新 spec 文档、登记 AGENTS.md
               │
               ▼
     [ 归档提交并向人类报备 ] ───► 本地 Git Commit，提供 Concise Report
```

---
**AGENTS.md Protocol Enforced | Collaborative Sandbox Engaged | High-Signal Output Locked**
