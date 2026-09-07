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

### ③ 绝对 ASCII 框线图示与公式默认法则（No LaTeX & No Mermaid）
*   为了确保在没有任何第三方渲染器的纯系统终端、Vim 或底层 Docker 容器内保持 100% 的可读性，本项目内所有架构图、数据流向图、库存与票额平衡计算公式，**默认且强制只能使用纯文本 ASCII 形式进行工整表达**。
*   **严禁主动引入 LaTeX 公式或 Mermaid 块**（除非用户在 Prompt 里显式书写 `Mermaid` 命令）。

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
