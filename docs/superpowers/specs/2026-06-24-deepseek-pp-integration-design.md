# deepseek-pp 功能融入 ds-free-api 设计文档

> 日期：2026-06-24
> 状态：待审核（v2 — 重新定位为 API 代理 + prompt 增强服务）
> 范围：将 deepseek-pp 浏览器扩展的核心功能复现到 ds-free-api（Rust web2api 代理）中

## 1. 概述

### 1.1 定位

本项目作为 **API 接入给其他 agent 调用**，不是 agent 本身。deepseek-pp 的功能按两种方式融入：

1. **提供 agent 可用的接口**：通过 Admin API 管理记忆、Skill、预设、项目、保存项、自动化、产物等数据
2. **处理转化**：agent 发标准 OpenAI 请求时，服务端自动把激活的预设、相关记忆、项目上下文、Skill 指令注入到发给 DeepSeek 的 prompt 中，agent 无需自己处理复杂的 prompt 增强逻辑

**不做 Agent 循环**：服务端不自动执行任何工具，不检测工具调用 XML 标签，不回传工具结果继续生成。所有工具执行由调用方 agent 自己完成。

### 1.2 排除范围

- **Agent 循环**：服务端不自动执行工具、不循环生成
- **内置工具执行**：不实现 memory_save/web_search/artifact_create 等服务端工具
- **Web 工具**：web_search / web_fetch 完全移除（agent 自己的 MCP/工具/skill 已覆盖）
- **MCP 系统**：agent 自己连接和管理 MCP，本项目不参与 MCP 工具发现/注入/执行
- **侧边栏 UI**：deepseek-pp 的浏览器侧边栏（本项目通过 admin 面板 + API 替代）
- **悬浮宠物**：纯浏览器 UI，不适用
- **云同步**：WebDAV/Google Drive/OneDrive（用户明确排除）
- **浏览器控制**：依赖 Chrome Debugger，服务端无法复现
- **Shell/Python 沙箱**：服务端执行命令有安全风险，排除
- **OfficeCLI 文档工具**：依赖 Shell MCP，排除

### 1.3 核心价值

agent 调用方的工作流：
1. 通过 Admin API 管理记忆/Skill/预设/项目等数据
2. 发标准 OpenAI chat 请求到 `/v1/chat/completions`
3. 服务端自动完成 prompt 增强（注入记忆+预设+项目+skill）后转发给 DeepSeek
4. 服务端返回标准 OpenAI 响应（若模型调用了用户定义的 `tools`，按现有 `<tool_call>` 格式解析返回）
5. agent 自己执行工具，再次发请求（标准 OpenAI 多轮）

## 2. 架构设计

### 2.1 请求处理管线（无 Agent 循环）

```
客户端请求（标准 OpenAI 格式）
  → normalize::apply         # 现有：验证、默认参数
  → tools::extract           # 现有：用户自定义工具（保持 <tool_call> 格式）
  → files::extract           # 现有：data URL / HTTP URL
  → augmentation::apply      # 新：注入记忆+skill+预设+项目上下文+i18n 模板
  → prompt::build            # 现有：ChatML → DeepSeek 原生标签
  → resolver::resolve        # 现有：模型解析、能力开关
  → ds_core::try_chat        # 现有：发给 DeepSeek
  → 流式响应
    → ConverterStream        # 现有：StreamEvent → OpenAI chunks
    → ToolCallStream         # 现有：解析用户自定义 <tool_call>
    → StopDetectStream       # 现有
  → SSE 输出到客户端
```

**与原方案的区别**：移除了 `BuiltinToolStream` 和 Agent 循环。服务端只做 prompt 增强 + 协议转化，不做工具执行。

### 2.2 工具系统

**仅保留用户自定义工具**（标准 OpenAI 流程）：
- 请求 `tools` 字段定义的工具 → 注入为 `<tool_call>[JSON数组]</tool_call>` 格式到 prompt
- 模型输出 `<tool_call>` 标签 → `ToolCallStream` 解析为 OpenAI `tool_calls` 返回客户端
- 客户端执行工具后，把结果作为新的 `tool` 角色消息发回（标准 OpenAI 多轮）

**不实现任何服务端自动执行的工具**。

### 2.3 存储设计

所有持久化数据使用 JSON 文件，存储在 `DS_DATA_DIR` 下：

```
{DS_DATA_DIR}/
├── config.toml          # 现有配置
├── stats.json           # 现有统计
├── runtime.log          # 现有日志
├── memories.json        # 新：记忆存储
├── skills.json          # 新：自定义 Skill 存储
├── presets.json         # 新：系统提示词预设
├── projects.json        # 新：项目上下文
├── saved_items.json     # 新：保存项
├── automations.json     # 新：自动化任务
├── automation_runs.json # 新：自动化运行历史
└── artifacts/           # 新：产物文件目录
```

每个 JSON 文件由 `StoreManager` 统一管理，原子写入（tmp + rename，0600 权限），与现有 `Config::save()` 模式一致。

### 2.4 Prompt 增强管线

`augmentation::apply` 在 `files::extract` 之后、`prompt::build` 之前执行，向 `ChatCompletionsRequest` 的 system 消息注入增强内容。

**注入顺序**（拼接到 system 消息尾部）：
1. 激活预设内容（按 `preset_cadence` 决定是否注入：每条/仅首条/关闭）
2. 系统提示词模板（i18n，含记忆块占位 + 记忆保存规则说明 + 工具调用格式提醒）
3. 项目上下文（项目指令 + 项目记忆，若请求关联了项目）
4. Skill 指令（若用户消息以 `/skillname` 开头）
5. 强制回复语言（若配置非 auto）

**记忆注入**：通过记忆选择器从 `memories.json` 筛选相关记忆，注入到系统提示词的 `## 已有记忆` 区块。

## 3. 子系统设计

### 3.1 记忆系统

**数据结构**：
```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct Memory {
    pub id: u64,
    pub scope: MemoryScope,        // Global | Project(String)
    pub r#type: MemoryType,        // User | Feedback | Topic | Reference
    pub name: String,
    pub content: String,
    pub tags: Vec<String>,
    pub pinned: bool,
    pub created_at: i64,
    pub updated_at: i64,
    pub access_count: u32,
    pub last_accessed_at: i64,
}
```

**记忆选择器**（移植自 deepseek-pp `selector.ts`）：
- 关键词分词：中文默认用简单字符分割（按标点和空格切分，2 字以上片段作为词）；`jieba-rs` 作为可选增强（启用后分词更精准，但增加编译时间和二进制体积）
- 评分：`pin(1000) + keyword_score(tag×20 + name×15 + content×5) + decay(access_count + freshness) + recency_bonus`
- Token 预算：1500（prompt > 3000 token 时递减到最低 800）
- 格式：`- #id [type] name: content`

**注入**：在系统提示词的 `## 已有记忆` 区块注入筛选后的记忆。注入后更新 `access_count` 和 `last_accessed_at`。

**工具**：不提供服务端工具。agent 若想让模型"保存记忆"，由 agent 自己解析模型意图后调用 `POST /admin/api/memories`。

**Admin API**：
- `GET /admin/api/memories` — 列出（支持 type/tag/scope 筛选）
- `POST /admin/api/memories` — 新增
- `PUT /admin/api/memories/{id}` — 更新
- `DELETE /admin/api/memories/{id}` — 删除
- `PUT /admin/api/memories/{id}/pin` — 置顶/取消
- `POST /admin/api/memories/import` — 批量导入 JSON
- `GET /admin/api/memories/export` — 导出 JSON

### 3.2 Skill 系统

**数据结构**：
```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct Skill {
    pub name: String,              // kebab-case 触发名
    pub description: String,
    pub instructions: String,      // Markdown 指令
    pub source: SkillSource,       // Builtin | Custom
    pub memory_enabled: bool,      // 是否在该 skill 上下文中注入记忆
    pub enabled: bool,
}
```

**内置 Skill**（移植自 deepseek-pp，排除 shell/OfficeCLI）：
- `memory` — 记忆管理指引（`/memory save|list|update|delete`，提示 agent 调用记忆 API）
- `ultra-think` — 极致深度思考
- `frontend-design` — 前端设计
- `doc-coauthoring` — 文档协作
- `brand-guidelines` — 品牌规范
- `skill-creator` — Skill 创建助手
- `algorithmic-art` — 算法艺术
- `canvas-design` — 视觉设计

**触发机制**：用户消息以 `/skillname args` 开头时，解析 skill 名和参数，将 skill 指令作为 prompt 前缀注入。支持链式调用：`/skill1 /skill2 实际输入`。

**注入**：skill 指令作为 system 消息的一部分，后接 `---` 分隔符。原用户消息保留在 user 角色中。

**Admin API**：
- `GET /admin/api/skills` — 列出全部（含内置）
- `POST /admin/api/skills` — 新增自定义
- `PUT /admin/api/skills/{name}` — 更新
- `DELETE /admin/api/skills/{name}` — 删除（内置不可删）
- `PUT /admin/api/skills/{name}/enabled` — 启用/停用

### 3.3 系统提示词预设

**数据结构**：
```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct SystemPromptPreset {
    pub id: String,
    pub name: String,
    pub content: String,
    pub active: bool,              // 同时只有一个激活
    pub created_at: i64,
    pub updated_at: i64,
}
```

**注入**：激活的预设内容拼接到 system 消息最前面，后接 `---` 分隔符。注入频率由 `preset_cadence` 配置：`every`（每条消息）/ `first`（仅首条，默认）/ `off`（关闭）。

**Admin API**：
- `GET /admin/api/presets` — 列出
- `POST /admin/api/presets` — 新增
- `PUT /admin/api/presets/{id}` — 更新
- `DELETE /admin/api/presets/{id}` — 删除
- `PUT /admin/api/presets/{id}/active` — 激活（自动取消其他激活）

### 3.4 项目系统

**数据结构**：
```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub description: String,
    pub instructions: String,      // 项目专属指令
    pub memories: Vec<Memory>,     // 项目专属记忆（独立于全局记忆）
    pub created_at: i64,
    pub updated_at: i64,
}
```

**关联机制**：web2api 无浏览器会话概念，通过以下方式关联项目（按优先级）：
1. 请求头 `x-ds-project: {project_id}`（优先）
2. 请求体扩展字段 `project_id`（回退）
3. 均未提供时不关联项目

**注入**：项目指令 + 项目记忆注入到系统提示词的 `## 项目上下文` 区块。项目记忆使用与全局记忆相同的选择器算法，但仅在该项目记忆范围内筛选。

**Admin API**：
- `GET /admin/api/projects` — 列出
- `POST /admin/api/projects` — 新增
- `PUT /admin/api/projects/{id}` — 更新
- `DELETE /admin/api/projects/{id}` — 删除
- `GET /admin/api/projects/{id}/memories` — 项目记忆列表
- `POST /admin/api/projects/{id}/memories` — 新增项目记忆
- `PUT /admin/api/projects/{id}/memories/{mid}` — 更新
- `DELETE /admin/api/projects/{id}/memories/{mid}` — 删除

### 3.5 保存项

**数据结构**：
```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct SavedItem {
    pub id: String,
    pub kind: SavedItemKind,        // Snippet | Bookmark
    pub title: String,
    pub content: String,
    pub tags: Vec<String>,
    pub created_at: i64,
}
```

**使用**：通过请求体扩展字段 `insert_saved_items: ["id1", "id2"]`（字符串数组，按顺序）将保存项内容插入到用户消息前，多个保存项用 `\n\n---\n\n` 分隔。

**Admin API**：
- `GET /admin/api/saved-items` — 列出（支持搜索/标签筛选）
- `POST /admin/api/saved-items` — 新增
- `PUT /admin/api/saved-items/{id}` — 更新
- `DELETE /admin/api/saved-items/{id}` — 删除
- `GET /admin/api/saved-items/export` — 导出 Markdown/JSON

### 3.6 自动化任务

**数据结构**：
```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct AutomationTask {
    pub id: String,
    pub name: String,
    pub prompt: String,             // 触发时发送的 prompt
    pub model: String,              // 使用的模型
    pub trigger: AutomationTrigger, // Manual | Cron(String)
    pub timezone: String,
    pub search_enabled: bool,
    pub thinking_enabled: bool,
    pub enabled: bool,
    pub last_run_at: Option<i64>,
    pub last_status: Option<AutomationStatus>, // Success | Failed | Running
    pub next_run_at: Option<i64>,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AutomationRun {
    pub id: String,
    pub task_id: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub status: AutomationStatus,
    pub response: Option<String>,   // 模型回复（截断存储，最大 50KB）
    pub error: Option<String>,
}
```

**调度**：后台 tokio 任务，使用 `tokio-cron-scheduler` crate。最小间隔 15 分钟。

**执行**：触发时创建独立 chat completion 请求（通过内部 `OpenAIAdapter::try_chat`），**不做 Agent 循环**——单轮请求，结果存入 `automation_runs.json`。若模型返回 `tool_calls`，记录为"需要工具执行"状态，不自动处理。

**Admin API**：
- `GET /admin/api/automations` — 列出
- `POST /admin/api/automations` — 新增
- `PUT /admin/api/automations/{id}` — 更新
- `DELETE /admin/api/automations/{id}` — 删除
- `POST /admin/api/automations/{id}/run` — 立即运行
- `PUT /admin/api/automations/{id}/pause` — 暂停
- `PUT /admin/api/automations/{id}/resume` — 恢复
- `GET /admin/api/automations/{id}/history` — 运行历史（分页）

### 3.7 对话导出

**实现**：新增导出端点，将 chat completion 请求历史导出为 HTML/Markdown/JSON。

**Admin API**：
- `POST /admin/api/export/conversation` — 导出指定对话
  - 请求体：`{ format: "html"|"markdown"|"json", messages: [...], readable: bool }`
  - 返回：文件流（Content-Disposition: attachment）
- `GET /admin/api/export/saved-items` — 导出保存项（Markdown/JSON）

### 3.8 可下载产物

**定位**：纯存储服务。agent 调用模型生成内容后，agent 自己通过 API 上传存储，获取下载链接。

**数据结构**：
```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct Artifact {
    pub id: String,
    pub filename: String,
    pub content_type: String,
    pub size: u64,
    pub created_at: i64,
    pub metadata: Option<serde_json::Value>, // agent 自定义元数据
}
```

**存储**：文件写入 `{DS_DATA_DIR}/artifacts/{id}`，元数据存入 `artifacts/index.json`。

**Admin API**：
- `GET /admin/api/artifacts` — 列出（分页，支持 filename 搜索）
- `POST /admin/api/artifacts` — 上传新产物（multipart/form-data 或 JSON base64）
- `GET /admin/api/artifacts/{id}` — 下载文件
- `GET /admin/api/artifacts/{id}/meta` — 获取元数据
- `DELETE /admin/api/artifacts/{id}` — 删除

### 3.9 i18n 提示词

**实现**：系统提示词模板支持中文/英文，通过 `[deepseek_pp] locale` 配置。

**模板**：移植自 deepseek-pp `i18n/resources/zh-CN.ts` 和 `en.ts` 的 `prompt.systemChat`、`prompt.systemThinking`、`prompt.toolFormatReminder` 等字段。移除 `prompt.webSearchGuidance`（web 工具已删除）。

**默认**：中文（与现有项目一致）。

### 3.10 提示词控制

**配置**（`[deepseek_pp]` 区块）：
```toml
[deepseek_pp]
enabled = true
locale = "zh"                    # zh | en
memory_enabled = true
system_prompt_enabled = true
preset_cadence = "first"         # every | first | off
force_response_language = "auto" # auto | zh | en
```

## 4. 配置变更

### 4.1 config.toml 新增区块

```toml
[deepseek_pp]
enabled = true
locale = "zh"
memory_enabled = true
system_prompt_enabled = true
preset_cadence = "first"
force_response_language = "auto"
```

### 4.2 config.example.toml 更新

在 `config.example.toml` 中添加 `[deepseek_pp]` 完整示例和注释。

### 4.3 请求扩展字段

标准 OpenAI 请求支持以下扩展字段（非破坏性，标准客户端会忽略）：
- `project_id: String` — 关联项目
- `insert_saved_items: Vec<String>` — 插入保存项 ID 列表

请求头扩展：
- `x-ds-project: {project_id}` — 关联项目（优先于请求体字段）

## 5. Admin API 端点汇总

| 方法 | 路径 | 功能 |
|------|------|------|
| GET | `/admin/api/deepseek-pp/status` | 功能总览状态 |
| GET/PUT | `/admin/api/deepseek-pp/settings` | 提示词控制设置 |
| GET/POST | `/admin/api/memories` | 记忆列表/新增 |
| PUT/DELETE | `/admin/api/memories/{id}` | 更新/删除记忆 |
| PUT | `/admin/api/memories/{id}/pin` | 置顶记忆 |
| POST | `/admin/api/memories/import` | 批量导入 |
| GET | `/admin/api/memories/export` | 导出 |
| GET/POST | `/admin/api/skills` | Skill 列表/新增 |
| PUT/DELETE | `/admin/api/skills/{name}` | 更新/删除 |
| PUT | `/admin/api/skills/{name}/enabled` | 启用/停用 |
| GET/POST | `/admin/api/presets` | 预设列表/新增 |
| PUT/DELETE | `/admin/api/presets/{id}` | 更新/删除 |
| PUT | `/admin/api/presets/{id}/active` | 激活 |
| GET/POST | `/admin/api/projects` | 项目列表/新增 |
| PUT/DELETE | `/admin/api/projects/{id}` | 更新/删除 |
| GET/POST | `/admin/api/projects/{id}/memories` | 项目记忆 |
| PUT/DELETE | `/admin/api/projects/{id}/memories/{mid}` | 更新/删除项目记忆 |
| GET/POST | `/admin/api/saved-items` | 保存项列表/新增 |
| PUT/DELETE | `/admin/api/saved-items/{id}` | 更新/删除 |
| GET | `/admin/api/saved-items/export` | 导出 |
| GET/POST | `/admin/api/automations` | 自动化列表/新增 |
| PUT/DELETE | `/admin/api/automations/{id}` | 更新/删除 |
| POST | `/admin/api/automations/{id}/run` | 立即运行 |
| PUT | `/admin/api/automations/{id}/pause` | 暂停 |
| PUT | `/admin/api/automations/{id}/resume` | 恢复 |
| GET | `/admin/api/automations/{id}/history` | 运行历史 |
| GET/POST | `/admin/api/artifacts` | 产物列表/上传 |
| GET/DELETE | `/admin/api/artifacts/{id}` | 下载/删除 |
| GET | `/admin/api/artifacts/{id}/meta` | 元数据 |
| POST | `/admin/api/export/conversation` | 导出对话 |
| GET | `/admin/api/export/saved-items` | 导出保存项 |

## 6. 前端页面

扩展现有 admin 面板（`web/src/pages/`），新增以下页面：

| 页面 | 路由 | 功能 |
|------|------|------|
| MemoryPage | `/memory` | 记忆管理（列表/筛选/编辑/置顶/导入导出） |
| SkillPage | `/skills` | Skill 管理（内置/自定义/启用控制） |
| PresetPage | `/presets` | 系统提示词预设管理 |
| ProjectPage | `/projects` | 项目管理（指令/项目记忆） |
| SavedPage | `/saved` | 保存项管理（搜索/标签/导出） |
| AutomationPage | `/automations` | 自动化任务管理（创建/调度/历史） |
| ArtifactPage | `/artifacts` | 产物列表/上传/下载 |
| PromptSettingsPage | `/settings/prompt` | 提示词控制（记忆/预设/语言开关） |

Layout 导航更新：在现有侧边栏添加「Agent 工作台」分组。

## 7. Rust 模块结构

```
src/
├── deepseek_pp/              # 新：deepseek-pp 功能模块
│   ├── mod.rs                # facade: re-exports
│   ├── augmentation.rs       # prompt 增强（记忆+skill+预设+项目+i18n 注入）
│   ├── i18n.rs               # 系统提示词模板（中英文）
│   ├── memory/
│   │   ├── mod.rs            # facade
│   │   ├── store.rs          # memories.json 读写
│   │   └── selector.rs       # 记忆筛选（关键词评分+衰减+预算）
│   ├── skill/
│   │   ├── mod.rs            # facade
│   │   ├── parser.rs         # /skill 命令解析
│   │   ├── registry.rs       # skill 注册表（内置+自定义）
│   │   └── builtin.rs        # 内置 skill 定义
│   ├── preset/
│   │   └── store.rs          # 预设存储
│   ├── project/
│   │   └── store.rs          # 项目存储
│   ├── saved_items/
│   │   └── store.rs          # 保存项存储
│   ├── artifact/
│   │   └── store.rs          # 产物存储（文件+元数据）
│   └── automation/
│       ├── mod.rs            # facade
│       ├── scheduler.rs      # cron 调度器
│       └── runner.rs         # 任务执行器（单轮 try_chat，不循环）
├── openai_adapter/
│   └── request/
│       └── augmentation.rs   # 新管线阶段：调用 deepseek_pp::augmentation::apply()
├── server/
│   ├── admin.rs              # 扩展：新增所有 admin API 端点
│   └── store.rs              # 扩展：新增 JSON 存储管理
└── config.rs                 # 扩展：DeepSeekPpConfig 结构
```

**调用关系**：
- `augmentation` 作为 `openai_adapter/request/` 管线中的新阶段（位于 `files` 和 `prompt` 之间），调用 `deepseek_pp::augmentation::apply()` 注入增强内容到 `ChatCompletionsRequest`
- 响应管线**不变**：仍为 `ConverterStream → ToolCallStream → StopDetectStream`，无 `BuiltinToolStream`

## 8. 实施阶段

按以下顺序分阶段实施，每阶段可独立验收：

### 阶段 1：Prompt 增强框架 + 记忆系统
- `deepseek_pp/` 模块骨架（mod.rs, augmentation.rs, i18n.rs）
- `openai_adapter/request/augmentation.rs` 管线阶段接入
- 记忆存储 + 选择器 + 注入逻辑
- 系统提示词模板（i18n 中英文）
- Admin API：记忆 CRUD + 导入导出
- 前端：MemoryPage + PromptSettingsPage

### 阶段 2：Skill + 预设系统
- Skill 注册表 + 解析器 + 内置 skill
- 预设存储 + 注入（按 cadence）
- Admin API + 前端：SkillPage + PresetPage

### 阶段 3：项目 + 保存项
- 项目存储 + 上下文注入 + 项目记忆
- 保存项存储 + 请求时插入
- 请求扩展字段解析（project_id / insert_saved_items / x-ds-project header）
- Admin API + 前端：ProjectPage + SavedPage

### 阶段 4：自动化 + 导出 + 产物
- 自动化调度器 + 任务执行器（单轮）
- 对话导出端点
- 产物存储 + 上传/下载
- Admin API + 前端：AutomationPage + ArtifactPage

## 9. 依赖新增

| Crate | 用途 | 必要性 |
|-------|------|--------|
| `tokio-cron-scheduler` | 自动化任务 cron 调度 | 必需 |
| `jieba-rs` | 中文分词（记忆选择器） | 可选（默认用简单字符分割） |

**移除的依赖**（相比 v1 方案）：
- `zip` — 产物改为单文件上传，不需要打包
- `scraper` — web_search 已移除

`wreq` 已用于 HTTP 客户端，自动化任务执行复用。

## 10. 风险与缓解

| 风险 | 缓解 |
|------|------|
| 记忆无限增长 | Token 预算限制注入量；admin 面板提供清理工具 |
| Prompt 增强增加 token 消耗 | 记忆预算 1500 上限；预设 cadence 可配置为 `off` |
| 自动化任务并发冲突 | 每任务独立会话；调度器互斥锁；最小间隔 15 分钟 |
| 自动化模型返回 tool_calls | 记录为"需要工具执行"状态，不自动处理（符合无 Agent 循环定位） |
| 产物存储磁盘溢出 | 上传大小限制（默认 10MB）；admin 面板提供批量删除 |
| 请求扩展字段被标准客户端拒绝 | 字段为可选，标准 OpenAI 客户端会忽略未知字段；header 方式更通用 |
