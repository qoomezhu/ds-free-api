# deepseek-pp 功能融入 ds-free-api 设计文档

> 日期：2026-06-24
> 状态：待审核
> 范围：将 deepseek-pp 浏览器扩展的核心功能复现到 ds-free-api（Rust web2api 代理）中

## 1. 概述

### 1.1 目标

将 [deepseek-pp](https://github.com/qoomezhu/deepseek-pp) 浏览器扩展的核心功能融入本项目（ds-free-api），使 web2api 代理具备 AI Agent 工作台能力：长期记忆、Skill、MCP 工具、项目上下文、系统提示词预设、联网搜索、可下载产物、对话导出、自动化任务和保存项。

### 1.2 排除范围

- **侧边栏 UI**：deepseek-pp 的浏览器侧边栏对话入口（本项目通过 admin 面板 + API 端点替代）
- **悬浮宠物**：DeepSeek 页面小鲸鱼（纯浏览器 UI，不适用）
- **云同步**：WebDAV/Google Drive/OneDrive 同步（用户明确排除）
- **浏览器控制**：依赖 Chrome Debugger + Accessibility Tree，服务端无法复现
- **Shell/Python 沙箱**：`shell_exec`/`python_exec`/`sandbox_run` 在服务器执行命令有安全风险，排除
- **OfficeCLI 文档工具**：依赖 Shell MCP，排除

### 1.3 核心架构决策

**混合 Agent 模式**：
- **内置工具**（memory_save、web_search 等）在服务端自动执行，通过 Agent 循环把结果回传并继续生成
- **用户自定义工具**（请求 `tools` 字段）走标准 OpenAI 流程，返回客户端执行
- 客户端只看到最终回复（内置工具的 XML 标签从输出中剥离）

## 2. 架构设计

### 2.1 Agent 循环（核心）

```
客户端请求
  → normalize → tools(用户工具) → files → prompt(注入记忆+skill+预设+项目+内置工具schema)
  → resolver → ds_core::try_chat
  → 流式响应
    → ConverterStream: StreamEvent → OpenAI chunks
    → BuiltinToolStream: 检测内置工具 XML 标签（<memory_save>、<web_search> 等）
      → 若检测到：缓冲完整工具调用，从输出流中剥离标签
      → 执行工具（服务端）
      → 将工具结果作为新 user 消息追加
      → 发起新一轮 ds_core::try_chat（带工具结果上下文）
      → 循环，直到无内置工具调用或达到最大步数
    → ToolCallStream: 解析用户自定义工具 <tool_call>（标准 OpenAI）
    → StopDetectStream
  → SSE 输出到客户端
```

**关键设计**：
- 内置工具使用**直接 XML 标签**格式（`<memory_save>{JSON}</memory_save>`），可出现在回复任意位置
- 用户自定义工具保持现有 `<tool_call>[JSON数组]</tool_call>` 格式
- Agent 循环最大步数：10（可配置），步间间隔 1s
- 流式输出：内置工具执行期间向客户端发送 `/* 执行工具中... */` 注释块保持连接

### 2.2 工具系统

#### 内置工具（服务端自动执行）

| 工具名 | 说明 | 执行方式 |
|--------|------|---------|
| `memory_save` | 保存长期记忆 | 写入 memories.json |
| `memory_update` | 更新记忆 | 修改 memories.json |
| `memory_delete` | 删除记忆 | 修改 memories.json |
| `web_search` | Bing 搜索 | wreq 抓取 Bing + HTML 解析 |
| `web_fetch` | 获取网页文本 | wreq 下载 + 文本提取 |
| `artifact_create` | 创建可下载文件 | 存储到 artifacts 目录 |
| `artifact_bundle` | 打包多文件 ZIP | 生成 ZIP 存储到 artifacts 目录 |
| `skill_creator` | 生成 Skill 草稿 | 返回草稿供用户确认 |
| `memory_import` | 批量导入记忆 | 解析+去重写入 |
| `task_complete` | 标记任务完成 | 终止 Agent 循环 |

#### 用户自定义工具（返回客户端）

保持现有行为不变：`<tool_call>[{name, arguments}]</tool_call>` → 解析为 OpenAI `tool_calls` 返回客户端。

#### MCP 工具（服务端自动执行）

从已连接的 MCP 服务发现工具，注入为直接 XML 标签格式，服务端调用 MCP 服务执行。

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
├── mcp_servers.json     # 新：MCP 服务配置（也可在 config.toml）
├── automations.json     # 新：自动化任务
└── artifacts/           # 新：生成的产物文件
```

每个 JSON 文件由 `StoreManager` 统一管理，原子写入（tmp + rename，0600 权限），与现有 `Config::save()` 模式一致。

### 2.4 Prompt 增强管线

在现有 `request/prompt.rs` 的 `build()` 之前，新增 `augmentation` 阶段：

```
ChatCompletionsRequest
  → normalize::apply         # 现有
  → tools::extract           # 现有：用户工具
  → files::extract           # 现有
  → augmentation::apply      # 新：注入记忆+skill+预设+项目+内置工具schema
  → prompt::build            # 现有：ChatML → DeepSeek 标签
  → resolver::resolve        # 现有
```

`augmentation::apply` 注入顺序（拼接到 system 消息尾部）：
1. 激活预设内容（首条消息注入）
2. 系统提示词模板（含记忆块 + 内置工具 schema + 记忆保存规则 + 搜索规则）
3. 项目上下文（项目指令 + 项目记忆）
4. Skill 指令（若检测到 `/skill` 命令）
5. 强制回复语言（若配置）

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

**注入**：在系统提示词的 `## 已有记忆` 区块注入筛选后的记忆。

**工具**：`memory_save`/`memory_update`/`memory_delete`，服务端执行，修改 `memories.json`。

**Admin API**：
- `GET /admin/api/memories` — 列出（支持 type/tag 筛选）
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
    pub memory_enabled: bool,
    pub enabled: bool,
}
```

**内置 Skill**（移植自 deepseek-pp，排除 shell/OfficeCLI）：
- `memory` — 记忆管理：`/memory save|list|update|delete`
- `ultra-think` — 极致深度思考
- `frontend-design` — 前端设计
- `doc-coauthoring` — 文档协作
- `brand-guidelines` — 品牌规范
- `skill-creator` — Skill 创建助手
- `algorithmic-art` — 算法艺术
- `canvas-design` — 视觉设计

**触发机制**：用户消息以 `/skillname args` 开头时，解析 skill 名和参数，将 skill 指令替换/包装用户输入。支持链式调用：`/skill1 /skill2 实际输入`。

**注入**：skill 指令作为 prompt 前缀，后接 `---` 分隔符和用户实际输入。

**Admin API**：
- `GET /admin/api/skills` — 列出全部
- `POST /admin/api/skills` — 新增自定义
- `PUT /admin/api/skills/{name}` — 更新
- `DELETE /admin/api/skills/{name}` — 删除
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

**注入**：激活的预设内容拼接到 system 消息最前面，后接 `---` 分隔符。注入频率可配置：每条消息 / 仅首条 / 关闭。

**Admin API**：
- `GET /admin/api/presets` — 列出
- `POST /admin/api/presets` — 新增
- `PUT /admin/api/presets/{id}` — 更新
- `DELETE /admin/api/presets/{id}` — 删除
- `PUT /admin/api/presets/{id}/active` — 激活

### 3.4 Web 工具（web_search / web_fetch）

**web_search**：
- 搜索引擎：Bing（`cn.bing.com` / `www.bing.com` 轮询，无需 API key）
- 实现：wreq GET 请求，HTML 解析 `<li class="b_algo">` 提取标题/URL/摘要
- 返回：topK 条结果（默认 5，最大 10），格式化为 Markdown 链接列表
- 超时：8s/域名，总计 18s

**web_fetch**：
- 实现：wreq GET 请求，提取可见文本
- HTML 处理：移除 script/style/nav/footer/header 标签，剥离 HTML 标签，解码实体
- 截断：最大 50000 字符
- 超时：15s

**注入**：当 web_search 工具启用时，在系统提示词中注入 `## 网络搜索规则` 区块。

**配置**：`[tools]` 区块控制启用/停用。

### 3.5 MCP 系统

**数据结构**：
```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct McpServerConfig {
    pub id: String,
    pub name: String,
    pub transport: McpTransport,   // Sse | Http
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub enabled: bool,
    pub auto_execute: bool,        // true=服务端自动执行并回传结果；false=作为用户工具返回客户端（标准 OpenAI tool_calls 流程）
    pub timeouts: McpTimeouts,     // 连接/请求/发现超时
    pub result_bytes_limit: usize,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}
```

**传输协议**：仅支持 SSE 和 Streamable HTTP（服务端无浏览器 Native Host，不支持 stdio）。

**工具发现**：连接 MCP 服务，调用 `tools/list`，缓存工具列表。

**注入**：启用的 MCP 服务的工具作为直接 XML 标签注入系统提示词的 Available Tools 区块。

**执行**：Agent 循环中检测到 MCP 工具标签时，通过 MCP 协议调用 `tools/call`，结果回传。

**Admin API**：
- `GET /admin/api/mcp/servers` — 列出
- `POST /admin/api/mcp/servers` — 新增
- `PUT /admin/api/mcp/servers/{id}` — 更新
- `DELETE /admin/api/mcp/servers/{id}` — 删除
- `POST /admin/api/mcp/servers/{id}/test` — 测试连接
- `POST /admin/api/mcp/servers/{id}/refresh` — 刷新工具列表
- `GET /admin/api/mcp/servers/{id}/tools` — 获取发现工具
- `GET /admin/api/mcp/servers/{id}/history` — 调用历史

### 3.6 项目系统

**数据结构**：
```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub description: String,
    pub instructions: String,
    pub memories: Vec<Memory>,     // 项目专属记忆
    pub created_at: i64,
    pub updated_at: i64,
}
```

**关联机制**：web2api 无浏览器会话概念，通过以下方式关联项目（按优先级）：
1. 请求头 `x-ds-project: {project_id}`（优先）
2. 请求体扩展字段 `project_id`（回退）
3. 均未提供时不关联项目

**注入**：项目指令 + 项目记忆注入到系统提示词的 `## 项目上下文` 区块。

**Admin API**：
- `GET /admin/api/projects` — 列出
- `POST /admin/api/projects` — 新增
- `PUT /admin/api/projects/{id}` — 更新
- `DELETE /admin/api/projects/{id}` — 删除
- `GET /admin/api/projects/{id}/memories` — 项目记忆
- `POST /admin/api/projects/{id}/memories` — 新增项目记忆
- `PUT /admin/api/projects/{id}/memories/{mid}` — 更新
- `DELETE /admin/api/projects/{id}/memories/{mid}` — 删除

### 3.7 保存项

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

### 3.8 自动化任务

**数据结构**：
```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct AutomationTask {
    pub id: String,
    pub name: String,
    pub prompt: String,
    pub model: String,             // 使用的模型
    pub trigger: AutomationTrigger, // Manual | Cron(String)
    pub timezone: String,
    pub search_enabled: bool,
    pub thinking_enabled: bool,
    pub enabled: bool,
    pub last_run_at: Option<i64>,
    pub last_status: Option<AutomationStatus>,
    pub next_run_at: Option<i64>,
    pub created_at: i64,
}
```

**调度**：后台 tokio 任务，使用 `tokio-cron-scheduler` crate。最小间隔 15 分钟。

**执行**：触发时创建独立 chat completion 请求（通过内部 `OpenAIAdapter::try_chat`），复用 Agent 循环链路。结果记录到任务历史。

**Admin API**：
- `GET /admin/api/automations` — 列出
- `POST /admin/api/automations` — 新增
- `PUT /admin/api/automations/{id}` — 更新
- `DELETE /admin/api/automations/{id}` — 删除
- `POST /admin/api/automations/{id}/run` — 立即运行
- `PUT /admin/api/automations/{id}/pause` — 暂停
- `PUT /admin/api/automations/{id}/resume` — 恢复
- `GET /admin/api/automations/{id}/history` — 运行历史

### 3.9 对话导出

**实现**：新增导出端点，将 chat completion 请求历史导出为 HTML/Markdown/JSON。

**Admin API**：
- `POST /admin/api/export/conversation` — 导出指定对话（传入 messages 数组，返回文件）
  - 参数：`format` (html|markdown|json), `messages`, `readable` (bool)
- `GET /admin/api/export/saved-items` — 导出保存项

### 3.10 可下载产物

**工具**：`artifact_create`（单文件）、`artifact_bundle`（ZIP 多文件）

**实现**：
- `artifact_create`：将文件内容写入 `{DS_DATA_DIR}/artifacts/{id}`，记录元数据
- `artifact_bundle`：用 `zip` crate 打包多文件

**获取**：
- `GET /admin/api/artifacts` — 列出
- `GET /admin/api/artifacts/{id}` — 下载文件
- `DELETE /admin/api/artifacts/{id}` — 删除

**注入**：artifact 工具作为内置工具注入系统提示词。

### 3.11 i18n 提示词

**实现**：系统提示词模板支持中文/英文，通过 `[deepseek_pp] locale` 配置。

**模板**：移植自 deepseek-pp `i18n/resources/zh-CN.ts` 和 `en.ts` 的 `prompt.systemChat`、`prompt.systemThinking`、`prompt.webSearchGuidance`、`prompt.toolFormatReminder` 等字段。

**默认**：中文（与现有项目一致）。

### 3.12 提示词控制

**配置**（`[deepseek_pp]` 区块）：
```toml
[deepseek_pp]
enabled = true
locale = "zh"                    # zh | en
memory_enabled = true
system_prompt_enabled = true
preset_cadence = "first"         # default | first | every | off
force_response_language = "auto" # auto | zh | en
max_agent_steps = 10
agent_step_interval_ms = 1000

[deepseek_pp.tools]
web_search = true
web_fetch = true
artifact = true
skill_creator = true
memory_import = true
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
max_agent_steps = 10
agent_step_interval_ms = 1000

[deepseek_pp.tools]
web_search = true
web_fetch = true
artifact = true
skill_creator = true
memory_import = true

[[deepseek_pp.mcp_servers]]
id = "example"
name = "示例 MCP"
transport = "sse"
url = "http://localhost:3001/sse"
enabled = false
auto_execute = true
```

### 4.2 config.example.toml 更新

在 `config.example.toml` 中添加 `[deepseek_pp]` 完整示例和注释。

## 5. Admin API 端点汇总

| 方法 | 路径 | 功能 |
|------|------|------|
| GET | `/admin/api/deepseek-pp/status` | 功能总览状态 |
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
| GET/POST | `/admin/api/mcp/servers` | MCP 列表/新增 |
| PUT/DELETE | `/admin/api/mcp/servers/{id}` | 更新/删除 |
| POST | `/admin/api/mcp/servers/{id}/test` | 测试连接 |
| POST | `/admin/api/mcp/servers/{id}/refresh` | 刷新工具 |
| GET | `/admin/api/mcp/servers/{id}/tools` | 工具列表 |
| GET/POST | `/admin/api/automations` | 自动化列表/新增 |
| PUT/DELETE | `/admin/api/automations/{id}` | 更新/删除 |
| POST | `/admin/api/automations/{id}/run` | 立即运行 |
| PUT | `/admin/api/automations/{id}/pause` | 暂停 |
| PUT | `/admin/api/automations/{id}/resume` | 恢复 |
| GET | `/admin/api/automations/{id}/history` | 运行历史 |
| GET | `/admin/api/artifacts` | 产物列表 |
| GET/DELETE | `/admin/api/artifacts/{id}` | 下载/删除 |
| POST | `/admin/api/export/conversation` | 导出对话 |
| GET/PUT | `/admin/api/deepseek-pp/settings` | 提示词控制设置 |

## 6. 前端页面

扩展现有 admin 面板（`web/src/pages/`），新增以下页面：

| 页面 | 路由 | 功能 |
|------|------|------|
| MemoryPage | `/memory` | 记忆管理（列表/筛选/编辑/置顶/导入导出） |
| SkillPage | `/skills` | Skill 管理（内置/自定义/启用控制） |
| PresetPage | `/presets` | 系统提示词预设管理 |
| ProjectPage | `/projects` | 项目管理（指令/记忆） |
| SavedPage | `/saved` | 保存项管理（搜索/标签/导出） |
| McpPage | `/mcp` | MCP 服务管理（连接/测试/工具） |
| AutomationPage | `/automations` | 自动化任务管理（创建/调度/历史） |
| ArtifactPage | `/artifacts` | 产物列表/下载 |
| PromptSettingsPage | `/settings/prompt` | 提示词控制（记忆/预设/语言开关） |

Layout 导航更新：在现有侧边栏添加「Agent 工作台」分组。

## 7. Rust 模块结构

```
src/
├── deepseek_pp/              # 新：deepseek-pp 功能模块
│   ├── mod.rs                # facade: re-exports
│   ├── augmentation.rs       # prompt 增强（记忆+skill+预设+项目+工具注入），由 openai_adapter/request/prompt.rs 调用
│   ├── agent_loop.rs         # Agent 循环（工具执行+结果回传+继续生成）
│   ├── builtin_tools.rs      # 内置工具注册表和执行器
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
│   ├── mcp/
│   │   ├── mod.rs            # facade
│   │   ├── client.rs         # MCP 客户端（SSE/HTTP）
│   │   ├── store.rs          # MCP 服务配置存储
│   │   └── executor.rs       # MCP 工具执行
│   ├── web_tools/
│   │   ├── search.rs         # Bing 搜索
│   │   └── fetch.rs          # 网页抓取
│   ├── artifact/
│   │   └── store.rs          # 产物存储
│   └── automation/
│       ├── mod.rs            # facade
│       ├── scheduler.rs      # cron 调度器
│       └── runner.rs         # 任务执行器
├── openai_adapter/
│   ├── request/
│   │   └── augmentation.rs   # 新管线阶段：调用 deepseek_pp::augmentation::apply()
│   └── response/
│       └── builtin_tool_stream.rs  # 内置工具 XML 检测流（调用 deepseek_pp::agent_loop）
├── server/
│   ├── admin.rs              # 扩展：新增所有 admin API 端点
│   └── store.rs              # 扩展：新增 JSON 存储管理
└── config.rs                 # 扩展：DeepSeekPpConfig 结构
```

**调用关系**：`augmentation` 作为 `openai_adapter/request/` 管线中的新阶段（位于 `files` 和 `prompt` 之间），调用 `deepseek_pp::augmentation::apply()` 注入增强内容到 `ChatCompletionsRequest`；`openai_adapter/response/builtin_tool_stream.rs` 检测到内置工具标签后调用 `deepseek_pp::agent_loop::execute_tool()` 并触发新一轮生成。

## 8. 实施阶段

虽然本 spec 覆盖所有子系统，实施按以下顺序分阶段进行，每阶段可独立验收：

### 阶段 1：Agent 循环框架 + 记忆系统
- `deepseek_pp/` 模块骨架
- 内置工具注册表和执行框架
- `BuiltinToolStream`（响应流中检测 XML 标签）
- Agent 循环（工具执行 → 结果回传 → 继续生成）
- 记忆存储 + 选择器 + 注入 + memory_save/update/delete 工具
- 系统提示词模板（i18n）
- Admin API：记忆 CRUD
- 前端：MemoryPage

### 阶段 2：Skill + 预设系统
- Skill 注册表 + 解析器 + 内置 skill
- 预设存储 + 注入
- Admin API + 前端页面

### 阶段 3：Web 工具
- web_search（Bing 搜索）
- web_fetch（网页抓取）
- Admin API：工具开关
- 前端：工具设置

### 阶段 4：MCP 系统
- MCP 客户端（SSE/HTTP）
- 工具发现 + 注入 + 执行
- Admin API + 前端：McpPage

### 阶段 5：项目 + 保存项
- 项目存储 + 上下文注入
- 保存项存储
- Admin API + 前端页面

### 阶段 6：自动化 + 导出 + 产物
- 自动化调度器 + 任务执行器
- 对话导出端点
- artifact 工具 + 存储
- Admin API + 前端页面

## 9. 依赖新增

| Crate | 用途 |
|-------|------|
| `zip` | artifact_bundle 打包 |
| `tokio-cron-scheduler` | 自动化任务调度 |
| `scraper` | Bing 搜索结果 HTML 解析（或用正则） |
| `jieba-rs` | 中文分词（记忆选择器，可选） |

`wreq` 已用于 HTTP 客户端，web_search/web_fetch 复用。

## 10. 风险与缓解

| 风险 | 缓解 |
|------|------|
| Agent 循环增加延迟 | 最大步数限制 + 步间间隔；流式输出保持连接 |
| 内置工具 XML 误解析 | 严格标签名匹配（仅注册的工具名）；滑动窗口检测 |
| 记忆无限增长 | Token 预算限制注入量；定期清理提示 |
| MCP 服务不可用 | 超时 + 降级（跳过不可用工具） |
| 自动化任务并发冲突 | 每任务独立会话；调度器互斥锁 |
| Bing 搜索被封锁 | 多域名轮询 + User-Agent 伪装；可配置代理 |
