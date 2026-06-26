# ds_core

DeepSeek API 客户端库。封装了与 DeepSeek 后端交互的完整流程：登录鉴权、账号池管理、PoW 计算、会话管理、文件上传、SSE 流解析和超限回退策略。

## 职责边界

ds_core 是一个**独立的库 crate**，不依赖主 crate 的任何类型：

- **不依赖** HTTP 框架（axum/warp 等）
- **不感知** OpenAI / Anthropic 协议
- **不涉及** API 密钥鉴权、请求统计、管理后台
- 所有配置通过 `DsCoreConfig` / `AccountConfig` 注入

## 模块架构

```
ds_core/src/
├── lib.rs           ── 公开 API 重导出 + DsCore/CoreError
├── accounts.rs      ── Accounts 门面，整合 client/pool/solver
├── accounts/
│   ├── client.rs    ── DsClient：DeepSeek REST 客户端
│   ├── pool.rs      ── AccountPool：账号池、状态管理、恢复
│   └── pow.rs       ── PowSolver：WASM PoW 求解器
├── chat.rs          ── Chat 门面，按 prompt 大小分流
├── chat/
│   ├── request.rs   ── 三种请求路径（正常/历史文件/分块写入）
│   └── response.rs  ── ResponseStream：SSE 解析 + 精简事件协议
└── config.rs        ── DsCoreConfig / AccountConfig
```

## 核心流程

```
主 crate
  │  DsCore::v0_chat(ChatRequest) → Result<ChatResponse, CoreError>
  ▼
DsCore
  │  转发到 Chat::v0_chat()
  ▼
Chat
  │  判断 prompt 是否超限
  ├── 不超限 → v0_chat_once()：获取账号 → 创建 session → 上传文件 → PoW → completion
  ├── 超限 default → v0_chat_oversized_file()：拆分历史为文件上传 + 内联最新轮
  └── 超限 expert → v0_chat_oversized_chunk()：分块写入 session + 末块正常 completion
  ▼
ResponseStream
  │  StreamEvent 流（Meta, ThinkStart, ThinkDelta, ContentStart, ContentDelta, Done）
  ▼
主 crate 消费 StreamEvent 流
```

## 核心类型

### `DsCore` — 统一入口

```rust
pub struct DsCore { /* accounts + chat */ }

impl DsCore {
    // 创建实例（初始化账号池、加载 WASM、健康检查）
    pub async fn new(config: &DsCoreConfig, account_creds: Vec<AccountConfig>)
        -> Result<Self, CoreError>;

    // 发起对话，返回精简协议事件流
    pub async fn v0_chat(&self, req: ChatRequest, request_id: &str)
        -> Result<ChatResponse, CoreError>;

    // 查询账号状态
    pub fn account_statuses(&self) -> Vec<AccountStatus>;

    // 动态增删账号
    pub async fn add_account(&self, creds: &AccountConfig) -> Result<String, PoolError>;
    pub async fn remove_account(&self, email_or_mobile: &str) -> Result<String, PoolError>;

    // 重新登录
    pub async fn re_login_single(&self, email_or_mobile: &str) -> Result<(), String>;

    // 优雅关闭
    pub async fn shutdown(&self);

    // 热重载配置
    pub async fn reload_config(&self, config: &DsCoreConfig) -> Result<(), CoreError>;
}
```

### `ChatRequest` — 对话请求

```rust
pub struct ChatRequest {
    pub prompt: String,           // 对话 prompt（透传给 DeepSeek 后端）
    pub thinking_enabled: bool,   // 是否启用思考
    pub search_enabled: bool,     // 是否启用联网搜索
    pub model_type: String,       // 模型类型：如 "default" / "expert"
    pub files: Vec<FilePayload>,  // 待上传文件
}
```

`prompt` 使用 DeepSeek 原生标签格式：
```
<｜User｜>你好<｜Assistant｜>
```

### `ChatResponse` — 对话响应

```rust
pub struct ChatResponse {
    pub stream: Pin<Box<dyn Stream<Item = Result<StreamEvent, CoreError>> + Send>>,
}
```

### `StreamEvent` — 精简响应协议

`StreamEvent` 将 DeepSeek 底层复杂的 p/o/v patch 协议抽象为 6 种结构化事件。

| 事件 | 含义 |
|------|------|
| `Meta { account_id }` | 流起始，携带使用的账号 ID |
| `ThinkStart` | 模型开始思考 |
| `ThinkDelta { content }` | 思考内容的增量片段 |
| `ContentStart` | 模型开始输出最终内容 |
| `ContentDelta { content }` | 最终内容的增量片段 |
| `Done { finish_reason, accumulated_token_usage }` | 流结束。`finish_reason = Some("stop")` 正常完成，`None` 异常终止；`accumulated_token_usage` 携带累计 token 消耗 |

**事件序列保证**：
```
Meta → (ThinkStart → ThinkDelta* → ContentStart → ContentDelta* | ContentStart → ContentDelta*) → Done
```
即：`ThinkStart` 后紧跟 0 或多个 `ThinkDelta`；`ContentStart` 后紧跟 0 或多个 `ContentDelta`；思考阶段和内容阶段最多各出现一次。

### `CoreError` — 错误类型

```rust
pub enum CoreError {
    Overloaded,                           // 无可用账号
    ProofOfWorkFailed(PowError),          // PoW 计算失败
    ProviderError(String),                // 提供商错误（网络、业务等）
    Stream(String),                       // 流处理错误
}
```

### `AccountStatus` — 账号状态

```rust
pub struct AccountStatus {
    pub email: String,
    pub mobile: String,
    pub state: String,         // "idle" / "busy" / "error" / "invalid"
    pub last_released_ms: i64,
    pub error_count: u8,
}
```

## 配置类型

```rust
pub struct DsCoreConfig {
    pub api_base: String,              // DeepSeek API 基础 URL
    pub wasm_url: String,              // PoW WASM 文件 URL
    pub user_agent: String,            // 浏览器 UA
    pub client_version: String,        // X-Client-Version
    pub client_platform: String,       // X-Client-Platform
    pub client_locale: String,         // X-Client-Locale
    pub proxy_url: Option<String>,     // 代理 URL（非 US IP 绕过 WAF）
    pub model_types: Vec<String>,      // 模型类型列表
    pub input_character_limits: Vec<u32>, // 每种模型的字符数限制
}

pub struct AccountConfig {
    pub email: String,
    pub mobile: String,
    pub area_code: String,
    pub password: String,
}
```

## 账号池模型

- 1 账号 = 1 并发。多并发需要多个账号。
- `AccountGuard` 通过 `Drop` 自动释放账号，保证不泄漏。
- 账号初始化：login → create_session → health_check → update_title
- 失败账号标记为 `Invalid`（仍入池以便前台展示）。
- 后台恢复任务每 60 秒扫描 `Error` 账号并尝试重新登录。
- 连续 3 次登录失败标记为 `Invalid`。

## 请求分流策略

Chat 模块根据 prompt 字符数是否超过模型限制的 75% 决定路由：

| 条件 | 路径 | 说明 |
|------|------|------|
| prompt 不超限 | `v0_chat_once` | 直发 completion |
| 超限 + `model_type=expert` | `v0_chat_oversized_chunk` | 分块写入 session，末块正常 completion |
| 超限 + 其他 model_type | `v0_chat_oversized_file` | 拆分历史为文件上传 |

**历史拆分逻辑**：找到最后一个 `<｜Assistant｜>` 块之前的全部对话，包装为文件内容标记格式（`[file content end]`/`[文档结束]`/`[content end]` 等变体随机轮换，反代加固）上传为**随机文件名** txt（`notes-xxxxxx.txt` 等）；最后一个 assistant 块 + 最新的 user 转内联发送。

## 文件上传

- 主 crate 通过 `FilePayload` 传入文件数据。
- 上传过程：`upload_file(multipart)` → `fetch_files` 轮询至 SUCCESS（最多 30 次，2s 间隔）。
- 同时需要计算针对 `/api/v0/file/upload_file` 的 PoW。

## PoW 流程

1. `create_pow_challenge(target_path)` 获取 challenge / salt / difficulty
2. `PowSolver::solve(&challenge)` WASM 计算答案
3. `PowResult::to_header()` → base64 编码
4. 放入 `X-Ds-Pow-Response` header

## 错误处理链

```
DsClient  HTTP 层错误 ─┐
          业务错误 ────┤
          JSON 错误 ───┤
          WAF 拦截 ────┤
                      ▼
               ClientError
                  │
                  ▼
              PoolError        ── 账号池错误
              PowError         ── PoW 计算错误
                  │
                  ▼
              CoreError        ── 统一错误类型
                  │
                  ▼
          主 crate 根据协议转换
```
