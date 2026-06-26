# ds_core

DeepSeek API client library. Encapsulates the full interaction flow with the DeepSeek backend: authentication, account pool management, Proof-of-Work computation, session management, file upload, SSE stream parsing, and oversized prompt fallback strategies.

## Responsibility Boundary

ds_core is a **standalone library crate** with no dependency on the main crate:

- **No dependency** on HTTP frameworks (axum/warp etc.)
- **No awareness** of OpenAI / Anthropic protocols
- **No involvement** in API key auth, request statistics, or admin panel
- All configuration injected via `DsCoreConfig` / `AccountConfig`

## Module Architecture

```
ds_core/src/
├── lib.rs           ── Public API re-exports + DsCore/CoreError
├── accounts.rs      ── Accounts facade, integrates client/pool/solver
├── accounts/
│   ├── client.rs    ── DsClient: DeepSeek REST client
│   ├── pool.rs      ── AccountPool: pool, state management, recovery
│   └── pow.rs       ── PowSolver: WASM PoW solver
├── chat.rs          ── Chat facade, prompt-size dispatch
├── chat/
│   ├── request.rs   ── Three request paths (normal/file-upload/chunked)
│   └── response.rs  ── ResponseStream: SSE parsing + streamlined event protocol
└── config.rs        ── DsCoreConfig / AccountConfig
```

## Core Flow

```
Main crate
  │  DsCore::v0_chat(ChatRequest) → Result<ChatResponse, CoreError>
  ▼
DsCore
  │  Forward to Chat::v0_chat()
  ▼
Chat
  │  Check if prompt exceeds limit
  ├── Normal → v0_chat_once(): get account → create session → upload files → PoW → completion
  ├── Oversized default → v0_chat_oversized_file(): split history as file upload + inline latest turn
  └── Oversized expert → v0_chat_oversized_chunk(): write chunks to session + last chunk normal completion
  ▼
ResponseStream
  │  StreamEvent stream (Meta, ThinkStart, ThinkDelta, ContentStart, ContentDelta, Done)
  ▼
Main crate consumes StreamEvent stream
```

## Core Types

### `DsCore` — Unified Entry Point

```rust
pub struct DsCore { /* accounts + chat */ }

impl DsCore {
    pub async fn new(config: &DsCoreConfig, account_creds: Vec<AccountConfig>)
        -> Result<Self, CoreError>;

    pub async fn v0_chat(&self, req: ChatRequest, request_id: &str)
        -> Result<ChatResponse, CoreError>;

    pub fn account_statuses(&self) -> Vec<AccountStatus>;

    pub async fn add_account(&self, creds: &AccountConfig) -> Result<String, PoolError>;
    pub async fn remove_account(&self, email_or_mobile: &str) -> Result<String, PoolError>;
    pub async fn re_login_single(&self, email_or_mobile: &str) -> Result<(), String>;
    pub async fn shutdown(&self);
    pub async fn reload_config(&self, config: &DsCoreConfig) -> Result<(), CoreError>;
}
```

### `ChatRequest`

```rust
pub struct ChatRequest {
    pub prompt: String,           // Chat prompt (passed through to DeepSeek backend)
    pub thinking_enabled: bool,
    pub search_enabled: bool,
    pub model_type: String,       // e.g. "default" / "expert"
    pub files: Vec<FilePayload>,  // files to upload
}
```


### `ChatResponse`

```rust
pub struct ChatResponse {
    pub stream: Pin<Box<dyn Stream<Item = Result<StreamEvent, CoreError>> + Send>>;
}
```

### `StreamEvent` — Streamlined Response Protocol

`StreamEvent` abstracts DeepSeek's complex p/o/v patch protocol into 6 structured event types:

| Event | Description |
|-------|-------------|
| `Meta { account_id }` | Stream start, carries the account ID in use |
| `ThinkStart` | Model starts thinking |
| `ThinkDelta { content }` | Incremental thinking content fragment |
| `ContentStart` | Model starts producing final content |
| `ContentDelta { content }` | Incremental final content fragment |
| `Done { finish_reason, accumulated_token_usage }` | Stream end. `Some("stop")` = normal, `None` = abnormal; `accumulated_token_usage` carries cumulative token count |

**Event sequence guarantees**:
```
Meta → (ThinkStart → ThinkDelta* → ContentStart → ContentDelta* | ContentStart → ContentDelta*) → Done
```

### `CoreError`

```rust
pub enum CoreError {
    Overloaded,                           // No available accounts
    ProofOfWorkFailed(PowError),          // PoW computation failed
    ProviderError(String),                // Provider error (network, business, etc.)
    Stream(String),                       // Stream processing error
}
```

### `AccountStatus`

```rust
pub struct AccountStatus {
    pub email: String,
    pub mobile: String,
    pub state: String,         // "idle" / "busy" / "error" / "invalid"
    pub last_released_ms: i64,
    pub error_count: u8,
}
```

## Configuration Types

```rust
pub struct DsCoreConfig {
    pub api_base: String,              // DeepSeek API base URL
    pub wasm_url: String,              // PoW WASM file URL
    pub user_agent: String,
    pub client_version: String,        // X-Client-Version
    pub client_platform: String,       // X-Client-Platform
    pub client_locale: String,         // X-Client-Locale
    pub proxy_url: Option<String>,     // Proxy URL (non-US IP to bypass WAF)
    pub model_types: Vec<String>,      // Model type list
    pub input_character_limits: Vec<u32>, // Character limit per model type
}

pub struct AccountConfig {
    pub email: String,
    pub mobile: String,
    pub area_code: String,
    pub password: String,
}
```

## Account Pool Model

- 1 account = 1 concurrency. Scale with more accounts.
- `AccountGuard` auto-releases on `Drop` — no leak.
- Account init: login → create_session → health_check → update_title
- Failed accounts are marked `Invalid` (still tracked for display).
- Background recovery task scans `Error` accounts every 60s and retries login.
- 3 consecutive login failures → `Invalid`.

## Request Dispatch Strategy

Chat routes based on whether prompt character count exceeds 75% of the model limit:

| Condition | Path | Description |
|-----------|------|-------------|
| Prompt within limit | `v0_chat_once` | Direct completion |
| Oversized + `model_type=expert` | `v0_chat_oversized_chunk` | Chunked session write, last chunk normal |
| Oversized + other model_type | `v0_chat_oversized_file` | Split history as file upload |

**History split logic**: Dialogue before the last `<｜Assistant｜>` block is wrapped with file-content markers (`[file content end]`/`[文档结束]`/`[content end]` variants randomly rotated for anti-detection) and uploaded as a **randomly-named** txt file (e.g. `notes-xxxxxx.txt`); the last assistant block + latest user message are sent inline.

## Error Handling Chain

```
DsClient  HTTP errors ──┐
          Business errors ─┤
          JSON errors ────┤
          WAF block ──────┤
                         ▼
                  ClientError
                     │
                     ▼
                 PoolError        ── Pool errors
                 PowError         ── PoW errors
                     │
                     ▼
                 CoreError        ── Unified error type
                     │
                     ▼
          Main crate translates per protocol
```
