# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> This file serves dual duty as both `AGENTS.md` (the real file) and `CLAUDE.md` (symlink → `AGENTS.md`).
> Edit `AGENTS.md` directly; `CLAUDE.md` stays in sync automatically.

---

## Project Overview

Rust API proxy exposing free DeepSeek model endpoints. Translates standard OpenAI-compatible and Anthropic-compatible requests to DeepSeek's internal protocol with account pool rotation, PoW challenge handling, and streaming response support.

**Runtime:** Rust **1.95.0** (pinned in `rust-toolchain.toml`) with **edition 2024**.
**Workspace:** Cargo workspace with two crates — `ds-free-api` (binary + server + adapters) and `ds_core` (DeepSeek API client library).
**Build prerequisites:** `cmake`, `g++`, `libclang-dev` — required to compile `wreq` (BoringSSL).

**Key dependencies and why they exist:**
- `wasmtime` — executes DeepSeek's PoW WASM solver; the entire PoW system depends on this
- `tiktoken-rs` — client-side prompt token counting (DeepSeek returns 0 for `prompt_tokens`)
- `pin-project-lite` — underpins every streaming response wrapper (`ConverterStream`, `ToolCallStream`, `RepairStream`, `StopDetectStream`)
- `axum` / `wreq` — HTTP server and client respectively; `wreq` uses BoringSSL with Chrome 136 TLS fingerprint for WAF bypass
- `tokio` with `signal` feature — async runtime with graceful shutdown on SIGTERM/SIGINT

---

## Architecture

### Module Structure

The project is a **Cargo workspace** with two crates:

**`ds_core/`** — standalone library crate for DeepSeek API interaction:
```
ds_core/src/
├── lib.rs           # Public API: re-exports DsCore, CoreError, etc.
├── accounts.rs      # Facade: Accounts struct, wraps pool/client/solver
├── accounts/        # Account sub-modules
│   ├── client.rs    # Raw HTTP client: API endpoints, Envelope parsing
│   ├── pool.rs      # Account pool: init, selection, AccountGuard
│   └── pow.rs       # PoW solver: wasmtime WASM loader, DeepSeekHashV1
├── chat.rs          # Facade: Chat struct, prompt-size dispatch
├── chat/            # Chat sub-modules
│   ├── request.rs   # Chat orchestration: 3 request paths (normal/file/chunk)
│   └── response.rs  # SSE parsing, StreamEvent protocol, GuardedStream
└── config.rs        # DsCoreConfig and AccountConfig types
```

**`ds-free-api`** (root crate) — binary + server + protocol adapters:
```
src/
├── main.rs              # Binary entry (~10 lines): init runtime_log, parse CLI, run server
├── lib.rs               # Public API surface: re-exports all public types
├── config.rs            # Config load/save, Arc<RwLock<Config>>
│
├── openai_adapter/      # OpenAI protocol adapter
│   ├── openai_adapter.rs # Facade: OpenAIAdapter, OpenAIAdapterError, StreamResponse
│   ├── types.rs         # Request/response structs (ChatCompletionsRequest, etc.)
│   ├── models.rs        # Model registry and listing endpoints
│   ├── request.rs       # Facade for request submodules (also contains #[cfg(test)] tests)
│   ├── request/         # Request pipeline: normalize → tools → files → prompt → resolver
│   │   ├── normalize.rs # Validation, default params
│   │   ├── tools.rs     # Tool definition → prompt injection
│   │   ├── files.rs     # Data URL → FilePayload, HTTP URL → search mode
│   │   ├── prompt.rs    # ChatML → DeepSeek native tags, tool injection
│   │   └── resolver.rs  # Model resolution, capability toggles
│   ├── response.rs      # Facade + StreamCfg struct
│   └── response/        # Response pipeline: converter → tool_parser → repair → stop_detect
│       ├── converter.rs # ConverterStream: StreamEvent → ChatCompletionsResponseChunk
│       └── tool_parser.rs   # ToolCallStream: XML tag detection, sliding-window repair
│
├── anthropic_compat.rs  # Facade: Anthropic protocol translator (on top of openai_adapter)
├── anthropic_compat/    # Anthropic compat submodules
│   ├── types.rs         # MessagesRequest/Response structs
│   ├── models.rs        # Anthropic-format model list generation
│   ├── request.rs       # Anthropic JSON → OpenAI request mapping
│   ├── response.rs      # Facade for response submodules
│   └── response/
│       ├── stream.rs    # OpenAI SSE → Anthropic SSE events
│       └── aggregate.rs # OpenAI JSON → Anthropic JSON
│
├── server.rs            # Facade: router, auth middleware, graceful shutdown
├── server/              # HTTP server submodules
│   ├── admin.rs         # Admin panel route handlers
│   ├── auth.rs          # JWT sign/verify, password setup/login, rate limiter
│   ├── error.rs         # ServerError: API error JSON responses
│   ├── handlers.rs      # Business route handlers (OpenAI + Anthropic)
│   ├── runtime_log.rs   # File log redirection (stdout → runtime.log)
│   ├── stats.rs         # Request stats recording
│   ├── store.rs         # StoreManager: delegates admin/keys to Config::save()
│   └── stream.rs        # SseBody: wraps StreamResponse → axum::body::Body
```
**Additional resources:**
- `config.example.toml` — authoritative configuration reference with all fields documented
- `examples/adapter_cli.rs` + `examples/adapter_cli/` — debug CLI + JSON request samples
- `py-e2e-tests/` — Python e2e test suite (uv-managed, JSON-driven scenarios)
- `docker/Dockerfile` + `docker/docker-compose.yaml` — Docker deployment (ghcr.io image)
- `docs/` — `code-style.md`（代码注释、命名、错误消息约定），`logging-spec.md`（日志级别、target、模块级过滤），`deepseek-prompt-injection.md`（DeepSeek 原生标签、工具调用注入策略），`development.md`（环境配置、首次启动、Release 构建）
- `ds_core/raw-api-reference.md` — DeepSeek 后端 API 参考（端点、信封格式、SSE 增量协议、PoW、WAF 绕过）

### Binary / Library Split

`main.rs` is a ~10-line wrapper: init `runtime_log`, read `DS_DATA_DIR`, parse CLI args via `Config::load_with_args()` → `(Config, PathBuf)`, call `server::run(config, config_path)`. The crate can be built both as a library (`cargo build --lib`) and a binary (`cargo build --bin ds-free-api`). `lib.rs` defines the full public API surface.

### Workspace Crates

The workspace has two crates. Run `cargo` commands from the **workspace root**:

- **Root crate** (`ds-free-api`): binary, server, adapters — `cargo build`, `cargo test`
- **`ds_core/`**: standalone DeepSeek library — `cargo build -p ds_core`, `cargo test -p ds_core`

Cargo resolves from the workspace root, so commands run from inside `ds_core/` won't work. The `just` commands in this file also assume the workspace root.

### Facade Module Pattern

`accounts.rs`, `chat.rs`, `openai_adapter.rs`, `server.rs`, `request.rs`, `response.rs`, `anthropic_compat.rs` are **facades**:
- They declare submodules with `mod` (keeping implementation private)
- They re-export only the minimal public interface via `pub use`
- They sometimes contain `#[cfg(test)]` test modules

This means the file tree does not directly map to the public API. To understand what a module exposes externally, read its facade file, not the directory listing.

### StreamResponse Type

`StreamResponse` is the unifying bridge between adapter layers and the HTTP server:
- Every adapter's streaming method returns `StreamResponse` (a boxed `Stream<Item = Result<Bytes>> + Send`)
- `server/stream.rs::SseBody` wraps `StreamResponse` and converts it into an `axum::body::Body`
- This decouples the adapters from the HTTP framework — they produce bytes, the server handles SSE framing


### CI Build Pipeline

On tag push (`.github/workflows/release.yml`):

```
build-frontend (bun install --frozen-lockfile + bun run build)
  ├── build-linux-gnu (cargo build)    │
  ├── build-linux-musl (cross/cargo)   │── release (tar.gz + zip)
  ├── build-macos (cargo build)  │
  └── build-windows (cargo build)│
  └── docker (ghcr.io image)
```

`build-frontend` produces a `web-dist` artifact. Each platform build job downloads it
before compiling Rust, so `rust_embed` embeds the real frontend assets.

### Frontend (`web/`)

Vite + React + shadcn/ui SPA under `web/`. Built by `bun run build` in `web/` (typecheck: `bun run typecheck`).
The binary embeds `web/dist/` via `rust_embed` at compile time.

```
web/
├── src/
│   ├── App.tsx            # Routes (login + protected layout + pages)
│   ├── lib/               # Shared libraries
│   │   ├── api.ts         # Typed API client for all admin endpoints
│   │   ├── auth-context.ts # Auth context provider
│   │   ├── auth.tsx       # JWT auth context (localStorage token)
│   │   ├── use-auth.ts    # Auth hook for components
│   │   └── utils.ts       # Utility functions
│   ├── pages/             # ConfigPage, DashboardPage, Layout, LoginPage, LogsPage, ModelsPage
│   └── components/ui/     # shadcn/ui primitives (badge, button, card, input, etc.)
├── public/favicon.svg     # → symlink to assets/logo.svg
├── index.html
├── package.json
└── vite.config.ts
```

The frontend includes i18n support (`web/src/i18n/`, `web/src/locales/zh/`, `web/src/locales/en/`)
and a language switcher component (`web/src/components/LanguageSwitcher.tsx`).
It also has a theme switcher (system/light/dark) in the layout.
Library files include `api.ts`, `auth.tsx`, `auth-context.ts`, `use-auth.ts`, and `utils.ts`.
Pages: `ConfigPage`, `DashboardPage`, `Layout`, `LoginPage`, `LogsPage`, `ModelsPage`.

**Admin panel config editor**: `ConfigPage.tsx` fetches from `GET /admin/api/config`,
edits all sections (accounts, api_keys, server, deepseek, models, proxy, tool_call tags),
submits via `PUT /admin/api/config` (full replace + hot-reload). Passwords/key values
sent as `***`/empty are merged with existing values server-side.

**Dev mode (HMR)**: Run `cd web && bun run dev` (Vite HMR) alongside `just serve`.
Backend reads from `web/dist/` filesystem when available.
---

## Principles

### 1. Single Responsibility
Every module has one job. Cross-module boundaries are strict:
- `config.rs`: Configuration load & save only, no client creation or business logic
- `client.rs`: Raw HTTP calls only, no token caching, retry, or SSE parsing
- `accounts.rs`: Account pool management only, no network requests
- `pow.rs`: WASM computation only, no account management or request sending
- `anthropic_compat.rs`: Protocol translation only, no direct `ds_core` access

### 2. Minimal Viable
- No premature abstractions: Extract traits/structs when needed, not before
- No redundant code: Remove unused imports, avoid over-documenting, no pre-written tests
- Delay dependency introduction: only add deps when actually needed

### 3. Control Complexity
- Explicit over implicit: Dependencies injected via parameters, no global state
- Composition over inheritance: Small modules composed via functions, no deep inheritance
- Clear boundaries: Modules interact via explicit interfaces, no internal logic leakage

---

## Key Architectural Patterns

### Account Pool Model

1 account = 1 session = 1 concurrency. Scale via more accounts in `config.toml`.

`AccountGuard` wraps `Arc<Account>`. It marks account as `busy` (via `AtomicBool`) on creation and releases on `Drop`. Held in `GuardedStream` to keep account busy during streaming.

### Account Initialization Flow

`AccountPool::init()` spins up accounts concurrently (capped at 13 via `tokio::sync::Semaphore`):
1. `login` — obtain Bearer token
2. `create_session` — create chat session
3. `health_check` — test completion (with PoW) to verify writable session
4. `update_title` — rename session to "managed-by-ai-free-api"

Each account retries 3x with 2s delay on failure. If an account fails all retries it's marked as `InitFailed`.

### Request Flow (per-chat)

`v0_chat()` → `apply_request_jitter` → 字符数判断路径 → `get_account()` → `create_session()` →（可选）`upload_files()` / chunked 串接 → `compute_pow()` → `completion()` → `parse_ready()` → `ResponseStream`

Each `v0_chat()` call creates a dedicated session (chunked 路径整请求共享一个 session), then streams the response. The response is a `StreamEvent` stream (not raw SSE bytes) — the **精简响应协议** abstracts away DeepSeek's p/o/v patch protocol into typed events: `Meta`, `ThinkStart`, `ThinkDelta`, `ContentStart`, `ContentDelta`, `Done`. The session is destroyed when the stream ends via `ResponseStream::drop`, which also calls `stop_stream` on abnormal disconnects. Sessions are tracked in `active_sessions: Arc<Mutex<HashMap<String, ActiveSession>>>`.

The `Chat` module dispatches across 3 request paths based on **字符数** (`prompt.chars().count()`)，阈值 = `input_character_limits[model_type] * 75%`（硬编码）：
- **Normal path** (`v0_chat_once`): prompt 未超阈值，直发
- **History-split path** (`v0_chat_oversized_file`): 超阈值且非 expert，拆分历史块上传为随机文件名 txt（反代加固），inline 部分直发
- **Chunked path** (`v0_chat_oversized_chunk`): 超阈值且 expert，纯字符切分多次 completion，靠 `parent_message_id` 串接（**不写文件**）

### Single-Struct Pipeline (OpenAI)

The adapter uses a **single struct** (`ChatCompletionsRequest`) through the entire request pipeline — no intermediate types:

```
ChatCompletionsRequest
  → normalize::apply |
  → tools::extract   |  reads ChatCompletionsRequest fields directly
  → files::extract   |
  → prompt::build    |
  → resolver::resolve|
  → try_chat (ds_core::ChatRequest)
  → if req.stream → ChatCompletionsResponseChunk | else → ChatCompletionsResponse
```(tiktoken 计数在 `OpenAIAdapter::chat_completions()` 内联完成，非独立 pipeline 模块)

### Response Pipeline (OpenAI) — Stream Chain

```
StreamEvent (ds_core) → ConverterStream (converter)
                      → ToolCallStream (tool_parser)
                      → (RepairStream — optional tool call repair)
                      → StopDetectStream (stop_detect + obfuscation)
                      → SSE bytes
```

SSE parsing and DeepSeek's p/o/v patch protocol are handled inside `ds_core/src/chat/response.rs`, which emits `StreamEvent` items. The adapter layer no longer touches raw bytes.

All stream wrappers use `pin_project_lite::pin_project!` macro and implement `Stream` with `poll_next`. Each wrapper is a pinned struct with an inner stream and state, using `Projection` to access fields in `poll_next`.

### Tool Calls via XML

Tool definitions are injected as natural-language conversational schema into the System message tail (see `docs/deepseek-prompt-injection.md`). Each tool uses an independent XML tag `<tool_name>{json}</tool_name>`, where the tag name is the tool name. Response tags are parsed back into structured JSON via `ToolCallStream`:

1. **Per-tool XML tag detector** scans content chunks for `<tool_name>` open tags (tag name derived from request tools)
2. **State machine**: `Normal` (scan for open tag, release safe prefix, retain possible partial-tag tail) → `Suppressing` (collect body until `</tool_name>` close tag, parse JSON)
3. **Partial tag detection**: handles chunks split mid-tag (`get_partial_xml_tool_tag_tail_length`)
4. **JSON repair**: backslash escaping, unquoted keys
5. **Extra tool names**: configurable via `TagConfig.tool_names` (merged from request tools + `extra_tool_names` in `config.toml`)
6. **`<invoke>` XML fallback** for legacy tag formats
7. `arguments` field normalized to always be a JSON string

Primary tag: per-tool XML `<tool_name>` (tag name = tool name). Configurable extra tool names via `TagConfig` in `config.toml`.

### History Splitting & File Upload

**仅当 prompt 超过 `input_character_limits[model_type] * 75%` 阈值时触发**（`v0_chat` 中 `prompt.chars().count() > threshold`），由 `v0_chat_oversized_file` 调用 `split_history_prompt()`：
- 拆分点 = prompt 中最后一个 `<｜Assistant｜>` 块。该块本身走 **inline**，其前的所有块作为 history
- history 文本外层用文件内容标记包裹（`[file content end]`/`[文档结束]`/`[content end]` 三种变体随机轮换，避免固定字符串指纹），上传为**随机文件名** `notes-xxxxxx.txt`/`doc-xxxxxx.txt` 等（不再使用固定 `EMPTY.txt`）
- External files (data URLs) upload individually with a separate PoW computation targeting `/api/v0/file/upload_file`
- Upload polling: 2s 间隔 × 30 次（最多 60s），轮询 `fetch_files` 直到 `SUCCESS`/`FAILED` 或超时
- History 上传失败时退回完整 prompt 内联发送（markers 容错兜底）

### Capability Toggles

Request fields mapped in `request/resolver.rs`:
- **Reasoning**: defaults to `"high"` (on). Set `"none"` to disable.
- **Web search**: `web_search_options` enables; omitted = off.
- **File upload**: data URL content parts → auto upload to session; HTTP URLs → search mode.
- **Response format**: `response_format` → JSON/schema text injection in prompt.

### Overloaded Retry

`OpenAIAdapter::try_chat()` retries up to **6 times** with **exponential backoff** (1s → 2s → 4s → 8s → 16s) on `CoreError::Overloaded`, triggered by DeepSeek's `rate_limit_reached` SSE hint or all accounts busy.

### Oversized Prompt Fallback

`v0_chat()` ([ds_core/src/chat/request.rs](file:///workspace/ds_core/src/chat/request.rs)) 按字符数路由，**不存在 `oversized_prompt` 配置段**：

```rust
let limit = input_character_limit_for(model_type);
let threshold = limit * 75 / 100;          // 硬编码 75%
if prompt.chars().count() > threshold {
    match model_type {
        "expert" => v0_chat_oversized_chunk,   // 分块路径
        _        => v0_chat_oversized_file,    // 历史拆分路径（见上一节）
    };
}
// 否则 v0_chat_once（正常路径）
```

- **路径选择基于 `prompt.chars().count()`，不是 token 数**。tiktoken 计数仅用于 usage 上报，不参与路由
- **History-split 路径** (`v0_chat_oversized_file`)：把历史块上传为随机文件名 txt（`notes-{hex}.txt` 等，反代加固），inline 部分直发（见上一节）
- **Chunked 路径** (`v0_chat_oversized_chunk`，仅 expert)：**不写文件**，而是按 `chunk_size = limit * 75%` 纯字符切分 prompt（不感知 ChatML 标签边界），多次调用 completion，靠 `parent_message_id` 把多个响应串成 session 内的连续消息。非末 chunk 强制 `thinking=false, search=false`；末 chunk 恢复原始配置并返回流

### Anthropic Compatibility Layer

Pure protocol translator on top of `openai_adapter` — no direct `ds_core` access:
- Request: `Anthropic JSON → to_openai_request() → OpenAIAdapter::chat_completions() / try_chat()`
- Response: `OpenAI SSE/JSON → from_chat_completion_stream() / from_chat_completion_bytes() → Anthropic SSE/JSON`
- ID mapping: `chatcmpl-{hex}` → `msg_{hex}`, `call_{suffix}` → `toolu_{suffix}`
- `ToolUnion` in `types.rs` defaults to `Custom` type when absent (backward compat with Claude Code)

### Error Translation Chain

Errors propagate upward with translation at each module boundary:

1. **`ds_core/client.rs`**: `ClientError` (`Http` | `Status` | `Business` | `Json` | `InvalidHeader`)
   - Parses DeepSeek's wrapper envelope `{code, msg, data: {biz_code, biz_msg, biz_data}}` via `Envelope::into_result()`
2. **`ds_core/pool.rs`**: `PoolError` (`AllAccountsFailed` | `Client`(ClientError) | `Pow`(PowError) | `Validation` | `Exists`)
3. **`ds_core/lib.rs`**: `CoreError` (`Overloaded` | `ProofOfWorkFailed` | `ProviderError` | `Stream`)
4. **`openai_adapter.rs`**: `OpenAIAdapterError` (`BadRequest` | `Overloaded` | `ProviderError` | `Internal` | `ToolCallRepairNeeded`)
5. **`anthropic_compat.rs`**: `AnthropicCompatError` (`BadRequest` | `Overloaded` | `Internal`)
6. **`server/error.rs`**: `ServerError` (`Adapter`(OpenAIAdapterError) | `Anthropic`(AnthropicCompatError) | `Unauthorized` | `NotFound`(String))

All errors use `thiserror` derive macro.

### Request Tracing & Account Header

Each request gets a `req-{n}` ID at the handler level, threaded through adapter → `ds_core`. Key log points carry `req=` for cross-layer tracing:
```bash
RUST_LOG=debug 2>&1 | grep 'req=req-1'
```
The `x-ds-account` HTTP response header carries the account identifier upstream.

### HTTP Routes

| Endpoint | Handler | Description |
|----------|---------|-------------|
| `GET /` | `server::root` | Redirect to /admin |
| `GET /health` | `server::health` | Health check (`{"status": "ok"}`) |
| `POST /v1/chat/completions` | `handlers::chat_completions` | OpenAI chat completion |
| `GET /v1/models` | `handlers::list_models` | List models |
| `GET /v1/models/{id}` | `handlers::get_model` | Get model |
| `POST /anthropic/v1/messages` | `handlers::anthropic_messages` | Anthropic messages |
| `GET /anthropic/v1/models` | `handlers::anthropic_list_models` | List models (Anthropic format) |
| `GET /anthropic/v1/models/{id}` | `handlers::anthropic_get_model` | Get model (Anthropic format) |

Optional Bearer auth via `[[api_keys]]` in config; no auth when empty.|

### Model ID Mapping

`model_types` in `[ds_core]` config (default: `["default", "expert", "vision"]`) maps to OpenAI model ID `deepseek-{type}` (e.g., `deepseek-default`, `deepseek-expert`, `deepseek-vision`). Anthropic compat uses the same IDs.

---

## Conventions

### Code

```rust
// Import grouping: std → third-party → crate → local, separated by blank lines
use std::sync::Arc;

use serde::Deserialize;

use crate::config::Config;

use super::inner::Helper;
```

- **Visibility**: `pub(crate)` for types not part of the public API; facade modules keep submodules private with `mod`
- **Comments**: Chinese in source files (team preference)
- **Error messages**: Chinese for user-facing output; English for internal/debug
- **Naming**: `snake_case` for modules/functions, `PascalCase` for types/enum variants, `SCREAMING_SNAKE_CASE` for constants
- **Module files**: `foo.rs` declares sub-modules, `foo/` contains implementation

### Comments

Follow `docs/code-style.md`:
- `//!` — module docs: first line = responsibility, then key design decisions
- `///` — public API docs: verb-led, note side effects and panic conditions
- `//` — inline: explain "why", not "what"

### Logging

- `log` crate with **explicit targets**. Untargeted logs (e.g., bare `log::info!`) are prohibited.
- Targets used:
  - `ds_core::accounts`, `ds_core::client`
  - `adapter` (for `openai_adapter`)
  - `http::server`, `http::request`, `http::response` (for `server`)
  - `anthropic_compat`, `anthropic_compat::models`, `anthropic_compat::request`, `anthropic_compat::response::stream`, `anthropic_compat::response::aggregate`
- See `docs/logging-spec.md` for full target/level mapping

### Config

- Uncommented values in `config.toml` = required; commented = optional with default
- `src/config.rs` is the single source for config loading — no other module reads config files
- `Config::load_with_args()` returns `(Config, PathBuf)` — the path is propagated to `AppState.config_path` for reload
- In the server layer, `Config` is wrapped in `Arc<tokio::sync::RwLock<Config>>` — runtime-mutable, admin panel changes auto-persist via `Config::save()`
- `Config::save()` writes atomically (tmp + rename, 0600 permissions). `Config` includes `AdminConfig` (password hash, JWT secret) and `api_keys: Vec<ApiKeyEntry>` — no separate JSON files

### Testing

- All tests are inline (`#[cfg(test)]` within `src/` files). No separate `tests/` directory.
- `request.rs` has sync unit tests for parsing logic
- `response.rs` has `tokio::test` async tests for stream aggregation
- `println!`/`eprintln!` allowed inside `#[cfg(test)]` for debugging failures; prohibited in library code

## Anti-Patterns

- Do **NOT** create separate config entry points — `src/config.rs` is the single source
- Do **NOT** implement provider logic outside its `*_core/` module
- Do **NOT** commit `config.toml` (only `config.example.toml`)
- Do **NOT** use `println!`/`eprintln!` in library code — use `log` crate with target
- Do **NOT** use untargeted log macros — always specify `target: "..."`
- Do **NOT** access `ds_core` directly from `anthropic_compat` — always go through `OpenAIAdapter`
- Do **NOT** use `#[allow(...)]` in any file except `ds_core/src/accounts/client.rs` — dead API methods and deserialized fields for API symmetry are expected only in the raw HTTP client layer. New lint exemptions in other files must be resolved (refactor or consume the value) rather than suppressed.
- Do **NOT** keep admin/auth config in separate JSON files (`admin.json`, `api_keys.json`) — they are merged into `Config` fields and persisted via `Config::save()` into `config.toml`
- Do **NOT** run `git checkout`, `git commit`, or `gh` commands without explicit user permission — always ask before destructive or persistent operations
---

## Troubleshooting

| Issue | Symptom | Likely Cause / Fix |
|-------|---------|--------------------|
| WASM load failure | `PowError::Execution` on startup | DeepSeek recompiled WASM. PowSolver now uses dynamic export probing (no hardcoded symbols). Update `wasm_url` in `config.toml` if WASM URL changed |
| WAF blocking (non-US) | AWS WAF Challenge response (status 202) | Configure a non-US proxy in `config.toml` `[proxy]` |
| WAF blocking (fingerprint) | HTTP 403 or connection reset | `wreq` with BoringSSL automatically emulates Chrome 136 TLS fingerprint. If blocked, try updating `wreq` or switching emulation profile |
| Account init failure | All accounts stuck in init | Bad credentials (login fails first) or rate-limited (too many sessions). Check `[accounts]` in config |
| Tool call parse failure | No `tool_calls` in response, raw XML visible | Model output a tool tag whose name is not in the parse list. Add the tool name to `extra_tool_names` in `config.toml` `[ds_core.tool_call]` |
| Rate limited | Repeated `CoreError::Overloaded` | Add more accounts or reduce concurrency. 6x exponential backoff handles transient spikes |
| Session errors mid-stream | `invalid message id`, session not found | Usually handled by `GuardedStream::drop` cleanup. If persistent, check concurrent access to same account |
| Oversized prompt rejected | `413` or truncation errors | Prompt exceeds DeepSeek limit. 自动触发 oversized 路径：非 expert 走历史拆分上传随机文件名 txt，expert 走 chunked completion（不写文件）。阈值由 `input_character_limits` × 75% 决定，无 `oversized_prompt` 配置段 |
| Streaming stalls | No SSE events after initial connection | Check `RUST_LOG=adapter=trace,ds_core::accounts=debug,info` for where the pipeline halts |
| Working inside `ds_core/` | `cargo` not finding manifests | Run commands from workspace root, not from inside `ds_core/`. Use `cargo build -p ds_core` |

---

## Where to Look

| Task | Location | Notes |
|------|----------|-------|
| env vars | `src/config.rs` + `main.rs` | `DS_CONFIG_PATH` / `-c`, `DS_DATA_DIR` for data directory |
| Config loading | `src/config.rs` | Single unified entry, `-c` flag support |
| Config reference | `config.example.toml` | All fields documented with examples (authoritative) |
| DeepSeek chat flow | `ds_core/src/` | accounts → pow → completions → client |
| Chat orchestration + file upload | `ds_core/src/chat/request.rs` + `response.rs` | `v0_chat()`, 3 路径分流（normal/oversized_file/oversized_chunk）, history splitting, upload retry (2s×30), `ResponseStream` |
| OpenAI request parsing | `src/openai_adapter/request/` | normalize → tools → files → prompt → resolver |
| File upload extraction | `src/openai_adapter/request/files.rs` | data URL → FilePayload, HTTP URL → search mode |
| Token counting (tiktoken) | `src/openai_adapter.rs` | `OpenAIAdapter::bpe` field, inlined in `chat_completions()`. **仅用于 usage 上报**（DeepSeek 后端返回 0）；路径选择基于字符数，与此无关 |
| OpenAI response conversion | `src/openai_adapter/response/` | converter → tool_parser → repair → stop_detect |
| Tool call parser & stop sequences | `src/openai_adapter/response/tool_parser.rs` | `TagConfig` with `tool_names` (per-tool XML tags); stop filtering embedded |
| Stream pipeline config | `src/openai_adapter/response.rs` | `StreamCfg` struct (consolidates 8 stream params) |
| Anthropic compat layer | `src/anthropic_compat/` | Built on openai_adapter, no direct ds_core access |
| Anthropic streaming response | `src/anthropic_compat/response/stream.rs` | OpenAI SSE → Anthropic SSE event stream |
| Anthropic aggregate response | `src/anthropic_compat/response/aggregate.rs` | OpenAI JSON → Anthropic JSON |
| OpenAI protocol types | `src/openai_adapter/types.rs` | Request/response structs, `#![allow(dead_code)]` |
| Model listing | `src/openai_adapter/models.rs` | Model registry and listing |
| HTTP server/routes | `src/server/` | handlers → stream → error |
| PoW WASM solver | `ds_core/src/accounts/pow.rs` | wasmtime loading, dynamic export probing, DeepSeekHashV1 |
| DeepSeek HTTP client | `ds_core/src/accounts/client.rs` | `Envelope::into_result()`, WAF detection, all API methods |
| Unified debug CLI | `examples/adapter_cli.rs` | Modes: chat/raw/compare/concurrent/status/models |
| Example request JSON | `examples/adapter_cli/` | Pre-built ChatCompletionsRequest samples |
| Scripted regression test | `just adapter-cli -- source examples/adapter_cli-script.txt` | Runs all JSON samples in sequence |
| Docker deployment | `docker/Dockerfile` + `docker/docker-compose.yaml` | Pre-built ghcr.io image, bind mounts for config/data |
| e2e scenario test framework | `py-e2e-tests/` | JSON-driven scenarios with checks |
| CI pipeline | `.github/workflows/ci.yml` | `cargo check + clippy + fmt + audit + machete` + `cargo test` |
| Release workflow | `.github/workflows/release.yml` | Tag `v*` → 8 targets, 4 platforms, CHANGELOG release |
| Code style | `docs/code-style.md` | 注释、命名、错误消息约定 |
| Logging spec | `docs/logging-spec.md` | 日志级别、目标、模块级过滤 |
| Prompt injection strategy | `docs/deepseek-prompt-injection.md` | DeepSeek 原生标签、工具调用注入策略 |
| DeepSeek API reference | `ds_core/raw-api-reference.md` | 端点、信封格式、SSE 增量协议、PoW 算法 |
| Development guide | `docs/development.md` | 环境配置、首次启动、Release 构建 |
| Admin panel routes | `src/server/admin.rs` | Setup/login/config/status/stats/models/logs handlers |
| JWT auth + password | `src/server/auth.rs` | `setup_admin()`/`login_admin()`, JWT sign/verify, login rate limiter |
| Store manager | `src/server/store.rs` | API key validation, stats persistence, delegates admin/keys to `Config::save()` |
| Request stats | `src/server/stats.rs` | `RequestStats`, `StatsHandle`, background flush to `stats.json` |
| Runtime log | `src/server/runtime_log.rs` | stdout redirect to `runtime.log` with rotation |

---

## Commands

```bash
# Setup (config auto-created on first run; copy example only if you want defaults)

# Enable pre-commit hook (check + clippy + fmt + audit + machete + cargo test)
git config core.hooksPath .githooks

# One-pass check (check + clippy + fmt + audit + unused deps)
just check

# Run the HTTP server with basic logging
just serve
RUST_LOG=info just serve
# Trace through the entire SSE pipeline
RUST_LOG=adapter=trace,ds_core::accounts=debug,info just serve
# Module-level logging filters
RUST_LOG=ds_core::accounts=debug,ds_core::client=warn,info just serve
RUST_LOG=adapter=debug,anthropic_compat=debug just serve

# Run unified protocol debug CLI (modes: chat, raw, compare, concurrent N, status, models, model <id>)
just adapter-cli
RUST_LOG=debug just adapter-cli
# Script mode — runs all JSON samples in sequence (full regression)
just adapter-cli -- source examples/adapter_cli-script.txt
# Interactive mode with a specific config
cargo run --example adapter_cli -- -c /path/to/config.toml

# Run specific test modules
just test-adapter-request
just test-adapter-response
just test-adapter-request converter_emits_role_and_content -- --exact

# Run a single Rust test (use -- --exact for precise name matching)
cargo test converter_emits_role_and_content -- --exact

# Run all Rust tests
cargo test

# Run only library tests (skips example compilation, faster iteration)
cargo test --lib

# e2e tests (requires `uv`, server on port 22217)
just e2e-basic    # Basic: 基础功能测试（OpenAI + Anthropic 双端点）
just e2e-repair   # Repair: 工具调用损坏修复专项测试
just e2e-stress   # Stress: 全部场景 × 3 次迭代压测
# See docs/development.md for full e2e CLI parameters (filter, parallel, model, report, etc.)

# Start server with e2e config
just e2e-serve

# Individual checks
cargo check
cargo clippy -- -D warnings
cargo fmt --check
cargo audit        # requires: cargo install cargo-audit
cargo machete      # requires: cargo install cargo-machete
cargo outdated     # requires: cargo install cargo-outdated

# Build
cargo build
cargo build --release

# Release (tag push triggers CI: 8 targets, 4 platforms, aarch64 on ARM runners)
git tag v0.x.x
git push origin v0.x.x
# CI extracts changelog from CHANGELOG.md, creates GitHub release
```