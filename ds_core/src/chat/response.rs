//! 流处理模块 —— 精简响应协议与 DeepSeek SSE 解析
//!
//! 协议定义：将 DeepSeek 原始 SSE 字节流（p/o/v patch 协议）转换为结构化事件序列，
//! 主 crate 只需消费 StreamEvent，不再感知底层格式。

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use pin_project_lite::pin_project;

use crate::CoreError;
use crate::accounts::ClientError;
use crate::accounts::{AccountGuard, DsClient, StopStreamPayload};

// ── 精简响应协议 ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum StreamEvent {
    Meta {
        account_id: String,
    },
    ThinkStart,
    ThinkDelta {
        content: String,
    },
    ContentStart,
    ContentDelta {
        content: String,
    },
    Done {
        finish_reason: Option<String>,
        accumulated_token_usage: Option<u32>,
    },
}

// ── 内部类型 ──────────────────────────────────────────────────────────

pub(crate) struct ActiveSession {
    pub(crate) token: String,
    pub(crate) session_id: String,
    pub(crate) message_id: i64,
}

pub(crate) struct SessionHandle {
    pub(crate) client: DsClient,
    pub(crate) token: String,
    pub(crate) session_id: String,
    pub(crate) message_id: i64,
    pub(crate) sessions: Arc<Mutex<HashMap<String, ActiveSession>>>,
    /// 是否持久化 session（不删除对话）
    pub(crate) persist: bool,
}

impl SessionHandle {
    fn cleanup(&self, finished: bool) {
        self.sessions.lock().unwrap().remove(&self.session_id);
        let client = self.client.clone();
        let token = self.token.clone();
        let session_id = self.session_id.clone();
        let message_id = self.message_id;
        let persist = self.persist;

        tokio::spawn(async move {
            if !finished {
                let payload = StopStreamPayload {
                    chat_session_id: session_id.clone(),
                    message_id,
                };
                if let Err(e) = client.stop_stream(&token, &payload).await {
                    log::warn!(target: "ds_core::accounts", "stop_stream failed: {}", e);
                }
            }

            // 持久化模式：只保留 session，不删除对话。
            // ponytail: stop_stream 仍保留；否则客户端断流时上游会继续生成。
            if persist {
                log::debug!(
                    target: "ds_core::accounts",
                    "session persist mode: 保留 session {} (不删除对话)", session_id
                );
                return;
            }

            if let Err(e) = client.delete_session(&token, &session_id).await {
                log::warn!(target: "ds_core::accounts", "delete_session failed: {}", e);
            }
        });
    }
}

// ── DeepSeek Patch 状态机 ─────────────────────────────────────────────

const FRAG_THINK: &str = "THINK";
const FRAG_RESPONSE: &str = "RESPONSE";

/// Fragment 状态
struct Fragment {
    ty: String,
    content: String,
}

/// 维护 DeepSeek 响应的 patch 状态，对齐前端 DeltaParser
///
/// - `p` / `o` 跨事件持久化
/// - `o` 默认 "SET"
struct PatchState {
    current_path: Option<String>,
    current_op: Option<String>,
    fragments: Vec<Fragment>,
    status: Option<String>,
    accumulated_token_usage: Option<u32>,
}

impl PatchState {
    fn new() -> Self {
        Self {
            current_path: None,
            current_op: None,
            fragments: Vec::new(),
            status: None,
            accumulated_token_usage: None,
        }
    }

    /// 消费一帧 SSE 文本，返回零个或多个 StreamEvent
    fn apply_frame(&mut self, frame: &str) -> Result<Vec<StreamEvent>, CoreError> {
        if frame.is_empty() {
            return Ok(Vec::new());
        }

        // 提取 event 和 data 行
        let event_type = frame
            .lines()
            .find_map(|l| l.trim().strip_prefix("event:"))
            .map(|v| v.trim());

        let data = frame
            .lines()
            .find_map(|l| l.trim().strip_prefix("data:"))
            .map(|v| v.trim());

        let mut events = Vec::new();

        // event: ready → 不产出事件（在初始化阶段已处理）
        // event: hint → 检查错误
        if let Some("hint") = event_type
            && let Some(data) = data
        {
            return Err(hint_to_error(data));
        }

        // 解析 data JSON
        if let Some(data) = data
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(data)
        {
            events.extend(self.apply_patch(val));
        }

        Ok(events)
    }

    /// 应用 p/o/v patch
    fn apply_patch(&mut self, val: serde_json::Value) -> Vec<StreamEvent> {
        // p/o 跨事件持久化
        if let Some(p) = val.get("p").and_then(|v| v.as_str()) {
            self.current_path = Some(p.to_string());
        }
        if let Some(o) = val.get("o").and_then(|v| v.as_str()) {
            self.current_op = Some(o.to_string());
        }

        let op = self.current_op.as_deref().unwrap_or("SET").to_string();
        let path = self.current_path.as_deref().unwrap_or("").to_string();

        let Some(v) = val.get("v") else {
            return Vec::new();
        };

        // 初始快照：无 path 且 v 含 response
        if self.current_path.is_none()
            && let Some(response) = v.get("response")
        {
            return self.apply_initial_snapshot(response);
        }

        // BATCH 递归分解
        if op == "BATCH" && v.is_array() {
            return self.apply_batch(&path, v);
        }

        self.apply_path(&path, &op, v)
    }

    fn apply_batch(&mut self, parent_path: &str, arr: &serde_json::Value) -> Vec<StreamEvent> {
        let mut events = Vec::new();
        let Some(arr) = arr.as_array() else {
            return events;
        };

        // 子解析器独立状态
        let (mut sub_path, mut sub_op) = (String::new(), String::from("SET"));

        for item in arr {
            if let Some(p) = item.get("p").and_then(|v| v.as_str()) {
                sub_path = p.to_string();
            }
            if let Some(o) = item.get("o").and_then(|v| v.as_str()) {
                sub_op = o.to_string();
            }

            let Some(v) = item.get("v") else {
                continue;
            };

            let full_path = if parent_path.is_empty() {
                sub_path.clone()
            } else if sub_path.is_empty() {
                parent_path.to_string()
            } else {
                format!("{}/{}", parent_path, sub_path)
            };

            if sub_op == "BATCH" {
                let mut batch_events = self.apply_batch(&full_path, v);
                events.append(&mut batch_events);
            } else {
                let mut path_events = self.apply_path(&full_path, &sub_op, v);
                events.append(&mut path_events);
            }
        }

        events
    }

    fn apply_initial_snapshot(&mut self, response: &serde_json::Value) -> Vec<StreamEvent> {
        let mut events = Vec::new();

        if let Some(s) = response.get("status").and_then(|v| v.as_str()) {
            self.status = Some(s.to_string());
        }

        if let Some(n) = response
            .get("accumulated_token_usage")
            .and_then(|v| v.as_u64())
        {
            self.accumulated_token_usage = Some(u32::try_from(n).unwrap_or(u32::MAX));
        }

        if let Some(arr) = response.get("fragments").and_then(|f| f.as_array()) {
            self.fragments.clear();
            for frag in arr {
                let Some(ty) = frag.get("type").and_then(|t| t.as_str()) else {
                    continue;
                };
                let content = frag
                    .get("content")
                    .and_then(|c| c.as_str())
                    .unwrap_or("")
                    .to_string();
                self.fragments.push(Fragment {
                    ty: ty.to_string(),
                    content: content.clone(),
                });
                if !content.is_empty() {
                    match ty {
                        FRAG_THINK => events.push(StreamEvent::ThinkDelta { content }),
                        FRAG_RESPONSE => events.push(StreamEvent::ContentDelta { content }),
                        _ => {}
                    }
                }
            }
        }

        events
    }

    fn apply_path(&mut self, path: &str, op: &str, val: &serde_json::Value) -> Vec<StreamEvent> {
        let mut events = Vec::new();

        match path {
            "response/status" | "/response/status" => {
                if let Some(s) = val.as_str() {
                    self.status = Some(s.to_string());
                }
            }
            "response/accumulated_token_usage"
            | "accumulated_token_usage"
            | "/response/accumulated_token_usage"
            | "/accumulated_token_usage" => {
                if let Some(n) = val.as_u64() {
                    self.accumulated_token_usage = Some(u32::try_from(n).unwrap_or(u32::MAX));
                }
            }
            "response/fragments/-1/content" | "/response/fragments/-1/content" => {
                if let Some(s) = val.as_str()
                    && let Some(frag) = self.fragments.last_mut()
                {
                    match frag.ty.as_str() {
                        FRAG_THINK => {
                            frag.content.push_str(s);
                            events.push(StreamEvent::ThinkDelta {
                                content: s.to_string(),
                            });
                        }
                        FRAG_RESPONSE => {
                            frag.content.push_str(s);
                            events.push(StreamEvent::ContentDelta {
                                content: s.to_string(),
                            });
                        }
                        _ => {}
                    }
                }
            }
            "response/fragments" | "/response/fragments" if op == "APPEND" => {
                if let Some(arr) = val.as_array() {
                    for item in arr {
                        if let Some(ty) = item.get("type").and_then(|t| t.as_str()) {
                            let content = item
                                .get("content")
                                .and_then(|c| c.as_str())
                                .unwrap_or("")
                                .to_string();
                            self.fragments.push(Fragment {
                                ty: ty.to_string(),
                                content: content.clone(),
                            });
                            if !content.is_empty() {
                                match ty {
                                    FRAG_THINK => events.push(StreamEvent::ThinkDelta { content }),
                                    FRAG_RESPONSE => {
                                        events.push(StreamEvent::ContentDelta { content });
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        events
    }
}

// ── 响应阶段跟踪 ─────────────────────────────────────────────────────

#[derive(PartialEq)]
enum Phase {
    Init,
    Thinking,
    Content,
    Done,
}

pin_project! {
    pub(crate) struct ResponseStream {
        raw: Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send>>,
        _guard: AccountGuard,
        session: SessionHandle,

        buf: Vec<u8>,
        patch_state: PatchState,
        phase: Phase,
        pending: Vec<StreamEvent>,
        meta_sent: bool,
        account_id: String,
        finished: bool,
    }

    impl PinnedDrop for ResponseStream {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            this.session.cleanup(*this.finished);
        }
    }
}

impl ResponseStream {
    pub(crate) fn new(
        raw: Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send>>,
        guard: AccountGuard,
        session: SessionHandle,
        account_id: String,
    ) -> Self {
        Self {
            raw,
            _guard: guard,
            session,
            buf: Vec::new(),
            patch_state: PatchState::new(),
            phase: Phase::Init,
            pending: Vec::new(),
            meta_sent: false,
            account_id,
            finished: false,
        }
    }
}

impl Stream for ResponseStream {
    type Item = Result<StreamEvent, CoreError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // 首个事件始终是 Meta
        if !*this.meta_sent {
            *this.meta_sent = true;
            return Poll::Ready(Some(Ok(StreamEvent::Meta {
                account_id: this.account_id.clone(),
            })));
        }

        // 先清空 pending 队列
        if let Some(evt) = this.pending.pop() {
            return Poll::Ready(Some(Ok(evt)));
        }

        if *this.phase == Phase::Done {
            return Poll::Ready(None);
        }

        loop {
            if let Some(frame) = take_frame(this.buf) {
                let events = match this.patch_state.apply_frame(&frame) {
                    Ok(evts) => evts,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };

                if events.is_empty() {
                    continue;
                }

                // 过滤：阶段切换信号插入 + 延后排队
                let mut filtered = Vec::new();
                for evt in events {
                    match &evt {
                        StreamEvent::ThinkDelta { .. }
                            if *this.phase == Phase::Init || *this.phase == Phase::Content =>
                        {
                            *this.phase = Phase::Thinking;
                            filtered.push(StreamEvent::ThinkStart);
                        }
                        StreamEvent::ContentDelta { .. }
                            if *this.phase == Phase::Init || *this.phase == Phase::Thinking =>
                        {
                            *this.phase = Phase::Content;
                            filtered.push(StreamEvent::ContentStart);
                        }
                        _ => {}
                    }
                    filtered.push(evt);
                }

                // 检查 status 是否已结束
                if let Some(status) = &this.patch_state.status
                    && (status == "FINISHED" || status == "INCOMPLETE")
                {
                    *this.phase = Phase::Done;
                    let finish = (status == "FINISHED").then(|| "stop".to_string());
                    let usage = this.patch_state.accumulated_token_usage;
                    if finish.is_some() && !has_response_content(this.patch_state) {
                        log::warn!(
                            target: "ds_core::accounts",
                            "状态机 FINISHED 但无 RESPONSE 内容"
                        );
                    }
                    filtered.push(StreamEvent::Done {
                        finish_reason: finish,
                        accumulated_token_usage: usage,
                    });
                }

                // 第一个事件立即返回，其余入 pending
                if let Some(first) = filtered.first().cloned() {
                    for rest in filtered.into_iter().skip(1).rev() {
                        this.pending.push(rest);
                    }
                    return Poll::Ready(Some(Ok(first)));
                }
            }

            match this.raw.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    this.buf.extend_from_slice(&bytes);
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(CoreError::Stream(e.to_string()))));
                }
                Poll::Ready(None) => {
                    *this.finished = true;
                    // 处理缓冲区中剩余数据
                    if !this.buf.is_empty() {
                        let drained: Vec<u8> = this.buf.drain(..).collect();
                        let frame = String::from_utf8_lossy(&drained);
                        let events = this.patch_state.apply_frame(&frame)?;
                        if !events.is_empty()
                            && let Some(first) = events.into_iter().next()
                        {
                            return Poll::Ready(Some(Ok(first)));
                        }
                    }
                    // 未正常结束时，发送 Done
                    if *this.phase != Phase::Done {
                        *this.phase = Phase::Done;
                        return Poll::Ready(Some(Ok(StreamEvent::Done {
                            finish_reason: None,
                            accumulated_token_usage: None,
                        })));
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ── SSE 帧提取 ───────────────────────────────────────────────────────

fn take_frame(buf: &mut Vec<u8>) -> Option<String> {
    let pos = buf.windows(2).position(|w| w == b"\n\n")?;
    let frame_bytes: Vec<u8> = buf.drain(..pos).collect();
    buf.drain(..2);
    Some(String::from_utf8_lossy(&frame_bytes).to_string())
}

fn hint_to_error(data: &str) -> CoreError {
    let val: serde_json::Value = serde_json::from_str(data).unwrap_or_default();
    let content = val
        .get("content")
        .or_else(|| val.get("finish_reason"))
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");

    if content.contains("rate_limit") {
        CoreError::Overloaded
    } else if content.contains("input_exceeds_limit") {
        CoreError::ProviderError("输入内容超长，请缩短后重试".into())
    } else {
        CoreError::ProviderError(format!("hint: {}", content))
    }
}

// ── 内部辅助（供 ResponseStream 使用） ─────────────────────────────────

/// 检查是否有非空的 RESPONSE 内容（用于 FINISHED 告警）
fn has_response_content(state: &PatchState) -> bool {
    state
        .fragments
        .iter()
        .any(|f| f.ty == "RESPONSE" && !f.content.is_empty())
}

// ── 公开辅助（供 request.rs 初始化阶段使用） ──────────────────────────

pub(crate) fn split_two_events(buf: &str) -> Option<(&str, &str)> {
    let parts: Vec<&str> = buf.splitn(3, "\n\n").collect();
    (parts.len() >= 3).then(|| (parts[0], parts[1]))
}

pub(crate) fn check_hint(event_block: &str) -> Option<CoreError> {
    let is_hint = event_block.lines().any(|l| {
        l.trim()
            .strip_prefix("event:")
            .is_some_and(|v| v.trim() == "hint")
    });
    if !is_hint {
        return None;
    }
    if event_block.contains("rate_limit") {
        return Some(CoreError::Overloaded);
    }
    if event_block.contains("input_exceeds_limit") {
        return Some(CoreError::ProviderError(
            "输入内容超长，请缩短后重试".into(),
        ));
    }
    None
}

pub(crate) fn parse_ready_message_ids(chunk: &[u8]) -> (i64, i64) {
    let text = std::str::from_utf8(chunk).ok();
    if let Some(text) = text {
        for line in text.lines() {
            if let Some(data) = line.strip_prefix("data: ")
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(data)
                && let (Some(r), Some(s)) = (
                    val.get("request_message_id").and_then(|v| v.as_i64()),
                    val.get("response_message_id").and_then(|v| v.as_i64()),
                )
            {
                return (r, s);
            }
        }
    }
    (1, 2)
}

pub(crate) fn parse_json_error(text: &str, request_id: &str) -> CoreError {
    let raw = text.trim();
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(raw)
        && let Some(code) = val.get("code").and_then(|c| c.as_i64())
    {
        let msg = val
            .get("msg")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown")
            .to_string();
        log::error!(
            target: "ds_core::accounts",
            "req={} JSON 错误响应: code={}, msg={}", request_id, code, msg
        );
        return match code {
            1001 | 1201 => CoreError::Overloaded,
            40301 => CoreError::ProviderError(format!("INVALID_POW_RESPONSE: {}", msg)),
            _ => CoreError::ProviderError(format!("API error code={}: {}", code, msg)),
        };
    }
    log::error!(
        target: "ds_core::accounts",
        "req={} 无法解析的响应: {}", request_id, raw.chars().take(200).collect::<String>()
    );
    CoreError::Stream(format!(
        "无法解析的响应: {}",
        raw.chars().take(200).collect::<String>()
    ))
}

pub(crate) async fn wait_ready_and_update(
    stream: &mut Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send>>,
    request_id: &str,
    chunk_index: usize,
    total_chunks: usize,
) -> Result<(i64, Vec<u8>), CoreError> {
    let mut buf = Vec::new();
    let mut ready_msg_id: Option<i64> = None;
    loop {
        let chunk = stream
            .next()
            .await
            .ok_or_else(|| {
                let raw = String::from_utf8_lossy(&buf);
                if raw.trim().starts_with('{') {
                    return parse_json_error(&raw, request_id);
                }
                CoreError::Stream(format!(
                    "req={} 分块 {}/{} 收到空流",
                    request_id, chunk_index, total_chunks
                ))
            })?
            .map_err(|e| CoreError::Stream(e.to_string()))?;
        buf.extend_from_slice(&chunk);
        let text = String::from_utf8_lossy(&buf);

        let events: Vec<&str> = text.split("\n\n").collect();
        let n_complete = if text.ends_with("\n\n") {
            events.len()
        } else {
            events.len().saturating_sub(1)
        };

        for event in events[..n_complete].iter() {
            if event.is_empty() {
                continue;
            }
            if let Some(err) = check_hint(event) {
                return Err(err);
            }
            if event.lines().any(|l| {
                l.trim()
                    .strip_prefix("event:")
                    .is_some_and(|v| v.trim() == "ready")
            }) {
                ready_msg_id = Some(parse_ready_message_ids(event.as_bytes()).1);
            }
            if let Some(id) = ready_msg_id
                && event.lines().any(|l| {
                    l.trim()
                        .strip_prefix("event:")
                        .is_some_and(|v| v.trim() == "update_session")
                })
            {
                return Ok((id, buf));
            }
        }
    }
}

pub(crate) async fn wait_close(
    stream: &mut Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send>>,
    buf: &mut Vec<u8>,
    request_id: &str,
    chunk_index: usize,
    total_chunks: usize,
) -> Result<(), CoreError> {
    loop {
        let text = String::from_utf8_lossy(buf);
        let events: Vec<&str> = text.split("\n\n").collect();
        let n_complete = if text.ends_with("\n\n") {
            events.len()
        } else {
            events.len().saturating_sub(1)
        };

        for event in events[..n_complete].iter() {
            if event.lines().any(|l| {
                l.trim()
                    .strip_prefix("event:")
                    .is_some_and(|v| v.trim() == "close")
            }) {
                return Ok(());
            }
        }

        let chunk = stream
            .next()
            .await
            .ok_or_else(|| {
                CoreError::Stream(format!(
                    "req={} 分块 {}/{} 流在 close 前结束",
                    request_id, chunk_index, total_chunks
                ))
            })?
            .map_err(|e| CoreError::Stream(e.to_string()))?;
        buf.extend_from_slice(&chunk);
    }
}
