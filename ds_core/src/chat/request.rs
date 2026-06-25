//! 对话分流模块 —— prompt 超限判断与三种请求路径
//!
//! - 正常路径（v0_chat_once）：完整 prompt 直发
//! - 历史拆分路径（v0_chat_oversized_file）：超限 default 模型，拆历史为文件上传
//! - 分块路径（v0_chat_oversized_chunk）：超限 expert 模型，分块 completion 写入

use std::pin::Pin;

use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::CoreError;
use crate::accounts::CompletionPayload;

use super::response::{
    ActiveSession, ResponseStream, SessionHandle, StreamEvent, check_hint, parse_json_error,
    parse_ready_message_ids, split_two_events, wait_close, wait_ready_and_update,
};

// ── 常量 ──────────────────────────────────────────────────────────────

const TAG_START: &str = "<｜";
const TAG_END: &str = "｜>";
const SESSION_HISTORY_FILE: &str = "EMPTY.txt";

// ── 公开类型 ──────────────────────────────────────────────────────────

/// 文件载荷
#[derive(Debug, Clone)]
pub struct FilePayload {
    pub filename: String,
    pub content: Vec<u8>,
    pub content_type: String,
}

/// 对话请求
#[derive(Debug, Clone)]
pub struct ChatRequest {
    pub prompt: String,
    pub thinking_enabled: bool,
    pub search_enabled: bool,
    pub model_type: String,
    pub files: Vec<FilePayload>,
}

/// v0_chat 返回值：精简协议事件流
pub struct ChatResponse {
    pub stream: Pin<Box<dyn Stream<Item = Result<StreamEvent, CoreError>> + Send>>,
}

// ── Chat 请求方法 ─────────────────────────────────────────────────────

use super::Chat;

impl Chat {
    /// 对话入口：判断 prompt 大小，选择正常路径或回退方案
    pub async fn v0_chat(
        &self,
        req: ChatRequest,
        request_id: &str,
    ) -> Result<ChatResponse, CoreError> {
        // 请求前随机延迟，模拟真实浏览器节奏
        self.apply_request_jitter().await;

        let limit = self.input_character_limit_for(&req.model_type);
        let threshold = (limit as u64 * 75 / 100) as usize;
        let oversized = req.prompt.chars().count() > threshold;

        // 超限时按模型类型选择回退方案
        if oversized {
            log::debug!(
                target: "ds_core::accounts",
                "req={} prompt 超限 ({} chars > {} threshold), model_type={}, 触发回退方案",
                request_id,
                req.prompt.chars().count(),
                threshold,
                req.model_type,
            );
            return match req.model_type.as_str() {
                "expert" => self.v0_chat_oversized_chunk(&req, request_id).await,
                _ => self.v0_chat_oversized_file(&req, request_id).await,
            };
        }

        // 不超限：所有模型统一直发（完整 prompt，无历史拆分，无文件上传回退）
        const MAX_ATTEMPTS: usize = 3;
        for attempt in 0..MAX_ATTEMPTS {
            let first_try = attempt == 0;
            match self
                .v0_chat_once(&req, &req.prompt, "", request_id, first_try)
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(CoreError::Overloaded) => {
                    if attempt + 1 >= MAX_ATTEMPTS {
                        return Err(CoreError::Overloaded);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
                Err(e) => {
                    log::warn!(
                        target: "ds_core::accounts",
                        "req={} 请求失败 (attempt {}/{}): {}",
                        request_id, attempt + 1, MAX_ATTEMPTS, e
                    );
                    if attempt + 1 >= MAX_ATTEMPTS {
                        return Err(e);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
        }
        Err(CoreError::Overloaded)
    }

    /// 回退方案 A：历史文件上传（default / vision）
    async fn v0_chat_oversized_file(
        &self,
        req: &ChatRequest,
        request_id: &str,
    ) -> Result<ChatResponse, CoreError> {
        const MAX_ATTEMPTS: usize = 3;

        let (inline_prompt, history_content) = split_history_prompt(&req.prompt);

        if !history_content.is_empty() {
            log::debug!(
                target: "ds_core::accounts",
                "req={} 触发历史拆分, history_size={}", request_id, history_content.len()
            );
        }

        for attempt in 0..MAX_ATTEMPTS {
            let first_try = attempt == 0;
            match self
                .v0_chat_once(req, &inline_prompt, &history_content, request_id, first_try)
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(CoreError::Overloaded) => {
                    if attempt + 1 >= MAX_ATTEMPTS {
                        return Err(CoreError::Overloaded);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
                Err(e) => {
                    log::warn!(
                        target: "ds_core::accounts",
                        "req={} 请求失败 (attempt {}/{}): {}",
                        request_id, attempt + 1, MAX_ATTEMPTS, e
                    );
                    if attempt + 1 >= MAX_ATTEMPTS {
                        return Err(e);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
        }
        Err(CoreError::Overloaded)
    }

    /// 回退方案 B：分块 completion 写入 session（expert，绕过文件上传限制）
    async fn v0_chat_oversized_chunk(
        &self,
        req: &ChatRequest,
        request_id: &str,
    ) -> Result<ChatResponse, CoreError> {
        // 1. 获取账号
        let guard = self
            .accounts
            .get_account_with_wait(30_000)
            .await
            .ok_or_else(|| {
                log::warn!(
                    target: "ds_core::accounts",
                    "req={} 账号池无可用账号", request_id
                );
                CoreError::Overloaded
            })?;
        let account = guard.account();
        let account_id = account.display_id().to_string();
        let token = account.token().to_string();

        log::debug!(
            target: "ds_core::accounts",
            "req={} 分块写入: model_type=expert, account={}", request_id, account_id
        );

        // 2. 创建 session（所有 chunk 共享）
        let session_id = match self.accounts.create_session(&token).await {
            Ok(id) => id,
            Err(e) => {
                self.accounts.mark_error(&account_id);
                return Err(e);
            }
        };

        // 3. 按 75% limit 切分 prompt
        let limit = self.input_character_limit_for(&req.model_type);
        let chunk_size = (limit as u64 * 75 / 100) as usize;
        let chunks = split_prompt_chunks(&req.prompt, chunk_size);

        // 4. Feed 非末 chunk 到 session
        let mut parent_message_id: Option<i64> = None;
        for (i, chunk) in chunks[..chunks.len() - 1].iter().enumerate() {
            let pow_header = match self
                .accounts
                .compute_pow_for_target(&token, "/api/v0/chat/completion")
                .await
            {
                Ok(h) => h,
                Err(e) => {
                    self.accounts.mark_error(&account_id);
                    let _ = self.accounts.delete_session(&token, &session_id).await;
                    return Err(e);
                }
            };

            let payload = CompletionPayload {
                chat_session_id: session_id.clone(),
                parent_message_id,
                model_type: req.model_type.clone(),
                prompt: chunk.clone(),
                ref_file_ids: vec![],
                thinking_enabled: false,
                search_enabled: false,
                preempt: false,
            };

            let mut stream = match self
                .accounts
                .completion(&token, &pow_header, &payload)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    self.accounts.mark_error(&account_id);
                    let _ = self.accounts.delete_session(&token, &session_id).await;
                    return Err(e);
                }
            };

            // 等 ready + update_session
            let (stop_id, mut close_buf) =
                wait_ready_and_update(&mut stream, request_id, i + 1, chunks.len() - 1).await?;

            parent_message_id = Some(stop_id);

            // 发送停止信号（fire-and-forget）
            let stop_payload = crate::accounts::StopStreamPayload {
                chat_session_id: session_id.clone(),
                message_id: stop_id,
            };
            let _ = self.accounts.stop_stream(&token, &stop_payload).await;

            // 消费流直到 close 事件
            wait_close(
                &mut stream,
                &mut close_buf,
                request_id,
                i + 1,
                chunks.len() - 1,
            )
            .await?;

            log::debug!(
                target: "ds_core::accounts",
                "req={} 分块 {}/{} parent={:?}", request_id, i + 1, chunks.len() - 1, parent_message_id
            );
        }

        // 5. 末 chunk：正常 completion
        let last_chunk = chunks.into_iter().last().unwrap();
        let pow_header = match self
            .accounts
            .compute_pow_for_target(&token, "/api/v0/chat/completion")
            .await
        {
            Ok(h) => h,
            Err(e) => {
                self.accounts.mark_error(&account_id);
                let _ = self.accounts.delete_session(&token, &session_id).await;
                return Err(e);
            }
        };

        let payload = CompletionPayload {
            chat_session_id: session_id.clone(),
            parent_message_id,
            model_type: req.model_type.clone(),
            prompt: last_chunk,
            ref_file_ids: vec![],
            thinking_enabled: req.thinking_enabled,
            search_enabled: req.search_enabled,
            preempt: false,
        };

        let mut raw_stream = match self
            .accounts
            .completion(&token, &pow_header, &payload)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                self.accounts.mark_error(&account_id);
                let _ = self.accounts.delete_session(&token, &session_id).await;
                return Err(e);
            }
        };

        // 收集前两个 SSE 事件（ready + hint/update_session）
        let mut buf = Vec::new();
        let mut text_buf = String::new();
        let (ready_block, second_block) = loop {
            let chunk = raw_stream
                .next()
                .await
                .ok_or_else(|| {
                    let raw = String::from_utf8_lossy(&buf);
                    if let Some(biz_code) = raw
                        .lines()
                        .find_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                        .and_then(|v| v.pointer("/data/biz_code").and_then(|c| c.as_i64()))
                    {
                        let biz_msg = raw
                            .lines()
                            .find_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                            .and_then(|v| {
                                v.pointer("/data/biz_msg")
                                    .and_then(|m| m.as_str().map(String::from))
                            })
                            .unwrap_or_default();
                        log::error!(
                            target: "ds_core::accounts",
                            "req={} SSE 流返回业务错误: biz_code={}, biz_msg={}",
                            request_id, biz_code, biz_msg
                        );
                        self.accounts.mark_error(&account_id);
                        return CoreError::ProviderError(format!(
                            "biz_code={}, {}",
                            biz_code, biz_msg
                        ));
                    }
                    if raw.trim().starts_with('{') {
                        self.accounts.mark_error(&account_id);
                        return parse_json_error(&raw, request_id);
                    }
                    log::error!(
                        target: "ds_core::accounts",
                        "req={} 空 SSE 流, 已收到 {} 字节: {}", request_id, buf.len(), raw
                    );
                    CoreError::Stream(format!("空 SSE 流 (已收到 {} 字节)", buf.len()))
                })?
                .map_err(|e| CoreError::Stream(e.to_string()))?;
            log::trace!(
                target: "ds_core::accounts",
                "req={} <<< ({} bytes) {}", request_id, chunk.len(), String::from_utf8_lossy(&chunk)
            );
            buf.extend_from_slice(&chunk);
            text_buf.push_str(&String::from_utf8_lossy(&chunk));

            if let Some((first, second)) = split_two_events(&text_buf) {
                break (first.to_owned(), second.to_owned());
            }
        };

        let (_, stop_id) = parse_ready_message_ids(ready_block.as_bytes());

        // 检查 hint 事件
        if let Some(err) = check_hint(&second_block) {
            if let CoreError::Overloaded = &err {
                log::warn!(
                    target: "ds_core::accounts",
                    "req={} hint 限流: rate_limit_reached", request_id
                );
                self.accounts.mark_error(&account_id);
            } else {
                let hint_detail = second_block
                    .lines()
                    .find_map(|l| l.strip_prefix("data: "))
                    .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
                    .and_then(|v| {
                        v.get("content")
                            .or_else(|| v.get("finish_reason"))
                            .and_then(|c| c.as_str().map(String::from))
                    })
                    .unwrap_or_else(|| "(unknown)".into());
                log::warn!(
                    target: "ds_core::accounts",
                    "req={} hint 错误: {}", request_id, hint_detail
                );
            }
            let _ = self.accounts.delete_session(&token, &session_id).await;
            log::debug!(
                target: "ds_core::accounts",
                "req={} hint 后清理 session: id={}", request_id, session_id
            );
            return Err(err);
        }

        log::debug!(
            target: "ds_core::accounts",
            "req={} SSE ready: resp_msg={}", request_id, stop_id
        );

        // 注册活跃 session
        {
            let mut map = self.active_sessions.lock().unwrap();
            map.insert(
                session_id.clone(),
                ActiveSession {
                    token: token.clone(),
                    session_id: session_id.clone(),
                    message_id: stop_id,
                },
            );
        }

        // 用原始 buf 重建流
        let stream =
            futures::stream::once(futures::future::ready(Ok(Bytes::from(buf)))).chain(raw_stream);

        Ok(ChatResponse {
            stream: Box::pin(ResponseStream::new(
                Box::pin(stream),
                guard,
                SessionHandle {
                    client: self.accounts.client_clone().await,
                    token,
                    session_id,
                    message_id: stop_id,
                    sessions: self.active_sessions.clone(),
                },
                account_id.clone(),
            )),
        })
    }

    /// 单次请求尝试（不含重试逻辑）
    async fn v0_chat_once(
        &self,
        req: &ChatRequest,
        inline_prompt: &str,
        history_content: &str,
        request_id: &str,
        first_try: bool,
    ) -> Result<ChatResponse, CoreError> {
        // 1. 获取空闲账号
        let guard = if first_try {
            self.accounts.get_account_with_wait(30_000).await
        } else {
            self.accounts.get_account()
        }
        .ok_or_else(|| {
            log::warn!(
                target: "ds_core::accounts",
                "req={} 账号池无可用账号", request_id
            );
            CoreError::Overloaded
        })?;

        let account = guard.account();
        let account_id = account.display_id().to_string();
        let token = account.token().to_string();

        log::debug!(
            target: "ds_core::accounts",
            "req={} 分配账号: model_type={}, account={}",
            request_id, req.model_type, account_id
        );

        // 2. 创建临时 session
        let session_id = match self.accounts.create_session(&token).await {
            Ok(id) => id,
            Err(e) => {
                self.accounts.mark_error(&account_id);
                return Err(e);
            }
        };
        log::debug!(
            target: "ds_core::accounts",
            "req={} 创建 session: id={}", request_id, session_id
        );

        // 3. 上传文件：先历史文件，再外部文件
        let mut ref_file_ids: Vec<String> = Vec::new();
        let mut history_upload_failed = false;

        if !history_content.is_empty() {
            match self
                .accounts
                .upload_and_poll(
                    &token,
                    SESSION_HISTORY_FILE,
                    "text/plain",
                    history_content.as_bytes(),
                    request_id,
                )
                .await
            {
                Ok(file_id) => ref_file_ids.push(file_id),
                Err(e) => {
                    log::warn!(
                        target: "ds_core::accounts",
                        "req={} 历史文件上传失败，退回内联发送: {}", request_id, e
                    );
                    history_upload_failed = true;
                }
            }
        }

        for file in &req.files {
            match self
                .accounts
                .upload_and_poll(
                    &token,
                    &file.filename,
                    &file.content_type,
                    &file.content,
                    request_id,
                )
                .await
            {
                Ok(file_id) => ref_file_ids.push(file_id),
                Err(e) => {
                    log::warn!(
                        target: "ds_core::accounts",
                        "req={} 外部文件上传失败 ({}): {}", request_id, file.filename, e
                    );
                    return Err(CoreError::ProviderError(format!(
                        "外部文件上传失败 ({}): {}",
                        file.filename, e
                    )));
                }
            }
        }

        // 4. 计算 PoW
        let pow_header = match self
            .accounts
            .compute_pow_for_target(&token, "/api/v0/chat/completion")
            .await
        {
            Ok(h) => h,
            Err(e) => {
                self.accounts.mark_error(&account_id);
                return Err(e);
            }
        };

        // 5. 发起 completion
        let completion_prompt: &str = if history_upload_failed {
            &req.prompt
        } else {
            inline_prompt
        };

        let payload = CompletionPayload {
            chat_session_id: session_id.clone(),
            parent_message_id: None,
            model_type: req.model_type.clone(),
            prompt: completion_prompt.to_string(),
            ref_file_ids,
            thinking_enabled: req.thinking_enabled,
            search_enabled: req.search_enabled,
            preempt: false,
        };

        let mut raw_stream = match self
            .accounts
            .completion(&token, &pow_header, &payload)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                self.accounts.mark_error(&account_id);
                return Err(e);
            }
        };

        // 6. 收集字节直到拿到前两个 SSE 事件（ready + hint/update_session）
        let mut buf = Vec::new();
        let mut text_buf = String::new();
        let (ready_block, second_block) = loop {
            let chunk = raw_stream
                .next()
                .await
                .ok_or_else(|| {
                    let raw = String::from_utf8_lossy(&buf);
                    if let Some(biz_code) = raw
                        .lines()
                        .find_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                        .and_then(|v| v.pointer("/data/biz_code").and_then(|c| c.as_i64()))
                    {
                        let biz_msg = raw
                            .lines()
                            .find_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                            .and_then(|v| {
                                v.pointer("/data/biz_msg")
                                    .and_then(|m| m.as_str().map(String::from))
                            })
                            .unwrap_or_default();
                        log::error!(
                            target: "ds_core::accounts",
                            "req={} SSE 流返回业务错误: biz_code={}, biz_msg={}",
                            request_id, biz_code, biz_msg
                        );
                        self.accounts.mark_error(&account_id);
                        return CoreError::ProviderError(format!(
                            "biz_code={}, {}",
                            biz_code, biz_msg
                        ));
                    }
                    log::error!(
                        target: "ds_core::accounts",
                        "req={} 空 SSE 流, 已收到 {} 字节: {}", request_id, buf.len(), raw
                    );
                    CoreError::Stream(format!("空 SSE 流 (已收到 {} 字节)", buf.len()))
                })?
                .map_err(|e| CoreError::Stream(e.to_string()))?;
            buf.extend_from_slice(&chunk);
            text_buf.push_str(&String::from_utf8_lossy(&chunk));

            if let Some((first, second)) = split_two_events(&text_buf) {
                break (first.to_owned(), second.to_owned());
            }
        };

        let (_, stop_id) = parse_ready_message_ids(ready_block.as_bytes());

        // 7. 检查 hint 事件
        if let Some(err) = check_hint(&second_block) {
            if let CoreError::Overloaded = &err {
                log::warn!(
                    target: "ds_core::accounts",
                    "req={} hint 限流: rate_limit_reached", request_id
                );
                self.accounts.mark_error(&account_id);
            } else {
                let hint_detail = second_block
                    .lines()
                    .find_map(|l| l.strip_prefix("data: "))
                    .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
                    .and_then(|v| {
                        v.get("content")
                            .or_else(|| v.get("finish_reason"))
                            .and_then(|c| c.as_str().map(String::from))
                    })
                    .unwrap_or_else(|| "(unknown)".into());
                log::warn!(
                    target: "ds_core::accounts",
                    "req={} hint 错误: {}", request_id, hint_detail
                );
            }
            let _ = self.accounts.delete_session(&token, &session_id).await;
            log::debug!(
                target: "ds_core::accounts",
                "req={} hint 后清理 session: id={}", request_id, session_id
            );
            return Err(err);
        }

        log::debug!(
            target: "ds_core::accounts",
            "req={} SSE ready: resp_msg={}", request_id, stop_id
        );

        // 8. 注册活跃 session
        {
            let mut map = self.active_sessions.lock().unwrap();
            map.insert(
                session_id.clone(),
                ActiveSession {
                    token: token.clone(),
                    session_id: session_id.clone(),
                    message_id: stop_id,
                },
            );
        }

        // 9. 用原始 buf 重建流
        let stream =
            futures::stream::once(futures::future::ready(Ok(Bytes::from(buf)))).chain(raw_stream);

        Ok(ChatResponse {
            stream: Box::pin(ResponseStream::new(
                Box::pin(stream),
                guard,
                SessionHandle {
                    client: self.accounts.client_clone().await,
                    token,
                    session_id,
                    message_id: stop_id,
                    sessions: self.active_sessions.clone(),
                },
                account_id.clone(),
            )),
        })
    }
}

// ── ChatML 解析与历史拆分 ──────────────────────────────────────────────

/// 按字符数切分 prompt 为 chunk（不感知标签边界）
fn split_prompt_chunks(prompt: &str, chunk_size: usize) -> Vec<String> {
    prompt
        .chars()
        .collect::<Vec<_>>()
        .chunks(chunk_size)
        .map(|c| c.iter().collect())
        .collect()
}

struct ChatBlock {
    role: String,
    content: String,
}

fn role_tag(role: &str) -> String {
    let mut r = role.to_string();
    if let Some(c) = r.get_mut(0..1) {
        c.make_ascii_uppercase();
    }
    format!("<｜{}｜>", r)
}

/// 解析 DeepSeek 原生标签格式的 prompt 为结构化块
fn parse_native_blocks(prompt: &str) -> Vec<ChatBlock> {
    let mut blocks = Vec::new();
    let mut pos = 0;
    while let Some(start_idx) = prompt[pos..].find(TAG_START) {
        let abs_start = pos + start_idx;
        let role_start = abs_start + TAG_START.len();
        let role_end = match prompt[role_start..].find(TAG_END) {
            Some(i) => role_start + i,
            None => break,
        };
        let role = prompt[role_start..role_end].trim().to_lowercase();
        let content_start = role_end + TAG_END.len();
        let content_end = prompt[content_start..]
            .find(TAG_START)
            .map_or(prompt.len(), |i| content_start + i);
        let content = prompt[content_start..content_end]
            .trim_end_matches('\n')
            .to_string();
        blocks.push(ChatBlock { role, content });
        pos = content_end;
    }
    blocks
}

/// 拆分 prompt 为 inline_prompt 和 history_content
///
/// 优先策略：找到最后一个 `<｜Assistant｜>` 块，
/// - inline = 仅该 assistant 块
/// - history = 其余所有块，包装为 [file content end] … [file content begin] 格式上传
fn split_history_prompt(prompt: &str) -> (String, String) {
    let blocks = parse_native_blocks(prompt);

    if let Some(ast_idx) = blocks.iter().rposition(|b| b.role == "assistant") {
        let mut inline = String::new();
        inline.push_str(&role_tag(&blocks[ast_idx].role));
        inline.push_str(&blocks[ast_idx].content);
        inline.push('\n');

        let mut history = String::new();
        history.push_str("[file content end]\n\n");
        for block in &blocks[..ast_idx] {
            history.push_str(&role_tag(&block.role));
            history.push_str(&block.content);
            history.push('\n');
        }
        history.push_str("[file name]: IGNORE\n[file content begin]\n");

        return (inline, history);
    }

    // 没有 assistant 块（理论不应发生），完整 prompt 内联
    (prompt.to_string(), String::new())
}
