//! OpenAI 响应转换 —— 将 StreamEvent 流映射为 OpenAI 响应格式
//!
//! 数据流：converter -> tool_parser -> repair -> stop_detect
//! - 仅 THINK / RESPONSE 片段映射到用户可见文本
//! - obfuscation 在最终 SSE 序列化阶段动态注入

mod converter;
mod tool_parser;

pub(crate) use tool_parser::TagConfig;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use log::{debug, info, trace, warn};
use pin_project_lite::pin_project;
use rand::RngExt;
use tokio::time::Sleep;

use ds_core::StreamEvent;

use crate::openai_adapter::{
    OpenAIAdapterError,
    types::{
        ChatCompletionsResponse, ChatCompletionsResponseChunk, Choice, ChunkChoice, Delta,
        FunctionCall, MessageResponse, ToolCall, Usage,
    },
};

static CHATCMPL_ID_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_chatcmpl_id() -> String {
    let n = CHATCMPL_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("chatcmpl-{:016x}", n)
}

pub(crate) fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

const OBFUSCATION_TARGET_LEN: usize = 512;
const OBFUSCATION_MIN_PAD: usize = 16;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);
const FINISH_STOP: &str = "stop";
const FINISH_TOOL_CALLS: &str = "tool_calls";

fn random_padding(len: usize) -> String {
    if len == 0 {
        return String::new();
    }
    let byte_len = (len * 3).div_ceil(4);
    let mut bytes = vec![0u8; byte_len];
    rand::rng().fill(&mut bytes);
    let s = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
    s[..len].to_string()
}

pub(crate) fn sse_serialize(
    chunk: &ChatCompletionsResponseChunk,
) -> Result<Bytes, OpenAIAdapterError> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(b"data: ");
    serde_json::to_writer(&mut buf, chunk).map_err(OpenAIAdapterError::from)?;
    buf.extend_from_slice(b"\n\n");
    Ok(Bytes::from(buf))
}

fn find_stop_pos(content: &str, stop: &[String]) -> Option<usize> {
    stop.iter().filter_map(|s| content.find(s)).min()
}

/// RepairStream 内部使用的流类型
type ChunkStream =
    Pin<Box<dyn Stream<Item = Result<ChatCompletionsResponseChunk, OpenAIAdapterError>> + Send>>;

/// 工具调用修复闭包类型
pub(crate) type RepairFn = Arc<
    dyn Fn(
            String,
        )
            -> Pin<Box<dyn Future<Output = Result<Vec<ToolCall>, OpenAIAdapterError>> + Send>>
        + Send
        + Sync,
>;

/// 执行 tool_calls 修复：将 StreamEvent 流中的 ContentDelta 提取为结构化 ToolCall
pub(crate) async fn execute_tool_repair(
    stream: Pin<Box<dyn Stream<Item = Result<StreamEvent, ds_core::CoreError>> + Send>>,
    tag_config: &TagConfig,
) -> Result<Vec<ToolCall>, OpenAIAdapterError> {
    use futures::StreamExt;
    let mut stream = stream;

    let mut text = String::new();
    while let Some(event) = stream.next().await {
        match event.map_err(OpenAIAdapterError::from)? {
            StreamEvent::ContentDelta { content } => {
                text.push_str(&content);
                if text.len() > tool_parser::MAX_XML_BUF_LEN {
                    return Err(OpenAIAdapterError::Internal(
                        "修复模型输出过长，放弃修复".into(),
                    ));
                }
            }
            StreamEvent::Done { .. } => break,
            _ => {}
        }
    }

    let trimmed = text.trim();
    if trimmed == "[]" || trimmed == "{}" {
        return Err(OpenAIAdapterError::Internal("修复模型返回空结果".into()));
    }

    // 修复模型应输出 per-tool XML 标签格式，直接解析
    let (calls, _) = tool_parser::parse_tool_calls_with(&text, tag_config).ok_or_else(|| {
        OpenAIAdapterError::Internal(format!(
            "修复模型返回无法解析为工具调用: {}",
            &text[..text.len().min(200)]
        ))
    })?;

    Ok(calls)
}

enum RepairState {
    Forwarding,
    Repairing {
        future: Pin<Box<dyn Future<Output = Result<Vec<ToolCall>, OpenAIAdapterError>> + Send>>,
    },
    RepairFailed(String),
    Done,
}

pin_project! {
    /// 工具调用修复流：在 ToolCallStream 之后、StopDetectStream 之前
    ///
    /// 当 ToolCallStream 返回 Err(ToolCallRepairNeeded) 时，
    /// 丢弃上游流（释放账号），通过 repair_fn 发起修复请求，
    /// 将修复后的 tool_calls 发送给客户端。
    struct RepairStream {
        #[pin]
        inner: Option<ChunkStream>,
        repair_fn: Option<RepairFn>,
        state: RepairState,
        model: String,
        #[pin]
        keepalive_deadline: Sleep,
    }
}

impl RepairStream {
    fn new(inner: ChunkStream, repair_fn: RepairFn, model: String) -> Self {
        Self {
            inner: Some(inner),
            repair_fn: Some(repair_fn),
            state: RepairState::Forwarding,
            model,
            keepalive_deadline: tokio::time::sleep_until(
                tokio::time::Instant::now() + KEEPALIVE_INTERVAL,
            ),
        }
    }
}

impl Stream for RepairStream {
    type Item = Result<ChatCompletionsResponseChunk, OpenAIAdapterError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.state {
                RepairState::Forwarding => {
                    match this.inner.as_mut().as_pin_mut().map(|p| p.poll_next(cx)) {
                        Some(Poll::Ready(Some(Ok(chunk)))) => {
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                        Some(Poll::Ready(Some(Err(OpenAIAdapterError::ToolCallRepairNeeded(
                            tool_text,
                        ))))) => {
                            warn!(
                                target: "adapter",
                                "RepairStream 捕获修复请求: len={}",
                                tool_text.len()
                            );
                            trace!(target: "adapter", ">>> repair: accepting tool_text len={}", tool_text.len());
                            drop(this.inner.as_mut().get_mut().take());
                            if let Some(f) = this.repair_fn.take() {
                                let future = f(tool_text);
                                *this.state = RepairState::Repairing { future };
                            } else {
                                *this.state =
                                    RepairState::RepairFailed("no repair function".into());
                            }
                            continue;
                        }
                        Some(Poll::Ready(Some(Err(e)))) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Some(Poll::Ready(None)) | None => {
                            return Poll::Ready(None);
                        }
                        Some(Poll::Pending) => {
                            return Poll::Pending;
                        }
                    }
                }

                RepairState::Repairing { future } => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(calls)) => {
                        info!(
                            target: "adapter",
                            "tool_calls 修复成功: {} 个工具调用",
                            calls.len()
                        );
                        trace!(target: "adapter", ">>> repair: success {} calls", calls.len());
                        *this.state = RepairState::Done;
                        return Poll::Ready(Some(Ok(converter::make_chunk(
                            this.model,
                            Delta {
                                tool_calls: Some(calls),
                                ..Default::default()
                            },
                            Some(FINISH_TOOL_CALLS),
                        ))));
                    }
                    Poll::Ready(Err(e)) => {
                        warn!(target: "adapter", "tool_calls 修复失败: {}", e);
                        *this.state = RepairState::RepairFailed(format!("修复失败: {}", e));
                        continue;
                    }
                    Poll::Pending => {
                        if this.keepalive_deadline.as_mut().poll(cx).is_ready() {
                            trace!(target: "adapter", ">>> keepalive(repair): 发送空工具增量");
                            this.keepalive_deadline
                                .as_mut()
                                .reset(tokio::time::Instant::now() + KEEPALIVE_INTERVAL);
                            return Poll::Ready(Some(Ok(ChatCompletionsResponseChunk {
                                id: "chatcmpl-keepalive".into(),
                                object: "chat.completion.chunk",
                                created: 0,
                                model: this.model.clone(),
                                choices: vec![ChunkChoice {
                                    index: 0,
                                    delta: Delta {
                                        tool_calls: Some(vec![ToolCall {
                                            id: String::new(),
                                            ty: "function".into(),
                                            function: Some(FunctionCall {
                                                name: String::new(),
                                                arguments: String::new(),
                                            }),
                                            custom: None,
                                            index: 0,
                                        }]),
                                        ..Default::default()
                                    },
                                    finish_reason: None,
                                    logprobs: None,
                                }],
                                usage: None,
                                service_tier: None,
                                system_fingerprint: None,
                            })));
                        }
                        return Poll::Pending;
                    }
                },

                RepairState::RepairFailed(msg) => {
                    let msg = std::mem::take(msg);
                    return Poll::Ready(Some(Err(OpenAIAdapterError::Internal(msg))));
                }

                RepairState::Done => return Poll::Ready(None),
            }
        }
    }
}

pin_project! {
    struct StopDetectStream<S> {
        #[pin]
        inner: S,
        stop: Vec<String>,
        stopped: bool,
        sent_len: usize,
        buffer: String,
        include_obfuscation: bool,
    }
}

impl<S> Stream for StopDetectStream<S>
where
    S: Stream<Item = Result<ChatCompletionsResponseChunk, OpenAIAdapterError>>,
{
    type Item = Result<ChatCompletionsResponseChunk, OpenAIAdapterError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(mut chunk))) => {
                    if *this.stopped {
                        if chunk.choices.is_empty() && chunk.usage.is_some() {
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                        // 允许 finish_reason 从 stop 升级为 tool_calls
                        if let Some(choice) = chunk.choices.first_mut()
                            && choice.delta.content.is_none()
                            && choice.delta.reasoning_content.is_none()
                            && choice.delta.tool_calls.is_none()
                            && choice.finish_reason == Some(FINISH_TOOL_CALLS)
                        {
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                        continue;
                    }

                    if let Some(choice) = chunk.choices.first_mut()
                        && let Some(ref content) = choice.delta.content
                    {
                        this.buffer.push_str(content);
                        if let Some(pos) = find_stop_pos(this.buffer, this.stop) {
                            trace!(target: "adapter", ">>> stop: truncate at {}", pos);
                            let truncated = &this.buffer[*this.sent_len..pos];
                            if truncated.is_empty() {
                                choice.delta.content = None;
                            } else {
                                choice.delta.content = Some(truncated.to_string());
                            }
                            choice.finish_reason = Some(FINISH_STOP);
                            *this.stopped = true;
                            this.buffer.clear();
                            *this.sent_len = pos;
                        } else {
                            *this.sent_len = this.buffer.len();
                        }
                    }
                    if *this.include_obfuscation && !chunk.choices.is_empty() {
                        let without = serde_json::to_string(&chunk)
                            .map_err(|e| OpenAIAdapterError::Internal(format!("json: {}", e)))?;
                        let overhead = r#","obfuscation":"""#.len();
                        let pad_len = if without.len() + overhead < OBFUSCATION_TARGET_LEN {
                            OBFUSCATION_TARGET_LEN - without.len() - overhead
                        } else {
                            OBFUSCATION_MIN_PAD
                        };
                        if let Some(choice) = chunk.choices.first_mut() {
                            choice.delta.obfuscation = Some(random_padding(pad_len));
                        }
                    }
                    return Poll::Ready(Some(Ok(chunk)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// 流式响应参数（减少 stream() 参数个数）
pub(crate) struct StreamCfg {
    pub include_usage: bool,
    pub include_obfuscation: bool,
    pub stop: Vec<String>,
    pub prompt_tokens: u32,
    pub repair_fn: Option<RepairFn>,
    pub tag_config: Arc<TagConfig>,
}

/// 流式响应：把 StreamEvent 流转换为 ChatCompletionsResponseChunk 流
pub(crate) fn stream<S>(ds_stream: S, model: String, cfg: StreamCfg) -> ChunkStream
where
    S: Stream<Item = Result<StreamEvent, ds_core::CoreError>> + Send + 'static,
{
    debug!(
        target: "adapter",
        "构建流式响应: model={}, include_usage={}, include_obfuscation={}, stop_count={}, repair={}",
        model, cfg.include_usage, cfg.include_obfuscation, cfg.stop.len(), cfg.repair_fn.is_some()
    );
    let converted = converter::ConverterStream::new(
        ds_stream.map(|r| r.map_err(OpenAIAdapterError::from)),
        model.clone(),
        cfg.include_usage,
        cfg.include_obfuscation,
        cfg.prompt_tokens,
    );
    let tool_parsed = tool_parser::ToolCallStream::new(converted, model.clone(), cfg.tag_config);
    let tool_boxed: Pin<
        Box<dyn Stream<Item = Result<ChatCompletionsResponseChunk, OpenAIAdapterError>> + Send>,
    > = Box::pin(tool_parsed);

    let after_repair: Pin<
        Box<dyn Stream<Item = Result<ChatCompletionsResponseChunk, OpenAIAdapterError>> + Send>,
    > = if let Some(f) = cfg.repair_fn {
        Box::pin(RepairStream::new(tool_boxed, f, model))
    } else {
        tool_boxed
    };

    let stop_detect = StopDetectStream {
        inner: after_repair,
        stop: cfg.stop,
        stopped: false,
        sent_len: 0,
        buffer: String::new(),
        include_obfuscation: cfg.include_obfuscation,
    };
    Box::pin(stop_detect)
}

/// 非流式响应：stream() 的下游收集器，纯重组无特殊逻辑
///
/// 始终保持为 stream() 的流式收集和重组：
/// - 所有核心处理（修复、转换、停止序列）都在 stream() 中完成
/// - 本函数仅将 stream() 的输出事件聚合并重组成单条 ChatCompletionsResponse JSON
/// - 不要在此函数中添加任何独立于 stream() 的处理逻辑
pub(crate) async fn aggregate<S>(
    ds_stream: S,
    model: String,
    cfg: StreamCfg,
) -> Result<ChatCompletionsResponse, OpenAIAdapterError>
where
    S: Stream<Item = Result<StreamEvent, ds_core::CoreError>> + Send + 'static,
{
    debug!(target: "adapter", "构建非流式响应: model={}, stop_count={}", model, cfg.stop.len());
    let chunk_stream = stream(
        ds_stream,
        model.clone(),
        StreamCfg {
            include_usage: true,
            include_obfuscation: false,
            ..cfg
        },
    );
    futures::pin_mut!(chunk_stream);

    let mut id = String::new();
    let mut created = 0u64;
    let mut content = String::new();
    let mut reasoning = String::new();
    let mut tool_calls: Option<Vec<ToolCall>> = None;
    let mut usage = None;
    let mut finish_reason: Option<&'static str> = None;

    while let Some(res) = chunk_stream.next().await {
        let chunk = res?;

        if id.is_empty() {
            id = chunk.id;
            created = chunk.created;
        }

        if let Some(u) = chunk.usage {
            usage = Some(Usage {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
                total_tokens: u.total_tokens,
                prompt_tokens_details: None,
                completion_tokens_details: None,
            });
        }

        if let Some(choice) = chunk.choices.into_iter().next() {
            if finish_reason.is_none() {
                finish_reason = choice.finish_reason;
            }
            if let Some(c) = choice.delta.content {
                content.push_str(&c);
            }
            if let Some(r) = choice.delta.reasoning_content {
                reasoning.push_str(&r);
            }
            if let Some(tc) = choice.delta.tool_calls
                && !tc.is_empty()
            {
                tool_calls = Some(tc);
            }
        }
    }

    let reasoning_content = if reasoning.is_empty() {
        None
    } else {
        Some(reasoning)
    };

    let has_tool_calls = tool_calls.is_some();
    let message_content = if content.is_empty() && !has_tool_calls {
        warn!(
            target: "adapter",
            "聚合响应内容为空: model={}, finish_reason={:?}, has_tool_calls={}, usage={:?}",
            model, finish_reason, tool_calls.is_some(), usage
        );
        None
    } else {
        Some(content)
    };
    let final_reason = if has_tool_calls {
        Some(FINISH_TOOL_CALLS)
    } else {
        finish_reason
    };

    let completion = ChatCompletionsResponse {
        id,
        object: "chat.completion",
        created,
        model,
        choices: vec![Choice {
            index: 0,
            message: MessageResponse {
                role: "assistant",
                content: message_content,
                reasoning_content,
                refusal: None,
                annotations: None,
                audio: None,
                function_call: None,
                tool_calls,
            },
            finish_reason: final_reason,
            logprobs: None,
        }],
        usage,
        service_tier: None,
        system_fingerprint: None,
    };

    debug!(
        target: "adapter",
        "非流式响应聚合完成: finish_reason={:?}, has_tool_calls={}, usage={:?}",
        completion.choices[0].finish_reason,
        completion.choices[0].message.tool_calls.is_some(),
        completion.usage
    );
    Ok(completion)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;

    use ds_core::StreamEvent;

    use super::*;

    fn default_tag_config() -> Arc<TagConfig> {
        // 测试用的 TagConfig，包含测试中用到的所有工具名
        Arc::new(TagConfig {
            tool_names: vec![
                "get_weather".into(),
                "f".into(),
                "astrbot_execute_shell".into(),
                "get_time".into(),
            ],
        })
    }

    /// 生成 per-tool XML 格式的工具调用文本
    fn tool_span(name: &str, args: &str) -> String {
        format!("<{name}>{args}</{name}>")
    }

    fn make_event_stream(
        pieces: &[(&str, &str)],
        usage_tokens: Option<u32>,
    ) -> Vec<Result<StreamEvent, ds_core::CoreError>> {
        let mut events: Vec<StreamEvent> = Vec::new();
        let mut has_think = false;
        let mut has_content = false;

        for (content, frag_type) in pieces {
            match *frag_type {
                "THINK" => {
                    if !has_think {
                        events.push(StreamEvent::ThinkStart);
                        has_think = true;
                    }
                    events.push(StreamEvent::ThinkDelta {
                        content: content.to_string(),
                    });
                }
                _ => {
                    if !has_content {
                        events.push(StreamEvent::ContentStart);
                        has_content = true;
                    }
                    events.push(StreamEvent::ContentDelta {
                        content: content.to_string(),
                    });
                }
            }
        }

        let finish = if has_content {
            Some("stop".to_string())
        } else {
            None
        };
        events.push(StreamEvent::Done {
            finish_reason: finish,
            accumulated_token_usage: usage_tokens,
        });

        events.into_iter().map(Ok).collect()
    }

    fn meta_event() -> Vec<Result<StreamEvent, ds_core::CoreError>> {
        vec![Ok(StreamEvent::Meta {
            account_id: "test".into(),
        })]
    }

    fn make_full_stream(
        pieces: &[(&str, &str)],
        usage_tokens: Option<u32>,
    ) -> Vec<Result<StreamEvent, ds_core::CoreError>> {
        let mut events = meta_event();
        events.extend(make_event_stream(pieces, usage_tokens));
        events
    }

    #[tokio::test]
    async fn aggregate_plain_text() {
        let events = make_full_stream(&[("hello world", "RESPONSE")], Some(41));
        let stream = futures::stream::iter(events);
        let resp = aggregate(
            stream,
            "deepseek-default".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.object, "chat.completion");
        assert_eq!(resp.model, "deepseek-default");
        let msg = &resp.choices[0].message;
        assert_eq!(msg.content.as_deref(), Some("hello world"));
        assert_eq!(resp.choices[0].finish_reason, Some("stop"));
        assert_eq!(resp.usage.as_ref().unwrap().completion_tokens, 41);
    }

    #[tokio::test]
    async fn aggregate_thinking() {
        let events = make_full_stream(&[("thinking", "THINK"), ("answer", "RESPONSE")], None);
        let stream = futures::stream::iter(events);
        let resp = aggregate(
            stream,
            "deepseek-expert".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )
        .await
        .unwrap();
        let msg = &resp.choices[0].message;
        assert_eq!(msg.reasoning_content.as_deref(), Some("thinking"));
        assert_eq!(msg.content.as_deref(), Some("answer"));
        assert_eq!(resp.choices[0].finish_reason, Some("stop"));
    }

    #[tokio::test]
    async fn aggregate_tool_calls() {
        let tool_xml = tool_span("get_weather", r#"{"city": "beijing"}"#);
        let events = make_full_stream(&[(&tool_xml, "RESPONSE")], None);
        let stream = futures::stream::iter(events);
        let resp = aggregate(
            stream,
            "deepseek-default".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )
        .await
        .unwrap();
        let msg = &resp.choices[0].message;
        assert_eq!(msg.content.as_deref(), Some(""));
        let calls = msg.tool_calls.as_ref().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].ty, "function");
        assert_eq!(calls[0].function.as_ref().unwrap().name, "get_weather");
        assert_eq!(
            calls[0].function.as_ref().unwrap().arguments,
            r#"{"city":"beijing"}"#
        );
        assert_eq!(resp.choices[0].finish_reason, Some("tool_calls"));
    }

    use std::pin::Pin;

    fn to_bytes_stream(
        st: ChunkStream,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, OpenAIAdapterError>> + Send>> {
        Box::pin(st.map(|r| r.and_then(|c| sse_serialize(&c))))
    }

    async fn collect_chunks(
        st: Pin<Box<dyn Stream<Item = Result<Bytes, OpenAIAdapterError>> + Send>>,
    ) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        let mut st = st;
        while let Some(res) = st.next().await {
            let text = String::from_utf8(res.unwrap().to_vec()).unwrap();
            let json = text
                .strip_prefix("data: ")
                .unwrap()
                .strip_suffix("\n\n")
                .unwrap();
            out.push(serde_json::from_str(json).unwrap());
        }
        out
    }

    #[tokio::test]
    async fn stream_plain_text() {
        let events = make_full_stream(&[("hi", "RESPONSE")], None);
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "m".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        assert!(chunks.len() >= 2);
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        let all_content: String = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert_eq!(all_content, "hi");
        assert_eq!(
            chunks.last().unwrap()["choices"][0]["finish_reason"],
            "stop"
        );
    }

    #[tokio::test]
    async fn stream_include_usage() {
        let events = make_full_stream(&[("x", "RESPONSE")], Some(12));
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "m".into(),
            super::StreamCfg {
                include_usage: true,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        assert!(chunks.len() >= 2);
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        let all_content: String = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert_eq!(all_content, "x");
        let usage_chunk = chunks
            .iter()
            .find(|c| c["usage"]["completion_tokens"].as_i64() == Some(12));
        assert!(usage_chunk.is_some(), "should have usage chunk");
        let finish_chunk = chunks.iter().rev().find(|c| {
            c["choices"].as_array().map_or(false, |a| !a.is_empty())
                && c["choices"][0]["finish_reason"].as_str().is_some()
        });
        assert_eq!(finish_chunk.unwrap()["choices"][0]["finish_reason"], "stop");
    }

    #[tokio::test]
    async fn stream_tool_calls() {
        let tool_xml = tool_span("f", r#"{}"#);
        let events = make_full_stream(&[(&tool_xml, "RESPONSE")], None);
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "m".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        assert!(chunks.len() >= 2);
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        let has_tool_calls = chunks
            .iter()
            .any(|c| c["choices"][0]["delta"]["tool_calls"].as_array().is_some());
        assert!(has_tool_calls, "should have a tool_calls chunk");
        let all_content: String = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert!(
            !all_content.contains("<f>"),
            "content should not contain tool_calls tags"
        );
        assert_eq!(
            chunks.last().unwrap()["choices"][0]["finish_reason"],
            "tool_calls"
        );
    }

    #[tokio::test]
    async fn stream_fragmented_tool_calls_with_thinking() {
        let tool_xml = tool_span("get_weather", r#"{"city": "北京"}"#);
        let events = make_full_stream(&[("思考中", "THINK"), (&tool_xml, "RESPONSE")], None);
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "m".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        assert!(chunks.len() >= 3);
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        let all_reasoning: String = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["reasoning_content"].as_str())
            .collect();
        assert!(all_reasoning.contains("思考中"), "should contain 思考中");
        let has_tool_calls = chunks
            .iter()
            .any(|c| c["choices"][0]["delta"]["tool_calls"].as_array().is_some());
        assert!(has_tool_calls, "should have a tool_calls chunk");
        let tc_chunk = chunks
            .iter()
            .find(|c| c["choices"][0]["delta"]["tool_calls"].as_array().is_some())
            .unwrap();
        let calls = tc_chunk["choices"][0]["delta"]["tool_calls"]
            .as_array()
            .unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["function"]["name"], "get_weather");
        assert_eq!(calls[0]["function"]["arguments"], r#"{"city":"北京"}"#);
        assert_eq!(
            chunks.last().unwrap()["choices"][0]["finish_reason"],
            "tool_calls"
        );
    }

    #[tokio::test]
    async fn stream_tool_calls_with_leading_text_fragmented() {
        let tool_xml = tool_span(
            "astrbot_execute_shell",
            r#"{"command": "cat /data/astrbot/skills/doubao-image-gen/SKILL.md"}"#,
        );
        let events = make_full_stream(
            &[
                ("好的，我来帮你用豆包生成图片。", "RESPONSE"),
                (&tool_xml, "RESPONSE"),
            ],
            None,
        );
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "m".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        assert!(chunks.len() >= 2);
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        let all_content: String = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert!(
            all_content.contains("好的，我来帮你用豆包生成图片"),
            "should contain leading text, got {all_content:?}"
        );
        let has_tool_calls = chunks
            .iter()
            .any(|c| c["choices"][0]["delta"]["tool_calls"].as_array().is_some());
        assert!(has_tool_calls, "should have a tool_calls chunk");
        let tc_chunk = chunks
            .iter()
            .find(|c| c["choices"][0]["delta"]["tool_calls"].as_array().is_some())
            .unwrap();
        let calls = tc_chunk["choices"][0]["delta"]["tool_calls"]
            .as_array()
            .unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["function"]["name"], "astrbot_execute_shell");
        let last = chunks.last().unwrap();
        assert_eq!(last["choices"][0]["finish_reason"], "tool_calls");
    }

    #[tokio::test]
    async fn stream_tool_calls_no_leading_text() {
        let tool_xml = tool_span("get_weather", r#"{"city": "beijing"}"#);
        let events = make_full_stream(&[(&tool_xml, "RESPONSE")], None);
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "deepseek-default".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        assert!(
            chunks.len() >= 2,
            "expected at least 2 chunks, got {}",
            chunks.len()
        );
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        let tc_idx = chunks
            .iter()
            .position(|c| c["choices"][0]["delta"]["tool_calls"].as_array().is_some())
            .expect("should have a chunk with tool_calls");
        let tc_chunk = &chunks[tc_idx];
        let calls = tc_chunk["choices"][0]["delta"]["tool_calls"]
            .as_array()
            .unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["function"]["name"], "get_weather");
        assert_eq!(calls[0]["function"]["arguments"], r#"{"city":"beijing"}"#);
        let last = chunks.last().unwrap();
        assert_eq!(
            last["choices"][0]["finish_reason"], "tool_calls",
            "finish_reason should be tool_calls, got {:?}",
            last["choices"][0]["finish_reason"]
        );
    }

    #[tokio::test]
    async fn stream_with_tool_search_and_open() {
        let events = make_full_stream(&[("hello", "RESPONSE")], None);
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "m".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        assert!(chunks.len() >= 2);
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        let all_content: String = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert_eq!(all_content, "hello");
        assert_eq!(
            chunks.last().unwrap()["choices"][0]["finish_reason"],
            "stop"
        );
    }

    #[tokio::test]
    async fn stream_include_obfuscation() {
        let events = make_full_stream(
            &[("这是一段足够长的中文文本用于测试混淆", "RESPONSE")],
            None,
        );
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "m".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: true,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        assert!(chunks.len() >= 2);
        for c in &chunks {
            if c["choices"][0]["delta"]["content"].as_str().is_some()
                || c["choices"][0]["finish_reason"].as_str().is_some()
            {
                assert!(
                    c["choices"][0]["delta"]["obfuscation"].as_str().is_some(),
                    "chunk with content or finish_reason should have obfuscation"
                );
                let len = serde_json::to_string(c).unwrap().len();
                assert!(
                    len >= 490 && len <= 530,
                    "chunk len {} out of expected 490..=530 range",
                    len
                );
            }
        }
        let all_content: String = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert!(
            all_content.contains("足够长的中文文本"),
            "should contain expected text, got {all_content:?}"
        );
        assert_eq!(
            chunks.last().unwrap()["choices"][0]["finish_reason"],
            "stop"
        );
    }

    #[tokio::test]
    async fn aggregate_tool_calls_with_leading_text() {
        let tool_xml = tool_span("get_weather", r#"{"city": "beijing"}"#);
        let events = make_full_stream(
            &[("好的，我来帮你。", "RESPONSE"), (&tool_xml, "RESPONSE")],
            None,
        );
        let stream = futures::stream::iter(events);
        let resp = aggregate(
            stream,
            "deepseek-default".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )
        .await
        .unwrap();
        let msg = &resp.choices[0].message;
        assert_eq!(msg.content.as_deref(), Some("好的，我来帮你。"));
        let calls = msg.tool_calls.as_ref().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.as_ref().unwrap().name, "get_weather");
        assert_eq!(
            calls[0].function.as_ref().unwrap().arguments,
            r#"{"city":"beijing"}"#
        );
        assert_eq!(resp.choices[0].finish_reason, Some("tool_calls"));
    }

    #[tokio::test]
    async fn aggregate_tool_calls_multi_chunk_fragments() {
        let tool_xml = tool_span("f", r#"{}"#);
        let events = make_full_stream(
            &[("让我来查一下。", "RESPONSE"), (&tool_xml, "RESPONSE")],
            None,
        );
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "m".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        let has_tool_calls = chunks
            .iter()
            .any(|c| c["choices"][0]["delta"]["tool_calls"].as_array().is_some());
        assert!(has_tool_calls, "should have a tool_calls chunk but didn't");
        let last = chunks.last().unwrap();
        assert_eq!(last["choices"][0]["finish_reason"], "tool_calls");
    }

    #[tokio::test]
    async fn stream_tool_calls_with_thinking_then_leading_text_then_fragmented_json() {
        let tool_xml = tool_span("get_weather", r#"{"city": "beijing"}"#);
        let events = make_full_stream(
            &[
                ("用户要查天气，我需要调用工具", "THINK"),
                ("好的，我来帮你查一下。", "RESPONSE"),
                (&tool_xml, "RESPONSE"),
            ],
            None,
        );
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "m".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        let has_tool_calls = chunks
            .iter()
            .any(|c| c["choices"][0]["delta"]["tool_calls"].as_array().is_some());
        assert!(has_tool_calls, "should have a tool_calls chunk but didn't");
        let last = chunks.last().unwrap();
        assert_eq!(last["choices"][0]["finish_reason"], "tool_calls");
    }

    #[tokio::test]
    async fn stream_tool_calls_json_split_right_after_tag() {
        let tool_xml = tool_span("f", r#"{}"#);
        let events = make_full_stream(&[("好的。", "RESPONSE"), (&tool_xml, "RESPONSE")], None);
        let stream = futures::stream::iter(events);
        let chunks = collect_chunks(to_bytes_stream(super::stream(
            stream,
            "m".into(),
            super::StreamCfg {
                include_usage: false,
                include_obfuscation: false,
                stop: vec![],
                prompt_tokens: 0,
                repair_fn: None,
                tag_config: default_tag_config(),
            },
        )))
        .await;
        let has_tool_calls = chunks
            .iter()
            .any(|c| c["choices"][0]["delta"]["tool_calls"].as_array().is_some());
        assert!(has_tool_calls, "should have a tool_calls chunk but didn't");
        let last = chunks.last().unwrap();
        assert_eq!(last["choices"][0]["finish_reason"], "tool_calls");
    }
}
