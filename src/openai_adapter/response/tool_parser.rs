//! 工具调用解析 —— per-tool XML 标签检测 `<tool_name>{json}</tool_name>`
//!
//! 移植自 deepseek-pp 的 per-tool XML 标签策略，替代原 `<|tool▁calls▁begin|>` 固定标签。
//! 每个工具使用独立的 XML 标签，标签名即工具名，标签体为 JSON 参数对象。
//!
//! 算法核心：
//! - Normal 状态：扫描文本寻找 `<tool_name>` 开标签，未找到则释放安全部分，
//!   保留可能是部分标签的尾部
//! - Suppressing 状态：收集标签体直到 `</tool_name>` 闭标签，解析 JSON
//! - 支持流中顺序多个不同工具的调用

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use pin_project_lite::pin_project;

use log::{debug, trace, warn};

use crate::openai_adapter::OpenAIAdapterError;
use crate::openai_adapter::types::{
    ChatCompletionsResponseChunk, ChunkChoice, Delta, FunctionCall, ToolCall,
};

static CALL_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
pub(crate) const MAX_XML_BUF_LEN: usize = 64 * 1024;
const PARTIAL_TAG_WHITESPACE_LIMIT: usize = 8;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);

/// 工具标签配置：承载当前请求可识别的工具名列表
#[derive(Debug, Clone, Default)]
pub struct TagConfig {
    /// 可识别的工具名列表（来自请求的 tools 定义 + 配置的额外工具名）
    pub tool_names: Vec<String>,
}

impl TagConfig {
    /// 从配置的额外工具名构建（用于无请求上下文的场景，如测试）
    pub fn from_config(cfg: &crate::config::ToolCallTagConfig) -> Self {
        Self {
            tool_names: cfg.extra_tool_names.clone(),
        }
    }

    /// 从请求的工具定义和配置的额外工具名合并构建
    pub fn from_request(
        req: &crate::openai_adapter::types::ChatCompletionsRequest,
        extra: &[String],
    ) -> Self {
        let mut names: Vec<String> = req
            .tools
            .as_ref()
            .map(|tools| {
                tools
                    .iter()
                    .filter_map(|t| t.function.as_ref().map(|f| f.name.clone()))
                    .collect()
            })
            .unwrap_or_default();
        for name in extra {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        Self { tool_names: names }
    }
}

// ── XML 标签检测（移植自 deepseek-pp core/tool/xml-tags.ts）──────────────

/// XML 工具标签匹配结果
struct XmlToolTagMatch {
    /// '<' 的字节位置
    index: usize,
    /// '>' 之后的位置
    end_index: usize,
    /// 工具名
    name: String,
    /// 完整标签文本
    raw: String,
    /// 是否为闭标签
    closing: bool,
}

/// 在 text 中从 from_index 开始查找第一个完整的工具标签
fn find_first_xml_tool_tag(
    text: &str,
    tool_names: &[String],
    closing: bool,
    from_index: usize,
) -> Option<XmlToolTagMatch> {
    if text.is_empty() || tool_names.is_empty() {
        return None;
    }

    let mut search_from = from_index;
    while search_from < text.len() {
        let index = text[search_from..].find('<')? + search_from;
        let tag_end = text[index + 1..].find('>')? + index + 1;

        if let Some(parsed) = parse_complete_xml_tool_tag(text, index, tag_end, tool_names)
            && parsed.closing == closing
        {
            return Some(parsed);
        }

        // 跳过此标签，处理标签内嵌套 '<' 的情况
        let candidate = &text[index..=tag_end];
        search_from = if candidate[1..].contains('<') {
            index + 1
        } else {
            tag_end + 1
        };
    }

    None
}

fn parse_complete_xml_tool_tag(
    text: &str,
    index: usize,
    tag_end: usize,
    tool_names: &[String],
) -> Option<XmlToolTagMatch> {
    let bytes = text.as_bytes();
    let mut cursor = index + 1;
    cursor = skip_whitespace(bytes, cursor, tag_end);

    let closing = if bytes.get(cursor) == Some(&b'/') {
        cursor += 1;
        cursor = skip_whitespace(bytes, cursor, tag_end);
        true
    } else {
        false
    };

    if cursor >= tag_end || !is_tool_name_start_byte(bytes[cursor]) {
        return None;
    }
    let name_start = cursor;
    cursor += 1;
    while cursor < tag_end && is_tool_name_char_byte(bytes[cursor]) {
        cursor += 1;
    }

    let name = std::str::from_utf8(&bytes[name_start..cursor]).ok()?.to_string();
    if !tool_names.contains(&name) {
        return None;
    }

    cursor = skip_whitespace(bytes, cursor, tag_end);
    if cursor != tag_end {
        return None;
    }

    Some(XmlToolTagMatch {
        index,
        end_index: tag_end + 1,
        name,
        raw: text[index..=tag_end].to_string(),
        closing,
    })
}

/// 检测文本尾部是否可能是工具标签的前缀（用于流式处理）
fn get_partial_xml_tool_tag_tail_length(
    text: &str,
    tool_names: &[String],
    closing: bool,
) -> usize {
    if text.is_empty() || tool_names.is_empty() {
        return 0;
    }

    let max_name_length = tool_names.iter().map(|n| n.len()).max().unwrap_or(0);
    let limit = std::cmp::min(
        text.len(),
        2 + max_name_length + PARTIAL_TAG_WHITESPACE_LIMIT * 2,
    );

    let search_start = floor_char_boundary(text, text.len().saturating_sub(limit));
    let search_range = &text[search_start..];

    let mut search_from = search_range.len();
    while search_from > 0 {
        if let Some(pos) = search_range[..search_from].rfind('<') {
            let tail = &search_range[pos..];
            if is_partial_xml_tool_tag(tail, tool_names, closing) {
                return tail.len();
            }
            search_from = pos;
        } else {
            break;
        }
    }
    0
}

fn is_partial_xml_tool_tag(value: &str, tool_names: &[String], closing: bool) -> bool {
    if !value.starts_with('<') {
        return false;
    }

    let bytes = value.as_bytes();
    let mut cursor = 1;

    let before_slash = skip_limited_whitespace(bytes, cursor);
    if before_slash == bytes.len() {
        return true;
    }
    cursor = before_slash;

    if bytes.get(cursor) == Some(&b'/') {
        if !closing {
            return false;
        }
        cursor += 1;
        let before_name = skip_limited_whitespace(bytes, cursor);
        if before_name == bytes.len() {
            return true;
        }
        cursor = before_name;
    } else if closing {
        return false;
    }

    if cursor >= bytes.len() || !is_tool_name_start_byte(bytes[cursor]) {
        return false;
    }
    let name_start = cursor;
    cursor += 1;
    while cursor < bytes.len() && is_tool_name_char_byte(bytes[cursor]) {
        cursor += 1;
    }

    let typed_name = std::str::from_utf8(&bytes[name_start..cursor]).unwrap_or("");
    if !tool_names.iter().any(|name| name.starts_with(typed_name)) {
        return false;
    }

    let after_name = skip_limited_whitespace(bytes, cursor);
    after_name == bytes.len()
}

fn skip_whitespace(bytes: &[u8], cursor: usize, end: usize) -> usize {
    let mut c = cursor;
    while c < end && is_whitespace_byte(bytes[c]) {
        c += 1;
    }
    c
}

fn skip_limited_whitespace(bytes: &[u8], cursor: usize) -> usize {
    let mut c = cursor;
    let mut count = 0;
    while c < bytes.len() && count < PARTIAL_TAG_WHITESPACE_LIMIT && is_whitespace_byte(bytes[c]) {
        c += 1;
        count += 1;
    }
    c
}

fn is_whitespace_byte(b: u8) -> bool {
    matches!(b, b' ' | b'\n' | b'\r' | b'\t' | 0x0C)
}

fn is_tool_name_start_byte(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_tool_name_char_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-')
}

// ── 公共 API ─────────────────────────────────────────────────────────────

/// 检查文本是否包含任何工具开标签
#[cfg(test)]
pub(crate) fn contains_start_tag_with(s: &str, cfg: &TagConfig) -> bool {
    find_first_xml_tool_tag(s, &cfg.tool_names, false, 0).is_some()
}

/// 解析文本中的所有工具调用
///
/// 扫描所有 `<tool_name>{json}</tool_name>` 模式，返回工具调用列表和
/// 移除工具标签后的剩余文本。同时支持 `<invoke name="...">` legacy 格式。
pub fn parse_tool_calls_with(xml: &str, cfg: &TagConfig) -> Option<(Vec<ToolCall>, String)> {
    if cfg.tool_names.is_empty() {
        return None;
    }

    let mut calls = Vec::new();
    let mut remaining = String::with_capacity(xml.len());
    let mut cursor = 0;

    loop {
        let open_tag = find_first_xml_tool_tag(xml, &cfg.tool_names, false, cursor);
        let Some(open) = open_tag else {
            remaining.push_str(&xml[cursor..]);
            break;
        };

        // 代码块内的标签跳过
        if is_inside_code_fence(xml, open.index) {
            remaining.push_str(&xml[cursor..open.end_index]);
            cursor = open.end_index;
            continue;
        }

        // 添加开标签之前的文本
        remaining.push_str(&xml[cursor..open.index]);

        // 查找匹配的闭标签
        let close_tag = find_first_xml_tool_tag(xml, std::slice::from_ref(&open.name), true, open.end_index);
        let Some(close) = close_tag else {
            // 没有闭标签，尝试解析到文本末尾
            let body = &xml[open.end_index..];
            if let Some(mut call) = parse_tool_call_body(&open.name, body) {
                call.index = calls.len() as u32;
                calls.push(call);
            }
            break;
        };

        let body = &xml[open.end_index..close.index];
        if let Some(mut call) = parse_tool_call_body(&open.name, body) {
            call.index = calls.len() as u32;
            calls.push(call);
        }
        cursor = close.end_index;
    }

    // 也尝试 legacy <invoke> 格式
    if calls.is_empty() {
        if let Some((invoke_calls, invoke_remaining)) = parse_invoke_calls(xml) {
            return Some((invoke_calls, invoke_remaining));
        }
        return None;
    }

    Some((calls, remaining))
}

/// 解析单个工具调用的 JSON body
fn parse_tool_call_body(tool_name: &str, body: &str) -> Option<ToolCall> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Some(make_tool_call(tool_name, serde_json::Value::Object(
            serde_json::Map::new(),
        )));
    }

    // 尝试直接解析
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed)
        && value.is_object()
    {
        return Some(make_tool_call(tool_name, value));
    }

    // 尝试 JSON 修复
    if let Some(repaired) = repair_json(trimmed)
        && let Ok(value) = serde_json::from_str::<serde_json::Value>(&repaired)
        && value.is_object()
    {
        return Some(make_tool_call(tool_name, value));
    }

    None
}

fn make_tool_call(name: &str, args: serde_json::Value) -> ToolCall {
    ToolCall {
        id: next_call_id(),
        ty: "function".to_string(),
        function: Some(FunctionCall {
            name: name.to_string(),
            arguments: serde_json::to_string(&args).unwrap_or_else(|_| "{}".into()),
        }),
        custom: None,
        index: 0,
    }
}

// ── JSON 修复（保留原逻辑）──────────────────────────────────────────────

fn repair_invalid_backslashes(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some(&next)
                    if matches!(next, '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't' | 'u') =>
                {
                    out.push('\\');
                    out.push(next);
                    chars.next();
                }
                Some(&next) => {
                    out.push('\\');
                    out.push('\\');
                    out.push(next);
                    chars.next();
                }
                None => {
                    out.push('\\');
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn repair_unquoted_keys(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 32);
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len();
    let mut i = 0;
    while i < len {
        if (chars[i] == '{' || chars[i] == ',') && i + 1 < len {
            out.push(chars[i]);
            i += 1;
            while i < len && chars[i].is_whitespace() {
                out.push(chars[i]);
                i += 1;
            }
            if i < len && (chars[i].is_alphabetic() || chars[i] == '_') {
                let key_start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                if i < len && chars[i] == ':' {
                    out.push('"');
                    out.extend(&chars[key_start..i]);
                    out.push('"');
                } else {
                    out.extend(&chars[key_start..i]);
                    continue;
                }
            }
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

fn repair_json(s: &str) -> Option<String> {
    let step1 = repair_invalid_backslashes(s);
    if serde_json::from_str::<serde_json::Value>(&step1).is_ok() {
        return Some(step1);
    }
    let step2 = repair_unquoted_keys(&step1);
    if serde_json::from_str::<serde_json::Value>(&step2).is_ok() {
        return Some(step2);
    }
    None
}

// ── Legacy <invoke> 格式回退 ─────────────────────────────────────────────

fn parse_invoke_calls(text: &str) -> Option<(Vec<ToolCall>, String)> {
    use std::collections::BTreeMap;
    let mut calls = Vec::new();
    let mut remaining = String::with_capacity(text.len());
    let mut cursor = 0;
    let lower = text.to_lowercase();

    while let Some(invoke_start) = lower[cursor..].find("<invoke ") {
        let abs_start = cursor + invoke_start;
        remaining.push_str(&text[cursor..abs_start]);

        let name_attr = &text[abs_start..];
        let name_start = name_attr.find("name=\"")? + 6;
        let name_end = name_attr[name_start..].find('"')?;
        let name = &name_attr[name_start..name_start + name_end];
        let close_tag = "</invoke>";
        let rest = &lower[abs_start..];
        let close_pos = rest.find(close_tag)?;
        let invoke_body = &text[abs_start..abs_start + close_pos + close_tag.len()];

        let mut params: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        let mut ppos = 0;
        let body_lower = invoke_body.to_lowercase();
        while let Some(p_start) = body_lower[ppos..].find("<parameter ") {
            let p_abs = ppos + p_start;
            let p_attr = &invoke_body[p_abs..];
            let p_name_start = p_attr.find("name=\"")? + 6;
            let p_name_end = p_attr[p_name_start..].find('"')?;
            let p_name = &p_attr[p_name_start..p_name_start + p_name_end];
            let p_body_start = p_attr.find('>')? + 1;
            let p_close = "</parameter>";
            let p_close_pos = p_attr[p_body_start..].find(p_close)?;
            let p_value = &p_attr[p_body_start..p_body_start + p_close_pos];
            let val: serde_json::Value = serde_json::from_str(p_value.trim())
                .unwrap_or_else(|_| serde_json::Value::String(p_value.to_string()));
            params.insert(p_name.to_string(), val);
            let p_end = p_body_start + p_close_pos + p_close.len();
            ppos += p_start + p_end;
        }

        let arguments = serde_json::to_string(&params).unwrap_or_else(|_| "{}".into());
        calls.push(ToolCall {
            id: next_call_id(),
            ty: "function".to_string(),
            function: Some(FunctionCall {
                name: name.to_string(),
                arguments,
            }),
            custom: None,
            index: calls.len() as u32,
        });
        cursor = abs_start + close_pos + close_tag.len();
    }
    remaining.push_str(&text[cursor..]);

    if calls.is_empty() {
        None
    } else {
        Some((calls, remaining))
    }
}

// ── 辅助函数 ─────────────────────────────────────────────────────────────

fn next_call_id() -> String {
    let n = CALL_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("call_{:016x}", n)
}

fn floor_char_boundary(s: &str, max: usize) -> usize {
    if max >= s.len() {
        return s.len();
    }
    let mut i = max;
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn is_inside_code_fence(xml: &str, tag_pos: usize) -> bool {
    xml[..tag_pos].matches("```").count() % 2 == 1
}

fn make_end_chunk(
    model: &str,
    delta: Delta,
    finish_reason: &'static str,
) -> ChatCompletionsResponseChunk {
    ChatCompletionsResponseChunk {
        id: "chatcmpl-end".to_string(),
        object: "chat.completion.chunk",
        created: 0,
        model: model.to_string(),
        choices: vec![ChunkChoice {
            index: 0,
            delta,
            finish_reason: Some(finish_reason),
            logprobs: None,
        }],
        usage: None,
        service_tier: None,
        system_fingerprint: None,
    }
}

// ── 流式工具调用解析器 ───────────────────────────────────────────────────

#[derive(Debug)]
enum ToolParseState {
    /// 正常状态：扫描文本寻找开标签
    Normal { buffer: String },
    /// 抑制状态：收集标签体直到闭标签
    Suppressing {
        body: String,
        tool_name: String,
        open_tag: String,
    },
}

pin_project! {
    pub struct ToolCallStream<S> {
        #[pin]
        inner: S,
        state: ToolParseState,
        model: String,
        has_tool_calls: bool,
        finish_emitted: bool,
        repair_pending: Option<String>,
        tag_config: Arc<TagConfig>,
        call_index: u32,
        last_keepalive: tokio::time::Instant,
    }
}

impl<S> ToolCallStream<S> {
    pub fn new(inner: S, model: String, tag_config: Arc<TagConfig>) -> Self {
        Self {
            inner,
            state: ToolParseState::Normal {
                buffer: String::new(),
            },
            model,
            has_tool_calls: false,
            finish_emitted: false,
            repair_pending: None,
            tag_config,
            call_index: 0,
            last_keepalive: tokio::time::Instant::now(),
        }
    }
}

impl<S> Stream for ToolCallStream<S>
where
    S: Stream<Item = Result<ChatCompletionsResponseChunk, OpenAIAdapterError>>,
{
    type Item = Result<ChatCompletionsResponseChunk, OpenAIAdapterError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(tool_text) = this.repair_pending.take() {
            debug!(target: "adapter", "tool_parser 发出修复请求");
            return Poll::Ready(Some(Err(OpenAIAdapterError::ToolCallRepairNeeded(
                tool_text,
            ))));
        }

        loop {
            // Suppressing 状态下的 keepalive
            if matches!(&this.state, ToolParseState::Suppressing { .. })
                && this.last_keepalive.elapsed() >= KEEPALIVE_INTERVAL
            {
                trace!(target: "adapter", ">>> keepalive: 发送空工具增量");
                *this.last_keepalive = tokio::time::Instant::now();
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

            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(mut chunk))) => {
                    let Some(choice) = chunk.choices.first_mut() else {
                        return Poll::Ready(Some(Ok(chunk)));
                    };

                    if let Some(content) = choice.delta.content.take() {
                        if content.is_empty() {
                            choice.delta.content = Some(content);
                            return Poll::Ready(Some(Ok(chunk)));
                        }

                        match &mut this.state {
                            ToolParseState::Normal { buffer } => {
                                buffer.push_str(&content);
                                let tool_names = &this.tag_config.tool_names;

                                if let Some(open) = find_first_xml_tool_tag(
                                    buffer,
                                    tool_names,
                                    false,
                                    0,
                                ) {
                                    trace!(target: "adapter", ">>> 检测到开标签: name={}, buf_len={}", open.name, buffer.len());
                                    let before = buffer[..open.index].to_string();
                                    let rest =
                                        std::mem::take(buffer)[open.end_index..].to_string();
                                    *this.state = ToolParseState::Suppressing {
                                        body: rest,
                                        tool_name: open.name,
                                        open_tag: open.raw,
                                    };
                                    choice.delta.content =
                                        if before.is_empty() { None } else { Some(before) };
                                    return Poll::Ready(Some(Ok(chunk)));
                                }

                                // 未找到开标签，检查部分标签尾部
                                let tail_len = get_partial_xml_tool_tag_tail_length(
                                    buffer,
                                    tool_names,
                                    false,
                                );
                                let safe = floor_char_boundary(
                                    buffer,
                                    buffer.len().saturating_sub(tail_len),
                                );
                                if safe > 0 {
                                    choice.delta.content = Some(buffer[..safe].to_string());
                                    buffer.drain(..safe);
                                    return Poll::Ready(Some(Ok(chunk)));
                                }
                                // 全部保留在缓冲区
                                continue;
                            }

                            ToolParseState::Suppressing {
                                body,
                                tool_name,
                                open_tag,
                            } => {
                                body.push_str(&content);
                                if body.len() > MAX_XML_BUF_LEN {
                                    debug!(target: "adapter", "tool_parser 缓冲超限，回退纯文本");
                                    let flushed = format!("{}{}", open_tag, body);
                                    *this.state = ToolParseState::Normal {
                                        buffer: String::new(),
                                    };
                                    choice.delta.content = Some(flushed);
                                    return Poll::Ready(Some(Ok(chunk)));
                                }

                                // 查找闭标签
                                let single_name = vec![tool_name.clone()];
                                if let Some(close) = find_first_xml_tool_tag(
                                    body,
                                    &single_name,
                                    true,
                                    0,
                                ) {
                                    // 先克隆所需字段，再 take body，避免借用冲突
                                    let tool_name_owned = tool_name.clone();
                                    let open_tag_owned = open_tag.clone();
                                    let full_body = std::mem::take(body);
                                    let body_content = full_body[..close.index].to_string();
                                    let rest = full_body[close.end_index..].to_string();

                                    // 解析工具调用
                                    if let Some(mut call) =
                                        parse_tool_call_body(&tool_name_owned, &body_content)
                                    {
                                        debug!(
                                            target: "adapter",
                                            ">>> 解析出工具调用: {}", tool_name_owned
                                        );
                                        call.index = *this.call_index;
                                        *this.call_index += 1;
                                        *this.has_tool_calls = true;
                                        choice.delta.content = None;
                                        choice.delta.tool_calls = Some(vec![call]);
                                        *this.state =
                                            ToolParseState::Normal { buffer: rest };
                                        return Poll::Ready(Some(Ok(chunk)));
                                    }

                                    // 解析失败，请求修复
                                    warn!(
                                        target: "adapter",
                                        "tool_parser 解析失败→请求修复: {}",
                                        tool_name_owned
                                    );
                                    let tool_text = format!(
                                        "{}{}{}",
                                        open_tag_owned, body_content, close.raw
                                    );
                                    *this.state = ToolParseState::Normal { buffer: rest };
                                    *this.repair_pending = Some(tool_text);
                                    return Poll::Ready(Some(Ok(chunk)));
                                }
                                // 未找到闭标签，继续积累
                                continue;
                            }
                        }
                    }

                    // 非 content 字段（role, finish_reason, usage 等）— 内联处理
                    let choice = chunk.choices.first_mut().unwrap();
                    match &mut this.state {
                        ToolParseState::Normal { buffer } => {
                            if choice.finish_reason.is_some() {
                                if !buffer.is_empty() {
                                    choice.delta.content = Some(std::mem::take(buffer));
                                }
                                if *this.has_tool_calls
                                    && choice.finish_reason == Some("stop")
                                {
                                    choice.finish_reason = Some("tool_calls");
                                }
                                *this.finish_emitted = true;
                                return Poll::Ready(Some(Ok(chunk)));
                            }
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                        ToolParseState::Suppressing {
                            body,
                            tool_name,
                            open_tag,
                        } => {
                            if choice.finish_reason.is_some() {
                                let tool_name_owned = tool_name.clone();
                                let open_tag_owned = open_tag.clone();
                                let full_body = std::mem::take(body);
                                // body 中可能已包含闭标签，提取闭标签之前的内容
                                let body_content = match find_first_xml_tool_tag(
                                    &full_body,
                                    std::slice::from_ref(&tool_name_owned),
                                    true,
                                    0,
                                ) {
                                    Some(close) => full_body[..close.index].to_string(),
                                    None => full_body,
                                };
                                if let Some(mut call) =
                                    parse_tool_call_body(&tool_name_owned, &body_content)
                                {
                                    debug!(target: "adapter", "tool_parser 流结束时解析出工具调用: {}", tool_name_owned);
                                    call.index = *this.call_index;
                                    *this.call_index += 1;
                                    *this.has_tool_calls = true;
                                    choice.delta.content = None;
                                    choice.delta.tool_calls = Some(vec![call]);
                                    if choice.finish_reason == Some("stop") {
                                        choice.finish_reason = Some("tool_calls");
                                    }
                                    *this.finish_emitted = true;
                                    *this.state = ToolParseState::Normal {
                                        buffer: String::new(),
                                    };
                                    return Poll::Ready(Some(Ok(chunk)));
                                }
                                warn!(target: "adapter", "tool_parser 流结束→请求修复");
                                let tool_text = format!(
                                    "{}{}</{}>",
                                    open_tag_owned, body_content, tool_name_owned
                                );
                                *this.state = ToolParseState::Normal {
                                    buffer: String::new(),
                                };
                                *this.repair_pending = Some(tool_text);
                                return Poll::Ready(Some(Ok(chunk)));
                            }
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    // 流结束处理 — 内联 handle_stream_end
                    let prev_state = std::mem::replace(
                        this.state,
                        ToolParseState::Normal {
                            buffer: String::new(),
                        },
                    );
                    match prev_state {
                        ToolParseState::Normal { buffer } => {
                            if !buffer.is_empty() {
                                // 边缘情况：缓冲区中可能包含未处理的开标签
                                if let Some((calls, _remaining)) =
                                    parse_tool_calls_with(&buffer, this.tag_config.as_ref())
                                    && !calls.is_empty()
                                {
                                    *this.has_tool_calls = true;
                                    *this.finish_emitted = true;
                                    let chunk = make_end_chunk(
                                        this.model,
                                        Delta {
                                            tool_calls: Some(calls),
                                            ..Default::default()
                                        },
                                        "tool_calls",
                                    );
                                    return Poll::Ready(Some(Ok(chunk)));
                                }

                                let finish = if *this.has_tool_calls {
                                    "tool_calls"
                                } else {
                                    "stop"
                                };
                                let chunk = make_end_chunk(
                                    this.model,
                                    Delta {
                                        content: Some(buffer),
                                        ..Default::default()
                                    },
                                    finish,
                                );
                                return Poll::Ready(Some(Ok(chunk)));
                            }
                            if !*this.finish_emitted {
                                *this.finish_emitted = true;
                                let finish = if *this.has_tool_calls {
                                    "tool_calls"
                                } else {
                                    "stop"
                                };
                                let chunk =
                                    make_end_chunk(this.model, Delta::default(), finish);
                                return Poll::Ready(Some(Ok(chunk)));
                            }
                            return Poll::Ready(None);
                        }
                        ToolParseState::Suppressing {
                            body,
                            tool_name,
                            open_tag,
                        } => {
                            // body 中可能已包含闭标签，提取闭标签之前的内容
                            let body_content = match find_first_xml_tool_tag(
                                &body,
                                std::slice::from_ref(&tool_name),
                                true,
                                0,
                            ) {
                                Some(close) => body[..close.index].to_string(),
                                None => body,
                            };
                            if let Some(mut call) =
                                parse_tool_call_body(&tool_name, &body_content)
                            {
                                debug!(target: "adapter", "tool_parser 流结束时解析出工具调用: {}", tool_name);
                                call.index = *this.call_index;
                                *this.call_index += 1;
                                *this.has_tool_calls = true;
                                let chunk = make_end_chunk(
                                    this.model,
                                    Delta {
                                        tool_calls: Some(vec![call]),
                                        ..Default::default()
                                    },
                                    "tool_calls",
                                );
                                return Poll::Ready(Some(Ok(chunk)));
                            }
                            warn!(target: "adapter", "tool_parser 流结束→请求修复");
                            let tool_text =
                                format!("{}{}</{}>", open_tag, body_content, tool_name);
                            return Poll::Ready(Some(Err(
                                OpenAIAdapterError::ToolCallRepairNeeded(tool_text),
                            )));
                        }
                    }
                }
                Poll::Pending => break,
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(names: &[&str]) -> TagConfig {
        TagConfig {
            tool_names: names.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn tool(name: &str, args: &str) -> String {
        format!("<{name}>{args}</{name}>")
    }

    #[test]
    fn parse_single_tool_call() {
        let xml = tool("get_weather", r#"{"city": "北京"}"#);
        let (calls, remaining) = parse_tool_calls_with(&xml, &cfg(&["get_weather"])).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.as_ref().unwrap().name, "get_weather");
        assert_eq!(
            calls[0].function.as_ref().unwrap().arguments,
            r#"{"city":"北京"}"#
        );
    }

    #[test]
    fn parse_tool_call_with_surrounding_text() {
        let xml = format!("好的，我来查一下。{}", tool("get_weather", r#"{"city": "北京"}"#));
        let (calls, remaining) = parse_tool_calls_with(&xml, &cfg(&["get_weather"])).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(remaining, "好的，我来查一下。");
    }

    #[test]
    fn parse_multiple_tool_calls() {
        let xml = format!(
            "{}{}",
            tool("get_weather", r#"{"city": "北京"}"#),
            tool("get_time", r#"{"tz": "bj"}"#)
        );
        let (calls, remaining) = parse_tool_calls_with(&xml, &cfg(&["get_weather", "get_time"])).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function.as_ref().unwrap().name, "get_weather");
        assert_eq!(calls[1].function.as_ref().unwrap().name, "get_time");
    }

    #[test]
    fn parse_tool_call_empty_args() {
        let xml = tool("do_thing", "{}");
        let (calls, _) = parse_tool_calls_with(&xml, &cfg(&["do_thing"])).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.as_ref().unwrap().arguments, "{}");
    }

    #[test]
    fn parse_tool_call_no_args() {
        let xml = tool("do_thing", "");
        let (calls, _) = parse_tool_calls_with(&xml, &cfg(&["do_thing"])).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.as_ref().unwrap().arguments, "{}");
    }

    #[test]
    fn parse_tool_call_with_unquoted_keys() {
        let xml = tool("get_weather", r#"{city: "北京"}"#);
        let (calls, _) = parse_tool_calls_with(&xml, &cfg(&["get_weather"])).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].function.as_ref().unwrap().arguments,
            r#"{"city":"北京"}"#
        );
    }

    #[test]
    fn parse_tool_call_with_invalid_backslashes() {
        let xml = tool("read_file", r#"{"path": "C:\Users\name"}"#);
        let (calls, _) = parse_tool_calls_with(&xml, &cfg(&["read_file"])).unwrap();
        assert_eq!(calls.len(), 1);
    }

    #[test]
    fn parse_tool_call_with_both_repairs() {
        let xml = tool("read_file", r#"{path: "C:\file"}"#);
        let (calls, _) = parse_tool_calls_with(&xml, &cfg(&["read_file"])).unwrap();
        assert_eq!(calls.len(), 1);
    }

    #[test]
    fn parse_tool_call_inside_code_fence_skipped() {
        let xml = format!(
            "示例：\n```json\n{}\n```",
            tool("get_weather", r#"{"city": "北京"}"#)
        );
        assert!(parse_tool_calls_with(&xml, &cfg(&["get_weather"])).is_none());
    }

    #[test]
    fn parse_tool_call_not_inside_code_fence() {
        let xml = tool("get_weather", r#"{"city": "北京"}"#);
        assert!(parse_tool_calls_with(&xml, &cfg(&["get_weather"])).is_some());
    }

    #[test]
    fn parse_unknown_tool_name_ignored() {
        let xml = tool("unknown_tool", r#"{"x": 1}"#);
        assert!(parse_tool_calls_with(&xml, &cfg(&["get_weather"])).is_none());
    }

    #[test]
    fn parse_tool_call_with_newlines() {
        let xml = format!("<get_weather>\n{{\"city\": \"北京\"}}\n</get_weather>");
        let (calls, _) = parse_tool_calls_with(&xml, &cfg(&["get_weather"])).unwrap();
        assert_eq!(calls.len(), 1);
    }

    #[test]
    fn parse_tool_call_with_whitespace_in_tag() {
        let xml = "< get_weather >{\"city\":\"北京\"}</ get_weather >";
        let (calls, _) = parse_tool_calls_with(&xml, &cfg(&["get_weather"])).unwrap();
        assert_eq!(calls.len(), 1);
    }

    #[test]
    fn parse_tool_calls_interleaved_with_text() {
        let xml = format!(
            "让我先查天气。{}再查时间。{}",
            tool("get_weather", r#"{"city": "北京"}"#),
            tool("get_time", r#"{"tz": "bj"}"#)
        );
        let (calls, remaining) = parse_tool_calls_with(&xml, &cfg(&["get_weather", "get_time"])).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(remaining, "让我先查天气。再查时间。");
    }

    #[test]
    fn parse_invoke_legacy() {
        let xml = r#"<invoke name="get_weather"><parameter name="city" string="true">北京</parameter></invoke>"#;
        let (calls, _) = parse_tool_calls_with(&xml, &cfg(&["get_weather"])).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.as_ref().unwrap().name, "get_weather");
    }

    #[test]
    fn contains_start_tag_detects_open_tag() {
        let xml = tool("get_weather", "{}");
        assert!(contains_start_tag_with(&xml, &cfg(&["get_weather"])));
    }

    #[test]
    fn contains_start_tag_ignores_unknown() {
        let xml = tool("unknown", "{}");
        assert!(!contains_start_tag_with(&xml, &cfg(&["get_weather"])));
    }

    #[test]
    fn contains_start_tag_empty_config() {
        let xml = tool("get_weather", "{}");
        assert!(!contains_start_tag_with(&xml, &cfg(&[])));
    }

    #[test]
    fn repair_backslashes_passes_valid_escapes() {
        assert_eq!(repair_invalid_backslashes(r#"hello\nworld"#), r#"hello\nworld"#);
    }

    #[test]
    fn repair_backslashes_fixes_invalid_escapes() {
        assert_eq!(repair_invalid_backslashes(r#"C:\Users\name"#).len(), 14);
    }

    #[test]
    fn repair_unquoted_keys_basic() {
        assert_eq!(
            repair_unquoted_keys(r#"{name: "get_weather"}"#),
            r#"{"name": "get_weather"}"#
        );
    }

    #[test]
    fn partial_tag_detection_open() {
        // 完整的开标签前缀
        assert!(is_partial_xml_tool_tag("<get_we", &["get_weather".into()], false));
        assert!(is_partial_xml_tool_tag("<get_weather", &["get_weather".into()], false));
        assert!(is_partial_xml_tool_tag("<", &["get_weather".into()], false));
        // 不匹配的前缀
        assert!(!is_partial_xml_tool_tag("<unknown", &["get_weather".into()], false));
    }

    #[test]
    fn partial_tag_detection_close() {
        assert!(is_partial_xml_tool_tag("</get_we", &["get_weather".into()], true));
        assert!(is_partial_xml_tool_tag("</get_weather", &["get_weather".into()], true));
        assert!(!is_partial_xml_tool_tag("<get_we", &["get_weather".into()], true));
    }

    #[test]
    fn partial_tag_tail_length() {
        let text = "hello <get_we";
        let len = get_partial_xml_tool_tag_tail_length(text, &["get_weather".into()], false);
        assert_eq!(len, 7); // "<get_we"
    }

    #[test]
    fn partial_tag_tail_length_no_partial() {
        let text = "hello world";
        let len = get_partial_xml_tool_tag_tail_length(text, &["get_weather".into()], false);
        assert_eq!(len, 0);
    }
}
