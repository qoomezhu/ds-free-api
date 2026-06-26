//! Prompt 构建 —— 将 OpenAI messages 转换为 DeepSeek 原生标签格式
//!
//! 使用 `<｜System｜>`、`<｜User｜>`、`<｜Assistant｜>`、`<｜tool▁outputs▁begin｜>` 作为角色标记。
//! 工具定义以对话式自然语言注入到 System 消息中，工具调用使用 per-tool XML 标签格式
//! `<tool_name>{json}</tool_name>`。

use super::tools::ToolContext;
use crate::openai_adapter::types::{ChatCompletionsRequest, ContentPart, Message, MessageContent};

/// 请求级随机源：用 SystemTime 纳秒低 32 位做轮换索引，无需引入 rand 依赖
fn jitter_index(bound: usize) -> usize {
    if bound <= 1 {
        return 0;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    (nanos % bound as u64) as usize
}

/// 工具调用指令文本变体（都要求模型用 per-tool XML 输出，措辞不同）
///
/// 反代加固：避免每次请求出现完全相同的指令字符串被后端识别为机器特征。
/// 所有变体语义等价，均禁止 `<invoke>`/`<tool_call>` 包装格式。
fn tool_instruction_text() -> &'static str {
    const VARIANTS: [&str; 4] = [
        "使用工具时，请直接输出对应工具的 XML 标签（如 `<tool_name>{{...}}</tool_name>`）。不要使用 `<invoke name=\"...\">...</invoke>` 或 `<tool_call>...</tool_call>` 等包装格式。",
        "调用工具请输出形如 `<tool_name>{{...}}</tool_name>` 的 XML 标签，禁止使用 `<invoke>` 或 `<tool_call>` 包装。",
        "工具调用格式：直接输出 `<tool_name>{{...}}</tool_name>`，不要包裹在 `<invoke>`/`<tool_call>` 中。",
        "如需调用工具，请以 `<tool_name>{{...}}</tool_name>` 格式输出，避免使用 `<invoke>` 或 `<tool_call>` 标签。",
    ];
    VARIANTS[jitter_index(VARIANTS.len())]
}

/// 合并连续相同 role 的 message，避免 DeepSeek 模型对连续同角色标签产生混淆
fn merge_messages(messages: &[Message]) -> Vec<Message> {
    let mut merged: Vec<Message> = Vec::new();
    for msg in messages {
        if let Some(last) = merged.last_mut()
            && last.role == msg.role
            && msg.role != "tool"
        // tool 由 build() 分组合并
        {
            // 合并 content
            if let Some(ref content) = msg.content {
                match &mut last.content {
                    Some(last_content) => match (last_content, content) {
                        (MessageContent::Text(a), MessageContent::Text(b)) => {
                            a.push('\n');
                            a.push_str(b);
                        }
                        (MessageContent::Parts(a), MessageContent::Parts(b)) => {
                            a.extend(b.clone());
                        }
                        // 不同类型 → 都转 text 拼接
                        (last_c, new_c) => {
                            let new_text = format_content(new_c);
                            let last_text = format_content(last_c);
                            *last_c = MessageContent::Text(format!("{}\n{}", last_text, new_text));
                        }
                    },
                    None => {
                        last.content = msg.content.clone();
                    }
                }
            }
            // 合并 tool_calls
            if let Some(ref calls) = msg.tool_calls {
                match &mut last.tool_calls {
                    Some(last_calls) => last_calls.extend(calls.clone()),
                    None => last.tool_calls = msg.tool_calls.clone(),
                }
            }
            // 覆盖字段：取最后一条的值
            if msg.name.is_some() {
                last.name.clone_from(&msg.name);
            }
            if msg.tool_call_id.is_some() {
                last.tool_call_id.clone_from(&msg.tool_call_id);
            }
            if msg.function_call.is_some() {
                last.function_call.clone_from(&msg.function_call);
            }
            if msg.refusal.is_some() {
                last.refusal.clone_from(&msg.refusal);
            }
            if msg.audio.is_some() {
                last.audio.clone_from(&msg.audio);
            }
            continue;
        }
        merged.push(msg.clone());
    }
    merged
}

/// 生成 response_format 对应的提示文本
fn format_response_text(rf: &crate::openai_adapter::types::ResponseFormat) -> String {
    match rf.ty.as_str() {
        "json_object" => {
            "请直接输出合法的 JSON 对象，不要包含任何 markdown 代码块标记或其他解释性文字。".into()
        }
        "json_schema" => {
            let schema_text = rf
                .json_schema
                .as_ref()
                .map(|s| serde_json::to_string(s).unwrap_or_default())
                .unwrap_or_default();
            if schema_text.is_empty() {
                "以 JSON 的形式输出。".into()
            } else {
                format!(
                    "以 JSON 的形式输出，输出的 JSON 需遵守以下的格式：\n\n~~~json\n{}\n~~~",
                    schema_text
                )
            }
        }
        "text" => String::new(),
        _ => format!("请以 {} 格式输出。", rf.ty),
    }
}

/// 构建 DeepSeek 原生标签格式的 prompt 字符串
///
/// 工具定义和调用指令以自然语言注入到 System 消息尾部，
/// 不再使用 `<think>` 块注入和"嗯，我刚刚被系统提醒"前缀。
pub(crate) fn build(req: &ChatCompletionsRequest, tool_ctx: &ToolContext) -> String {
    let messages = merge_messages(&req.messages);
    let mut parts: Vec<String> = Vec::with_capacity(messages.len());
    let mut i = 0;
    while i < messages.len() {
        if messages[i].role == "tool" {
            let mut tool_contents = Vec::new();
            while i < messages.len() && messages[i].role == "tool" {
                if let Some(c) = &messages[i].content {
                    tool_contents.push(format_content(c));
                }
                i += 1;
            }
            let inner: String = tool_contents
                .iter()
                .map(|c| format!("<｜tool▁output▁begin｜>{}<｜tool▁output▁end｜>", c))
                .collect();
            parts.push(format!(
                "<｜tool▁outputs▁begin｜>{}<｜tool▁outputs▁end｜>",
                inner
            ));
        } else {
            parts.push(format_message(&messages[i]));
            i += 1;
        }
    }

    // 收集工具和格式相关的注入内容
    let mut sections: Vec<String> = Vec::new();

    if let Some(text) = tool_ctx.defs_text.as_deref() {
        sections.push(format!("## Tools\n\n{text}\n\n{}", tool_instruction_text()));
    }
    if let Some(text) = tool_ctx.instruction_text.as_deref() {
        sections.push(format!("## 调用指令\n{text}"));
    }

    // response_format 降级
    let format_text = req
        .response_format
        .as_ref()
        .map(format_response_text)
        .unwrap_or_default();
    if !format_text.is_empty() {
        sections.push(format!("## 输出格式\n{format_text}"));
    }

    // 将工具和格式内容注入到 System 消息尾部
    if !sections.is_empty() {
        let injection = format!("\n\n{}", sections.join("\n\n"));
        if let Some(sys) = parts.iter_mut().find(|p| p.starts_with("<｜System｜>")) {
            if let Some(end) = sys.rfind('\n') {
                sys.insert_str(end, &injection);
            } else {
                sys.push_str(&injection);
            }
        } else {
            parts.insert(0, format!("<｜System｜>{}\n", injection.trim_start()));
        }
    }

    // 确保末尾有 <｜Assistant｜> 供 split_history_prompt 做拆分点
    if !parts.iter().any(|p| p.starts_with("<｜Assistant｜>")) {
        parts.push("<｜Assistant｜>\n".to_string());
    }

    parts.join("")
}

fn role_tag(role: &str) -> String {
    let mut r = role.to_string();
    if let Some(c) = r.get_mut(0..1) {
        c.make_ascii_uppercase();
    }
    format!("<｜{}｜>", r)
}

fn format_message(msg: &Message) -> String {
    let body = match msg.role.as_str() {
        "assistant" => format_assistant(msg),
        "tool" => format_tool(msg),
        "function" => format_function(msg),
        _ => format_generic(msg),
    };
    let tag = if msg.role == "tool" {
        String::new() // tool 用自有标签，不需要 <｜Tool｜>
    } else {
        role_tag(&msg.role)
    };
    let prefix = if msg.role == "user" {
        "<｜end▁of▁sentence｜>"
    } else {
        ""
    };
    format!("{}{}{}", prefix, tag, body)
}

fn format_generic(msg: &Message) -> String {
    let mut parts = Vec::new();
    if let Some(name) = &msg.name {
        parts.push(format!("(name: {name})"));
    }
    if let Some(content) = &msg.content {
        parts.push(format_content(content));
    }
    parts.join("\n")
}

fn format_assistant(msg: &Message) -> String {
    let mut parts = Vec::new();
    if let Some(content) = &msg.content {
        parts.push(format_content(content));
    }
    // 工具调用使用 per-tool XML 标签格式
    if let Some(tool_calls) = &msg.tool_calls {
        for tc in tool_calls {
            if let Some(func) = &tc.function {
                let args = serde_json::from_str::<serde_json::Value>(&func.arguments)
                    .unwrap_or(serde_json::Value::Null);
                let args_str = serde_json::to_string(&args).unwrap_or_else(|_| "null".into());
                parts.push(format!("<{}>\n{}\n</{}>", func.name, args_str, func.name));
            }
        }
    }
    if let Some(fc) = &msg.function_call {
        let args = serde_json::from_str::<serde_json::Value>(&fc.arguments)
            .unwrap_or(serde_json::Value::Null);
        let args_str = serde_json::to_string(&args).unwrap_or_else(|_| "null".into());
        parts.push(format!("<{}>\n{}\n</{}>", fc.name, args_str, fc.name));
    }
    if let Some(refusal) = &msg.refusal {
        parts.push(format!("(refusal: {refusal})"));
    }
    parts.join("\n")
}

fn format_tool(msg: &Message) -> String {
    let content = msg.content.as_ref().map(format_content).unwrap_or_default();
    format!(
        "<｜tool▁outputs▁begin｜><｜tool▁output▁begin｜>{}<｜tool▁output▁end｜><｜tool▁outputs▁end｜>",
        content
    )
}

fn format_function(msg: &Message) -> String {
    let mut parts = Vec::new();
    if let Some(name) = &msg.name {
        parts.push(format!("(name: {name})"));
    }
    if let Some(content) = &msg.content {
        parts.push(format_content(content));
    }
    parts.join("\n")
}

fn format_content(content: &MessageContent) -> String {
    match content {
        MessageContent::Text(text) => text.clone(),
        MessageContent::Parts(parts) => {
            parts.iter().map(format_part).collect::<Vec<_>>().join("\n")
        }
    }
}

fn format_part(part: &ContentPart) -> String {
    match part.ty.as_str() {
        "text" => part.text.clone().unwrap_or_default(),
        "refusal" => part.refusal.clone().unwrap_or_default(),
        "image_url" => part.image_url.as_ref().map_or_else(
            || "[图片]".to_string(),
            |img| {
                if img.url.starts_with("http://") || img.url.starts_with("https://") {
                    format!("[请访问这个链接: {}]", img.url)
                } else {
                    let detail = img.detail.as_deref().unwrap_or("auto");
                    format!("[图片: detail={detail}]")
                }
            },
        ),
        "input_audio" => {
            let fmt = part
                .input_audio
                .as_ref()
                .map(|a| a.format.as_str())
                .unwrap_or("unknown");
            format!("[音频: format={fmt}]")
        }
        "file" => {
            let filename = part
                .file
                .as_ref()
                .and_then(|f| f.filename.as_deref())
                .unwrap_or("unknown");
            let desc = part.text.as_deref().filter(|t| !t.is_empty());
            desc.map_or_else(
                || format!("[文件: filename={filename}]"),
                |d| format!("[文件: {d} (filename={filename})]"),
            )
        }
        _ => format!("[未支持的内容类型: {}]", part.ty),
    }
}
