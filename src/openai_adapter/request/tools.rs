//! 工具解析 —— 校验 tools/tool_choice 并生成提示词注入文本
//!
//! 采用 deepseek-pp 的 per-tool XML 标签策略：每个工具使用独立标签
//! `<tool_name>{json}</tool_name>`，标签名即工具名，标签体为 JSON 参数对象。
//! 工具 schema 以对话式自然语言描述呈现，避免 rigid rules 和重复提醒。

use crate::openai_adapter::types::{
    AllowedTools, AllowedToolsChoice, ChatCompletionsRequest, CustomTool, CustomToolFormat,
    FunctionDefinition, Tool, ToolChoice,
};

/// 提取后的工具上下文
pub(crate) struct ToolContext {
    /// 工具 schema 文本（对话式描述 + per-tool XML 调用格式）
    pub defs_text: Option<String>,
    /// 根据 tool_choice / parallel_tool_calls 追加的行为指令
    pub instruction_text: Option<String>,
}

fn has_tools(req: &ChatCompletionsRequest) -> bool {
    req.tools.as_ref().map(|t| !t.is_empty()).unwrap_or(false)
}

/// 从请求中提取并校验工具信息
///
/// 当 tool_choice 为 none 时返回空的 ToolContext，不生成任何注入文本。
pub(crate) fn extract(req: &ChatCompletionsRequest) -> Result<ToolContext, String> {
    let default_choice = if has_tools(req) {
        ToolChoice::Mode("auto".to_string())
    } else {
        ToolChoice::Mode("none".to_string())
    };
    let tool_choice = req.tool_choice.as_ref().unwrap_or(&default_choice);

    validate_tool_choice(tool_choice, req.tools.as_deref())?;

    if matches!(tool_choice, ToolChoice::Mode(m) if m == "none") {
        return Ok(ToolContext {
            defs_text: None,
            instruction_text: None,
        });
    }

    let mut instruction_lines = Vec::new();

    match tool_choice {
        ToolChoice::Mode(mode) => {
            if mode == "required" {
                instruction_lines.push("注意：你必须调用一个或多个工具。".to_string());
            }
        }
        ToolChoice::AllowedTools(AllowedToolsChoice { allowed_tools, .. }) => {
            build_allowed_tools_instruction(allowed_tools, &mut instruction_lines);
        }
        ToolChoice::Named(named) => {
            instruction_lines.push(format!(
                "注意：你必须调用 '{}' 工具。",
                named.function.name
            ));
        }
        ToolChoice::Custom(custom) => {
            instruction_lines.push(format!(
                "注意：你必须调用 '{}' 自定义工具。",
                custom.custom.name
            ));
        }
    }

    if req.parallel_tool_calls == Some(false) {
        instruction_lines.push("注意：一次只能调用一个工具。".to_string());
    }

    let defs_text = if has_tools(req) {
        Some(render_tool_schemas(req.tools.as_ref().unwrap())?)
    } else {
        None
    };

    let instruction_text = if instruction_lines.is_empty() {
        None
    } else {
        Some(instruction_lines.join("\n"))
    };

    Ok(ToolContext {
        defs_text,
        instruction_text,
    })
}

fn validate_tool_choice(tc: &ToolChoice, tools: Option<&[Tool]>) -> Result<(), String> {
    match tc {
        ToolChoice::Mode(mode) => {
            if !matches!(mode.as_str(), "none" | "auto" | "required") {
                return Err(format!("tool_choice 无效模式: {}", mode));
            }
            if matches!(mode.as_str(), "auto" | "required")
                && tools.map(|t| t.is_empty()).unwrap_or(true)
            {
                return Err("tool_choice 为 'auto' 或 'required' 时必须提供 tools".into());
            }
            Ok(())
        }
        ToolChoice::Named(_) | ToolChoice::Custom(_) => {
            if tools.is_none() {
                return Err("tool_choice 指定了具体工具时必须提供 tools".into());
            }
            Ok(())
        }
        ToolChoice::AllowedTools(AllowedToolsChoice { allowed_tools, .. }) => {
            if tools.is_none() {
                return Err("tool_choice 指定了 allowed_tools 时必须提供 tools".into());
            }
            if !matches!(allowed_tools.mode.as_str(), "auto" | "required") {
                return Err(format!(
                    "allowed_tools.mode 必须是 'auto' 或 'required'，收到: {}",
                    allowed_tools.mode
                ));
            }
            Ok(())
        }
    }
}

fn build_allowed_tools_instruction(allowed_tools: &AllowedTools, lines: &mut Vec<String>) {
    if let Some(tool_list) = &allowed_tools.tools {
        let names: Vec<String> = tool_list
            .iter()
            .filter_map(|v| v.get("function").and_then(|f| f.get("name")))
            .filter_map(|n| n.as_str().map(|s| s.to_string()))
            .collect();
        if !names.is_empty() {
            lines.push(format!(
                "注意：你只能从以下允许的工具中选择：{}。",
                names.join(", ")
            ));
        }
    }

    if allowed_tools.mode == "required" {
        lines.push("注意：你必须调用一个或多个工具。".to_string());
    }
}

/// 渲染所有工具的 schema（deepseek-pp 对话式格式）
///
/// 每个工具格式：
/// ```text
/// ### Tool {name}
/// Description: {description}
/// Valid call format for {name}:
/// <{name}>
/// {example_payload}
/// </{name}>
/// Parameters JSON Schema: {schema}
/// ```
fn render_tool_schemas(tools: &[Tool]) -> Result<String, String> {
    let mut blocks = Vec::new();
    for (i, tool) in tools.iter().enumerate() {
        match tool.ty.as_str() {
            "function" => {
                let func = tool.function.as_ref().ok_or_else(|| {
                    format!("tools[{}] 类型为 'function' 时必须提供 function 定义", i)
                })?;
                blocks.push(render_function_schema(func)?);
            }
            "custom" => {
                let custom = tool.custom.as_ref().ok_or_else(|| {
                    format!("tools[{}] 类型为 'custom' 时必须提供 custom 定义", i)
                })?;
                blocks.push(render_custom_schema(custom));
            }
            _ => return Err(format!("tools[{}] 不支持的类型: {}", i, tool.ty)),
        }
    }
    Ok(blocks.join("\n\n"))
}

fn render_function_schema(func: &FunctionDefinition) -> Result<String, String> {
    if func.name.trim().is_empty() {
        return Err("tools 中 function 缺少必填字段 'name'".into());
    }

    let name = &func.name;
    let description = func.description.as_deref().unwrap_or("").trim();
    let schema = serde_json::to_string(&func.parameters).unwrap_or_else(|_| "{}".into());

    // 示例参数：从 schema 中提取或使用默认值
    let example = example_payload(name);

    let mut block = format!("### Tool {name}\n");
    if description.is_empty() {
        block.push_str("Description: (无描述)\n");
    } else {
        block.push_str(&format!("Description: {description}\n"));
    }
    block.push_str(&format!("Valid call format for {name}:\n"));
    block.push_str(&format!("<{name}>\n{example}\n</{name}>\n"));
    block.push_str(&format!("Parameters JSON Schema: {schema}"));

    Ok(block)
}

fn render_custom_schema(custom: &CustomTool) -> String {
    let name = &custom.name;
    let description = custom.description.as_deref().unwrap_or("").trim();
    let method = match &custom.format {
        Some(CustomToolFormat::Text) => "text".to_string(),
        Some(CustomToolFormat::Grammar { grammar }) => {
            format!("grammar(syntax: {})", grammar.syntax)
        }
        None => "无约束".to_string(),
    };

    let mut block = format!("### Tool {name} (custom, format: {method})\n");
    if description.is_empty() {
        block.push_str("Description: (无描述)\n");
    } else {
        block.push_str(&format!("Description: {description}\n"));
    }
    block.push_str(&format!("Valid call format for {name}:\n"));
    block.push_str(&format!("<{name}>\n(自定义格式内容)\n</{name}>"));

    block
}

/// 根据工具名返回示例参数 JSON
fn example_payload(name: &str) -> String {
    match name {
        "Read" | "read_file" => r#"{"file_path": "/path/to/file"}"#.to_string(),
        "Bash" | "execute_command" | "exec_command" => r#"{"command": "ls -la"}"#.to_string(),
        "Write" | "write_to_file" => {
            r#"{"file_path": "/path/to/file", "content": "hello"}"#.to_string()
        }
        "Edit" => {
            r#"{"file_path": "/path/to/file", "old_string": "foo", "new_string": "bar"}"#
                .to_string()
        }
        "Glob" => r#"{"pattern": "**/*.rs", "path": "."}"#.to_string(),
        "search_files" => r#"{"query": "TODO", "path": "."}"#.to_string(),
        "get_weather" => r#"{"city": "Beijing"}"#.to_string(),
        "get_time" => r#"{"timezone": "Asia/Shanghai"}"#.to_string(),
        "list_files" => r#"{"path": "."}"#.to_string(),
        _ => r#"{"key": "value"}"#.to_string(),
    }
}
