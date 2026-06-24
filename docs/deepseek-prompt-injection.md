这里是来自 [DeepSeek API Docs](https://api-docs.deepseek.com/zh-cn/quick_start/token_usage) 的`added_tokens`的汇总:

| 标记符                                                 | special  | normalized | 用途                                                         |
| ------------------------------------------------------ | -------- | ---------- | ------------------------------------------------------------ |
| `<think></think>`                                      | false    | **true**   | **推理链容器**（Chain-of-Thought）。DeepSeek-R1 等推理模型在生成最终回答前，会在此标签内输出内部思考过程，对外通常折叠显示。 |
| `<｜fim▁hole｜>` / `<｜fim▁begin｜>` / `<｜fim▁end｜>` | false    | **true**   | **Fill-In-the-Middle（代码中间补全）**。`begin` 和 `end` 标记前缀/后缀代码块，`hole` 标记需要模型填充的中间位置。 |
| `<｜User｜>` / `<｜Assistant｜>`                       | false    | **true**   | **角色锚点**。替代传统的 `User:` / `Assistant:` 文本前缀，作为更鲁棒的结构化分隔符，防止角色混淆攻击（prompt injection）。 |
| `<\|EOT\|>`                                              | **true** | **true**   | **End of Turn**。标记当前轮次（turn）的结束，是模型停止生成的信号之一。 |
| `<｜tool▁calls▁begin｜>` / `<｜tool▁calls▁end｜>`      | false    | **true**   | **工具调用列表容器**。包裹本轮所有需要调用的工具。           |
| `<｜tool▁call▁begin｜>` / `<｜tool▁call▁end｜>`        | false    | **true**   | **单个工具调用容器**。内部通常包含 JSON 格式的函数名和参数。 |
| `<｜tool▁outputs▁begin｜>` / `<｜tool▁outputs▁end｜>`  | false    | **true**   | **工具返回结果列表容器**。                                   |
| `<｜tool▁output▁begin｜>` / `<｜tool▁output▁end｜>`    | false    | **true**   | **单个工具返回结果容器**。                                   |
| `<｜tool▁sep｜>`                                       | false    | **true**   | **工具分隔符**。用于分隔同一轮中的多个工具调用或返回结果。   |
| `<｜begin▁of▁sentence｜>` / `<｜end▁of▁sentence｜>`    | **true** | false      | **序列级边界标记**（BOS/EOS）。标记整个输入/输出序列的物理开始和结束。 |
| `<｜▁pad▁｜>`                                          | **true** | false      | **填充标记**（PAD）。batch 推理时用于对齐序列长度，模型不会对其生成注意力。 |

然后这里是实际在deepseek网页端的对话测试

![image-20260429105126264](assets/图1.png)

通过如上的这个张图可以在被网页后端过滤后真正可以使用的标记符只有`<think></think>` `<｜User｜>` `<｜Assistant｜>`, 所以

- 我打算使用 `< | System | >`进行妥协的系统提示词注入;
- ~~将通过指令规则限定模型使用特殊的模式进行工具调用, 使用 `< | Tool | >`表示工具调用结果;~~
- 同时如下图所示, `<think>`在不闭合的情况下可以引导模型进行思考, 这样就可以进行更加强力规则的注入(reminder);

![image-20260429110516352](assets/图2.png)

## 后续实验发现

经过实际测试, 原生标签 `<｜tool▁calls▁begin｜>` 作为主标签时模型严重混淆, 怀疑后端对 `<｜...｜>` 全角格式有特殊处理或过滤。

尝试了折中方案 `<|tool▁calls▁begin|>` / `<|tool▁calls▁end|>` 作为工具调用标签:

- 使用 ASCII `|` 替代全角 `｜`, 既不触发后端过滤, 又保留了类原生标签的结构感
- **效果意外很好**, 模型识别和遵循度明显提升, 幻觉也大幅减少
- 可能原因是 tokenizer 对 `<|...|>` 格式有已有的 token 模式, 模型对这个"结构模板"有更好的遵循倾向

## 账号封禁与策略迁移

固定标签 `<|tool▁calls▁begin|>` 在长期使用后被 DeepSeek 网页版后端识别为机器特征, 触发账号封禁。原因分析:

- 固定标签 + 单个 JSON 数组的格式过于规整, 与人类自然对话差异显著
- 标签文本本身是 tokenizer 已有 token 模式, 但作为"工具调用容器"出现频率异常
- 多轮对话中标签重复出现, 形成可被规则匹配的指纹

为此, 将 [deepseek-pp](https://github.com/qoomezhu/deepseek-pp) 项目的注入逻辑移植到本仓库, 替换原有固定标签策略。

## 当前策略: per-tool XML 标签（移植自 deepseek-pp）

每个工具使用独立的 XML 标签, 标签名即工具名, 标签体为 JSON 参数对象:

```
<get_weather>
{"city": "Beijing"}
</get_weather>
```

### 工具 schema 注入格式

采用对话式自然语言描述, 而非 rigid rules, 避免触发后端的"系统提示词指纹"检测。每个工具的 schema 注入到 System 消息尾部, 格式如下:

```
## Tools

### Tool get_weather
Description: 获取指定城市的天气信息
Valid call format for get_weather:
<get_weather>
{"city": "Beijing"}
</get_weather>
Parameters JSON Schema: {"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}

### Tool calc
Description: 计算数学表达式
Valid call format for calc:
<calc>
{"expression": "1+1"}
</calc>
Parameters JSON Schema: {"type":"object","properties":{"expression":{"type":"string"}},"required":["expression"]}
```

### 工具调用历史回放

assistant 消息中的 `tool_calls` 同样以 per-tool XML 标签格式回放, 与模型输出格式保持一致:

```
<get_weather>
{"city": "Beijing"}
</get_weather>
```

### 解析端实现

`src/openai_adapter/response/tool_parser.rs` 实现流式状态机解析:

- **Normal 状态**: 扫描文本寻找 `<tool_name>` 开标签, 未找到则释放安全部分, 保留可能是部分标签的尾部
- **Suppressing 状态**: 收集标签体直到 `</tool_name>` 闭标签, 解析 JSON
- 支持流中顺序多个不同工具的调用
- 支持部分标签检测（处理 chunk 在标签中间被切分的情况）
- 保留 JSON 修复逻辑（反斜杠转义、未引用 key 修复）
- 保留 `<invoke>` legacy 回退

### 工具名来源

- 主列表: 从请求的 `tools` 字段中自动提取 `function.name`
- 额外列表: 通过 `config.toml` 的 `[ds_core.tool_call]` → `extra_tool_names` 配置
- 合并后传入 `TagConfig.tool_names`, 用于流式解析时的标签匹配

### 与原策略的对比

| 维度 | 原策略（已废弃） | 新策略（per-tool XML） |
| --- | --- | --- |
| 标签格式 | `<\|tool▁calls▁begin\|>[...]<\|tool▁calls▁end\|>` | `<tool_name>{json}</tool_name>` |
| 标签来源 | 固定字符串 | 从请求 tools 定义派生 |
| 调用容器 | 单个 JSON 数组 | 每个调用独立标签 |
| schema 注入 | rigid rules + reminder | 对话式自然语言描述 |
| 注入位置 | `<think>` 块内 | System 消息尾部 |
| 封禁风险 | 高（固定指纹） | 低（标签随工具名变化） |

### 配置参考

```toml
[ds_core.tool_call]
# 额外可识别的工具名（未在请求 tools 中定义, 但模型可能输出的标签名）
extra_tool_names = ["custom_tool_a", "custom_tool_b"]
```
