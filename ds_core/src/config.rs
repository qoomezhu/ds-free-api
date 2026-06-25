//! DeepSeek 核心配置 —— 独立于根 crate 的 Config
//!
//! 由根 crate 的 `Config` 构造转换而来。

/// 反代行为伪装配置 —— 模拟真实浏览器节奏，降低封号风险
#[derive(Debug, Clone)]
pub struct BehaviorConfig {
    /// 请求前随机延迟范围（毫秒）：[min, max]
    ///
    /// 真实浏览器在发消息前有思考、打字停顿，固定间隔会被识别为机械行为。
    /// 默认 [500, 3000]：人类对话思考+输入的常见耗时区间。
    pub request_jitter_ms: (u64, u64),
    /// 单账号每日请求上限，达到后该账号当日不再被选中
    ///
    /// 真实用户每日对话量有限，单账号高频调用会触发 DeepSeek 黄色熔断
    /// （单账号 GPU 耗时突增 300%）。建议 50-100。
    pub daily_request_limit: u32,
    /// PoW 计算后随机延迟范围（毫秒）：[min, max]
    ///
    /// 反代用 wasmtime 秒算 PoW，真实浏览器需要 200-800ms。算完后延迟发送
    /// 可避免被 PoW 时延异常标记。设为 [0, 0] 禁用。
    /// 默认 [200, 800]：浏览器 JS 计算 DeepSeekHashV1 的常见耗时区间。
    pub pow_delay_ms: (u64, u64),
}

impl Default for BehaviorConfig {
    fn default() -> Self {
        Self {
            request_jitter_ms: (500, 3000),
            daily_request_limit: 80,
            pow_delay_ms: (200, 800),
        }
    }
}

/// ds_core 所需的配置（从根 crate Config 的子集构造）
#[derive(Debug, Clone)]
pub struct DsCoreConfig {
    pub api_base: String,
    pub wasm_url: String,
    pub user_agent: String,
    pub client_version: String,
    pub client_platform: String,
    pub client_locale: String,
    pub proxy_url: Option<String>,
    pub model_types: Vec<String>,
    pub input_character_limits: Vec<u32>,
    pub behavior: BehaviorConfig,
}

/// 单个账号配置
#[derive(Debug, Clone)]
pub struct AccountConfig {
    pub email: String,
    pub mobile: String,
    pub area_code: String,
    pub password: String,
}
