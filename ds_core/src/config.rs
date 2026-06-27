//! DeepSeek 核心配置 —— 独立于根 crate 的 Config
//!
//! 由根 crate 的 `Config` 构造转换而来。

/// 反代行为伪装配置 —— 模拟真实浏览器节奏，降低封号风险
#[derive(Debug, Clone)]
pub struct BehaviorConfig {
    /// 请求前随机延迟范围（毫秒）：[min, max]
    ///
    /// 真实浏览器在发消息前有思考、打字停顿，固定间隔会被识别为机械行为。
    /// 默认 [2000, 8000]：更接近真实人类对话节奏。
    pub request_jitter_ms: (u64, u64),
    /// 单账号每日请求上限，达到后该账号当日不再被选中
    pub daily_request_limit: u32,
    /// PoW 计算后随机延迟范围（毫秒）：[min, max]
    pub pow_delay_ms: (u64, u64),
    /// 是否持久化 session（不删除对话）。每次请求创建+销毁是最大封号特征。
    pub persist_sessions: bool,
    /// 是否跳过启动时的批量健康检查。批量登录触发 DeepSeek 检测导致全封。
    pub skip_startup_health_check: bool,
}

impl Default for BehaviorConfig {
    fn default() -> Self {
        Self {
            request_jitter_ms: (2000, 8000),
            daily_request_limit: 20,
            pow_delay_ms: (200, 800),
            persist_sessions: true,
            skip_startup_health_check: true,
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
