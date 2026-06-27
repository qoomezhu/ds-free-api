//! 账号池管理 —— 多账号负载均衡
//!
//! 1 account = 1 session = 1 concurrency。多并发需横向扩展账号数。

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU32, Ordering};
use std::time::SystemTime;

use dashmap::DashMap;
use futures::TryStreamExt;
use log::{debug, error, info, warn};
use tokio::sync::RwLock;

use super::client::{ClientError, CompletionPayload, DsClient, LoginPayload};
use super::pow::{PowError, PowSolver};
use crate::config::AccountConfig;

/// 账号状态枚举
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountState {
    Idle = 0,
    Busy = 1,
    Error = 2,
    Invalid = 3,
}

impl AccountState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Idle,
            1 => Self::Busy,
            2 => Self::Error,
            _ => Self::Invalid,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Busy => "busy",
            Self::Error => "error",
            Self::Invalid => "invalid",
        }
    }
}

/// 账号状态信息
#[derive(serde::Serialize)]
pub struct AccountStatus {
    pub email: String,
    pub mobile: String,
    pub state: String,
    /// 最后释放时间戳（ms），0 表示从未使用
    pub last_released_ms: i64,
    /// 连续登录失败次数
    pub error_count: u8,
}

pub struct Account {
    token: std::sync::RwLock<Arc<str>>,
    email: String,
    mobile: String,
    state: AtomicU8,
    /// 账号最近一次释放的时间戳（ms），用于冷却判断
    last_released: AtomicI64,
    /// 连续登录失败次数
    error_count: AtomicU8,
    /// 原始凭据（用于重新登录）
    creds: AccountConfig,
    /// 当日已发起的请求数（UTC 日期切换时重置）
    daily_count: AtomicU32,
    /// daily_count 对应的 UTC 日期序号（自纪元起的天数），用于跨日重置
    daily_day: AtomicU32,
}

/// 连续登录失败上限，达到后标记为 Invalid
const MAX_ERROR_COUNT: u8 = 3;

impl Account {
    pub fn token(&self) -> Arc<str> {
        self.token.read().unwrap().clone()
    }

    pub fn display_id(&self) -> &str {
        if self.email.is_empty() {
            &self.mobile
        } else {
            &self.email
        }
    }

    pub fn state(&self) -> AccountState {
        AccountState::from_u8(self.state.load(Ordering::Relaxed))
    }

    pub fn is_busy(&self) -> bool {
        self.state() == AccountState::Busy
    }

    pub fn is_available(&self) -> bool {
        self.state() == AccountState::Idle
    }

    /// 当日已发起的请求数。跨日时返回 0（内部自动重置）。
    pub fn daily_count(&self) -> u32 {
        let today = utc_day_index();
        let stored_day = self.daily_day.load(Ordering::Relaxed);
        if stored_day != today {
            // 跨日：尝试重置计数。并发场景下失败的线程读到已重置的值即可
            if self
                .daily_day
                .compare_exchange(stored_day, today, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.daily_count.store(0, Ordering::Relaxed);
            }
        }
        self.daily_count.load(Ordering::Relaxed)
    }

    /// 递增当日请求计数（在账号被选中后调用）。跨日自动重置为 1。
    pub fn inc_daily_count(&self) {
        let today = utc_day_index();
        let stored_day = self.daily_day.load(Ordering::Relaxed);
        if stored_day != today
            && self
                .daily_day
                .compare_exchange(stored_day, today, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.daily_count.store(1, Ordering::Relaxed);
            return;
        }
        self.daily_count.fetch_add(1, Ordering::Relaxed);
    }

    /// 创建一个 Invalid 状态的账号（初始化失败时使用，仍加入池以便前台展示）
    fn new_invalid(creds: AccountConfig) -> Self {
        Self {
            token: std::sync::RwLock::new(String::new().into()),
            email: creds.email.clone(),
            mobile: creds.mobile.clone(),
            state: AtomicU8::new(AccountState::Invalid as u8),
            last_released: AtomicI64::new(0),
            error_count: AtomicU8::new(MAX_ERROR_COUNT),
            creds,
            daily_count: AtomicU32::new(0),
            daily_day: AtomicU32::new(utc_day_index()),
        }
    }
}

/// 当前 UTC 日期序号（自纪元起的天数），用于跨日重置 daily_count
fn utc_day_index() -> u32 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| (d.as_secs() / 86_400) as u32)
        .unwrap_or(0)
}

/// 持有期间账号标记为 busy，Drop 时自动释放
pub struct AccountGuard {
    account: Arc<Account>,
}

impl AccountGuard {
    pub fn account(&self) -> &Account {
        &self.account
    }
}

impl Drop for AccountGuard {
    fn drop(&mut self) {
        // 只有 Busy 状态才释放回 Idle（避免覆盖 Error/Invalid）
        self.account
            .state
            .compare_exchange(
                AccountState::Busy as u8,
                AccountState::Idle as u8,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .ok();
        let d = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let now_ms = (d.as_secs() * 1000 + u64::from(d.subsec_millis())) as i64;
        self.account.last_released.store(now_ms, Ordering::Relaxed);
    }
}

pub struct AccountPool {
    /// key = display_id (email or mobile), value = Account
    accounts: DashMap<String, Arc<Account>>,
    client: RwLock<Option<DsClient>>,
    solver: RwLock<Option<PowSolver>>,
    /// 单账号每日请求上限，0 表示不限制（来自 BehaviorConfig，热更新生效）
    daily_request_limit: RwLock<u32>,
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    /// 所有账号初始化失败（没有可用账号）
    #[error("所有账号初始化失败")]
    AllAccountsFailed,

    /// 下游客户端错误（网络、API 错误等）
    #[error("客户端错误: {0}")]
    Client(#[from] ClientError),

    /// PoW 计算失败（WASM 执行错误）
    #[error("PoW 计算失败: {0}")]
    Pow(#[from] PowError),

    /// 账号配置验证失败
    #[error("账号配置错误: {0}")]
    Validation(String),

    /// 账号已存在
    #[error("账号已存在: {0}")]
    AlreadyExists(String),

    /// 账号不存在
    #[error("账号不存在: {0}")]
    NotFound(String),

    /// 账号正在使用中，无法删除
    #[error("账号正在使用中: {0}")]
    AccountBusy(String),
}

impl AccountPool {
    pub fn new() -> Self {
        Self {
            accounts: DashMap::new(),
            client: RwLock::new(None),
            solver: RwLock::new(None),
            daily_request_limit: RwLock::new(0),
        }
    }

    /// 设置单账号每日请求上限（0 表示不限制），热更新时调用
    pub async fn set_daily_request_limit(&self, limit: u32) {
        *self.daily_request_limit.write().await = limit;
    }

    pub async fn init(
        &self,
        creds: Vec<AccountConfig>,
        client: &DsClient,
        solver: &PowSolver,
        skip_health_check: bool,
    ) -> Result<(), PoolError> {
        if creds.is_empty() {
            return Ok(());
        }

        use futures::future::join_all;
        use std::sync::Arc;
        use tokio::sync::Semaphore;

        // 防封号：账号初始化必须串行，避免同 IP 瞬间批量登录
        let semaphore = Arc::new(Semaphore::new(1));
        let futures: Vec<_> = creds
            .into_iter()
            .map(|creds| {
                let client = client.clone();
                let solver = solver.clone();
                let sem = semaphore.clone();
                let skip_hc = skip_health_check;
                async move {
                    let _permit = sem.acquire().await.expect("信号量未关闭");
                    let display_id = if creds.email.is_empty() {
                        creds.mobile.clone()
                    } else {
                        creds.email.clone()
                    };
                    let account = match if skip_hc {
                        init_account_login_only(&creds, &client).await
                    } else {
                        init_account(&creds, &client, &solver).await
                    } {
                        Ok(account) => {
                            info!(target: "ds_core::accounts", "Account {} initialized successfully", display_id);
                            account
                        }
                        Err(e) => {
                            warn!(target: "ds_core::accounts", "Account {} initialization failed: {}", display_id, e);
                            // 即使初始化失败也加入池，标记为 Invalid 以便前台展示
                            Account::new_invalid(creds.clone())
                        }
                    };
                    Some((display_id, Arc::new(account)))
                }
            })
            .collect();

        let results: Vec<(String, Arc<Account>)> =
            join_all(futures).await.into_iter().flatten().collect();
        let idle_count = results
            .iter()
            .filter(|(_, a)| a.state() == AccountState::Idle)
            .count();

        for (id, account) in &results {
            self.accounts.insert(id.clone(), Arc::clone(account));
        }

        if idle_count == 0 {
            warn!(target: "ds_core::accounts", "All accounts failed to initialize — they may be disabled or have invalid credentials");
        } else if results.len() > 1 && idle_count < results.len() {
            warn!(target: "ds_core::accounts", "{}/{} accounts unavailable", results.len() - idle_count, results.len());
        }
        Ok(())
    }

    /// 动态添加账号（运行时初始化）
    pub async fn add_account(
        &self,
        creds: &AccountConfig,
        client: &DsClient,
        _solver: &PowSolver,
    ) -> Result<String, PoolError> {
        let display_id = if creds.email.is_empty() {
            creds.mobile.clone()
        } else {
            creds.email.clone()
        };

        // 检查是否已存在（DashMap O(1) 查找）
        if self.accounts.contains_key(&display_id) {
            return Err(PoolError::AlreadyExists(display_id));
        }

        // ponytail: 动态加号默认只登录，不做 health_check；
        // health_check 会创建+删除 test session，是封号高危特征。
        let account = init_account_login_only(creds, client).await?;
        let _id = account.display_id().to_string();
        self.accounts.insert(display_id.clone(), Arc::new(account));
        info!(target: "ds_core::accounts", "Account {} added dynamically", display_id);
        Ok(display_id)
    }

    /// 动态移除账号（仅空闲账号可移除）
    pub async fn remove_account(&self, email_or_mobile: &str) -> Result<String, PoolError> {
        let account = self
            .accounts
            .get(email_or_mobile)
            .ok_or_else(|| PoolError::NotFound(email_or_mobile.to_string()))?;

        if account.is_busy() {
            return Err(PoolError::AccountBusy(email_or_mobile.to_string()));
        }

        // 也允许移除 Error/Invalid 状态的账号
        drop(account);
        let (_, removed) = self
            .accounts
            .remove(email_or_mobile)
            .ok_or_else(|| PoolError::NotFound(email_or_mobile.to_string()))?;
        let id = removed.display_id().to_string();
        info!(target: "ds_core::accounts", "Account {} removed", id);
        Ok(id)
    }

    /// 获取空闲最久的可用账号，带等待：无可用账号时最多等待 `timeout_ms` 毫秒
    pub async fn get_account_with_wait(&self, timeout_ms: u64) -> Option<AccountGuard> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            if let Some(g) = self.get_account() {
                return Some(g);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// 获取空闲最久的可用账号（不等待，立即返回）
    ///
    /// 遍历所有账号，选冷却已过且空闲时间最长的那个，最大化每次使用间隔。
    /// 跳过已达当日请求上限的账号（若配置了上限）。
    /// DashMap 无锁读，不阻塞并发请求。
    pub fn get_account(&self) -> Option<AccountGuard> {
        if self.accounts.is_empty() {
            return None;
        }

        // 读取日上限配置（0 表示不限制）。try_read 避免热更新写阻塞时卡住选号
        let daily_limit = self.daily_request_limit.try_read().map(|l| *l).unwrap_or(0);

        let d = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let now_ms = (d.as_secs() * 1000 + u64::from(d.subsec_millis())) as i64;

        let mut best: Option<Arc<Account>> = None;
        let mut best_idle = i64::MIN;

        for entry in self.accounts.iter() {
            let account = entry.value();
            if !account.is_available() {
                continue;
            }
            // 当日请求已达上限的账号跳过
            if daily_limit > 0 && account.daily_count() >= daily_limit {
                continue;
            }
            let idle = now_ms - account.last_released.load(Ordering::Relaxed);
            if idle > best_idle {
                best_idle = idle;
                best = Some(Arc::clone(account));
            }
        }

        let account = best?;
        account
            .state
            .compare_exchange(
                AccountState::Idle as u8,
                AccountState::Busy as u8,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .ok()?;
        // 选中后递增当日计数（即使后续请求失败也计数，避免失败重试绕过限制）
        account.inc_daily_count();
        Some(AccountGuard { account })
    }

    /// 获取所有账号的详细状态
    pub fn account_statuses(&self) -> Vec<AccountStatus> {
        self.accounts
            .iter()
            .map(|entry| {
                let a = entry.value();
                AccountStatus {
                    email: a.email.clone(),
                    mobile: a.mobile.clone(),
                    state: a.state().as_str().to_string(),
                    last_released_ms: a.last_released.load(Ordering::Relaxed),
                    error_count: a.error_count.load(Ordering::Relaxed),
                }
            })
            .collect()
    }

    /// 优雅关闭（新流程无持久 session，无需清理）
    pub async fn shutdown(&self, _client: &DsClient) {}

    /// 存储 client 和 solver 供恢复任务使用
    pub async fn set_client_solver(&self, client: DsClient, solver: PowSolver) {
        *self.client.write().await = Some(client);
        *self.solver.write().await = Some(solver);
    }

    /// 标记账号为 Error 状态（请求失败时调用）
    pub fn mark_error(&self, email_or_mobile: &str) {
        if let Some(entry) = self.accounts.get(email_or_mobile) {
            let account = entry.value();
            // 只从 Busy 转到 Error（避免覆盖 Invalid）
            account
                .state
                .compare_exchange(
                    AccountState::Busy as u8,
                    AccountState::Error as u8,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .ok();
            warn!(target: "ds_core::accounts", "Account {} marked as Error", account.display_id());
        }
    }

    /// 手动重新登录指定账号（管理员触发）
    /// 成功 → Idle，失败 → error_count++，≥3 则 Invalid
    pub async fn re_login_single(&self, email_or_mobile: &str) -> Result<(), String> {
        let client_opt = self.client.read().await.clone();
        let solver_opt = self.solver.read().await.clone();
        let (Some(client), Some(solver)) = (client_opt, solver_opt) else {
            return Err("client/solver 未初始化".to_string());
        };

        let account = self
            .accounts
            .get(email_or_mobile)
            .ok_or_else(|| format!("账号 {} 不存在", email_or_mobile))?;
        let account = account.value();

        // 只允许 Error/Invalid 状态的账号重登
        let state = account.state();
        if state != AccountState::Error && state != AccountState::Invalid {
            return Err(format!(
                "账号状态为 {}，仅 Error/Invalid 可重登",
                state.as_str()
            ));
        }

        Self::re_login_account(account, &client, &solver).await;

        // 检查重登后状态
        let new_state = account.state();
        if new_state == AccountState::Idle {
            Ok(())
        } else {
            Err(format!("重登失败，当前状态: {}", new_state.as_str()))
        }
    }

    /// 尝试重新登录 Error 状态的账号
    /// 成功 → Idle，失败 → error_count++，≥3 则 Invalid
    async fn re_login_account(account: &Account, client: &DsClient, _solver: &PowSolver) {
        let display_id = account.display_id().to_string();
        // ponytail: 恢复任务只刷新登录 token，不做 health_check。
        // 账号是否 muted 由真实请求发现；后台 test completion 会额外消耗风控额度。
        match init_account_login_only(&account.creds, client).await {
            Ok(new_account) => {
                // 更新 token
                *account.token.write().unwrap() = new_account.token.read().unwrap().clone();
                account
                    .state
                    .store(AccountState::Idle as u8, Ordering::Relaxed);
                account.error_count.store(0, Ordering::Relaxed);
                info!(target: "ds_core::accounts", "Account {} re-login successful", display_id);
            }
            Err(e) => {
                let count = account.error_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= MAX_ERROR_COUNT {
                    account
                        .state
                        .store(AccountState::Invalid as u8, Ordering::Relaxed);
                    error!(target: "ds_core::accounts", "Account {} re-login failed {} times, marked as Invalid: {}", display_id, count, e);
                } else {
                    warn!(target: "ds_core::accounts", "Account {} re-login failed (attempt {}): {}", display_id, count, e);
                }
            }
        }
    }

    /// 启动后台恢复任务：每 60 秒扫描 Error 账号并尝试重新登录
    pub fn start_recovery_task(self: &Arc<Self>) {
        let pool = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

                let client_opt = pool.client.read().await.clone();
                let solver_opt = pool.solver.read().await.clone();
                let (Some(client), Some(solver)) = (client_opt, solver_opt) else {
                    continue;
                };

                for entry in pool.accounts.iter() {
                    let account = entry.value();
                    if account.state() == AccountState::Error {
                        Self::re_login_account(account, &client, &solver).await;
                    }
                }
            }
        });
    }
}

async fn init_account(
    creds: &AccountConfig,
    client: &DsClient,
    solver: &PowSolver,
) -> Result<Account, PoolError> {
    try_init_account(creds, client, solver).await
}

/// 仅登录，不做健康检查（防封号模式）
///
/// 健康检查会创建 session → 发 test 消息 → 删除 session，
/// 这在批量启动时触发 DeepSeek 的批量登录检测，导致全封。
async fn init_account_login_only(
    creds: &AccountConfig,
    client: &DsClient,
) -> Result<Account, PoolError> {
    if creds.email.is_empty() && creds.mobile.is_empty() {
        return Err(PoolError::Validation(
            "email 和 mobile 不能同时为空".to_string(),
        ));
    }

    let login_payload = LoginPayload {
        email: if creds.email.is_empty() {
            None
        } else {
            Some(creds.email.clone())
        },
        mobile: if creds.mobile.is_empty() {
            None
        } else {
            Some(creds.mobile.clone())
        },
        password: creds.password.clone(),
        area_code: if creds.area_code.is_empty() {
            None
        } else {
            Some(creds.area_code.clone())
        },
        device_id: String::new(),
        os: "web".to_string(),
    };

    let login_data = client.login(&login_payload).await?;
    let token = login_data.user.token;

    debug!(
        target: "ds_core::accounts",
        "login_only: 账号登录成功 (跳过健康检查) email={:?} mobile={:?}",
        &creds.email, &creds.mobile
    );

    Ok(Account {
        token: std::sync::RwLock::new(token.into()),
        email: creds.email.clone(),
        mobile: creds.mobile.clone(),
        state: AtomicU8::new(AccountState::Idle as u8),
        last_released: AtomicI64::new(0),
        error_count: AtomicU8::new(0),
        creds: creds.clone(),
        daily_count: AtomicU32::new(0),
        daily_day: AtomicU32::new(utc_day_index()),
    })
}

async fn try_init_account(
    creds: &AccountConfig,
    client: &DsClient,
    solver: &PowSolver,
) -> Result<Account, PoolError> {
    // 验证：email 和 mobile 至少一个非空
    if creds.email.is_empty() && creds.mobile.is_empty() {
        return Err(PoolError::Validation(
            "email 和 mobile 不能同时为空".to_string(),
        ));
    }

    let login_payload = LoginPayload {
        email: if creds.email.is_empty() {
            None
        } else {
            Some(creds.email.clone())
        },
        mobile: if creds.mobile.is_empty() {
            None
        } else {
            Some(creds.mobile.clone())
        },
        password: creds.password.clone(),
        area_code: if creds.area_code.is_empty() {
            None
        } else {
            Some(creds.area_code.clone())
        },
        device_id: String::new(),
        os: "web".to_string(),
    };

    let login_data = client.login(&login_payload).await?;
    debug!(
        target: "ds_core::client",
        "登录响应: code={}, msg={}, user_id={}, email={:?}, mobile={:?}",
        login_data.code,
        login_data.msg,
        login_data.user.id,
        login_data.user.email,
        login_data.user.mobile_number
    );
    let token = login_data.user.token;

    let display_id = if creds.email.is_empty() {
        &creds.mobile
    } else {
        &creds.email
    };

    // 健康检查：创建临时 session → 发送 test completion → 删除 session
    let session_id = client.create_session(&token).await?;
    if let Err(e) = health_check(&token, &session_id, client, solver, "default", display_id).await {
        // 即使健康检查失败也要清理 session
        let _ = client.delete_session(&token, &session_id).await;
        return Err(e);
    }
    let _ = client.delete_session(&token, &session_id).await;

    Ok(Account {
        token: std::sync::RwLock::new(token.into()),
        email: creds.email.clone(),
        mobile: creds.mobile.clone(),
        state: AtomicU8::new(AccountState::Idle as u8),
        last_released: AtomicI64::new(0),
        error_count: AtomicU8::new(0),
        creds: creds.clone(),
        daily_count: AtomicU32::new(0),
        daily_day: AtomicU32::new(utc_day_index()),
    })
}

async fn health_check(
    token: &str,
    session_id: &str,
    client: &DsClient,
    solver: &PowSolver,
    model_type: &str,
    display_id: &str,
) -> Result<(), PoolError> {
    let start = std::time::Instant::now();
    let challenge = client
        .create_pow_challenge(token, "/api/v0/chat/completion")
        .await?;

    let result = solver.solve(&challenge)?;
    let pow_header = result.to_header();

    let payload = CompletionPayload {
        chat_session_id: session_id.to_string(),
        parent_message_id: None,
        model_type: model_type.to_string(),
        prompt: "只回复`Hello, world!`".to_string(),
        ref_file_ids: vec![],
        thinking_enabled: false,
        search_enabled: false,
        preempt: false,
    };

    let mut stream = client.completion(token, &pow_header, &payload).await?;
    // 消费流并检查是否收到正常 SSE（健康账号应有 ready/response 事件）
    let mut data = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        data.extend_from_slice(&chunk);
    }

    let text = String::from_utf8_lossy(&data);

    // 检测账号是否异常（muted / 限流等）
    if text.contains(r#""biz_code":"#) {
        error!(
            target: "ds_core::accounts",
            "health_check 检测到业务错误: account={}, response={}",
            display_id,
            text.lines().find(|l| l.contains("biz_code")).unwrap_or(&text)
        );
        return Err(PoolError::Validation("账号异常(muted/limited)".into()));
    }

    // 检查 SSE 流是否正常结束
    if !text.contains(r#""FINISHED""#) && !text.contains(r#""INCOMPLETE""#) {
        return Err(PoolError::Validation("SSE 流未正常结束".into()));
    }

    debug!(
        target: "ds_core::accounts",
        "health_check 完成 model_type={} account={} elapsed={:?}",
        model_type, display_id, start.elapsed()
    );
    Ok(())
}
