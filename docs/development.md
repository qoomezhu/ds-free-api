# 开发指南

## 环境要求

- Rust **1.95.0+**（见 `rust-toolchain.toml`）
- Bun **1.3+**（Web 面板构建与开发）
- `cmake`、`g++`、`libclang-dev`（编译 `wreq` 依赖的 BoringSSL，仅编译期需要）
- `just` 命令运行器（用于 `just serve` / `just check` 等快捷命令）

> 运行时无任何外部依赖，单二进制即可运行。

## 首次启动

```bash
# 1. 复制配置（可选，首次启动会自动创建最小配置）
cp config.example.toml config.toml

# 2. 构建 Web 前端（编译时嵌入二进制，每次前端变更需要重构建）
cd web && bun install && bun run build && cd ..

# 3. 运行开发服务器
just serve
```

服务器启动后访问 `http://localhost:22217` 自动跳转到管理面板。

> **前端热更新开发**：同时运行 `cd web && bun run dev`（Vite HMR 模式）
> 和 `just serve`，后端优先使用文件系统 `web/dist/` 目录中的静态文件。
> 无需每次前端改动都重构建二进制。

## Release 构建

```bash
# 1. 构建 Web 前端
cd web && bun install && bun run build && cd ..

# 2. 构建 Release 二进制
cargo build --release

# 3. 运行（也可直接运行二进制，无需 web/dist/ 目录）
./target/release/ds-free-api
```

Release 二进制通过 `rust_embed` 编译时嵌入前端资源，`web/dist/` 目录不存在时
自动使用嵌入资源。发布版无需额外文件。

### ARM 服务器本地编译

ARM 服务器（如 Ampere Altra、AWS Graviton、Apple Silicon）可直接本地编译：

```bash
# 安装编译依赖（Debian/Ubuntu）
sudo apt-get install -y cmake g++ libclang-dev

# 构建前端（若已安装 Bun）
cd web && bun install && bun run build && cd ..

# 编译
cargo build --release
./target/release/ds-free-api
```

无需交叉编译配置，原生 ARM 工具链即可。

## CI 自动构建

GitHub Actions（`.github/workflows/release.yml`）在 tag push 时自动执行 8 个目标的构建：

```
build-frontend (bun install --frozen-lockfile + bun run build)
  ├── build-linux-gnu    (x86_64 + aarch64)     ┐
  ├── build-linux-musl   (x86_64 + aarch64)     │
  ├── build-macos        (x86_64 + aarch64)     ┼── release (tar.gz + zip + SHA256SUMS)
  └── build-windows      (x86_64 + aarch64)     ┘
  └── docker (multi-arch: linux/amd64 + linux/arm64 → ghcr.io)
```

**构建矩阵**：

| 平台 | Target | 说明 |
| --- | --- | --- |
| Linux x86 (glibc) | `x86_64-unknown-linux-gnu` | 原生 x86 runner |
| Linux ARM (glibc) | `aarch64-unknown-linux-gnu` | 原生 ARM runner (`ubuntu-24.04-arm`) |
| Linux x86 (musl, 静态) | `x86_64-unknown-linux-musl` | musl-cross 交叉编译 |
| Linux ARM (musl, 静态) | `aarch64-unknown-linux-musl` | musl-cross 交叉编译 |
| macOS x86 | `x86_64-apple-darwin` | Intel Mac |
| macOS ARM | `aarch64-apple-darwin` | Apple Silicon |
| Windows x86 | `x86_64-pc-windows-msvc` | |
| Windows ARM | `aarch64-pc-windows-msvc` | |

`build-frontend` 产出 `web-dist` artifact，各编译 job 下载后再执行 `cargo build`，
保证 `rust_embed` 嵌入真实前端文件。

Docker 镜像自动推送到 `ghcr.io/qoomezhu/ds-free-api:latest`（multi-arch，同时支持 amd64 和 arm64）。

## Docker 部署（生产）

### 方式一：从 ghcr.io 拉取（推荐）

```bash
# ARM 服务器和 x86 服务器均可直接使用，Docker 自动选择对应架构
docker compose -f docker/docker-compose.yaml up -d
```

容器首次启动时自动创建最小配置，无需提前准备 `config.toml`。
配置和数据通过 bind mount 持久化到宿主机的 `docker/config/` 和 `docker/data/`。

### 方式二：本地构建镜像

从源码构建本地 Docker 镜像（用于开发或定制）：

```bash
# 1. 构建前端 + 编译二进制（当前平台）
cd web && bun install && bun run build && cd ..
cargo build --release

# 2. 准备 Docker 构建上下文（按 docker/Dockerfile 要求放置二进制）
mkdir -p target/docker/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
cp target/release/ds-free-api target/docker/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')/

# 3. 构建 Docker 镜像
docker build -f docker/Dockerfile -t ds-free-api .

# 4. 导出并传输到服务器
docker save ds-free-api | gzip > ds-free-api.tar.gz
scp ds-free-api.tar.gz user@server:/tmp/

# 5. 服务器加载并启动
ssh user@server
docker load < /tmp/ds-free-api.tar.gz
docker compose -f docker/docker-compose.yaml up -d
```

> 服务器原生环境（x86 或 ARM）可直接在服务器上执行上述构建，速度更快。
> Docker 镜像仅包含预编译二进制 + 嵌入的前端资源，无需在容器内编译。

## 部署到 ARM 服务器

本项目原生支持 ARM 架构，三种部署方式任选：

### 1. Docker（最简单）

```bash
# ARM 服务器直接拉取，Docker 自动选择 arm64 manifest
docker pull ghcr.io/qoomezhu/ds-free-api:latest
docker compose -f docker/docker-compose.yaml up -d
```

### 2. 预编译二进制

从 GitHub Release 下载 ARM 二进制：

```bash
# ARM 服务器（glibc，如 Ubuntu/Debian on ARM）
wget https://github.com/qoomezhu/ds-free-api/releases/latest/download/ds-free-api-vX.Y.Z-linux-aarch64-gnu.tar.gz
tar xzf ds-free-api-*.tar.gz && cd ds-free-api-*
./ds-free-api -c config.toml

# 或 musl 静态版（任意 ARM Linux，无 glibc 版本要求）
wget https://github.com/qoomezhu/ds-free-api/releases/latest/download/ds-free-api-vX.Y.Z-linux-aarch64-musl.tar.gz
```

### 3. 源码编译

见上方"ARM 服务器本地编译"章节。

## 命令参考

```bash
# 一键检查（check + clippy + fmt + audit + unused deps）
just check

# 运行测试
cargo test --lib

# 运行 HTTP 服务
just serve

# 统一协议调试 CLI（内置对话/比较/并发等模式）
just adapter-cli

# 使用 e2e 专属配置启动服务
just e2e-serve
```

## e2e 测试

`py-e2e-tests/` 是基于 JSON 场景驱动的端到端测试框架，无需 pytest 依赖。分为三层：

| 层级       | 命令              | 覆盖范围                                              |
| ---------- | ----------------- | ----------------------------------------------------- |
| **Basic**  | `just e2e-basic`  | 基础功能场景（双端点 OpenAI + Anthropic），安全并发数 |
| **Repair** | `just e2e-repair` | 工具调用异常格式修复专项（OpenAI 单端点），安全并发数 |
| **Stress** | `just e2e-stress` | 全部场景 × 3 次迭代，安全并发数 + 1 并发              |

先启动服务端：

```bash
just e2e-serve
```

再在另一个终端运行 e2e 测试：

```bash
# 基础场景测试
just e2e-basic

# 工具修复测试
just e2e-repair
```

场景文件在 `scenarios/` 中按类型独立存放：

```
py-e2e-tests/
├── scenarios/
│   ├── basic/
│   │   ├── openai/         # 7 个基础场景（对话、推理、流式、工具调用、文件上传、图片上传、HTTP链接）
│   │   └── anthropic/      # 7 个基础场景（对话、推理、流式、工具调用、文档上传、图片上传、HTTP链接）
│   └── repair/             # 10 个工具损坏格式场景
├── runner.py               # 单次运行入口
├── stress_runner.py        # 多迭代压测入口
└── config.toml             # e2e 专用服务端配置
```

每个场景为独立 JSON 文件，包含请求参数和校验规则：

```json
{
  "name": "场景名称",
  "endpoint": "openai|anthropic",
  "category": "basic|repair",
  "models": ["deepseek-default", "deepseek-expert", "deepseek-vision"],
  "messages": [{"role": "user", "content": "..."}],
  "tools": [...],
  "tool_choice": "auto",
  "request": {"stream": false},
  "checks": {
    "has_tool_calls": true,
    "tool_names": ["get_weather"],
    "finish_reason": "tool_calls",
    "no_error": true
  }
}
```

### e2e CLI 参数

**`just e2e-basic` 和 `just e2e-repair`（单次运行）：**

| 参数 | 作用 |
|------|------|
| `scenario_dir` | 场景目录，如 `scenarios/basic` 或 `scenarios/repair` |
| `--endpoint` | 端点过滤：`openai` / `anthropic` |
| `--model` | 模型过滤：`deepseek-default` / `deepseek-expert` |
| `--filter` | 场景名称关键字过滤（多个用空格分隔，如 `--filter 文件 图片`）|
| `--parallel` | 并行数，默认 `账号数 ÷ 2` |
| `--show-output` | 显示模型回复摘要、工具调用、结束原因 |
| `--report` | 输出 JSON 报告路径 |

**`just e2e-stress`（压测）：**

| 参数 | 作用 |
|------|------|
| `--iterations` | 每场景迭代次数，默认 3 |
| `--models` | 模型列表过滤 |
| `--filter` | 场景名称关键字过滤（多个用空格分隔）|
| `--parallel` | 并行数，默认 `账号数 ÷ 2 + 1` |
| `--show-output` | 显示模型输出 |
| `--report` | 输出 JSON 报告路径 |

使用示例：

```bash
# 快速验证新加的文件上传场景
just e2e-basic --filter 文件 图片 --show-output

# 仅查看 OpenAI 端点的 expert 模型
just e2e-basic --endpoint openai --model deepseek-expert

# 串行调试
just e2e-basic --endpoint openai --parallel 1 --show-output

# 压测：工具调用修复场景 × 5 次迭代
just e2e-stress --filter 修复 --iterations 5

# 输出 JSON 报告
just e2e-basic --report result.json
```

## 发布新版本

1. 更新 `Cargo.toml` 中的 `version` 字段
2. 在 `CHANGELOG.md` 中添加对应版本的变更记录
3. 提交并打 tag：

```bash
git tag v0.x.x
git push origin v0.x.x
```

CI 自动触发 8 平台构建 + Docker multi-arch 推送 + GitHub Release（draft），
版本号需与 `Cargo.toml` 一致（CI 会校验）。

## 更多文档

- [代码规范](code-style.md)
- [日志规范](logging-spec.md)
- [Prompt 注入策略](deepseek-prompt-injection.md)