#!/usr/bin/env python3
"""e2e 测试共用配置模块 —— 加载 py-e2e-tests/config.toml"""

import sys
from pathlib import Path

import tomllib


def load_config() -> dict:
    config_path = Path(__file__).parent / "config.toml"
    if not config_path.exists():
        print(f"[错误] 未找到 {config_path}")
        print(f"  请从项目根目录复制并编辑:")
        print(f"    cp config.example.toml {config_path}")
        print(f"  然后将 [[accounts]] 改为 accounts = []（e2e 测试无需真实账号）")
        sys.exit(1)

    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    ds = config.get("deepseek", {})
    model_types = ds.get("model_types", ["default", "expert", "vision"])
    models = [f"deepseek-{t}" for t in model_types]

    api_keys = config.get("api_keys", [])
    api_key = api_keys[0]["key"] if api_keys else "sk-test"
    port = config.get("server", {}).get("port", 22217)
    accounts = len(config.get("accounts", []))

    return {
        "api_key": api_key,
        "port": port,
        "models": models,
        "model_types": model_types,
        "input_character_limits": ds.get("input_character_limits", [2_621_440, 163_840, 2_621_440]),
        "accounts": accounts,
        "safe_concurrency": max(1, accounts // 2),
    }
