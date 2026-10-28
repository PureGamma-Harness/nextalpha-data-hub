from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from nextalpha_market_watcher.config import AppConfig, load_config

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILES = ["config.example.yaml", "config.prod.yaml", "config.docker.yaml"]


@pytest.fixture(autouse=True)
def _clear_webhook_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("WEBHOOK__URL", "WEBHOOK__ALERTS_URL", "WEBHOOK__PROXY_URL"):
        monkeypatch.delenv(name, raising=False)


def _write(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def test_env_fills_empty_yaml_webhook(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WEBHOOK__URL", "https://example.invalid/main")
    monkeypatch.setenv("WEBHOOK__ALERTS_URL", "https://example.invalid/alerts")
    path = _write(
        tmp_path,
        {"webhook": {"enabled": True, "url": "", "alerts_url": "", "proxy_url": None}},
    )
    cfg = load_config(path)
    assert cfg.webhook.enabled is True
    assert cfg.webhook.url == "https://example.invalid/main"
    assert cfg.webhook.alerts_url == "https://example.invalid/alerts"
    assert cfg.webhook.proxy_url is None


def test_env_fills_missing_webhook_section(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WEBHOOK__URL", "https://example.invalid/main")
    cfg = load_config(_write(tmp_path, {"log_level": "DEBUG"}))
    assert cfg.webhook.url == "https://example.invalid/main"
    assert cfg.log_level == "DEBUG"


def test_non_empty_yaml_value_wins_over_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WEBHOOK__URL", "https://example.invalid/env")
    cfg = load_config(_write(tmp_path, {"webhook": {"url": "https://example.invalid/yaml"}}))
    assert cfg.webhook.url == "https://example.invalid/yaml"


def test_service_mode_override(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, {"service_mode": "all"}), service_mode="alerts")
    assert cfg.service_mode == "alerts"


def test_empty_yaml_file(tmp_path: Path) -> None:
    path = tmp_path / "empty.yaml"
    path.write_text("", encoding="utf-8")
    assert isinstance(load_config(path), AppConfig)


@pytest.mark.parametrize("name", CONFIG_FILES)
def test_repo_configs_validate_and_contain_no_webhook_secrets(name: str) -> None:
    cfg = load_config(REPO_ROOT / name)
    assert cfg.webhook.url == ""
    assert cfg.webhook.alerts_url == ""
    raw = (REPO_ROOT / name).read_text(encoding="utf-8")
    assert "/bot/v2/hook/" not in raw
