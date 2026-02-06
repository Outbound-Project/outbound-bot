import pytest

from app.config import get_config, validate_config


def _base_env(monkeypatch):
    monkeypatch.setenv("BACKLOGS_DRIVE_PARENT_FOLDER_ID", "folder123")
    monkeypatch.setenv("BACKLOGS_DEST_SHEET_ID", "sheet123")


def test_validate_config_requires_missing(monkeypatch):
    monkeypatch.delenv("DRIVE_PARENT_FOLDER_ID", raising=False)
    monkeypatch.delenv("DEST_SHEET_ID", raising=False)
    monkeypatch.delenv("BACKLOGS_DRIVE_PARENT_FOLDER_ID", raising=False)
    monkeypatch.delenv("BACKLOGS_DEST_SHEET_ID", raising=False)
    config = get_config()
    with pytest.raises(ValueError):
        validate_config(config)


def test_validate_config_prod_requires_token(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("WEBHOOK_TOKEN", raising=False)
    monkeypatch.setenv("ALLOW_INSECURE_WEBHOOK", "false")
    config = get_config()
    with pytest.raises(ValueError):
        validate_config(config)


def test_validate_config_prod_allows_insecure(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("ALLOW_INSECURE_WEBHOOK", "true")
    config = get_config()
    validate_config(config)
