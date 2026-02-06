from flask import Flask, request

from app.auth import require_webhook_auth
from app.config import get_config


def _base_env(monkeypatch):
    monkeypatch.setenv("BACKLOGS_DRIVE_PARENT_FOLDER_ID", "folder123")
    monkeypatch.setenv("BACKLOGS_DEST_SHEET_ID", "sheet123")


def test_require_webhook_auth_allows_when_missing(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.delenv("WEBHOOK_TOKEN", raising=False)
    config = get_config()

    app = Flask(__name__)
    with app.test_request_context("/webhook"):
        assert require_webhook_auth(request, config) is None


def test_require_webhook_auth_rejects_invalid(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("WEBHOOK_TOKEN", "secret")
    config = get_config()

    app = Flask(__name__)
    with app.test_request_context("/webhook", headers={"X-Goog-Channel-Token": "wrong"}):
        resp = require_webhook_auth(request, config)
        assert resp is not None
        body, status = resp
        assert status == 401
