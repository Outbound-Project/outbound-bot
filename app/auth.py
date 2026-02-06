from __future__ import annotations

from flask import jsonify, Request

from .config import AppConfig


def require_webhook_auth(req: Request, config: AppConfig):
    if not config.webhook_token:
        return None
    token = req.headers.get("X-Goog-Channel-Token")
    if token != config.webhook_token:
        return jsonify({"error": "invalid token"}), 401
    return None
