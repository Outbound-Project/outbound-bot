from __future__ import annotations

from flask import Flask, jsonify, request

from .auth import require_webhook_auth
from .config import get_config, validate_config
from .drive_service import get_drive_client
from .sheets_service import get_sheets_client
from .utils import DedupeCache, build_dedupe_key, load_state, log, save_state
from .webhook import handle_drive_changes, register_changes_watch, safe_process_folder


def create_app() -> Flask:
    app = Flask(__name__)
    config = get_config()
    try:
        validate_config(config)
    except ValueError as exc:
        print(f"Configuration error: {exc}")
        raise

    dedupe_cache = DedupeCache()

    @app.get("/health")
    def health():
        return "ok", 200

    @app.get("/healthz")
    def healthz():
        return "ok", 200

    @app.get("/status")
    def status():
        state = load_state(config.state_path)
        return jsonify(
            {
                "last_run": state.get("last_run"),
                "last_processed_zip_time": state.get("last_processed_zip_time"),
                "last_import_row_count": state.get("last_import_row_count", 0),
                "processed_zip_count": len(state.get("processed_zip_ids", [])),
            }
        ), 200

    @app.post("/watch")
    def watch():
        auth_resp = require_webhook_auth(request, config)
        if auth_resp:
            return auth_resp
        state = load_state(config.state_path)
        drive = get_drive_client(config)
        try:
            res = register_changes_watch(drive, config, state)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        return jsonify(res), 200

    @app.get("/watch/status")
    def watch_status():
        state = load_state(config.state_path)
        return jsonify(
            {
                "channel_id": state.get("channel_id"),
                "channel_resource_id": state.get("channel_resource_id"),
                "channel_expiration": state.get("channel_expiration"),
                "page_token": state.get("page_token"),
            }
        ), 200

    @app.post("/watch/renew")
    def watch_renew():
        auth_resp = require_webhook_auth(request, config)
        if auth_resp:
            return auth_resp
        state = load_state(config.state_path)
        drive = get_drive_client(config)
        try:
            res = register_changes_watch(drive, config, state)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        return jsonify(res), 200

    @app.post("/watch/auto-renew")
    def watch_auto_renew():
        auth_resp = require_webhook_auth(request, config)
        if auth_resp:
            return auth_resp
        state = load_state(config.state_path)
        drive = get_drive_client(config)
        try:
            res = register_changes_watch(drive, config, state)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        return jsonify({"ok": True, "channel_id": res.get("id"), "expiration": res.get("expiration")}), 200

    @app.post("/run")
    def run_once():
        auth_resp = require_webhook_auth(request, config)
        if auth_resp:
            return auth_resp
        force = request.args.get("force", "").lower() in {"1", "true", "yes"}
        state = load_state(config.state_path)
        drive = get_drive_client(config)
        sheets = get_sheets_client(config)
        safe_process_folder(drive, sheets, config, state, ignore_last_dt=force)
        save_state(config.state_path, state)
        return jsonify({"ok": True, "forced": force}), 200

    @app.post("/webhook")
    def webhook():
        auth_resp = require_webhook_auth(request, config)
        if auth_resp:
            return auth_resp

        resource_state = request.headers.get("X-Goog-Resource-State", "")
        if resource_state == "sync":
            return "", 200

        dedupe_key = build_dedupe_key(
            [
                request.headers.get("X-Goog-Resource-Id", ""),
                request.headers.get("X-Goog-Message-Number", ""),
                request.headers.get("X-Goog-Channel-Id", ""),
                request.headers.get("X-Goog-Resource-State", ""),
            ]
        )
        if dedupe_key and dedupe_cache.seen(dedupe_key):
            return jsonify({"changed": False, "deduped": True}), 200

        log("Webhook request started.")
        state = load_state(config.state_path)
        drive = get_drive_client(config)
        sheets = get_sheets_client(config)
        changed = False
        try:
            changed = handle_drive_changes(drive, sheets, config, state)
        except Exception as exc:
            log(f"Webhook error: {exc}")
            return jsonify({"error": "webhook failure"}), 500
        finally:
            log("Webhook request finished.")
        return jsonify({"changed": changed}), 200

    return app


app = create_app()


def main() -> None:
    config = get_config()
    try:
        validate_config(config)
    except ValueError as exc:
        print(f"Configuration error: {exc}")
        raise

    from .webhook import process_folder

    state = load_state(config.state_path)
    drive = get_drive_client(config)
    sheets = get_sheets_client(config)

    print("Processing folder:", config.drive_parent_folder_id)
    process_folder(drive, sheets, config, config.drive_parent_folder_id, state, ignore_last_dt=False)
    save_state(config.state_path, state)


if __name__ == "__main__":
    main()
