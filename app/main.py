from __future__ import annotations

from flask import Flask, jsonify, request

from .auth import require_webhook_auth
from .config import WorkflowConfig, get_config, validate_config
from .drive_service import get_drive_client
from .sheets_service import get_sheets_client
from .utils import DedupeCache, build_dedupe_key, load_state, log, save_state
from .webhook import handle_drive_changes, register_changes_watch, safe_process_folder, process_folder


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

    def register_workflow_routes(prefix: str, workflow: WorkflowConfig) -> None:
        route_prefix = f"/{prefix}".rstrip("/")

        @app.get(f"{route_prefix}/status", endpoint=f"{prefix}_status")
        def workflow_status():
            state = load_state(workflow.state_path, workflow.state_key)
            return jsonify(
                {
                    "last_run": state.get("last_run"),
                    "last_processed_zip_time": state.get("last_processed_zip_time"),
                    "last_import_row_count": state.get("last_import_row_count", 0),
                    "processed_zip_count": len(state.get("processed_zip_ids", [])),
                }
            ), 200

        @app.post(f"{route_prefix}/watch", endpoint=f"{prefix}_watch")
        def workflow_watch():
            auth_resp = require_webhook_auth(request, config)
            if auth_resp:
                return auth_resp
            state = load_state(workflow.state_path, workflow.state_key)
            drive = get_drive_client(config)
            try:
                res = register_changes_watch(drive, workflow, state)
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400
            except Exception as exc:
                log(f"{workflow.name} watch error: {exc}")
                return jsonify({"error": "watch registration failed"}), 500
            return jsonify(res), 200

        @app.get(f"{route_prefix}/watch/status", endpoint=f"{prefix}_watch_status")
        def workflow_watch_status():
            state = load_state(workflow.state_path, workflow.state_key)
            return jsonify(
                {
                    "channel_id": state.get("channel_id"),
                    "channel_resource_id": state.get("channel_resource_id"),
                    "channel_expiration": state.get("channel_expiration"),
                    "page_token": state.get("page_token"),
                }
            ), 200

        @app.post(f"{route_prefix}/watch/renew", endpoint=f"{prefix}_watch_renew")
        def workflow_watch_renew():
            auth_resp = require_webhook_auth(request, config)
            if auth_resp:
                return auth_resp
            state = load_state(workflow.state_path, workflow.state_key)
            drive = get_drive_client(config)
            try:
                res = register_changes_watch(drive, workflow, state)
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400
            except Exception as exc:
                log(f"{workflow.name} watch renew error: {exc}")
                return jsonify({"error": "watch renewal failed"}), 500
            return jsonify(res), 200

        @app.post(f"{route_prefix}/watch/auto-renew", endpoint=f"{prefix}_watch_auto_renew")
        def workflow_watch_auto_renew():
            auth_resp = require_webhook_auth(request, config)
            if auth_resp:
                return auth_resp
            state = load_state(workflow.state_path, workflow.state_key)
            drive = get_drive_client(config)
            try:
                res = register_changes_watch(drive, workflow, state)
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400
            except Exception as exc:
                log(f"{workflow.name} watch auto-renew error: {exc}")
                return jsonify({"error": "watch auto-renew failed"}), 500
            return jsonify({"ok": True, "channel_id": res.get("id"), "expiration": res.get("expiration")}), 200

        @app.post(f"{route_prefix}/run", endpoint=f"{prefix}_run")
        def workflow_run_once():
            auth_resp = require_webhook_auth(request, config)
            if auth_resp:
                return auth_resp
            force = request.args.get("force", "").lower() in {"1", "true", "yes"}
            state = load_state(workflow.state_path, workflow.state_key)
            drive = get_drive_client(config)
            sheets = get_sheets_client(config)
            safe_process_folder(drive, sheets, workflow, state, ignore_last_dt=force)
            save_state(workflow.state_path, state, workflow.state_key)
            return jsonify({"ok": True, "forced": force}), 200

        @app.post(f"{route_prefix}/webhook", endpoint=f"{prefix}_webhook")
        def workflow_webhook():
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
            state = load_state(workflow.state_path, workflow.state_key)
            drive = get_drive_client(config)
            sheets = get_sheets_client(config)
            changed = False
            try:
                changed = handle_drive_changes(drive, sheets, workflow, state)
            except Exception as exc:
                log(f"Webhook error: {exc}")
                return jsonify({"error": "webhook failure"}), 500
            finally:
                log("Webhook request finished.")
            return jsonify({"changed": changed}), 200

    backlogs = config.workflows["backlogs"]
    register_workflow_routes("backlogs", backlogs)
    workflow2 = config.workflows.get("workflow2")
    if workflow2 and workflow2.drive_parent_folder_id and workflow2.dest_sheet_id:
        register_workflow_routes("workflow2", workflow2)

    @app.get("/status")
    def status():
        state = load_state(backlogs.state_path, backlogs.state_key)
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
        state = load_state(backlogs.state_path, backlogs.state_key)
        drive = get_drive_client(config)
        try:
            res = register_changes_watch(drive, backlogs, state)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            log(f"backlogs watch error: {exc}")
            return jsonify({"error": "watch registration failed"}), 500
        return jsonify(res), 200

    @app.get("/watch/status")
    def watch_status():
        state = load_state(backlogs.state_path, backlogs.state_key)
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
        state = load_state(backlogs.state_path, backlogs.state_key)
        drive = get_drive_client(config)
        try:
            res = register_changes_watch(drive, backlogs, state)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            log(f"backlogs watch renew error: {exc}")
            return jsonify({"error": "watch renewal failed"}), 500
        return jsonify(res), 200

    @app.post("/watch/auto-renew")
    def watch_auto_renew():
        auth_resp = require_webhook_auth(request, config)
        if auth_resp:
            return auth_resp
        state = load_state(backlogs.state_path, backlogs.state_key)
        drive = get_drive_client(config)
        try:
            res = register_changes_watch(drive, backlogs, state)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            log(f"backlogs watch auto-renew error: {exc}")
            return jsonify({"error": "watch auto-renew failed"}), 500
        return jsonify({"ok": True, "channel_id": res.get("id"), "expiration": res.get("expiration")}), 200

    @app.post("/run")
    def run_once():
        auth_resp = require_webhook_auth(request, config)
        if auth_resp:
            return auth_resp
        force = request.args.get("force", "").lower() in {"1", "true", "yes"}
        state = load_state(backlogs.state_path, backlogs.state_key)
        drive = get_drive_client(config)
        sheets = get_sheets_client(config)
        safe_process_folder(drive, sheets, backlogs, state, ignore_last_dt=force)
        save_state(backlogs.state_path, state, backlogs.state_key)
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
        state = load_state(backlogs.state_path, backlogs.state_key)
        drive = get_drive_client(config)
        sheets = get_sheets_client(config)
        changed = False
        try:
            changed = handle_drive_changes(drive, sheets, backlogs, state)
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
    backlogs = config.workflows["backlogs"]
    state = load_state(backlogs.state_path, backlogs.state_key)
    drive = get_drive_client(config)
    sheets = get_sheets_client(config)

    print("Processing folder:", backlogs.drive_parent_folder_id)
    process_folder(
        drive, sheets, backlogs, backlogs.drive_parent_folder_id, state, ignore_last_dt=False
    )
    save_state(backlogs.state_path, state, backlogs.state_key)


if __name__ == "__main__":
    main()
