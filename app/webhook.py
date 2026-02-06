from __future__ import annotations

from datetime import datetime, timezone
import uuid
from typing import Dict, List

from .config import WorkflowConfig
from .drive_service import download_zip, get_start_page_token, list_zip_files
from .sheets_service import (
    append_rows_to_sheet,
    clear_destination_sheet,
    process_zip,
    send_dashboard_images,
    update_backlogs_status,
    format_status_now,
)
from .utils import log, parse_rfc3339, save_state, with_retries


def collect_rows_from_folder(
    drive, workflow: WorkflowConfig, folder_id: str, state: Dict, ignore_last_dt: bool
) -> Dict:
    zips = list_zip_files(drive, folder_id)
    last_ts = state.get("last_processed_zip_time")
    last_dt = None if ignore_last_dt or not last_ts else parse_rfc3339(last_ts)

    new_rows: List[List[str]] = []
    max_dt = last_dt
    new_zip_ids: List[str] = []

    for z in zips:
        z_dt = parse_rfc3339(z["modifiedTime"])
        is_new = (
            workflow.force_overwrite
            or ignore_last_dt
            or (last_dt is None or z_dt > last_dt)
            or z["id"] not in state.get("processed_zip_ids", [])
        )
        if not is_new:
            continue
        print("ZIP:", z["name"])
        zip_bytes = download_zip(drive, z["id"])
        rows = process_zip(zip_bytes)
        if rows:
            new_rows.extend(rows[1:])
            new_zip_ids.append(z["id"])
            if max_dt is None or z_dt > max_dt:
                max_dt = z_dt

    return {"rows": new_rows, "zip_ids": new_zip_ids, "max_dt": max_dt}


def process_folder(
    drive, sheets, workflow: WorkflowConfig, folder_id: str, state: Dict, ignore_last_dt: bool
) -> None:
    update_backlogs_status(sheets, workflow, "Fetching data...")

    result = collect_rows_from_folder(drive, workflow, folder_id, state, ignore_last_dt)
    new_rows = result["rows"]
    new_zip_ids = result["zip_ids"]
    max_dt = result["max_dt"]
    now_display = format_status_now()

    if not new_rows:
        update_backlogs_status(sheets, workflow, now_display)
        print("No new ZIPs to import.")
        return

    append_rows_to_sheet(sheets, workflow, new_rows)

    processed_zip_ids = set(state.get("processed_zip_ids", []))
    processed_zip_ids.update(new_zip_ids)
    state["processed_zip_ids"] = list(processed_zip_ids)
    state["last_import_row_count"] = len(new_rows)
    if max_dt:
        state["last_processed_zip_time"] = max_dt.isoformat()
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(workflow.state_path, state, workflow.state_key)

    update_backlogs_status(sheets, workflow, now_display)
    if workflow.skip_seatalk_images:
        log("Skipping SeaTalk images (SKIP_SEATALK_IMAGES enabled).")
    else:
        send_dashboard_images(sheets, workflow, now_display)
    print("Import complete.")


def register_changes_watch(drive, workflow: WorkflowConfig, state: Dict) -> Dict:
    if not workflow.webhook_url:
        raise ValueError("WEBHOOK_URL is required to register a watch channel.")
    page_token = state.get("page_token") or get_start_page_token(drive)
    state["page_token"] = page_token

    channel_id = str(uuid.uuid4())
    body = {"id": channel_id, "type": "web_hook", "address": workflow.webhook_url}
    if workflow.webhook_token:
        body["token"] = workflow.webhook_token

    res = drive.changes().watch(pageToken=page_token, body=body).execute()
    state["channel_id"] = res.get("id")
    state["channel_resource_id"] = res.get("resourceId")
    state["channel_expiration"] = res.get("expiration")
    save_state(workflow.state_path, state, workflow.state_key)
    return res


def handle_drive_changes(drive, sheets, workflow: WorkflowConfig, state: Dict) -> bool:
    page_token = state.get("page_token")
    if not page_token:
        state["page_token"] = get_start_page_token(drive)
        save_state(workflow.state_path, state, workflow.state_key)
        return False

    changed = False
    deleted_zip = False
    new_start_token = None

    while page_token:
        res = drive.changes().list(
            pageToken=page_token,
            spaces="drive",
            fields=(
                "changes(fileId,file(name,mimeType,parents,trashed,createdTime,modifiedTime)),"
                "nextPageToken,newStartPageToken"
            ),
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()

        for change in res.get("changes", []):
            file = change.get("file")
            if not file:
                # If file metadata is missing, rely on tracked IDs.
                file_id = change.get("fileId")
                if file_id and file_id in state.get("processed_zip_ids", []):
                    deleted_zip = True
                continue
            if file.get("trashed"):
                name = str(file.get("name", "")).lower()
                parents = file.get("parents", []) or []
                file_id = change.get("fileId") or file.get("id")
                if (
                    name.endswith(".zip")
                    and (not parents or workflow.drive_parent_folder_id in parents)
                ) or (file_id and file_id in state.get("processed_zip_ids", [])):
                    deleted_zip = True
                continue
            parents = file.get("parents", [])
            if workflow.drive_parent_folder_id not in parents:
                continue
            changed = True

        page_token = res.get("nextPageToken")
        new_start_token = res.get("newStartPageToken", new_start_token)

    if new_start_token:
        state["page_token"] = new_start_token
        save_state(workflow.state_path, state, workflow.state_key)

    if deleted_zip:
        print("ZIP deleted in parent folder. Clearing destination sheet.")
        clear_destination_sheet(sheets, workflow, state)
        save_state(workflow.state_path, state, workflow.state_key)
        return True

    if changed:
        print("Change detected in parent folder. Scanning for new ZIPs.")
        process_folder(drive, sheets, workflow, workflow.drive_parent_folder_id, state, ignore_last_dt=False)
    return changed


def safe_process_folder(drive, sheets, workflow: WorkflowConfig, state: Dict, ignore_last_dt: bool) -> None:
    with_retries(
        lambda: process_folder(
            drive, sheets, workflow, workflow.drive_parent_folder_id, state, ignore_last_dt
        ),
        "Process folder",
    )
