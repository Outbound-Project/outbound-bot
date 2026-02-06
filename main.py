import io
import json
import os
import uuid
import zipfile
from datetime import datetime, timezone, timedelta
from typing import Dict, List

import pandas as pd
from flask import Flask, jsonify, request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ================= CONFIG =================
DRIVE_PARENT_FOLDER_ID = "1oU9kj5VIJIoNrR388wYCHSdtHGanRrgZ"

DEST_SHEET_ID = "1QGrwNXNHIdUl1nT1mF5_6o9LefZqfPIs2fLuzae3Res"
DEST_SHEET_TAB_NAME = "socpacked_generated_data"
FORCE_OVERWRITE = True
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")
BACKLOGS_STATUS_TAB = "Backlogs Summary"
BACKLOGS_STATUS_CELL = "F3"

FILTERS = {
    "Receiver type": "Station",
    "Current Station": "SOC 5",
}

COLUMNS = [
    "TO Number",
    "SPX Tracking Number",
    "Receiver Name",
    "TO Order Quantity",
    "Operator",
    "Create Time",
    "Complete Time",
    "Remark",
    "Receive Status",
    "Staging Area ID",
]

STATE_PATH = "state.json"
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "creds/service_account.json")
SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON", "")

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]
# =========================================


def load_state() -> Dict:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "processed_zip_ids": [],
            "last_processed_zip_time": None,
            "page_token": None,
            "channel_id": None,
            "channel_resource_id": None,
            "channel_expiration": None,
        }


def save_state(state: Dict) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def build_clients():
    if SERVICE_ACCOUNT_JSON:
        info = json.loads(SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=SCOPES
        )
    else:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
    drive = build("drive", "v3", credentials=creds)
    sheets = build("sheets", "v4", credentials=creds)
    return drive, sheets


def list_zip_files(drive, folder_id):
    q = f"'{folder_id}' in parents and trashed=false and name contains '.zip'"
    res = drive.files().list(q=q, fields="files(id,name,modifiedTime)").execute()
    return res.get("files", [])


def download_zip(drive, file_id):
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()


def process_zip(zip_bytes) -> List[List[str]]:
    rows = []
    header_written = False

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for name in z.namelist():
            if not name.lower().endswith(".csv"):
                continue

            wanted_cols = list(dict.fromkeys(COLUMNS + list(FILTERS.keys())))
            df: pd.DataFrame = pd.read_csv(
                z.open(name),
                usecols=lambda c: c.strip() in wanted_cols,
                dtype=str,
                keep_default_na=False,
            )
            if not isinstance(df, pd.DataFrame):
                # Type guard for static checkers; read_csv should return a DataFrame here.
                df = pd.DataFrame(df)
            df.columns = df.columns.str.strip()

            for k, v in FILTERS.items():
                df = pd.DataFrame(df.loc[df[k].astype(str).str.strip() == v])

            if df.empty:
                continue

            for c in COLUMNS:
                if c not in df.columns:
                    df[c] = ""

            df = pd.DataFrame(df.loc[:, COLUMNS])

            if not header_written:
                rows.append(COLUMNS)
                header_written = True

            rows.extend(df.astype(str).fillna("").values.tolist())

    return rows


def get_existing_rows(sheets) -> List[List[str]]:
    safe = DEST_SHEET_TAB_NAME.replace("'", "''")
    res = sheets.spreadsheets().values().get(
        spreadsheetId=DEST_SHEET_ID,
        range=f"'{safe}'!A:J",
    ).execute()
    return res.get("values", [])


def parse_rfc3339(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def format_status_timestamp(dt: datetime) -> str:
    hour = dt.strftime("%I").lstrip("0") or "12"
    minute = dt.strftime("%M")
    ampm = dt.strftime("%p")
    mon = dt.strftime("%b")
    day = dt.strftime("%d").lstrip("0")
    return f"{hour}:{minute} {ampm} {mon}-{day}"


def overwrite_sheet(sheets, values):
    safe = DEST_SHEET_TAB_NAME.replace("'", "''")

    sheets.spreadsheets().values().clear(
        spreadsheetId=DEST_SHEET_ID,
        range=f"'{safe}'!A:J",
        body={}
    ).execute()

    sheets.spreadsheets().values().update(
        spreadsheetId=DEST_SHEET_ID,
        range=f"'{safe}'!A1:J",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def update_backlogs_status(sheets, value: str):
    safe = BACKLOGS_STATUS_TAB.replace("'", "''")
    sheets.spreadsheets().values().update(
        spreadsheetId=DEST_SHEET_ID,
        range=f"'{safe}'!{BACKLOGS_STATUS_CELL}",
        valueInputOption="RAW",
        body={"values": [[value]]},
    ).execute()


def collect_rows_from_folder(drive, folder_id: str, state: Dict, ignore_last_dt: bool) -> Dict:
    zips = list_zip_files(drive, folder_id)
    last_ts = state.get("last_processed_zip_time")
    last_dt = None if ignore_last_dt or not last_ts else parse_rfc3339(last_ts)

    new_rows: List[List[str]] = []
    max_dt = last_dt
    new_zip_ids: List[str] = []

    for z in zips:
        z_dt = parse_rfc3339(z["modifiedTime"])
        is_new = (
            FORCE_OVERWRITE
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


def append_rows_to_sheet(sheets, new_rows: List[List[str]]):
    existing = get_existing_rows(sheets)
    if existing:
        header = existing[0]
        base_rows = existing[1:]
    else:
        header = COLUMNS
        base_rows = []

    all_rows = [header] + base_rows + new_rows
    overwrite_sheet(sheets, all_rows)


def process_folder(drive, sheets, folder_id: str, state: Dict, ignore_last_dt: bool):
    update_backlogs_status(sheets, "Fetching data...")

    result = collect_rows_from_folder(drive, folder_id, state, ignore_last_dt)
    new_rows = result["rows"]
    new_zip_ids = result["zip_ids"]
    max_dt = result["max_dt"]
    pht = timezone(timedelta(hours=8))
    now_display = format_status_timestamp(datetime.now(pht))

    if not new_rows:
        update_backlogs_status(sheets, now_display)
        print("No new ZIPs to import.")
        return

    append_rows_to_sheet(sheets, new_rows)

    processed_zip_ids = set(state.get("processed_zip_ids", []))
    processed_zip_ids.update(new_zip_ids)
    state["processed_zip_ids"] = list(processed_zip_ids)
    state["last_import_row_count"] = len(new_rows)
    if max_dt:
        state["last_processed_zip_time"] = max_dt.isoformat()
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(state)

    update_backlogs_status(sheets, now_display)
    print("Import complete.")


def get_start_page_token(drive) -> str:
    res = drive.changes().getStartPageToken().execute()
    return res["startPageToken"]


def register_changes_watch(drive, webhook_url: str, token: str, state: Dict) -> Dict:
    if not webhook_url:
        raise ValueError("WEBHOOK_URL is required to register a watch channel.")
    page_token = state.get("page_token") or get_start_page_token(drive)
    state["page_token"] = page_token

    channel_id = str(uuid.uuid4())
    body = {"id": channel_id, "type": "web_hook", "address": webhook_url}
    if token:
        body["token"] = token

    res = drive.changes().watch(pageToken=page_token, body=body).execute()
    state["channel_id"] = res.get("id")
    state["channel_resource_id"] = res.get("resourceId")
    state["channel_expiration"] = res.get("expiration")
    save_state(state)
    return res


def handle_drive_changes(drive, sheets, state: Dict) -> bool:
    page_token = state.get("page_token")
    if not page_token:
        state["page_token"] = get_start_page_token(drive)
        save_state(state)
        return False

    changed = False
    new_start_token = None

    while page_token:
        res = drive.changes().list(
            pageToken=page_token,
            spaces="drive",
            fields=(
                "changes(fileId,file(name,mimeType,parents,trashed,createdTime,modifiedTime)),"
                "nextPageToken,newStartPageToken"
            ),
        ).execute()

        for change in res.get("changes", []):
            file = change.get("file")
            if not file:
                continue
            if file.get("trashed"):
                continue
            parents = file.get("parents", [])
            if DRIVE_PARENT_FOLDER_ID not in parents:
                continue
            # Any change in the parent folder triggers a re-scan of that folder.
            changed = True

        page_token = res.get("nextPageToken")
        new_start_token = res.get("newStartPageToken", new_start_token)

    if new_start_token:
        state["page_token"] = new_start_token
        save_state(state)

    if changed:
        print("Change detected in parent folder. Scanning for new ZIPs.")
        process_folder(drive, sheets, DRIVE_PARENT_FOLDER_ID, state, ignore_last_dt=False)
    return changed


app = Flask(__name__)


@app.get("/healthz")
def healthz():
    return "ok", 200


@app.get("/status")
def status():
    state = load_state()
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
    state = load_state()
    drive, _ = build_clients()
    try:
        res = register_changes_watch(drive, WEBHOOK_URL, WEBHOOK_TOKEN, state)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(res), 200


@app.post("/webhook")
def webhook():
    token = request.headers.get("X-Goog-Channel-Token")
    if WEBHOOK_TOKEN and token != WEBHOOK_TOKEN:
        return jsonify({"error": "invalid token"}), 401

    resource_state = request.headers.get("X-Goog-Resource-State", "")
    if resource_state == "sync":
        return "", 200

    state = load_state()
    drive, sheets = build_clients()
    changed = handle_drive_changes(drive, sheets, state)
    return jsonify({"changed": changed}), 200


def main():
    state = load_state()
    drive, sheets = build_clients()

    print("Processing folder:", DRIVE_PARENT_FOLDER_ID)
    process_folder(drive, sheets, DRIVE_PARENT_FOLDER_ID, state, ignore_last_dt=False)


if __name__ == "__main__":
    main()
