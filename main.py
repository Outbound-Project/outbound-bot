import io
import json
import os
import uuid
import zipfile
from base64 import b64encode
from datetime import datetime, timezone, timedelta
import time
from typing import Dict, List
from urllib import request as urlrequest

import pandas as pd
from flask import Flask, jsonify, request
from PIL import Image, ImageDraw, ImageFont
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
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
SEATALK_WEBHOOK_URL = os.environ.get("SEATALK_WEBHOOK_URL", "")

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


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    print(f"[{ts}] {msg}")


def _should_retry_http_error(exc: Exception) -> bool:
    if isinstance(exc, HttpError):
        try:
            return 500 <= int(exc.resp.status) < 600
        except Exception:
            return True
    return False


def _with_retries(func, label: str, attempts: int = 5):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:
            if attempt >= attempts or not _should_retry_http_error(exc):
                raise
            log(f"{label} failed (attempt {attempt}/{attempts}); retrying in {delay:.1f}s: {exc}")
            time.sleep(delay)
            delay *= 2


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
    res = drive.files().list(
        q=q,
        fields="files(id,name,modifiedTime)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
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

    _with_retries(
        lambda: sheets.spreadsheets().values().clear(
            spreadsheetId=DEST_SHEET_ID,
            range=f"'{safe}'!A:J",
            body={}
        ).execute(),
        "Sheets clear"
    )

    _with_retries(
        lambda: sheets.spreadsheets().values().update(
            spreadsheetId=DEST_SHEET_ID,
            range=f"'{safe}'!A1:J",
            valueInputOption="RAW",
            body={"values": values}
        ).execute(),
        "Sheets update"
    )


def get_range_values(sheets, tab_name: str, a1_range: str) -> List[List[str]]:
    safe = tab_name.replace("'", "''")
    res = _with_retries(
        lambda: sheets.spreadsheets().values().get(
            spreadsheetId=DEST_SHEET_ID,
            range=f"'{safe}'!{a1_range}",
        ).execute(),
        "Sheets get range"
    ) or {}
    return res.get("values", [])


def normalize_table(values: List[List[str]]) -> List[List[str]]:
    if not values:
        return [["(no data)"]]
    max_cols = max(len(row) for row in values)
    normalized = []
    for row in values:
        padded = row + [""] * (max_cols - len(row))
        normalized.append(padded)
    return normalized


def render_table_image(values: List[List[str]]) -> bytes:
    values = normalize_table(values)
    font = ImageFont.load_default()
    char_px = max(6, int(font.getlength("W")))
    max_cell_chars = 18
    cell_padding = 4
    row_height = 20

    col_count = len(values[0])
    col_widths = []
    for col in range(col_count):
        max_len = max(len(str(row[col])) for row in values)
        max_len = min(max_len, max_cell_chars)
        col_widths.append(max_len * char_px + cell_padding * 2)

    width = sum(col_widths) + 1
    height = len(values) * row_height + 1
    img = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(img)

    y = 0
    for row in values:
        x = 0
        for idx, cell in enumerate(row):
            draw.rectangle([x, y, x + col_widths[idx], y + row_height], outline="#d9d9d9")
            text = str(cell)
            if len(text) > max_cell_chars:
                text = text[: max_cell_chars - 1] + "…"
            draw.text((x + cell_padding, y + 3), text, fill="#111111", font=font)
            x += col_widths[idx]
        y += row_height

    max_width = 2000
    max_height = 1400
    if img.width > max_width or img.height > max_height:
        ratio = min(max_width / img.width, max_height / img.height)
        new_size = (int(img.width * ratio), int(img.height * ratio))
        resample = getattr(getattr(Image, "Resampling", None), "LANCZOS", 3)
        img = img.resize(new_size, resample)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def seatalk_post(payload: Dict) -> None:
    if not SEATALK_WEBHOOK_URL:
        return
    data = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        SEATALK_WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlrequest.urlopen(req, timeout=30) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"SeaTalk webhook failed: {resp.status}")


def send_seatalk_image(image_bytes: bytes) -> None:
    encoded = b64encode(image_bytes).decode("ascii")
    seatalk_post({"tag": "image", "image_base64": {"content": encoded}})


def send_seatalk_text(text: str) -> None:
    seatalk_post({"tag": "text", "text": {"content": text}})


def send_dashboard_images(sheets, sent_ts_pht: str) -> None:
    if not SEATALK_WEBHOOK_URL:
        return

    images: List[List[List[str]]] = []
    images.append(get_range_values(sheets, "Backlogs Summary", "B2:R63"))
    images.append(get_range_values(sheets, "[SOC5] SOCPacked_Dashboard", "A1:T29"))

    header = get_range_values(sheets, "[SOC5] SOCPacked_Dashboard", "A1:U9")
    for cont_range in ["A30:U48", "A50:U94", "A95:U127", "A129:U157"]:
        cont = get_range_values(sheets, "[SOC5] SOCPacked_Dashboard", cont_range)
        combined = header + [[""]] + cont
        images.append(combined)

    for values in images:
        image_bytes = render_table_image(values)
        send_seatalk_image(image_bytes)

    send_seatalk_text(
        f"{{mention @all}} Sharing OB Pending for dispatch as of {sent_ts_pht}. Thank you!"
    )


def update_backlogs_status(sheets, value: str):
    safe = BACKLOGS_STATUS_TAB.replace("'", "''")
    _with_retries(
        lambda: sheets.spreadsheets().values().update(
            spreadsheetId=DEST_SHEET_ID,
            range=f"'{safe}'!{BACKLOGS_STATUS_CELL}",
            valueInputOption="RAW",
            body={"values": [[value]]},
        ).execute(),
        "Sheets update status"
    )


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
    send_dashboard_images(sheets, now_display)
    print("Import complete.")


def get_start_page_token(drive) -> str:
    res = drive.changes().getStartPageToken(supportsAllDrives=True).execute()
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
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
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

    log("Webhook request started.")
    state = load_state()
    drive, sheets = build_clients()
    changed = False
    try:
        changed = handle_drive_changes(drive, sheets, state)
    except Exception as exc:
        log(f"Webhook error: {exc}")
        raise
    finally:
        log("Webhook request finished.")
    return jsonify({"changed": changed}), 200


def main():
    state = load_state()
    drive, sheets = build_clients()

    print("Processing folder:", DRIVE_PARENT_FOLDER_ID)
    process_folder(drive, sheets, DRIVE_PARENT_FOLDER_ID, state, ignore_last_dt=False)


if __name__ == "__main__":
    main()
