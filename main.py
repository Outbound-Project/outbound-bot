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
SKIP_SEATALK_IMAGES = os.environ.get("SKIP_SEATALK_IMAGES", "").lower() in {"1", "true", "yes"}
FONT_PATH = os.environ.get("FONT_PATH", "assets/fonts/Inter.ttf")
BASE_FONT_SIZE = int(os.environ.get("BASE_FONT_SIZE", "12"))
IMAGE_SCALE = float(os.environ.get("IMAGE_SCALE", "2"))
MAX_IMAGE_WIDTH = int(os.environ.get("MAX_IMAGE_WIDTH", "4000"))
MAX_IMAGE_HEIGHT = int(os.environ.get("MAX_IMAGE_HEIGHT", "4000"))
MAX_IMAGE_BYTES = int(os.environ.get("MAX_IMAGE_BYTES", str(4 * 1024 * 1024)))

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

STATE_PATH = os.environ.get("STATE_PATH", "")
if not STATE_PATH:
    tmp_dir = os.environ.get("TMPDIR") or os.environ.get("TEMP") or "/tmp"
    STATE_PATH = os.path.join(tmp_dir, "state.json")
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


def clear_destination_sheet(sheets, state: Dict | None = None) -> None:
    safe = DEST_SHEET_TAB_NAME.replace("'", "''")
    _with_retries(
        lambda: sheets.spreadsheets().values().clear(
            spreadsheetId=DEST_SHEET_ID,
            range=f"'{safe}'!A:J",
            body={}
        ).execute(),
        "Sheets clear"
    )
    if state is not None:
        state["processed_zip_ids"] = []
        state["last_processed_zip_time"] = None
        state["last_import_row_count"] = 0
        state["last_run"] = datetime.now(timezone.utc).isoformat()
        save_state(state)


def _color_from_google(color: Dict, default: tuple[int, int, int]) -> tuple[int, int, int]:
    if not isinstance(color, dict):
        return default
    r = int(round(255 * float(color.get("red", 0.0))))
    g = int(round(255 * float(color.get("green", 0.0))))
    b = int(round(255 * float(color.get("blue", 0.0))))
    return (r, g, b)


def _get_grid_data(sheets, tab_name: str, a1_range: str) -> Dict:
    safe = tab_name.replace("'", "''")
    range_ref = f"'{safe}'!{a1_range}"
    fields = ",".join(
        [
            "sheets.properties.sheetId",
            "sheets.properties.title",
            "sheets.merges",
            "sheets.data.startRow",
            "sheets.data.startColumn",
            "sheets.data.rowMetadata.pixelSize",
            "sheets.data.columnMetadata.pixelSize",
            "sheets.data.rowData.values(formattedValue,effectiveValue,effectiveFormat)",
        ]
    )
    res = _with_retries(
        lambda: sheets.spreadsheets().get(
            spreadsheetId=DEST_SHEET_ID,
            ranges=[range_ref],
            includeGridData=True,
            fields=fields,
        ).execute(),
        "Sheets get grid"
    ) or {}
    sheets_data = res.get("sheets", [])
    if not sheets_data:
        return {}
    return sheets_data[0]


def _build_merge_map(merges: List[Dict], start_row: int, start_col: int, row_count: int, col_count: int):
    merge_map: Dict[tuple[int, int], Dict] = {}
    for m in merges or []:
        sr = m.get("startRowIndex", 0)
        er = m.get("endRowIndex", 0)
        sc = m.get("startColumnIndex", 0)
        ec = m.get("endColumnIndex", 0)
        if er <= start_row or sr >= start_row + row_count:
            continue
        if ec <= start_col or sc >= start_col + col_count:
            continue
        for r in range(sr, er):
            for c in range(sc, ec):
                merge_map[(r, c)] = {
                    "start_row": sr,
                    "end_row": er,
                    "start_col": sc,
                    "end_col": ec,
                }
    return merge_map


_font_cache: Dict[int, ImageFont.ImageFont] = {}


def _get_font(size: int) -> ImageFont.ImageFont:
    cached = _font_cache.get(size)
    if cached is not None:
        return cached
    try:
        font = ImageFont.truetype(FONT_PATH, size)
    except Exception:
        font = ImageFont.load_default()
    _font_cache[size] = font
    return font


def _font_height(font: ImageFont.ImageFont) -> int:
    try:
        bbox = font.getbbox("Ag")
        return bbox[3] - bbox[1]
    except Exception:
        return 12


def render_sheet_range_image(sheets, tab_name: str, a1_range: str) -> Image.Image:
    sheet = _get_grid_data(sheets, tab_name, a1_range)
    if not sheet:
        img = Image.new("RGB", (400, 60), "white")
        draw = ImageDraw.Draw(img)
        draw.text((10, 20), "No data", fill="#111111", font=ImageFont.load_default())
        return img

    data = sheet.get("data", [{}])[0]
    row_data = data.get("rowData", [])
    row_meta = data.get("rowMetadata", [])
    col_meta = data.get("columnMetadata", [])
    start_row = int(data.get("startRow", 0))
    start_col = int(data.get("startColumn", 0))

    row_count = len(row_data)
    col_count = max((len(r.get("values", [])) for r in row_data), default=0)

    scale = IMAGE_SCALE
    row_heights = []
    for idx in range(row_count):
        px = row_meta[idx].get("pixelSize") if idx < len(row_meta) else None
        base = int(px) if px else 21
        row_heights.append(max(1, int(base * scale)))

    col_widths = []
    for idx in range(col_count):
        px = col_meta[idx].get("pixelSize") if idx < len(col_meta) else None
        base = int(px) if px else 100
        col_widths.append(max(1, int(base * scale)))

    width = sum(col_widths) + 1
    height = sum(row_heights) + 1
    img = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(img)

    merges = _build_merge_map(sheet.get("merges", []), start_row, start_col, row_count, col_count)

    y = 0
    for r_idx in range(row_count):
        x = 0
        row_vals = row_data[r_idx].get("values", [])
        for c_idx in range(col_count):
            abs_r = start_row + r_idx
            abs_c = start_col + c_idx
            merge = merges.get((abs_r, abs_c))
            if merge and (merge["start_row"] != abs_r or merge["start_col"] != abs_c):
                x += col_widths[c_idx]
                continue

            cell_width = col_widths[c_idx]
            cell_height = row_heights[r_idx]
            if merge:
                cell_width = sum(
                    col_widths[c - start_col]
                    for c in range(merge["start_col"], merge["end_col"])
                    if start_col <= c < start_col + col_count
                )
                cell_height = sum(
                    row_heights[r - start_row]
                    for r in range(merge["start_row"], merge["end_row"])
                    if start_row <= r < start_row + row_count
                )

            cell = row_vals[c_idx] if c_idx < len(row_vals) else {}
            eff = cell.get("effectiveFormat", {})
            bg = _color_from_google(eff.get("backgroundColor"), (255, 255, 255))

            draw.rectangle([x, y, x + cell_width, y + cell_height], fill=bg)

            text = cell.get("formattedValue")
            if text is None:
                text = ""
            text = str(text)

            tf = eff.get("textFormat", {})
            fg = _color_from_google(tf.get("foregroundColor"), (17, 17, 17))
            bold = bool(tf.get("bold"))
            font_size = int(tf.get("fontSize", BASE_FONT_SIZE) * scale)
            font = _get_font(max(8, font_size))
            text_h = _font_height(font)

            align = eff.get("horizontalAlignment", "LEFT")
            pad = max(1, int(4 * scale))
            text_w = int(font.getlength(text)) if text else 0
            if align == "CENTER":
                tx = x + max(pad, (cell_width - text_w) // 2)
            elif align == "RIGHT":
                tx = x + max(pad, cell_width - text_w - pad)
            else:
                tx = x + pad
            ty = y + max(2, (cell_height - text_h) // 2)

            if text:
                draw.text((tx, ty), text, fill=fg, font=font)
                if bold:
                    draw.text((tx + max(1, int(scale)), ty), text, fill=fg, font=font)

            borders = eff.get("borders", {})
            line_w = max(1, int(scale))
            for side, x1, y1, x2, y2 in [
                ("top", x, y, x + cell_width, y),
                ("bottom", x, y + cell_height, x + cell_width, y + cell_height),
                ("left", x, y, x, y + cell_height),
                ("right", x + cell_width, y, x + cell_width, y + cell_height),
            ]:
                b = borders.get(side)
                if b:
                    color = _color_from_google(b.get("color"), (217, 217, 217))
                    draw.line([x1, y1, x2, y2], fill=color, width=line_w)

            x += col_widths[c_idx]
        y += row_heights[r_idx]

    return img


def _encode_image(img: Image.Image) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def _fit_image_bytes(img: Image.Image) -> bytes:
    scale = 1.0
    for _ in range(6):
        if scale < 1.0:
            new_size = (max(1, int(img.width * scale)), max(1, int(img.height * scale)))
            resized = img.resize(new_size, getattr(getattr(Image, "Resampling", None), "LANCZOS", 3))
        else:
            resized = img
        data = _encode_image(resized)
        if len(data) <= MAX_IMAGE_BYTES:
            return data
        scale *= 0.85
    return _encode_image(img)


def _split_image(img: Image.Image) -> List[Image.Image]:
    max_w = MAX_IMAGE_WIDTH
    max_h = MAX_IMAGE_HEIGHT
    if img.width <= max_w and img.height <= max_h:
        return [img]

    parts: List[Image.Image] = []
    y = 0
    while y < img.height:
        h = min(max_h, img.height - y)
        crop = img.crop((0, y, img.width, y + h))
        parts.append(crop)
        y += h
    return parts


def _image_to_bytes_list(img: Image.Image) -> List[bytes]:
    return [_fit_image_bytes(p) for p in _split_image(img)]


def render_sheet_range_images(sheets, tab_name: str, a1_range: str) -> List[bytes]:
    img = render_sheet_range_image(sheets, tab_name, a1_range)
    return _image_to_bytes_list(img)


def stack_images_vertically(images: List[Image.Image], padding: int = 8) -> Image.Image:
    imgs = [i for i in images if i]
    if not imgs:
        return Image.new("RGB", (400, 60), "white")

    width = max(i.width for i in imgs)
    height = sum(i.height for i in imgs) + padding * (len(imgs) - 1)
    combined = Image.new("RGB", (width, height), "white")

    y = 0
    for img in imgs:
        combined.paste(img, (0, y))
        y += img.height + padding

    return combined


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


def send_seatalk_text(text: str, at_all: bool = False) -> None:
    payload = {"tag": "text", "text": {"content": text, "format": 2}}
    if at_all:
        payload["text"]["at_all"] = True
    seatalk_post(payload)


def send_dashboard_images(sheets, sent_ts_pht: str) -> None:
    if not SEATALK_WEBHOOK_URL:
        return

    images: List[bytes] = []
    images.extend(render_sheet_range_images(sheets, "Backlogs Summary", "B2:R63"))
    images.extend(render_sheet_range_images(sheets, "[SOC5] SOCPacked_Dashboard", "A1:T29"))

    header_img = render_sheet_range_image(sheets, "[SOC5] SOCPacked_Dashboard", "A1:U9")
    for cont_range in ["A30:U48", "A50:U94", "A95:U127", "A129:U157"]:
        cont_img = render_sheet_range_image(sheets, "[SOC5] SOCPacked_Dashboard", cont_range)
        combined = stack_images_vertically([header_img, cont_img], padding=max(2, int(8 * IMAGE_SCALE)))
        images.extend(_image_to_bytes_list(combined))

    for idx, image_bytes in enumerate(images, start=1):
        try:
            send_seatalk_image(image_bytes)
            log(f"SeaTalk image sent {idx}/{len(images)}.")
        except Exception as exc:
            log(f"SeaTalk image failed {idx}/{len(images)}: {exc}")

    try:
        send_seatalk_text(
            f"Sharing OB Pending for dispatch as of {sent_ts_pht}. Thank you!",
            at_all=True,
        )
        log("SeaTalk text sent.")
    except Exception as exc:
        log(f"SeaTalk text failed: {exc}")


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
    if SKIP_SEATALK_IMAGES:
        log("Skipping SeaTalk images (SKIP_SEATALK_IMAGES enabled).")
    else:
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
                continue
            if file.get("trashed"):
                name = str(file.get("name", "")).lower()
                parents = file.get("parents", []) or []
                if name.endswith(".zip") and (not parents or DRIVE_PARENT_FOLDER_ID in parents):
                    deleted_zip = True
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

    if deleted_zip:
        print("ZIP deleted in parent folder. Clearing destination sheet.")
        clear_destination_sheet(sheets, state)
        return True

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


@app.get("/watch/status")
def watch_status():
    state = load_state()
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
    token = request.headers.get("X-Goog-Channel-Token")
    if WEBHOOK_TOKEN and token != WEBHOOK_TOKEN:
        return jsonify({"error": "invalid token"}), 401

    state = load_state()
    drive, _ = build_clients()
    try:
        res = register_changes_watch(drive, WEBHOOK_URL, WEBHOOK_TOKEN, state)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(res), 200


@app.post("/run")
def run_once():
    token = request.headers.get("X-Goog-Channel-Token")
    if WEBHOOK_TOKEN and token != WEBHOOK_TOKEN:
        return jsonify({"error": "invalid token"}), 401

    force = request.args.get("force", "").lower() in {"1", "true", "yes"}
    state = load_state()
    drive, sheets = build_clients()
    process_folder(drive, sheets, DRIVE_PARENT_FOLDER_ID, state, ignore_last_dt=force)
    return jsonify({"ok": True, "forced": force}), 200


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
