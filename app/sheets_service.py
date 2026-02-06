from __future__ import annotations

from base64 import b64encode
from datetime import datetime, timezone, timedelta
import io
import json
import time
import zipfile
from typing import Dict, List, Tuple
from urllib import request as urlrequest

import pandas as pd
from PIL import Image, ImageDraw, ImageFont
from googleapiclient.discovery import build

from .config import AppConfig, WorkflowConfig, build_credentials
from .utils import format_status_timestamp, log, with_retries


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

FILTERS = {
    "Receiver type": "Station",
    "Current Station": "SOC 5",
}

_sheets_client = None


def get_sheets_client(config: AppConfig):
    global _sheets_client
    if _sheets_client is None:
        creds = build_credentials(config)
        _sheets_client = build("sheets", "v4", credentials=creds)
    return _sheets_client


def process_zip(zip_bytes: bytes) -> List[List[str]]:
    rows: List[List[str]] = []
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


def overwrite_sheet(sheets, workflow: WorkflowConfig, values: List[List[str]]) -> None:
    safe = workflow.dest_sheet_tab_name.replace("'", "''")

    with_retries(
        lambda: sheets.spreadsheets().values().clear(
            spreadsheetId=workflow.dest_sheet_id,
            range=f"'{safe}'!A:J",
            body={},
        ).execute(),
        "Sheets clear",
    )

    with_retries(
        lambda: sheets.spreadsheets().values().update(
            spreadsheetId=workflow.dest_sheet_id,
            range=f"'{safe}'!A1:J",
            valueInputOption="RAW",
            body={"values": values},
        ).execute(),
        "Sheets update",
    )


def clear_destination_sheet(sheets, workflow: WorkflowConfig, state: Dict | None = None) -> None:
    safe = workflow.dest_sheet_tab_name.replace("'", "''")
    with_retries(
        lambda: sheets.spreadsheets().values().clear(
            spreadsheetId=workflow.dest_sheet_id,
            range=f"'{safe}'!A:J",
            body={},
        ).execute(),
        "Sheets clear",
    )
    if state is not None:
        state["processed_zip_ids"] = []
        state["last_processed_zip_time"] = None
        state["last_import_row_count"] = 0
        state["last_run"] = datetime.now(timezone.utc).isoformat()


def update_backlogs_status(sheets, workflow: WorkflowConfig, value: str) -> None:
    safe = workflow.backlogs_status_tab.replace("'", "''")
    with_retries(
        lambda: sheets.spreadsheets().values().update(
            spreadsheetId=workflow.dest_sheet_id,
            range=f"'{safe}'!{workflow.backlogs_status_cell}",
            valueInputOption="RAW",
            body={"values": [[value]]},
        ).execute(),
        "Sheets update status",
    )


def append_rows_to_sheet(sheets, workflow: WorkflowConfig, new_rows: List[List[str]]) -> None:
    header = COLUMNS
    all_rows = [header] + new_rows
    overwrite_sheet(sheets, workflow, all_rows)


_font_cache: Dict[int, ImageFont.FreeTypeFont | ImageFont.ImageFont] = {}


_grid_cache: Dict[Tuple[int, str, str], Tuple[float, Dict]] = {}


def _cache_get(key: Tuple[int, str, str], ttl_seconds: int) -> Dict | None:
    cached = _grid_cache.get(key)
    if not cached:
        return None
    ts, data = cached
    if (time.time() - ts) > ttl_seconds:
        _grid_cache.pop(key, None)
        return None
    return data


def _cache_set(key: Tuple[int, str, str], data: Dict) -> None:
    _grid_cache[key] = (time.time(), data)


def _color_from_google(color: Dict, default: tuple[int, int, int]) -> tuple[int, int, int]:
    if not isinstance(color, dict):
        return default
    r = int(round(255 * float(color.get("red", 0.0))))
    g = int(round(255 * float(color.get("green", 0.0))))
    b = int(round(255 * float(color.get("blue", 0.0))))
    return (r, g, b)


def _get_grid_data(sheets, workflow: WorkflowConfig, tab_name: str, a1_range: str, ttl_seconds: int = 60) -> Dict:
    safe = tab_name.replace("'", "''")
    range_ref = f"'{safe}'!{a1_range}"
    cache_key = (id(sheets), range_ref, workflow.dest_sheet_id)
    cached = _cache_get(cache_key, ttl_seconds)
    if cached is not None:
        return cached

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
    res = with_retries(
        lambda: sheets.spreadsheets().get(
            spreadsheetId=workflow.dest_sheet_id,
            ranges=[range_ref],
            includeGridData=True,
            fields=fields,
        ).execute(),
        "Sheets get grid",
    ) or {}
    sheets_data = res.get("sheets", [])
    if not sheets_data:
        return {}
    data = sheets_data[0]
    _cache_set(cache_key, data)
    return data


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


def _get_font(workflow: WorkflowConfig, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    cached = _font_cache.get(size)
    if cached is not None:
        return cached
    try:
        font = ImageFont.truetype(workflow.font_path, size)
    except Exception:
        font = ImageFont.load_default()
    _font_cache[size] = font
    return font


def _font_height(font: ImageFont.FreeTypeFont | ImageFont.ImageFont) -> int:
    try:
        bbox = font.getbbox("Ag")
        return int(bbox[3] - bbox[1])
    except Exception:
        return 12


def render_sheet_range_image(sheets, workflow: WorkflowConfig, tab_name: str, a1_range: str) -> Image.Image:
    sheet = _get_grid_data(sheets, workflow, tab_name, a1_range)
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

    scale = workflow.image_scale
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
            font_size = int(tf.get("fontSize", workflow.base_font_size) * scale)
            font = _get_font(workflow, max(8, font_size))
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


def _fit_image_bytes(workflow: WorkflowConfig, img: Image.Image) -> bytes:
    scale = 1.0
    for _ in range(6):
        if scale < 1.0:
            new_size = (max(1, int(img.width * scale)), max(1, int(img.height * scale)))
            resized = img.resize(new_size, getattr(getattr(Image, "Resampling", None), "LANCZOS", 3))
        else:
            resized = img
        data = _encode_image(resized)
        if len(data) <= workflow.max_image_bytes:
            return data
        scale *= 0.85
    return _encode_image(img)


def _split_image(workflow: WorkflowConfig, img: Image.Image) -> List[Image.Image]:
    max_w = workflow.max_image_width
    max_h = workflow.max_image_height
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


def _image_to_bytes_list(workflow: WorkflowConfig, img: Image.Image) -> List[bytes]:
    return [_fit_image_bytes(workflow, p) for p in _split_image(workflow, img)]


def render_sheet_range_images(sheets, workflow: WorkflowConfig, tab_name: str, a1_range: str) -> List[bytes]:
    img = render_sheet_range_image(sheets, workflow, tab_name, a1_range)
    return _image_to_bytes_list(workflow, img)


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


def seatalk_post(workflow: WorkflowConfig, payload: Dict) -> None:
    if not workflow.seatalk_webhook_url:
        return
    data = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        workflow.seatalk_webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlrequest.urlopen(req, timeout=30) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"SeaTalk webhook failed: {resp.status}")


def send_seatalk_image(workflow: WorkflowConfig, image_bytes: bytes) -> None:
    encoded = b64encode(image_bytes).decode("ascii")
    seatalk_post(workflow, {"tag": "image", "image_base64": {"content": encoded}})


def send_seatalk_text(workflow: WorkflowConfig, text: str, at_all: bool = False) -> None:
    payload = {"tag": "text", "text": {"content": text, "format": 2}}
    if at_all:
        payload["text"]["at_all"] = True
    seatalk_post(workflow, payload)


def send_dashboard_images(sheets, workflow: WorkflowConfig, sent_ts_pht: str) -> None:
    if not workflow.seatalk_webhook_url:
        return

    images: List[bytes] = []
    images.extend(render_sheet_range_images(sheets, workflow, "Backlogs Summary", "B2:R63"))
    images.extend(render_sheet_range_images(sheets, workflow, "[SOC5] SOCPacked_Dashboard", "A1:T29"))

    header_img = render_sheet_range_image(sheets, workflow, "[SOC5] SOCPacked_Dashboard", "A1:U9")
    for cont_range in ["A30:U48", "A50:U94", "A95:U127", "A129:U157"]:
        cont_img = render_sheet_range_image(sheets, workflow, "[SOC5] SOCPacked_Dashboard", cont_range)
        combined = stack_images_vertically([header_img, cont_img], padding=max(2, int(8 * workflow.image_scale)))
        images.extend(_image_to_bytes_list(workflow, combined))

    for idx, image_bytes in enumerate(images, start=1):
        try:
            send_seatalk_image(workflow, image_bytes)
            log(f"SeaTalk image sent {idx}/{len(images)}.")
        except Exception as exc:
            log(f"SeaTalk image failed {idx}/{len(images)}: {exc}")

    try:
        send_seatalk_text(
            workflow,
            f"Sharing OB Pending for dispatch as of {sent_ts_pht}. Thank you!",
            at_all=True,
        )
        log("SeaTalk text sent.")
    except Exception as exc:
        log(f"SeaTalk text failed: {exc}")


def format_status_now() -> str:
    pht = timezone(timedelta(hours=8))
    return format_status_timestamp(datetime.now(pht))
