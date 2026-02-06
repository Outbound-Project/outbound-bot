# Outbound Bot

## Setup
1. Create a virtual environment:
   - `python -m venv venv`
2. Activate it:
   - Windows PowerShell: `venv\Scripts\Activate.ps1`
3. Install dependencies:
   - `pip install -r requirements.txt`

## Required Environment Variables (Backlogs Workflow)
- `BACKLOGS_DRIVE_PARENT_FOLDER_ID`: Google Drive folder ID containing source ZIP files.
- `BACKLOGS_DEST_SHEET_ID`: Google Sheets spreadsheet ID for output.

Backward compatible aliases (still accepted for backlogs only):
- `DRIVE_PARENT_FOLDER_ID`
- `DEST_SHEET_ID`

## Recommended Environment Variables
- `SERVICE_ACCOUNT_FILE`: Path to service account JSON (default: `creds/service_account.json`).
- `SERVICE_ACCOUNT_JSON`: Raw JSON credentials string (overrides file if set).
- `WEBHOOK_URL`: Public URL for Drive change notifications (required to register watch).
- `WEBHOOK_TOKEN`: Shared secret for webhook validation (required in production unless `ALLOW_INSECURE_WEBHOOK=true`).
- `APP_ENV`: `development` (default) or `production`.

## Optional Environment Variables
Backlogs workflow (prefix with `BACKLOGS_`):
- `BACKLOGS_DEST_SHEET_TAB_NAME`: Sheet tab name (default: `socpacked_generated_data`).
- `BACKLOGS_FORCE_OVERWRITE`: `true`/`false` (default: `true`).
- `BACKLOGS_STATUS_TAB`: Status sheet tab (default: `Backlogs Summary`).
- `BACKLOGS_STATUS_CELL`: Status cell (default: `F3`).
- `BACKLOGS_SEATALK_WEBHOOK_URL`: SeaTalk webhook URL (falls back to `SEATALK_WEBHOOK_URL`).
- `BACKLOGS_SKIP_SEATALK_IMAGES`: `true`/`false` (default: `false`).

Workflow2 (prefix with `WORKFLOW2_`):
- `WORKFLOW2_DRIVE_PARENT_FOLDER_ID`
- `WORKFLOW2_DEST_SHEET_ID`
- `WORKFLOW2_DEST_SHEET_TAB_NAME`
- `WORKFLOW2_FORCE_OVERWRITE`
- `WORKFLOW2_STATUS_TAB`
- `WORKFLOW2_STATUS_CELL`
- `WORKFLOW2_SEATALK_WEBHOOK_URL`
- `WORKFLOW2_SKIP_SEATALK_IMAGES`

Shared:
- `SEATALK_WEBHOOK_URL`: Default SeaTalk webhook URL.
- `SKIP_SEATALK_IMAGES`: Default `true`/`false`.
- `FONT_PATH`: Font path (default: `assets/fonts/Inter.ttf`).
- `BASE_FONT_SIZE`: Default font size (default: `14`).
- `IMAGE_SCALE`: Image scale factor (default: `3`).
- `MAX_IMAGE_WIDTH`: Max image width (default: `7000`).
- `MAX_IMAGE_HEIGHT`: Max image height (default: `9000`).
- `MAX_IMAGE_BYTES`: Max image bytes (default: `4700000`).
- `STATE_PATH`: Path to state file (default: system temp `state.json`).
- `ALLOW_INSECURE_WEBHOOK`: `true`/`false` (default: `false`).
- `UPSTASH_REDIS_REST_URL` or `KV_REST_API_URL`: If set, state is stored in Vercel KV (Upstash).
- `UPSTASH_REDIS_REST_TOKEN` or `KV_REST_API_TOKEN`: Token for Vercel KV (Upstash).
- `STATE_KEY`: Optional KV key name (default: `outbound-bot:state`).

## Run Locally
- `python main.py`

The HTTP server is exposed via the `api/main.py` entrypoint when deployed (Vercel).

## Health Check
- `GET /health`

## Workflow Routes
Backlogs (default):
- `/backlogs/run`, `/backlogs/webhook`, `/backlogs/watch`, `/backlogs/watch/renew`, `/backlogs/watch/status`

Workflow2 (optional, if configured):
- `/workflow2/run`, `/workflow2/webhook`, `/workflow2/watch`, `/workflow2/watch/renew`, `/workflow2/watch/status`

## Tests
- `pytest`

## Lint
- `ruff check .`

## Migration Notes
- The committed virtual environment has been removed. Recreate it with `python -m venv venv` and reinstall dependencies.
- Required runtime configuration now comes from environment variables (see list above).
