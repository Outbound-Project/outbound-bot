# Outbound Bot

## Setup
1. Create a virtual environment:
   - `python -m venv venv`
2. Activate it:
   - Windows PowerShell: `venv\Scripts\Activate.ps1`
3. Install dependencies:
   - `pip install -r requirements.txt`

## Required Environment Variables
- `DRIVE_PARENT_FOLDER_ID`: Google Drive folder ID containing source ZIP files.
- `DEST_SHEET_ID`: Google Sheets spreadsheet ID for output.

## Recommended Environment Variables
- `SERVICE_ACCOUNT_FILE`: Path to service account JSON (default: `creds/service_account.json`).
- `SERVICE_ACCOUNT_JSON`: Raw JSON credentials string (overrides file if set).
- `WEBHOOK_URL`: Public URL for Drive change notifications (required to register watch).
- `WEBHOOK_TOKEN`: Shared secret for webhook validation (required in production unless `ALLOW_INSECURE_WEBHOOK=true`).
- `APP_ENV`: `development` (default) or `production`.

## Optional Environment Variables
- `DEST_SHEET_TAB_NAME`: Sheet tab name (default: `socpacked_generated_data`).
- `FORCE_OVERWRITE`: `true`/`false` (default: `true`).
- `BACKLOGS_STATUS_TAB`: Status sheet tab (default: `Backlogs Summary`).
- `BACKLOGS_STATUS_CELL`: Status cell (default: `F3`).
- `SEATALK_WEBHOOK_URL`: SeaTalk webhook URL.
- `SKIP_SEATALK_IMAGES`: `true`/`false` (default: `false`).
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

## Tests
- `pytest`

## Lint
- `ruff check .`

## Migration Notes
- The committed virtual environment has been removed. Recreate it with `python -m venv venv` and reinstall dependencies.
- Required runtime configuration now comes from environment variables (see list above).
