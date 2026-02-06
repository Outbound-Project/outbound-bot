# Codex Prompt — Apply All Fixes to Make Repo Import/Clone Much Faster + More Production-Ready

You are Codex. Work in this repository and implement the changes below. Prioritize actions that make **import/clone/build faster** and reduce repo size. Make changes as small and safe as possible, but complete.

## Goals (in order)
1) Make `git clone` / import **much faster** by removing committed virtualenv and other generated artifacts.
2) Make app startup reliable (fix syntax/config issues) and reduce cold-start time via lazy init.
3) Improve maintainability: modularize, enforce env config, safer webhook, better error handling.
4) Add minimal CI/test and docs so the project stays solid.

## Constraints
- Do not change product behavior unless needed for security/reliability.
- Keep public API endpoints stable (paths, payloads) unless clearly broken.
- Provide a short migration note in `README.md` for required env vars and deployment.
- Ensure the project runs locally after changes.

---

## Step 1 — Remove `venv/` and any large generated directories from Git (critical for faster import)
1. Detect if `venv/` (or `.venv/`) exists in the repository and is tracked by git.
2. Remove it from git tracking and filesystem (do not delete user local env; just ensure not committed):
   - Add to `.gitignore`: `venv/`, `.venv/`, `__pycache__/`, `*.pyc`, `.env`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `dist/`, `build/`, `*.egg-info/`, `.DS_Store`.
3. If there are other bulky tracked directories (e.g., `node_modules/`, `site-packages/`, `.cache/`), remove them and add to `.gitignore`.
4. Replace with dependency manifest:
   - If missing, create `requirements.txt` based on imports (and/or `pip freeze` equivalent if a lock exists).
   - If the project uses modern tooling, optionally create `pyproject.toml` + `requirements.lock` (only if simple).
5. Commit-ready change: repository should no longer include virtualenv or installed packages.

Deliverables:
- Updated `.gitignore`
- Removal of tracked virtualenv folders
- `requirements.txt` (or `pyproject.toml` + lock)

---

## Step 2 — Fix startup-breaking issues and validate configuration early
1. Fix any syntax errors in the main entrypoint (example: extra `)` in env parsing).
2. Create a centralized config module (e.g., `app/config.py`) that:
   - Loads required env vars (`DEST_SHEET_ID`, `DRIVE_PARENT_FOLDER_ID`, webhook config, etc.)
   - Provides defaults where safe
   - Validates and fails fast with clear errors if required vars are missing
3. Remove hard-coded environment-specific IDs from source. Replace with env vars.
4. Add a `validate_config()` call at app startup that prints a clear message and stops if invalid.

Deliverables:
- No syntax errors
- No hard-coded Sheet/Drive IDs in code
- Central config + validation

---

## Step 3 — Reduce cold start time and speed up runtime operations
1. Implement lazy initialization for Google API clients:
   - Do NOT create Drive/Sheets clients at import time.
   - Create them on first use (singleton per process).
2. Add caching for metadata calls that are repeated frequently (e.g., sheet metadata, headers).
3. Use batching for Sheets writes:
   - Replace row-by-row updates with `batchUpdate` / range writes where applicable.
4. Ensure operations are idempotent where possible (important for webhook retries):
   - Introduce a dedupe mechanism keyed on event id / file id + timestamp (store in memory cache or small state store).

Deliverables:
- Google clients created lazily
- Reduced API calls via cache
- Batch writes for Sheets where possible

---

## Step 4 — Webhook security and robustness
1. Enforce webhook authentication:
   - If webhook token/signature is configured, validate always.
   - Prefer: require token to be set in production; if missing, refuse to start unless `ALLOW_INSECURE_WEBHOOK=true`.
2. Improve webhook error handling:
   - Do not re-raise after logging.
   - Return controlled JSON errors with appropriate HTTP codes.
3. Add a `/health` endpoint returning 200.

Deliverables:
- Stronger webhook auth behavior
- Controlled error responses
- `/health` route

---

## Step 5 — Modularize the code (incremental refactor, no big bang)
Refactor monolithic main file into modules without changing behavior:
Suggested structure:
- `app/main.py` (web server entry)
- `app/config.py`
- `app/auth.py`
- `app/drive_service.py`
- `app/sheets_service.py`
- `app/webhook.py`
- `app/utils.py`

Move code in small safe steps:
- Start by extracting config and auth
- Then services
- Then webhook handler

Deliverables:
- New `app/` package
- Imports updated
- App runs the same

---

## Step 6 — Add fast developer setup + CI checks
1. Update `README.md`:
   - Setup steps (`python -m venv`, `pip install -r requirements.txt`)
   - Required env vars with examples
   - How to run locally
2. Add a minimal test suite:
   - At least: config parsing test, CSV parsing test (if exists), webhook auth test.
3. Add basic lint/format:
   - Use `ruff` (preferred) or `flake8`
   - Use `black` (optional)
4. Add GitHub Actions workflow:
   - Install deps
   - Run tests
   - Run lint

Deliverables:
- README updated
- `tests/` created with minimal tests
- `.github/workflows/ci.yml`

---

## Step 7 — Final verification checklist
1. `git status` clean, no `venv/` tracked.
2. Repo size reduced significantly (no dependency folders checked in).
3. App starts locally with env vars set.
4. `/health` returns 200.
5. Webhook rejects requests without auth when required.
6. Tests pass in CI.

---

## Output format
At the end:
1) Provide a concise summary of changes.
2) List any new env vars and defaults.
3) Provide commands to run locally and run tests.
4) Provide any migration notes (e.g., how to recreate venv).

Proceed to implement all changes now.
