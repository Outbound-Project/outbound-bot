from __future__ import annotations

from dataclasses import dataclass
import json
import os


@dataclass(frozen=True)
class WorkflowConfig:
    name: str
    drive_parent_folder_id: str
    dest_sheet_id: str
    dest_sheet_tab_name: str
    webhook_url: str
    webhook_token: str
    backlogs_status_tab: str
    backlogs_status_cell: str
    seatalk_webhook_url: str
    skip_seatalk_images: bool
    font_path: str
    base_font_size: int
    image_scale: float
    max_image_width: int
    max_image_height: int
    max_image_bytes: int
    state_path: str
    state_key: str
    force_overwrite: bool


@dataclass(frozen=True)
class AppConfig:
    webhook_url: str
    webhook_token: str
    allow_insecure_webhook: bool
    app_env: str
    service_account_file: str
    service_account_json: str
    workflows: dict[str, WorkflowConfig]


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return int(value)


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return float(value)


def _default_state_path() -> str:
    tmp_dir = os.environ.get("TMPDIR") or os.environ.get("TEMP") or "/tmp"
    return os.path.join(tmp_dir, "state.json")


def _build_workflow_config(prefix: str, name: str, base_webhook_url: str) -> WorkflowConfig:
    drive_parent_folder_id = os.environ.get(f"{prefix}DRIVE_PARENT_FOLDER_ID", "").strip()
    dest_sheet_id = os.environ.get(f"{prefix}DEST_SHEET_ID", "").strip()
    dest_sheet_tab_name = os.environ.get(f"{prefix}DEST_SHEET_TAB_NAME", "").strip()
    if prefix == "BACKLOGS_":
        if not drive_parent_folder_id:
            drive_parent_folder_id = os.environ.get("DRIVE_PARENT_FOLDER_ID", "").strip()
        if not dest_sheet_id:
            dest_sheet_id = os.environ.get("DEST_SHEET_ID", "").strip()
        if not dest_sheet_tab_name:
            dest_sheet_tab_name = os.environ.get("DEST_SHEET_TAB_NAME", "socpacked_generated_data").strip()
    if not dest_sheet_tab_name:
        dest_sheet_tab_name = "socpacked_generated_data"
    backlogs_status_tab = os.environ.get(f"{prefix}STATUS_TAB", "").strip()
    backlogs_status_cell = os.environ.get(f"{prefix}STATUS_CELL", "").strip()
    seatalk_webhook_url = os.environ.get(f"{prefix}SEATALK_WEBHOOK_URL", "").strip()
    if not seatalk_webhook_url:
        seatalk_webhook_url = os.environ.get("SEATALK_WEBHOOK_URL", "").strip()
    state_key = os.environ.get(f"{prefix}STATE_KEY", "").strip() or f"outbound-bot:{name}:state"
    if prefix == "BACKLOGS_":
        if not backlogs_status_tab:
            backlogs_status_tab = os.environ.get("BACKLOGS_STATUS_TAB", "Backlogs Summary").strip()
        if not backlogs_status_cell:
            backlogs_status_cell = os.environ.get("BACKLOGS_STATUS_CELL", "F3").strip()
    if not backlogs_status_tab:
        backlogs_status_tab = "Backlogs Summary"
    if not backlogs_status_cell:
        backlogs_status_cell = "F3"

    return WorkflowConfig(
        name=name,
        drive_parent_folder_id=drive_parent_folder_id,
        dest_sheet_id=dest_sheet_id,
        dest_sheet_tab_name=dest_sheet_tab_name,
        webhook_url=base_webhook_url,
        webhook_token=os.environ.get("WEBHOOK_TOKEN", "").strip(),
        backlogs_status_tab=backlogs_status_tab,
        backlogs_status_cell=backlogs_status_cell,
        seatalk_webhook_url=seatalk_webhook_url,
        skip_seatalk_images=_env_bool(f"{prefix}SKIP_SEATALK_IMAGES", _env_bool("SKIP_SEATALK_IMAGES", False)),
        font_path=os.environ.get("FONT_PATH", "assets/fonts/Inter.ttf").strip(),
        base_font_size=_env_int("BASE_FONT_SIZE", 14),
        image_scale=_env_float("IMAGE_SCALE", 3.0),
        max_image_width=_env_int("MAX_IMAGE_WIDTH", 7000),
        max_image_height=_env_int("MAX_IMAGE_HEIGHT", 9000),
        max_image_bytes=_env_int("MAX_IMAGE_BYTES", 4700000),
        state_path=os.environ.get("STATE_PATH", "").strip() or _default_state_path(),
        state_key=state_key,
        force_overwrite=_env_bool(f"{prefix}FORCE_OVERWRITE", _env_bool("FORCE_OVERWRITE", True)),
    )


def get_config() -> AppConfig:
    webhook_url = os.environ.get("WEBHOOK_URL", "").strip()
    workflows = {
        "backlogs": _build_workflow_config("BACKLOGS_", "backlogs", webhook_url),
        "workflow2": _build_workflow_config("WORKFLOW2_", "workflow2", webhook_url),
    }
    return AppConfig(
        webhook_url=webhook_url,
        webhook_token=os.environ.get("WEBHOOK_TOKEN", "").strip(),
        allow_insecure_webhook=_env_bool("ALLOW_INSECURE_WEBHOOK", False),
        app_env=os.environ.get("APP_ENV", "development").strip().lower(),
        service_account_file=os.environ.get("SERVICE_ACCOUNT_FILE", "creds/service_account.json").strip(),
        service_account_json=os.environ.get("SERVICE_ACCOUNT_JSON", "").strip(),
        workflows=workflows,
    )


def validate_config(config: AppConfig) -> None:
    backlogs = config.workflows.get("backlogs")
    missing = []
    if backlogs and not backlogs.drive_parent_folder_id:
        missing.append("BACKLOGS_DRIVE_PARENT_FOLDER_ID")
    if backlogs and not backlogs.dest_sheet_id:
        missing.append("BACKLOGS_DEST_SHEET_ID")
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    workflow2 = config.workflows.get("workflow2")
    if workflow2:
        has_any = bool(workflow2.drive_parent_folder_id or workflow2.dest_sheet_id)
        if has_any and not (workflow2.drive_parent_folder_id and workflow2.dest_sheet_id):
            raise ValueError(
                "WORKFLOW2_DRIVE_PARENT_FOLDER_ID and WORKFLOW2_DEST_SHEET_ID must both be set"
            )

    if config.app_env in {"production", "prod"}:
        if not config.webhook_token and not config.allow_insecure_webhook:
            raise ValueError(
                "WEBHOOK_TOKEN is required in production unless ALLOW_INSECURE_WEBHOOK=true"
            )


def build_credentials(config: AppConfig):
    from google.oauth2 import service_account

    if config.service_account_json:
        info = json.loads(config.service_account_json)
        return service_account.Credentials.from_service_account_info(
            info, scopes=SCOPES
        )
    return service_account.Credentials.from_service_account_file(
        config.service_account_file, scopes=SCOPES
    )


SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]
