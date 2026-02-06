from __future__ import annotations

from typing import Dict, List

from googleapiclient.discovery import build

from .config import AppConfig, build_credentials

_drive_client = None


def get_drive_client(config: AppConfig):
    global _drive_client
    if _drive_client is None:
        creds = build_credentials(config)
        _drive_client = build("drive", "v3", credentials=creds)
    return _drive_client


def list_zip_files(drive, folder_id: str) -> List[Dict]:
    q = f"'{folder_id}' in parents and trashed=false and name contains '.zip'"
    res = drive.files().list(
        q=q,
        fields="files(id,name,modifiedTime)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    return res.get("files", [])


def download_zip(drive, file_id: str) -> bytes:
    from googleapiclient.http import MediaIoBaseDownload
    import io

    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()


def get_start_page_token(drive) -> str:
    res = drive.changes().getStartPageToken(supportsAllDrives=True).execute()
    return res["startPageToken"]

