"""
Backup Module

Handles automatic backup creation before destructive operations,
backup naming conventions, and folder management after the
three-backup threshold is reached.
"""

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager


class BackupManager:
    """
    Manages spreadsheet backups. Stores first three alongside the original,
    then consolidates into a dedicated [Backups] folder.
    """

    BACKUP_PREFIX = "[Backup]"
    BACKUP_FOLDER_PREFIX = "[Backups]"
    BACKUP_THRESHOLD = 3

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        self.config = config
        self.logger = logger

    def create_backup(self, source_path: Path) -> Optional[Path]:
        """
        Create a timestamped backup of the specified file.

        Naming: [Backup] Original Name - YYYY-MM-DD_HHMMSS.ext

        Placement strategy:
        1. If a [Backups] folder already exists for this file, place directly in it.
        2. If not, count existing backups alongside the original:
           - Under threshold (3): place alongside the original
           - At/over threshold: create [Backups] folder, move existing backups
             into it (consolidation), then place the new backup there too.
        """
        if not source_path.exists():
            self.logger.error(f"Cannot create backup: source not found at {source_path}")
            return None

        date_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        original_name = source_path.stem
        extension = source_path.suffix
        backup_name = f"{self.BACKUP_PREFIX} {original_name} - {date_str}{extension}"

        parent_dir = source_path.parent
        backup_folder = self._find_backup_folder(parent_dir, original_name)

        if backup_folder and backup_folder.exists():
            # Strategy 1: Folder exists, place directly in it
            backup_path = backup_folder / backup_name
        else:
            existing_count = self._count_backups_alongside(parent_dir, original_name)
            if existing_count >= self.BACKUP_THRESHOLD:
                # Strategy 2b: Too many alongside — consolidate into a folder
                backup_folder = self._create_backup_folder(parent_dir, original_name)
                self._consolidate_backups(parent_dir, original_name, backup_folder)
                backup_path = backup_folder / backup_name
            else:
                # Strategy 2a: Still under threshold — place alongside
                backup_path = parent_dir / backup_name

        try:
            shutil.copy2(str(source_path), str(backup_path))
            self.logger.summary(f"Backup created: {backup_path.name}")
            self.logger.technical(f"Backup saved to: {backup_path}")
            return backup_path
        except (IOError, OSError) as e:
            self.logger.error(f"Failed to create backup: {e}")
            return None

    def create_google_sheets_backup(
        self, drive_service: Any, spreadsheet_id: str,
        folder_id: str, spreadsheet_name: str,
    ) -> Optional[str]:
        """Create a backup copy of a Google Sheets spreadsheet."""
        date_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        backup_name = f"{self.BACKUP_PREFIX} {spreadsheet_name} - {date_str}"

        try:
            backup_folder_name = f"{self.BACKUP_FOLDER_PREFIX} {spreadsheet_name}"
            backup_folder_id = self._find_google_backup_folder(
                drive_service, folder_id, backup_folder_name
            )

            target_folder = backup_folder_id or folder_id
            existing_count = self._count_google_backups(
                drive_service, target_folder, spreadsheet_name
            )

            if existing_count >= self.BACKUP_THRESHOLD and not backup_folder_id:
                backup_folder_id = self._create_google_backup_folder(
                    drive_service, folder_id, backup_folder_name
                )
                self._consolidate_google_backups(
                    drive_service, folder_id, backup_folder_id, spreadsheet_name
                )
                target_folder = backup_folder_id

            copy_metadata = {"name": backup_name, "parents": [target_folder]}
            backup = drive_service.files().copy(
                fileId=spreadsheet_id, body=copy_metadata
            ).execute()

            backup_id = backup.get("id", "")
            self.logger.summary(f"Google Sheets backup created: {backup_name}")
            return backup_id

        except Exception as e:
            self.logger.error(f"Failed to create Google Sheets backup: {e}")
            return None

    def _find_backup_folder(self, parent_dir: Path, original_name: str) -> Optional[Path]:
        folder = parent_dir / f"{self.BACKUP_FOLDER_PREFIX} {original_name}"
        return folder if folder.exists() else None

    def _count_backups_alongside(self, parent_dir: Path, original_name: str) -> int:
        return sum(
            1 for p in parent_dir.iterdir()
            if p.is_file() and p.name.startswith(f"{self.BACKUP_PREFIX} {original_name}")
        )

    def _create_backup_folder(self, parent_dir: Path, original_name: str) -> Path:
        folder = parent_dir / f"{self.BACKUP_FOLDER_PREFIX} {original_name}"
        folder.mkdir(parents=True, exist_ok=True)
        self.logger.summary(f"Backup folder created: {folder.name}")
        return folder

    def _consolidate_backups(
        self, parent_dir: Path, original_name: str, backup_folder: Path
    ) -> None:
        moved = 0
        for path in parent_dir.iterdir():
            if path.is_file() and path.name.startswith(f"{self.BACKUP_PREFIX} {original_name}"):
                shutil.move(str(path), str(backup_folder / path.name))
                moved += 1
        if moved:
            self.logger.summary(f"Consolidated {moved} backup(s) into {backup_folder.name}")

    def _find_google_backup_folder(self, drive_service, parent_id: str, name: str) -> Optional[str]:
        try:
            query = (
                f"name = '{name}' and '{parent_id}' in parents and "
                f"mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            )
            results = drive_service.files().list(q=query, fields="files(id)").execute()
            files = results.get("files", [])
            return files[0]["id"] if files else None
        except Exception:
            return None

    def _count_google_backups(self, drive_service, folder_id: str, name: str) -> int:
        try:
            query = (
                f"name contains '{self.BACKUP_PREFIX} {name}' and "
                f"'{folder_id}' in parents and trashed = false"
            )
            results = drive_service.files().list(q=query, fields="files(id)").execute()
            return len(results.get("files", []))
        except Exception:
            return 0

    def _create_google_backup_folder(self, drive_service, parent_id: str, name: str) -> str:
        metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = drive_service.files().create(body=metadata, fields="id").execute()
        self.logger.summary(f"Google Drive backup folder created: {name}")
        return folder.get("id", "")

    def _consolidate_google_backups(
        self, drive_service, source_id: str, backup_id: str, name: str
    ) -> None:
        try:
            query = (
                f"name contains '{self.BACKUP_PREFIX} {name}' and "
                f"'{source_id}' in parents and trashed = false"
            )
            results = drive_service.files().list(
                q=query, fields="files(id, name, parents)"
            ).execute()

            for file in results.get("files", []):
                drive_service.files().update(
                    fileId=file["id"],
                    addParents=backup_id,
                    removeParents=source_id,
                    fields="id, parents",
                ).execute()
        except Exception as e:
            self.logger.warning(f"Failed to consolidate Google Drive backups: {e}")
