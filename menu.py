"""
Backup Module

Handles automatic backup creation before destructive operations,
backup naming conventions, and folder management after the
three-backup threshold is reached.
"""

import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager


class BackupManager:
    """
    Manages spreadsheet backups.

    Backups are stored alongside the original file for the first three,
    then consolidated into a dedicated backup folder. All backups are
    retained indefinitely.
    """

    BACKUP_PREFIX = "[Backup]"
    BACKUP_FOLDER_PREFIX = "[Backups]"
    BACKUP_THRESHOLD = 3

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        """
        Initialize the BackupManager.

        Args:
            config: ConfigManager instance.
            logger: LoggingManager instance.
        """
        self.config = config
        self.logger = logger

    def create_backup(self, source_path: Path) -> Optional[Path]:
        """
        Create a backup of the specified file.

        Follows the naming convention: [Backup] Original Name - YYYY-MM-DD
        Handles the three-backup threshold and folder consolidation.

        Args:
            source_path: Path to the file to back up.

        Returns:
            Path to the created backup, or None if backup failed.
        """
        if not source_path.exists():
            self.logger.error(f"Cannot create backup: source file not found at {source_path}")
            return None

        date_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        original_name = source_path.stem
        extension = source_path.suffix
        backup_name = f"{self.BACKUP_PREFIX} {original_name} - {date_str}{extension}"

        # Determine where to put the backup
        parent_dir = source_path.parent
        backup_folder = self._find_backup_folder(parent_dir, original_name)

        if backup_folder and backup_folder.exists():
            # Backup folder already exists; place directly in it
            backup_path = backup_folder / backup_name
        else:
            # Check if we've hit the threshold
            existing_backups = self._count_backups_alongside(parent_dir, original_name)

            if existing_backups >= self.BACKUP_THRESHOLD:
                # Create the backup folder and consolidate
                backup_folder = self._create_backup_folder(parent_dir, original_name)
                self._consolidate_backups(parent_dir, original_name, backup_folder)
                backup_path = backup_folder / backup_name
            else:
                # Store alongside the original
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
        self,
        sheets_service,
        drive_service,
        spreadsheet_id: str,
        folder_id: str,
        spreadsheet_name: str,
    ) -> Optional[str]:
        """
        Create a backup of a Google Sheets spreadsheet in the same Drive folder.

        Args:
            sheets_service: Google Sheets API service object.
            drive_service: Google Drive API service object.
            spreadsheet_id: The ID of the spreadsheet to back up.
            folder_id: The Google Drive folder ID where the backup should be placed.
            spreadsheet_name: The original spreadsheet name.

        Returns:
            The file ID of the backup, or None if backup failed.
        """
        date_str = datetime.now().strftime("%Y-%m-%d")
        backup_name = f"{self.BACKUP_PREFIX} {spreadsheet_name} - {date_str}"

        try:
            # Check for existing backup folder
            backup_folder_name = f"{self.BACKUP_FOLDER_PREFIX} {spreadsheet_name}"
            backup_folder_id = self._find_google_backup_folder(
                drive_service, folder_id, backup_folder_name
            )

            # Count existing backups
            target_folder = backup_folder_id or folder_id
            existing_count = self._count_google_backups(
                drive_service, target_folder, spreadsheet_name
            )

            # If threshold reached and no folder yet, create one
            if existing_count >= self.BACKUP_THRESHOLD and not backup_folder_id:
                backup_folder_id = self._create_google_backup_folder(
                    drive_service, folder_id, backup_folder_name
                )
                self._consolidate_google_backups(
                    drive_service, folder_id, backup_folder_id, spreadsheet_name
                )
                target_folder = backup_folder_id

            # Create the backup copy
            copy_metadata = {
                "name": backup_name,
                "parents": [target_folder],
            }
            backup = (
                drive_service.files()
                .copy(fileId=spreadsheet_id, body=copy_metadata)
                .execute()
            )

            backup_id = backup.get("id", "")
            self.logger.summary(f"Google Sheets backup created: {backup_name}")
            self.logger.technical(f"Backup file ID: {backup_id}")
            return backup_id

        except Exception as e:
            self.logger.error(f"Failed to create Google Sheets backup: {e}")
            return None

    def _find_backup_folder(self, parent_dir: Path, original_name: str) -> Optional[Path]:
        """Find an existing backup folder for the given spreadsheet."""
        folder_name = f"{self.BACKUP_FOLDER_PREFIX} {original_name}"
        folder_path = parent_dir / folder_name
        return folder_path if folder_path.exists() else None

    def _count_backups_alongside(self, parent_dir: Path, original_name: str) -> int:
        """Count backup files stored alongside the original (not in a folder)."""
        count = 0
        for path in parent_dir.iterdir():
            if path.is_file() and path.name.startswith(f"{self.BACKUP_PREFIX} {original_name}"):
                count += 1
        return count

    def _create_backup_folder(self, parent_dir: Path, original_name: str) -> Path:
        """Create a dedicated backup folder."""
        folder_name = f"{self.BACKUP_FOLDER_PREFIX} {original_name}"
        folder_path = parent_dir / folder_name
        folder_path.mkdir(parents=True, exist_ok=True)
        self.logger.summary(f"Backup folder created: {folder_name}")
        self.logger.technical(f"Backup folder path: {folder_path}")
        return folder_path

    def _consolidate_backups(
        self, parent_dir: Path, original_name: str, backup_folder: Path
    ) -> None:
        """Move all existing backups from alongside the original into the backup folder."""
        moved_count = 0
        for path in parent_dir.iterdir():
            if path.is_file() and path.name.startswith(f"{self.BACKUP_PREFIX} {original_name}"):
                dest = backup_folder / path.name
                shutil.move(str(path), str(dest))
                moved_count += 1

        if moved_count > 0:
            self.logger.summary(
                f"Consolidated {moved_count} existing backup(s) into {backup_folder.name}"
            )

    def _find_google_backup_folder(
        self, drive_service, parent_folder_id: str, folder_name: str
    ) -> Optional[str]:
        """Find a backup folder in Google Drive by name."""
        try:
            query = (
                f"name = '{folder_name}' and "
                f"'{parent_folder_id}' in parents and "
                f"mimeType = 'application/vnd.google-apps.folder' and "
                f"trashed = false"
            )
            results = (
                drive_service.files()
                .list(q=query, fields="files(id, name)")
                .execute()
            )
            files = results.get("files", [])
            return files[0]["id"] if files else None
        except Exception:
            return None

    def _count_google_backups(
        self, drive_service, folder_id: str, spreadsheet_name: str
    ) -> int:
        """Count backup files in a Google Drive folder."""
        try:
            query = (
                f"name contains '{self.BACKUP_PREFIX} {spreadsheet_name}' and "
                f"'{folder_id}' in parents and "
                f"trashed = false"
            )
            results = (
                drive_service.files()
                .list(q=query, fields="files(id)")
                .execute()
            )
            return len(results.get("files", []))
        except Exception:
            return 0

    def _create_google_backup_folder(
        self, drive_service, parent_folder_id: str, folder_name: str
    ) -> str:
        """Create a backup folder in Google Drive."""
        metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        folder = drive_service.files().create(body=metadata, fields="id").execute()
        folder_id = folder.get("id", "")
        self.logger.summary(f"Google Drive backup folder created: {folder_name}")
        return folder_id

    def _consolidate_google_backups(
        self,
        drive_service,
        source_folder_id: str,
        backup_folder_id: str,
        spreadsheet_name: str,
    ) -> None:
        """Move existing backup files in Google Drive into the backup folder."""
        try:
            query = (
                f"name contains '{self.BACKUP_PREFIX} {spreadsheet_name}' and "
                f"'{source_folder_id}' in parents and "
                f"trashed = false"
            )
            results = (
                drive_service.files()
                .list(q=query, fields="files(id, name, parents)")
                .execute()
            )

            moved_count = 0
            for file in results.get("files", []):
                drive_service.files().update(
                    fileId=file["id"],
                    addParents=backup_folder_id,
                    removeParents=source_folder_id,
                    fields="id, parents",
                ).execute()
                moved_count += 1

            if moved_count > 0:
                self.logger.summary(
                    f"Consolidated {moved_count} Google Drive backup(s) into backup folder."
                )

        except Exception as e:
            self.logger.warning(f"Failed to consolidate Google Drive backups: {e}")
