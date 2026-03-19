"""
Google Sheets Module

Handles all Google Sheets and Google Drive API operations, including:
- Service account and OAuth authentication
- Spreadsheet creation, reading, writing
- Sharing permissions management
- Folder management
- Batch operations
- Active editor detection
"""

import time
from pathlib import Path
from typing import Any, Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.validation import SpreadsheetValidator, SYNC_COLUMNS


SYNC_HEADER_COLOR = {"red": 0.722, "green": 0.8, "blue": 0.894}


class GoogleSheetsError(Exception):
    """Raised when a Google Sheets/Drive operation fails."""
    pass


class GoogleSheetsManager:
    """
    Manages all Google Sheets and Google Drive operations.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        self.config = config
        self.logger = logger
        self.validator = SpreadsheetValidator(config, logger)
        self.throttle_sheets = config.get("throttling", "google_sheets", default=1.0)
        self.throttle_drive = config.get("throttling", "google_drive", default=0.5)
        self.batch_mode = config.get("throttling", "batch_mode", default=True)

        self._sheets_service = None
        self._drive_service = None
        self._spreadsheet_id = None

    def authenticate(self) -> bool:
        """
        Authenticate with Google APIs using configured credentials.

        Supports both service account and OAuth authentication.

        Returns:
            True if authentication was successful.
        """
        try:
            auth_method = self.config.get_credential("google", "auth_method", default="service_account")

            if auth_method == "service_account":
                return self._auth_service_account()
            elif auth_method == "oauth":
                return self._auth_oauth()
            else:
                raise GoogleSheetsError(f"Unknown auth method: {auth_method}")

        except ImportError:
            raise GoogleSheetsError(
                "Google API libraries not installed. "
                "Run: pip install archivesspace-accession-sync[google]"
            )

    def _auth_service_account(self) -> bool:
        """Authenticate using a service account key file."""
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        key_path = self.config.get_credential("google", "service_account_key_path", default="")
        if not key_path or not Path(key_path).exists():
            raise GoogleSheetsError(f"Service account key file not found: {key_path}")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=scopes
        )

        self._sheets_service = build("sheets", "v4", credentials=credentials)
        self._drive_service = build("drive", "v3", credentials=credentials)

        self.logger.summary("Authenticated with Google APIs (service account).")
        return True

    def _auth_oauth(self) -> bool:
        """Authenticate using OAuth credentials with token refresh."""
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
        import json

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        token_path = self.config.get_credential("google", "oauth_token_path", default="")
        creds = None

        # Load existing token
        if token_path and Path(token_path).exists():
            creds = Credentials.from_authorized_user_file(token_path, scopes)

        # Refresh or obtain new credentials
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception:
                    creds = None

            if not creds:
                client_id = self.config.get_credential("google", "oauth_client_id", default="")
                client_secret = self.config.get_credential("google", "oauth_client_secret", default="")

                if not client_id or not client_secret:
                    raise GoogleSheetsError("OAuth client ID and secret not configured.")

                client_config = {
                    "installed": {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                    }
                }

                flow = InstalledAppFlow.from_client_config(client_config, scopes)
                creds = flow.run_local_server(port=0)

            # Save token
            if token_path:
                Path(token_path).parent.mkdir(parents=True, exist_ok=True)
                with open(token_path, "w") as f:
                    f.write(creds.to_json())

        self._sheets_service = build("sheets", "v4", credentials=creds)
        self._drive_service = build("drive", "v3", credentials=creds)

        self.logger.summary("Authenticated with Google APIs (OAuth).")
        return True

    def spreadsheet_exists(self) -> bool:
        """Check whether the configured spreadsheet URL points to an existing sheet."""
        url = self.config.get("google_sheets", "spreadsheet_url", default="")
        if not url:
            return False

        self._spreadsheet_id = self._extract_spreadsheet_id(url)
        if not self._spreadsheet_id:
            return False

        try:
            time.sleep(self.throttle_sheets)
            self._sheets_service.spreadsheets().get(
                spreadsheetId=self._spreadsheet_id
            ).execute()
            return True
        except Exception:
            return False

    def create_spreadsheet(self, headers: list[str]) -> str:
        """
        Create a new Google Sheet with the specified headers.

        Args:
            headers: List of column header strings.

        Returns:
            The URL of the created spreadsheet.
        """
        spreadsheet_name = self.config.get_spreadsheet_name()

        # Create the spreadsheet
        body = {
            "properties": {"title": spreadsheet_name},
            "sheets": [
                {"properties": {"title": "Accession Data and Scores"}},
            ],
        }

        time.sleep(self.throttle_sheets)
        result = self._sheets_service.spreadsheets().create(body=body).execute()
        self._spreadsheet_id = result["spreadsheetId"]
        url = f"https://docs.google.com/spreadsheets/d/{self._spreadsheet_id}"

        # Write headers
        time.sleep(self.throttle_sheets)
        self._sheets_service.spreadsheets().values().update(
            spreadsheetId=self._spreadsheet_id,
            range="'Accession Data and Scores'!A1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()

        # Format headers (bold, centered)
        self._format_headers(headers)

        # Move to configured folder
        folder_id = self.config.get("google_sheets", "folder_id", default="")
        if folder_id:
            self._move_to_folder(self._spreadsheet_id, folder_id)

        # Apply sharing permissions
        self._apply_sharing_permissions()

        # Create hidden vocabulary sheets
        self._create_vocabulary_sheets()

        # Update config with the new URL
        self.config.set("google_sheets", "spreadsheet_url", value=url)
        self.config.save_config()

        self.logger.summary(f"Created new Google Sheet: {spreadsheet_name}")
        self.logger.technical(f"Spreadsheet URL: {url}")

        return url

    def read_data(self) -> tuple[list[str], list[dict]]:
        """
        Read all data from the spreadsheet.

        Returns:
            Tuple of (headers, rows) where rows is a list of dicts.
        """
        time.sleep(self.throttle_sheets)
        result = self._sheets_service.spreadsheets().values().get(
            spreadsheetId=self._spreadsheet_id,
            range="'Accession Data and Scores'",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()

        values = result.get("values", [])
        if not values:
            return [], []

        headers = values[0]
        rows = []
        for row_values in values[1:]:
            row_data = {}
            has_data = False
            for idx, header in enumerate(headers):
                val = row_values[idx] if idx < len(row_values) else None
                if val is not None and val != "":
                    has_data = True
                row_data[header] = val
            row_data["accession_id"] = row_data.get("Accession ID")
            if has_data:
                rows.append(row_data)

        return headers, rows

    def write_rows(self, headers: list[str], rows: list[dict], start_row: int = 2) -> None:
        """
        Write multiple rows of data to the spreadsheet.

        Uses batch mode if configured.
        """
        if self.batch_mode:
            self._write_rows_batch(headers, rows, start_row)
        else:
            self._write_rows_individual(headers, rows, start_row)

    def _write_rows_batch(self, headers: list[str], rows: list[dict], start_row: int) -> None:
        """Write rows using a single batch API call."""
        all_values = []

        for row_data in rows:
            row_values = []
            for header in headers:
                if self.validator.is_protected_column(header):
                    row_values.append(None)
                    continue

                formula = self.validator.get_column_formula(header, start_row + len(all_values))
                if formula:
                    row_values.append(formula)
                    continue

                if header.startswith("Subject Descriptor"):
                    descriptors = row_data.get("_subject_descriptors", [])
                    import re
                    match = re.search(r"#(\d+)", header)
                    sd_num = int(match.group(1)) if match else 0
                    if sd_num and sd_num <= len(descriptors):
                        row_values.append(descriptors[sd_num - 1])
                    else:
                        row_values.append("")
                    continue

                if header.startswith("[Sync]"):
                    sync_data = row_data.get("_sync_data", {})
                    if header == "[Sync] Status":
                        changes = row_data.get("_changes", [])
                        if changes:
                            row_values.append("Updated — " + ", ".join(changes))
                        elif row_data.get("_is_new"):
                            row_values.append("New")
                        else:
                            row_values.append("Up to date")
                    else:
                        row_values.append(sync_data.get(header, ""))
                    continue

                row_values.append(row_data.get(header, ""))

            all_values.append(row_values)

        if all_values:
            end_row = start_row + len(all_values) - 1
            last_col = chr(64 + len(headers)) if len(headers) <= 26 else "BZ"
            range_str = f"'Accession Data and Scores'!A{start_row}:{last_col}{end_row}"

            time.sleep(self.throttle_sheets)
            self._sheets_service.spreadsheets().values().update(
                spreadsheetId=self._spreadsheet_id,
                range=range_str,
                valueInputOption="USER_ENTERED",
                body={"values": all_values},
            ).execute()

            self.logger.technical(f"Batch wrote {len(all_values)} rows.")

    def _write_rows_individual(self, headers: list[str], rows: list[dict], start_row: int) -> None:
        """Write rows one at a time (non-batch mode)."""
        for offset, row_data in enumerate(rows):
            row_idx = start_row + offset
            row_values = [row_data.get(h, "") for h in headers]
            last_col = chr(64 + len(headers)) if len(headers) <= 26 else "BZ"

            time.sleep(self.throttle_sheets)
            self._sheets_service.spreadsheets().values().update(
                spreadsheetId=self._spreadsheet_id,
                range=f"'Accession Data and Scores'!A{row_idx}:{last_col}{row_idx}",
                valueInputOption="USER_ENTERED",
                body={"values": [row_values]},
            ).execute()

        self.logger.technical(f"Individually wrote {len(rows)} rows.")

    def delete_row(self, row_idx: int) -> None:
        """Delete a row from the spreadsheet (0-indexed for the API)."""
        sheet_id = self._get_sheet_id("Accession Data and Scores")

        request = {
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": row_idx - 1,
                    "endIndex": row_idx,
                }
            }
        }

        time.sleep(self.throttle_sheets)
        self._sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={"requests": [request]},
        ).execute()

        self.logger.technical(f"Deleted row {row_idx}.")

    def clear_data(self) -> None:
        """Clear all data rows while preserving headers and the spreadsheet."""
        time.sleep(self.throttle_sheets)

        # Get current row count
        result = self._sheets_service.spreadsheets().values().get(
            spreadsheetId=self._spreadsheet_id,
            range="'Accession Data and Scores'!A:A",
        ).execute()
        num_rows = len(result.get("values", []))

        if num_rows > 1:
            self._sheets_service.spreadsheets().values().clear(
                spreadsheetId=self._spreadsheet_id,
                range=f"'Accession Data and Scores'!A2:ZZ{num_rows}",
            ).execute()

        self.logger.summary("Cleared all data from Google Sheet (headers preserved).")

    def check_active_editors(self) -> list[str]:
        """
        Check for active editors on the spreadsheet.

        Returns:
            List of email addresses of active editors, or empty list.
        """
        try:
            time.sleep(self.throttle_drive)
            # Note: The Drive API doesn't directly expose active editors.
            # This checks recent activity as a proxy.
            revisions = self._drive_service.revisions().list(
                fileId=self._spreadsheet_id,
                fields="revisions(modifiedTime,lastModifyingUser)",
                pageSize=1,
            ).execute()

            editors = []
            for rev in revisions.get("revisions", []):
                user = rev.get("lastModifyingUser", {})
                email = user.get("emailAddress", "")
                if email:
                    editors.append(email)

            return editors

        except Exception:
            return []

    def verify_permissions(self) -> bool:
        """
        Verify and re-apply sharing permissions as configured.

        Returns:
            True if all permissions are correct.
        """
        sharing = self.config.get("google_sheets", "sharing", default=[])
        if not sharing:
            return True

        try:
            # Get current permissions
            time.sleep(self.throttle_drive)
            current_perms = self._drive_service.permissions().list(
                fileId=self._spreadsheet_id,
                fields="permissions(id,emailAddress,role)",
            ).execute()

            current_emails = {
                p.get("emailAddress", "").lower(): p
                for p in current_perms.get("permissions", [])
            }

            # Apply configured permissions
            for share_config in sharing:
                email = share_config.get("email", "").lower()
                role = share_config.get("role", "reader")
                notify = share_config.get("notify", True)

                if email not in current_emails or current_emails[email].get("role") != role:
                    time.sleep(self.throttle_drive)
                    self._drive_service.permissions().create(
                        fileId=self._spreadsheet_id,
                        body={"type": "user", "role": role, "emailAddress": email},
                        sendNotificationEmail=notify,
                    ).execute()
                    self.logger.technical(f"Applied permission: {email} as {role}")

            return True

        except Exception as e:
            self.logger.warning(f"Failed to verify permissions: {e}")
            return False

    def _apply_sharing_permissions(self) -> None:
        """Apply sharing permissions from config to the spreadsheet."""
        self.verify_permissions()

    def _move_to_folder(self, file_id: str, folder_id: str) -> None:
        """Move a file to the specified Google Drive folder."""
        try:
            time.sleep(self.throttle_drive)
            file = self._drive_service.files().get(
                fileId=file_id, fields="parents"
            ).execute()
            previous_parents = ",".join(file.get("parents", []))

            self._drive_service.files().update(
                fileId=file_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields="id, parents",
            ).execute()

            self.logger.technical(f"Moved file to folder: {folder_id}")

        except Exception as e:
            self.logger.warning(f"Failed to move file to folder: {e}")

    def _format_headers(self, headers: list[str]) -> None:
        """Format header row with bold text and sync column colors."""
        sheet_id = self._get_sheet_id("Accession Data and Scores")

        requests = [
            # Bold header row
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER",
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,horizontalAlignment)",
                }
            },
            # Freeze header row
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
        ]

        # Color sync columns
        for idx, header in enumerate(headers):
            if header.startswith("[Sync]"):
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": SYNC_HEADER_COLOR,
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                })

        time.sleep(self.throttle_sheets)
        self._sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={"requests": requests},
        ).execute()

    def _create_vocabulary_sheets(self) -> None:
        """Create hidden sheets for structured vocabularies."""
        sheet_names = [
            "Approved Subject Descriptors",
            "Access Issues Vocabulary",
            "Conservation Issues Vocabulary",
            "Digital Issues Vocabulary",
            "Documentation Issues Options",
            "Other Processing Options",
            "Physical Space Options",
            "Processing Project Types",
        ]

        requests = []
        for name in sheet_names:
            requests.append({
                "addSheet": {
                    "properties": {
                        "title": name,
                        "hidden": True,
                    }
                }
            })

        time.sleep(self.throttle_sheets)
        self._sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={"requests": requests},
        ).execute()

    def _get_sheet_id(self, sheet_name: str) -> int:
        """Get the sheet ID for a named sheet."""
        time.sleep(self.throttle_sheets)
        metadata = self._sheets_service.spreadsheets().get(
            spreadsheetId=self._spreadsheet_id
        ).execute()

        for sheet in metadata.get("sheets", []):
            if sheet["properties"]["title"] == sheet_name:
                return sheet["properties"]["sheetId"]

        return 0

    def _extract_spreadsheet_id(self, url: str) -> Optional[str]:
        """Extract spreadsheet ID from a Google Sheets URL."""
        import re
        match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
        return match.group(1) if match else None

    def get_spreadsheet_id(self) -> Optional[str]:
        """Return the current spreadsheet ID."""
        return self._spreadsheet_id

    def get_drive_service(self):
        """Return the Drive service for use by the backup module."""
        return self._drive_service

    def get_sheets_service(self):
        """Return the Sheets service."""
        return self._sheets_service
