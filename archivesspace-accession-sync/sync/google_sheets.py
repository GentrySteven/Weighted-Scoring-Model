"""
Google Sheets Module

Handles all Google Sheets and Google Drive API operations.
Supports service account and OAuth authentication.
Fixes: batch write properly skips protected columns instead of writing None.
"""

import re
import time
from pathlib import Path
from typing import Any, Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.validation import SpreadsheetValidator

# Conditional imports for graceful degradation
try:
    from googleapiclient.discovery import build as google_build
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False

SYNC_HEADER_COLOR = {"red": 0.722, "green": 0.8, "blue": 0.894}


class GoogleSheetsError(Exception):
    """Raised when a Google Sheets/Drive operation fails."""
    pass


class GoogleSheetsManager:
    """Manages all Google Sheets and Google Drive operations."""

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        if not GOOGLE_AVAILABLE:
            raise GoogleSheetsError(
                "Google API libraries not installed. Run:\n"
                "  pip install archivesspace-accession-sync[google]"
            )

        self.config = config
        self.logger = logger
        self.validator = SpreadsheetValidator(config, logger)
        self.throttle_sheets = config.get("throttling", "google_sheets", default=1.0)
        self.throttle_drive = config.get("throttling", "google_drive", default=0.5)
        self.batch_mode = config.get("throttling", "batch_mode", default=True)

        self._sheets_service = None
        self._drive_service = None
        self._spreadsheet_id: Optional[str] = None

    def authenticate(self) -> bool:
        """Authenticate with Google APIs."""
        auth_method = self.config.get_credential("google", "auth_method", default="service_account")
        try:
            if auth_method == "service_account":
                return self._auth_service_account()
            elif auth_method == "oauth":
                return self._auth_oauth()
            else:
                raise GoogleSheetsError(f"Unknown auth method: {auth_method}")
        except GoogleSheetsError:
            raise
        except Exception as e:
            self.logger.error(f"Google authentication failed: {e}")
            return False

    def _auth_service_account(self) -> bool:
        """Authenticate using a service account key file."""
        from google.oauth2 import service_account

        key_path = self.config.get_credential("google", "service_account_key_path", default="")
        if not key_path or not Path(key_path).exists():
            raise GoogleSheetsError(f"Service account key file not found: {key_path}")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = service_account.Credentials.from_service_account_file(key_path, scopes=scopes)
        self._sheets_service = google_build("sheets", "v4", credentials=credentials)
        self._drive_service = google_build("drive", "v3", credentials=credentials)
        self.logger.summary("Authenticated with Google APIs (service account).")
        return True

    def _auth_oauth(self) -> bool:
        """Authenticate using OAuth credentials."""
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        token_path = self.config.get_credential("google", "oauth_token_path", default="")
        creds = None

        if token_path and Path(token_path).exists():
            creds = Credentials.from_authorized_user_file(token_path, scopes)

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

                flow = InstalledAppFlow.from_client_config(
                    {"installed": {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                    }},
                    scopes,
                )
                creds = flow.run_local_server(port=0)

            if token_path:
                Path(token_path).parent.mkdir(parents=True, exist_ok=True)
                with open(token_path, "w") as f:
                    f.write(creds.to_json())

        self._sheets_service = google_build("sheets", "v4", credentials=creds)
        self._drive_service = google_build("drive", "v3", credentials=creds)
        self.logger.summary("Authenticated with Google APIs (OAuth).")
        return True

    def spreadsheet_exists(self) -> bool:
        """Check whether the configured spreadsheet exists."""
        url = self.config.get("google_sheets", "spreadsheet_url", default="")
        if not url:
            return False
        self._spreadsheet_id = self._extract_spreadsheet_id(url)
        if not self._spreadsheet_id:
            return False
        try:
            time.sleep(self.throttle_sheets)
            self._sheets_service.spreadsheets().get(spreadsheetId=self._spreadsheet_id).execute()
            return True
        except Exception:
            return False

    def create_spreadsheet(self, headers: list[str]) -> str:
        """Create a new Google Sheet with headers and formatting."""
        from sync.scoring_criteria import build_values_array, SHEET_NAME as SCORING_SHEET

        name = self.config.get_spreadsheet_name()

        # Build sheet definitions — main sheet plus scoring criteria sheet
        # if dimensions are configured in data.yml
        sheet_definitions = [{"properties": {"title": "Accession Data and Scores"}}]

        criteria = self.config.get_data("scoring_criteria", default={})
        dimensions = criteria.get("dimensions", {})
        if dimensions:
            sheet_definitions.append({"properties": {"title": SCORING_SHEET}})

        body = {
            "properties": {"title": name},
            "sheets": sheet_definitions,
        }
        time.sleep(self.throttle_sheets)
        result = self._sheets_service.spreadsheets().create(body=body).execute()
        self._spreadsheet_id = result["spreadsheetId"]
        url = f"https://docs.google.com/spreadsheets/d/{self._spreadsheet_id}"

        # Write headers to main sheet
        time.sleep(self.throttle_sheets)
        self._sheets_service.spreadsheets().values().update(
            spreadsheetId=self._spreadsheet_id,
            range="'Accession Data and Scores'!A1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()

        # Populate scoring criteria sheet if configured
        if dimensions:
            values, _ = build_values_array(dimensions)
            if values:
                time.sleep(self.throttle_sheets)
                self._sheets_service.spreadsheets().values().update(
                    spreadsheetId=self._spreadsheet_id,
                    range=f"'{SCORING_SHEET}'!A1",
                    valueInputOption="USER_ENTERED",  # Allows formula evaluation
                    body={"values": values},
                ).execute()
                self.logger.technical(
                    f"Populated scoring criteria sheet with {len(dimensions)} dimensions."
                )

        # Move to folder, format headers, apply sharing, create vocab sheets
        folder_id = self.config.get("google_sheets", "folder_id", default="")
        if folder_id:
            self._move_to_folder(self._spreadsheet_id, folder_id)
        self._apply_sharing_permissions()

        self.config.set("google_sheets", "spreadsheet_url", value=url)
        self.config.save_config()

        self.logger.summary(f"Created new Google Sheet: {name}")
        return url

    def read_data(self) -> tuple[list[str], list[dict]]:
        """Read all data from the spreadsheet."""
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
        rows: list[dict] = []
        for row_values in values[1:]:
            row_data: dict[str, Any] = {}
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

    def write_rows(
        self, headers: list[str], rows: list[dict],
        start_row: int = 2, is_completion_event: bool = False,
    ) -> None:
        """Write rows. Uses batch or individual mode based on config."""
        if self.batch_mode:
            self._write_rows_batch(headers, rows, start_row, is_completion_event)
        else:
            self._write_rows_individual(headers, rows, start_row, is_completion_event)

    def _write_rows_batch(
        self, headers: list[str], rows: list[dict],
        start_row: int, is_completion_event: bool = False,
    ) -> None:
        """
        Write rows using a single batch API call for efficiency.

        The Google Sheets API writes entire rows at once, so we can't skip
        individual cells the way we can with openpyxl. For protected columns,
        we must read the existing values first and include them in the batch
        to avoid overwriting them with empty values.

        The column handling logic mirrors excel.py's write_rows:
        1. Protected columns: read existing value and pass it through
        2. Formula columns: write the formula string
        3. Subject Descriptor columns: populate from _subject_descriptors list
        4. Sync columns: populate from _sync_data dict
        5. Regular columns: write from row data
        """
        # Pre-read existing data so we can preserve protected column values.
        # Without this, the batch write would clear protected cells because
        # the API replaces the entire row range at once.
        existing_data: dict[int, dict[str, Any]] = {}
        if any(self.validator.is_protected_column(h, is_completion_event) for h in headers):
            try:
                _, existing_rows = self.read_data()
                for row in existing_rows:
                    aid = row.get("accession_id")
                    if aid is not None:
                        existing_data[int(aid)] = row
            except Exception:
                pass

        # Build the values array for the batch API call
        all_values: list[list[Any]] = []
        for row_idx_offset, row_data in enumerate(rows):
            row_values: list[Any] = []
            actual_row = start_row + row_idx_offset
            acc_id = row_data.get("Accession ID")

            for header in headers:
                # Protected columns: look up the existing value from our
                # pre-read data and pass it through unchanged
                if self.validator.is_protected_column(header, is_completion_event):
                    existing_row = existing_data.get(int(acc_id) if acc_id else 0, {})
                    existing_val = existing_row.get(header, "")
                    row_values.append(existing_val if existing_val else "")
                    continue

                # Formula columns: use dynamic column map for correct references
                column_map = {h: i + 1 for i, h in enumerate(headers)}
                formula = self.validator.get_column_formula(header, actual_row, column_map)
                if formula:
                    row_values.append(formula)
                    continue

                # Subject descriptors: match by column number
                if header.startswith("Subject Descriptor"):
                    descriptors = row_data.get("_subject_descriptors", [])
                    match = re.search(r"#(\d+)", header)
                    sd_num = int(match.group(1)) if match else 0
                    if sd_num and sd_num <= len(descriptors):
                        row_values.append(descriptors[sd_num - 1])
                    else:
                        row_values.append("")
                    continue

                # Sync columns: build status from change detection results
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

                # Regular data columns
                row_values.append(row_data.get(header, ""))

            all_values.append(row_values)

        if all_values:
            from sync.utils import col_letter as cl
            last_col = cl(len(headers))
            end_row = start_row + len(all_values) - 1
            range_str = f"'Accession Data and Scores'!A{start_row}:{last_col}{end_row}"

            time.sleep(self.throttle_sheets)
            self._sheets_service.spreadsheets().values().update(
                spreadsheetId=self._spreadsheet_id,
                range=range_str,
                valueInputOption="USER_ENTERED",
                body={"values": all_values},
            ).execute()
            self.logger.technical(f"Batch wrote {len(all_values)} rows.")

    def _write_rows_individual(
        self, headers: list[str], rows: list[dict],
        start_row: int, is_completion_event: bool = False,
    ) -> None:
        """Write rows one at a time."""
        from sync.utils import col_letter as cl
        last_col = cl(len(headers))

        for offset, row_data in enumerate(rows):
            row_idx = start_row + offset
            row_values = [row_data.get(h, "") for h in headers]

            time.sleep(self.throttle_sheets)
            self._sheets_service.spreadsheets().values().update(
                spreadsheetId=self._spreadsheet_id,
                range=f"'Accession Data and Scores'!A{row_idx}:{last_col}{row_idx}",
                valueInputOption="USER_ENTERED",
                body={"values": [row_values]},
            ).execute()

        self.logger.technical(f"Individually wrote {len(rows)} rows.")

    def delete_row(self, row_idx: int) -> None:
        """Delete a row (0-indexed for API)."""
        sheet_id = self._get_sheet_id("Accession Data and Scores")
        time.sleep(self.throttle_sheets)
        self._sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={"requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": row_idx - 1,
                        "endIndex": row_idx,
                    }
                }
            }]},
        ).execute()

    # -------------------------------------------------------------------------
    # Polymorphic API parity with ExcelManager
    # -------------------------------------------------------------------------
    # The following methods exist to provide API parity between GoogleSheetsManager
    # and ExcelManager so that cli.py can call them on either object without
    # hasattr() guards. Without these, Google Sheets users silently lose row
    # update and deletion functionality (only appends work).

    def transaction(self):
        """
        Return a no-op context manager for API parity with ExcelManager.

        The Google Sheets API is inherently transactional on a per-call
        basis (each update is an HTTP request), so there is no meaningful
        local transaction to group. Callers can use `with manager.transaction()`
        on either backend; for Google Sheets it simply enters and exits
        without side effects.
        """
        from contextlib import nullcontext
        return nullcontext(self)

    def file_exists(self) -> bool:
        """
        Alias for spreadsheet_exists() — present for API parity with
        ExcelManager.file_exists() so polymorphic code in cli.py works
        correctly for both backends.
        """
        return self.spreadsheet_exists()

    def get_file_path(self) -> Optional[str]:
        """
        Return the Google Sheets URL (there is no file path).

        Present for API parity with ExcelManager.get_file_path(). Callers
        that pass this to openpyxl will fail — callers that use it for
        logging or display purposes will see the spreadsheet URL instead.
        """
        return self.config.get("google_sheets", "spreadsheet_url", default=None)

    def get_column_map(self) -> dict[str, int]:
        """
        Get the current column name to 1-indexed position mapping.

        Reads only the header row from the main sheet. Present for API
        parity with ExcelManager.get_column_map().
        """
        if not self._sheets_service or not self._spreadsheet_id:
            self.authenticate()
        time.sleep(self.throttle_sheets)
        result = self._sheets_service.spreadsheets().values().get(
            spreadsheetId=self._spreadsheet_id,
            range="'Accession Data and Scores'!1:1",
        ).execute()
        values = result.get("values", [[]])
        headers = values[0] if values else []
        return {h: i + 1 for i, h in enumerate(headers) if h}

    def find_row_by_accession_id(self, accession_id: int) -> Optional[int]:
        """
        Find the 1-indexed row number for a given Accession ID.

        Reads the Accession ID column from the main sheet and returns the
        first matching row number. Returns None if not found or if the
        Accession ID column cannot be located.

        Note: this incurs an API call. For bulk operations, consider
        caching the column values beforehand.
        """
        column_map = self.get_column_map()
        acc_id_col = column_map.get("Accession ID")
        if acc_id_col is None:
            return None

        # Convert column index to A1 letter notation
        col_letter = self._column_index_to_letter(acc_id_col)

        time.sleep(self.throttle_sheets)
        result = self._sheets_service.spreadsheets().values().get(
            spreadsheetId=self._spreadsheet_id,
            range=f"'Accession Data and Scores'!{col_letter}:{col_letter}",
        ).execute()
        values = result.get("values", [])

        # values[0] is the header row, data starts at values[1]
        for idx, row in enumerate(values[1:], start=2):
            if row and row[0]:
                try:
                    if int(row[0]) == accession_id:
                        return idx
                except (ValueError, TypeError):
                    continue
        return None

    def update_row(
        self, headers: list[str], row_data: dict, row_idx: int, **kwargs
    ) -> None:
        """
        Update a single row at the given 1-indexed position.

        Delegates to write_rows() with start_row set to the target row,
        which handles all the column-type-aware logic (protected columns,
        formulas, subject descriptors, sync tracking) the same way as
        ExcelManager.update_row().
        """
        self.write_rows(headers, [row_data], start_row=row_idx, **kwargs)

    def _column_index_to_letter(self, idx: int) -> str:
        """
        Convert a 1-indexed column number to its A1 letter notation.

        Examples: 1 -> 'A', 26 -> 'Z', 27 -> 'AA', 702 -> 'ZZ'.
        """
        letters = ""
        while idx > 0:
            idx, remainder = divmod(idx - 1, 26)
            letters = chr(65 + remainder) + letters
        return letters

    def clear_data(self) -> None:
        """Clear all data rows while preserving headers."""
        time.sleep(self.throttle_sheets)
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
        self.logger.summary("Cleared all data from Google Sheet.")

    def verify_permissions(self) -> bool:
        """Verify and re-apply sharing permissions."""
        sharing = self.config.get("google_sheets", "sharing", default=[])
        if not sharing:
            return True
        try:
            time.sleep(self.throttle_drive)
            current_perms = self._drive_service.permissions().list(
                fileId=self._spreadsheet_id,
                fields="permissions(id,emailAddress,role)",
            ).execute()
            current_emails = {
                p.get("emailAddress", "").lower(): p
                for p in current_perms.get("permissions", [])
            }
            for share in sharing:
                email = share.get("email", "").lower()
                role = share.get("role", "reader")
                notify = share.get("notify", True)
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
        self.verify_permissions()

    def _move_to_folder(self, file_id: str, folder_id: str) -> None:
        try:
            time.sleep(self.throttle_drive)
            file = self._drive_service.files().get(fileId=file_id, fields="parents").execute()
            previous = ",".join(file.get("parents", []))
            self._drive_service.files().update(
                fileId=file_id, addParents=folder_id,
                removeParents=previous, fields="id, parents",
            ).execute()
        except Exception as e:
            self.logger.warning(f"Failed to move file to folder: {e}")

    def _get_sheet_id(self, name: str) -> int:
        time.sleep(self.throttle_sheets)
        metadata = self._sheets_service.spreadsheets().get(
            spreadsheetId=self._spreadsheet_id
        ).execute()
        for sheet in metadata.get("sheets", []):
            if sheet["properties"]["title"] == name:
                return sheet["properties"]["sheetId"]
        return 0

    def _extract_spreadsheet_id(self, url: str) -> Optional[str]:
        match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
        return match.group(1) if match else None

    def get_spreadsheet_id(self) -> Optional[str]:
        return self._spreadsheet_id

    def get_drive_service(self):
        return self._drive_service

    def get_sheets_service(self):
        return self._sheets_service
