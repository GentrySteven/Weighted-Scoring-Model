"""
Excel Module

Handles all Excel spreadsheet operations using openpyxl, including:
- Creating new spreadsheets with correct structure
- Reading existing spreadsheet data
- Writing and updating cell values
- Formatting (column auto-sizing, sync column colors, checkboxes)
- Formula insertion
- File lock detection
"""

import os
import platform
import subprocess
import time
from pathlib import Path
from typing import Any, Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.validation import SpreadsheetValidator, SYNC_COLUMNS, SCORING_COLUMNS


# Color for sync column headers (light gray-blue)
SYNC_HEADER_COLOR = "B8CCE4"


class ExcelError(Exception):
    """Raised when an Excel operation fails."""
    pass


class ExcelManager:
    """
    Manages all Excel spreadsheet operations.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        self.config = config
        self.logger = logger
        self.validator = SpreadsheetValidator(config, logger)
        self.target_dir = Path(config.get("excel", "target_directory", default=""))
        self.spreadsheet_name = config.get_spreadsheet_name()
        self.file_path = self.target_dir / f"{self.spreadsheet_name}.xlsx"

    def file_exists(self) -> bool:
        """Check whether the spreadsheet file exists."""
        return self.file_path.exists()

    def get_file_path(self) -> Path:
        """Return the full file path."""
        return self.file_path

    def is_file_locked(self) -> tuple[bool, str]:
        """
        Check if the Excel file is locked by another process.

        Returns:
            Tuple of (is_locked, lock_info) where lock_info describes
            the locking process if identifiable.
        """
        lock_file = self.file_path.parent / f"~${self.file_path.name}"
        if lock_file.exists():
            lock_info = self._identify_lock_holder()
            return True, lock_info

        # Try opening the file for writing
        try:
            with open(self.file_path, "a"):
                pass
            return False, ""
        except (IOError, PermissionError):
            lock_info = self._identify_lock_holder()
            return True, lock_info

    def _identify_lock_holder(self) -> str:
        """Attempt to identify which process has the file locked."""
        system = platform.system()

        try:
            if system == "Windows":
                # Use handle.exe or PowerShell to find the process
                result = subprocess.run(
                    ["powershell", "-Command",
                     f"Get-Process | Where-Object {{$_.MainWindowTitle -like '*{self.spreadsheet_name}*'}} | Select-Object ProcessName, Id"],
                    capture_output=True, text=True, timeout=5
                )
                if result.stdout.strip():
                    return f"Locked by: {result.stdout.strip()}"

            elif system in ("Linux", "Darwin"):
                result = subprocess.run(
                    ["lsof", str(self.file_path)],
                    capture_output=True, text=True, timeout=5
                )
                if result.stdout.strip():
                    lines = result.stdout.strip().split("\n")
                    if len(lines) > 1:
                        parts = lines[1].split()
                        if len(parts) >= 2:
                            return f"Locked by: {parts[0]} (PID: {parts[1]})"

        except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
            pass

        return "Locked by another application"

    def wait_for_unlock(self) -> bool:
        """
        Wait for the file to become unlocked, with retries.

        Returns:
            True if the file is now unlocked, False if all retries exhausted.
        """
        max_retries = self.config.get("retry", "file_lock_retries", default=5)
        interval = self.config.get("retry", "file_lock_interval", default=60)

        for attempt in range(max_retries):
            is_locked, lock_info = self.is_file_locked()
            if not is_locked:
                return True

            remaining = max_retries - attempt - 1
            self.logger.summary(
                f"File is locked. {lock_info}. "
                f"Retrying in {interval} seconds ({remaining} retries remaining)."
            )
            print(
                f"\n  The Excel file is currently locked.\n"
                f"  {lock_info}\n"
                f"  Retrying in {interval} seconds ({remaining} retries remaining).\n"
                f"  Close the file in Excel to allow the sync to proceed.\n"
            )
            time.sleep(interval)

        return False

    def create_spreadsheet(self, headers: list[str]) -> Path:
        """
        Create a new Excel spreadsheet with the specified headers.

        Args:
            headers: List of column header strings.

        Returns:
            Path to the created file.
        """
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill, Alignment
            from openpyxl.worksheet.datavalidation import DataValidation
        except ImportError:
            raise ExcelError("openpyxl is not installed. Run: pip install archivesspace-accession-sync[excel]")

        wb = Workbook()
        ws = wb.active
        ws.title = "Accession Data and Scores"

        # Write headers
        for col_idx, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col_idx, value=header)
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center", wrap_text=True)

            # Color sync columns
            if header.startswith("[Sync]"):
                cell.fill = PatternFill(
                    start_color=SYNC_HEADER_COLOR,
                    end_color=SYNC_HEADER_COLOR,
                    fill_type="solid",
                )

        # Auto-size columns
        self._auto_size_columns(ws)

        # Add data validation for checkbox columns (format detection)
        format_keywords = self.config.get("format_keywords", default={})
        for col_idx, header in enumerate(headers, 1):
            if header in format_keywords:
                dv = DataValidation(
                    type="list",
                    formula1='"TRUE,FALSE"',
                    allow_blank=True,
                )
                dv.error = "Please select TRUE or FALSE"
                dv.errorTitle = "Invalid value"
                col_letter = self._col_letter(col_idx)
                dv.add(f"{col_letter}2:{col_letter}1048576")
                ws.add_data_validation(dv)

        # Create hidden sheets for structured vocabularies
        self._create_vocabulary_sheets(wb)

        # Save
        self.target_dir.mkdir(parents=True, exist_ok=True)
        wb.save(str(self.file_path))

        self.logger.summary(f"Created new Excel spreadsheet: {self.file_path.name}")
        return self.file_path

    def read_data(self) -> tuple[list[str], list[dict]]:
        """
        Read all data from the spreadsheet.

        Returns:
            Tuple of (headers, rows) where rows is a list of dicts
            mapping header names to cell values.
        """
        from openpyxl import load_workbook

        wb = load_workbook(str(self.file_path), data_only=True)
        ws = wb["Accession Data and Scores"]

        headers = []
        for col_idx in range(1, ws.max_column + 1):
            val = ws.cell(row=1, column=col_idx).value
            headers.append(val or "")

        rows = []
        for row_idx in range(2, ws.max_row + 1):
            row_data = {}
            has_data = False
            for col_idx, header in enumerate(headers, 1):
                val = ws.cell(row=row_idx, column=col_idx).value
                if val is not None:
                    has_data = True
                row_data[header] = val

            # Map accession_id for lookup
            row_data["accession_id"] = row_data.get("Accession ID")
            if has_data:
                rows.append(row_data)

        wb.close()
        return headers, rows

    def write_rows(self, headers: list[str], rows: list[dict], start_row: int = 2) -> None:
        """
        Write multiple rows of data to the spreadsheet.

        Args:
            headers: Column headers for mapping.
            rows: List of row data dicts.
            start_row: First data row (1-indexed).
        """
        from openpyxl import load_workbook

        wb = load_workbook(str(self.file_path))
        ws = wb["Accession Data and Scores"]

        for row_offset, row_data in enumerate(rows):
            row_idx = start_row + row_offset

            for col_idx, header in enumerate(headers, 1):
                # Skip protected columns
                if self.validator.is_protected_column(header):
                    existing = ws.cell(row=row_idx, column=col_idx).value
                    if existing is not None:
                        continue

                # Handle formula columns
                formula = self.validator.get_column_formula(header, row_idx)
                if formula:
                    ws.cell(row=row_idx, column=col_idx, value=formula)
                    continue

                # Handle subject descriptors
                if header.startswith("Subject Descriptor"):
                    descriptors = row_data.get("_subject_descriptors", [])
                    sd_num = self._extract_descriptor_number(header)
                    if sd_num and sd_num <= len(descriptors):
                        ws.cell(row=row_idx, column=col_idx, value=descriptors[sd_num - 1])
                    continue

                # Handle sync tracking
                if header.startswith("[Sync]"):
                    sync_data = row_data.get("_sync_data", {})
                    ws.cell(row=row_idx, column=col_idx, value=sync_data.get(header, ""))
                    continue

                # Handle sync status
                if header == "[Sync] Status":
                    changes = row_data.get("_changes", [])
                    if changes:
                        status = "Updated — " + ", ".join(changes)
                    elif row_data.get("_is_new"):
                        status = "New"
                    else:
                        status = "Up to date"
                    ws.cell(row=row_idx, column=col_idx, value=status)
                    continue

                # Regular data columns
                value = row_data.get(header)
                if value is not None:
                    ws.cell(row=row_idx, column=col_idx, value=value)

        # Auto-size after writing
        self._auto_size_columns(ws)

        wb.save(str(self.file_path))
        self.logger.technical(f"Wrote {len(rows)} rows to spreadsheet.")

    def update_row(self, headers: list[str], row_data: dict, row_idx: int) -> None:
        """
        Update a single row in the spreadsheet.

        Args:
            headers: Column headers.
            row_data: Row data dict.
            row_idx: Row number (1-indexed).
        """
        self.write_rows(headers, [row_data], start_row=row_idx)

    def delete_row(self, row_idx: int) -> None:
        """
        Delete a row from the spreadsheet.

        Args:
            row_idx: Row number to delete (1-indexed).
        """
        from openpyxl import load_workbook

        wb = load_workbook(str(self.file_path))
        ws = wb["Accession Data and Scores"]
        ws.delete_rows(row_idx)
        wb.save(str(self.file_path))
        self.logger.technical(f"Deleted row {row_idx}.")

    def clear_data(self) -> None:
        """
        Clear all data rows from the spreadsheet while preserving
        the file, headers, and structure.
        """
        from openpyxl import load_workbook

        wb = load_workbook(str(self.file_path))
        ws = wb["Accession Data and Scores"]

        # Delete all rows after the header
        if ws.max_row > 1:
            ws.delete_rows(2, ws.max_row - 1)

        wb.save(str(self.file_path))
        self.logger.summary("Cleared all data from spreadsheet (headers preserved).")

    def _auto_size_columns(self, ws) -> None:
        """Auto-size all columns based on content width."""
        for col in ws.columns:
            max_length = 0
            col_letter = None
            for cell in col:
                if col_letter is None:
                    col_letter = cell.column_letter
                if cell.value:
                    cell_length = len(str(cell.value))
                    max_length = max(max_length, cell_length)

            if col_letter and max_length > 0:
                adjusted_width = min(max_length + 2, 50)
                ws.column_dimensions[col_letter].width = adjusted_width

    def _create_vocabulary_sheets(self, wb) -> None:
        """Create hidden sheets for structured vocabularies."""
        vocab_sheets = [
            "Approved Subject Descriptors",
            "Access Issues Vocabulary",
            "Conservation Issues Vocabulary",
            "Digital Issues Vocabulary",
            "Documentation Issues Options",
            "Other Processing Options",
            "Physical Space Options",
        ]

        for sheet_name in vocab_sheets:
            ws = wb.create_sheet(title=sheet_name)
            ws.sheet_state = "hidden"

        # Populate Documentation and Use Issues defaults
        ws = wb["Documentation Issues Options"]
        defaults = self.config.get("documentation_use_issues_options", default=[])
        for idx, option in enumerate(defaults, 1):
            ws.cell(row=idx, column=1, value=option)

        # Populate Processing Project Types
        ws = wb.create_sheet(title="Processing Project Types")
        ws.sheet_state = "hidden"
        types = self.config.get("processing_project_types", default=[])
        for idx, ptype in enumerate(types, 1):
            ws.cell(row=idx, column=1, value=ptype)

    def _col_letter(self, col_idx: int) -> str:
        """Convert a 1-indexed column number to a letter (A, B, ..., Z, AA, AB, ...)."""
        result = ""
        while col_idx > 0:
            col_idx, remainder = divmod(col_idx - 1, 26)
            result = chr(65 + remainder) + result
        return result

    def _extract_descriptor_number(self, header: str) -> Optional[int]:
        """Extract the number from a 'Subject Descriptor (#N)' header."""
        import re
        match = re.search(r"#(\d+)", header)
        return int(match.group(1)) if match else None

    def find_row_by_accession_id(self, accession_id: int) -> Optional[int]:
        """
        Find the row number for a given accession ID.

        Args:
            accession_id: The accession's internal database ID.

        Returns:
            Row number (1-indexed) or None if not found.
        """
        from openpyxl import load_workbook

        wb = load_workbook(str(self.file_path), data_only=True)
        ws = wb["Accession Data and Scores"]

        # Find the Accession ID column
        acc_id_col = None
        for col_idx in range(1, ws.max_column + 1):
            if ws.cell(row=1, column=col_idx).value == "Accession ID":
                acc_id_col = col_idx
                break

        if acc_id_col is None:
            wb.close()
            return None

        for row_idx in range(2, ws.max_row + 1):
            cell_val = ws.cell(row=row_idx, column=acc_id_col).value
            if cell_val is not None and int(cell_val) == accession_id:
                wb.close()
                return row_idx

        wb.close()
        return None
