"""
Validation Module

Handles spreadsheet structure validation, including:
- Column detection by name (not position)
- Required column verification
- Repair and rebuild logic
- User-added column preservation
"""

from typing import Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager


# Required data columns (order doesn't matter - detected by name)
REQUIRED_COLUMNS = [
    "Accession Status",
    "Base URL (Use for Hyperlink Only)",
    "Accession ID",
    "Base URL and Accession ID (Use for Hyperlink Only)",
    "Identifier (Use for Hyperlink Only)",
    "Accession Number",
    "Donor Name",
    "Accession Date",
    "Priority",
    "Classification",
    "Accession Extent - Physical (Linear Feet)",
    "Accession Extent - Digital (GB)",
]

# Formula columns that should not be overwritten
FORMULA_COLUMNS = [
    "Base URL and Accession ID (Use for Hyperlink Only)",
    "Accession Number",
    "Total Number of Formats",
    "Total Number of Subject Descriptors",
    "Total Number of Issues",
]

# Manually assigned columns that should be protected during sync
PROTECTED_COLUMNS = [
    "Documentation and Use Issues",
    "Physical Space Management Issues",
    "Notes",
    "Kind of Processing Project",
]

# Sync tracking column prefix
SYNC_PREFIX = "[Sync]"

SYNC_COLUMNS = [
    "[Sync] Accession lock_version",
    "[Sync] Collection Management lock_version",
    "[Sync] Extents lock_version",
    "[Sync] Linked Agents IDs",
    "[Sync] Linked Agents Values",
    "[Sync] Subjects IDs",
    "[Sync] Subjects Values",
    "[Sync] Classifications IDs",
    "[Sync] Classifications Values",
    "[Sync] Digital Objects IDs",
    "[Sync] Digital Objects Values",
    "[Sync] Top Containers IDs",
    "[Sync] Top Containers Values",
    "[Sync] Status",
]

# Scoring columns (protected, contain formulas)
SCORING_COLUMNS = [
    "Time in Backlog (UWS)",
    "Priority (UWS)",
    "Subject Descriptors (UWS)",
    "Time in Backlog (Weight)",
    "Priority (Weight)",
    "Subject Descriptors (Weight)",
    "Time in Backlog (WS)",
    "Priority (WS)",
    "Subject Descriptors (WS)",
    "Final Accession Score",
]


class ValidationError(Exception):
    """Raised when spreadsheet validation fails."""

    pass


class ValidationResult:
    """Result of a spreadsheet validation check."""

    def __init__(self):
        self.is_valid: bool = True
        self.missing_columns: list[str] = []
        self.extra_columns: list[str] = []
        self.column_map: dict[str, int] = {}  # column_name -> column_index
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def add_missing(self, column: str) -> None:
        """Record a missing required column."""
        self.is_valid = False
        self.missing_columns.append(column)
        self.errors.append(f"Required column missing: {column}")

    def add_warning(self, message: str) -> None:
        """Record a warning (non-fatal issue)."""
        self.warnings.append(message)

    def add_error(self, message: str) -> None:
        """Record an error (fatal issue)."""
        self.is_valid = False
        self.errors.append(message)


class SpreadsheetValidator:
    """
    Validates spreadsheet structure by checking column headers.
    Columns are identified by name, not position, so rearrangement
    is not treated as an error.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        """
        Initialize the validator.

        Args:
            config: ConfigManager instance.
            logger: LoggingManager instance.
        """
        self.config = config
        self.logger = logger

    def get_expected_columns(self) -> list[str]:
        """
        Build the complete list of expected column headers based on configuration.

        Returns:
            Ordered list of expected column header strings.
        """
        columns = list(REQUIRED_COLUMNS)

        # Add format columns from config
        format_keywords = self.config.get("format_keywords", default={})
        for format_name in format_keywords:
            if format_name not in columns:
                columns.append(format_name)
        columns.append("Total Number of Formats")

        # Add subject descriptor columns
        num_sd = self.config.get("subject_descriptors", "num_columns", default=9)
        for i in range(1, num_sd + 1):
            columns.append(f"Subject Descriptor (#{i})")
        columns.append("Total Number of Subject Descriptors")

        # Add issue columns
        issue_columns = [
            "Access Issues",
            "Conservation Issues",
            "Digital Issues",
            "Documentation and Use Issues",
            "Other Processing Information",
            "Physical Space Management Issues",
        ]
        columns.extend(issue_columns)
        columns.append("Total Number of Issues")

        # Add scoring columns
        columns.extend(SCORING_COLUMNS)

        # Add local tracking columns
        columns.extend(["Notes", "Month Completed", "Kind of Processing Project"])

        # Add sync columns at the end
        columns.extend(SYNC_COLUMNS)

        return columns

    def validate(self, headers: list[str]) -> ValidationResult:
        """
        Validate a spreadsheet's column headers against expected columns.

        Args:
            headers: List of column header strings from the spreadsheet.

        Returns:
            ValidationResult with details about any issues found.
        """
        result = ValidationResult()
        expected = self.get_expected_columns()

        # Build column map (header_name -> index)
        header_lower_map = {}
        for idx, header in enumerate(headers):
            if header:
                result.column_map[header] = idx
                header_lower_map[header.lower().strip()] = header

        # Check for missing required columns
        for expected_col in expected:
            if expected_col not in result.column_map:
                # Try case-insensitive match
                if expected_col.lower().strip() not in header_lower_map:
                    result.add_missing(expected_col)
                else:
                    # Found with different case - record a warning
                    actual_name = header_lower_map[expected_col.lower().strip()]
                    result.add_warning(
                        f"Column '{expected_col}' found with different case: '{actual_name}'"
                    )
                    result.column_map[expected_col] = result.column_map[actual_name]

        # Identify extra (user-added) columns
        expected_set = {c.lower().strip() for c in expected}
        for header in headers:
            if header and header.lower().strip() not in expected_set:
                result.extra_columns.append(header)

        if result.extra_columns:
            result.add_warning(
                f"Found {len(result.extra_columns)} user-added column(s): "
                f"{', '.join(result.extra_columns)}"
            )

        # Log results
        if result.is_valid:
            self.logger.summary("Spreadsheet validation passed.")
            for warning in result.warnings:
                self.logger.warning(warning)
        else:
            self.logger.error("Spreadsheet validation failed.")
            for error in result.errors:
                self.logger.error(error)

        return result

    def is_protected_column(self, column_name: str) -> bool:
        """
        Check if a column should be protected from sync overwrites.

        Protected columns include manually assigned values, formulas,
        and scoring columns.
        """
        if column_name in PROTECTED_COLUMNS:
            return True
        if column_name in FORMULA_COLUMNS:
            return True
        if column_name in SCORING_COLUMNS:
            return True
        if column_name == "Month Completed":
            return True
        if column_name == "Notes":
            return True
        return False

    def is_sync_column(self, column_name: str) -> bool:
        """Check if a column is a sync tracking column."""
        return column_name.startswith(SYNC_PREFIX)

    def is_formula_column(self, column_name: str) -> bool:
        """Check if a column should contain a formula."""
        return column_name in FORMULA_COLUMNS

    def get_column_formula(self, column_name: str, row: int) -> Optional[str]:
        """
        Get the formula for a formula column at a given row.

        Args:
            column_name: The column header name.
            row: The row number (1-indexed).

        Returns:
            The formula string, or None if not a formula column.
        """
        if column_name == "Base URL and Accession ID (Use for Hyperlink Only)":
            return f"=CONCAT(B{row},C{row})"
        if column_name == "Accession Number":
            return f"=HYPERLINK(D{row},E{row})"
        if column_name == "Total Number of Formats":
            # Dynamic range based on format columns
            format_keywords = self.config.get("format_keywords", default={})
            num_formats = len(format_keywords)
            if num_formats > 0:
                # Format columns start at column N (col 14)
                start_col = self._col_letter(14)
                end_col = self._col_letter(14 + num_formats - 1)
                return f"=COUNTIF({start_col}{row}:{end_col}{row},TRUE)"
            return ""
        if column_name == "Total Number of Subject Descriptors":
            num_sd = self.config.get("subject_descriptors", "num_columns", default=9)
            # Subject descriptor columns start at column AC (col 29)
            start_col = self._col_letter(29)
            end_col = self._col_letter(29 + num_sd - 1)
            return f"=COUNTA({start_col}{row}:{end_col}{row})"
        if column_name == "Total Number of Issues":
            return f'=COUNTIF(AM{row}:AR{row},"*")'

        return None

    @staticmethod
    def _col_letter(col_idx: int) -> str:
        """
        Convert a 1-indexed column number to an Excel column letter.

        Args:
            col_idx: 1-indexed column number (1=A, 26=Z, 27=AA, etc.)

        Returns:
            Column letter string (e.g., "A", "Z", "AA", "AK").
        """
        result = ""
        while col_idx > 0:
            col_idx, remainder = divmod(col_idx - 1, 26)
            result = chr(65 + remainder) + result
        return result
