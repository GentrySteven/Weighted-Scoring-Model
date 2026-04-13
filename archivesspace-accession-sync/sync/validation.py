"""
Validation Module

Handles spreadsheet structure validation, including:
- Column detection by name (not position)
- Required column verification
- Dynamic formula generation using actual column positions
- Repair and rebuild logic
- User-added column preservation
"""

from typing import Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.utils import col_letter


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

FORMULA_COLUMNS = [
    "Base URL and Accession ID (Use for Hyperlink Only)",
    "Accession Number",
    "Total Number of Formats",
    "Total Number of Subject Descriptors",
    "Total Number of Issues",
]

PROTECTED_COLUMNS = [
    "Documentation and Use Issues",
    "Physical Space Management Issues",
    "Notes",
    "Kind of Processing Project",
]

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
        self.column_map: dict[str, int] = {}
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

        # Format columns
        format_keywords = self.config.get("format_keywords", default={})
        for format_name in format_keywords:
            if format_name not in columns:
                columns.append(format_name)
        columns.append("Total Number of Formats")

        # Subject descriptor columns
        num_sd = self.config.get("subject_descriptors", "num_columns", default=9)
        for i in range(1, num_sd + 1):
            columns.append(f"Subject Descriptor (#{i})")
        columns.append("Total Number of Subject Descriptors")

        # Issue columns
        columns.extend([
            "Access Issues",
            "Conservation Issues",
            "Digital Issues",
            "Documentation and Use Issues",
            "Other Processing Information",
            "Physical Space Management Issues",
        ])
        columns.append("Total Number of Issues")

        # Scoring columns — dynamically generated from configured dimensions
        columns.extend(self.get_scoring_columns())

        # Local tracking columns
        columns.extend(["Notes", "Month Completed", "Kind of Processing Project"])

        # Sync columns at the end
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

        # Build column map (header_name -> 1-indexed column number)
        header_lower_map: dict[str, str] = {}
        for idx, header in enumerate(headers):
            if header:
                result.column_map[header] = idx + 1  # 1-indexed
                header_lower_map[header.lower().strip()] = header

        # Check for missing required columns
        for expected_col in expected:
            if expected_col not in result.column_map:
                if expected_col.lower().strip() not in header_lower_map:
                    result.add_missing(expected_col)
                else:
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

    def is_protected_column(self, column_name: str, is_completion_event: bool = False) -> bool:
        """
        Check if a column should be protected from sync overwrites.

        Protected columns include manually assigned values, formulas,
        and scoring columns. Scoring columns are detected by their suffix
        — (UWS), (Weight), (WS) — to support dynamically configured
        dimensions in addition to the hardcoded SCORING_COLUMNS list.

        Args:
            column_name: The column header name.
            is_completion_event: If True, Month Completed is writable.
        """
        if column_name in PROTECTED_COLUMNS:
            return True
        if column_name in FORMULA_COLUMNS:
            return True
        if column_name in SCORING_COLUMNS:
            return True
        # Dynamic scoring columns: any column ending with (UWS), (Weight), (WS),
        # or named "Final Accession Score"
        if column_name.endswith(" (UWS)") or column_name.endswith(" (Weight)") or column_name.endswith(" (WS)"):
            return True
        if column_name == "Final Accession Score":
            return True
        if column_name == "Month Completed":
            return not is_completion_event
        if column_name == "Notes":
            return True
        return False

    def is_sync_column(self, column_name: str) -> bool:
        """Check if a column is a sync tracking column."""
        return column_name.startswith(SYNC_PREFIX)

    def is_formula_column(self, column_name: str) -> bool:
        """Check if a column should contain a formula."""
        return column_name in FORMULA_COLUMNS

    def get_column_formula(
        self,
        column_name: str,
        row: int,
        column_map: Optional[dict[str, int]] = None,
    ) -> Optional[str]:
        """
        Get the formula for a formula column at a given row.

        Uses the column_map to generate formulas with correct column
        references regardless of column position. Falls back to the
        expected column order if no map is provided.

        Args:
            column_name: The column header name.
            row: The row number (1-indexed).
            column_map: Dict mapping column names to 1-indexed positions.

        Returns:
            The formula string, or None if not a formula column.
        """
        if column_map is None:
            # Build a map from expected column order
            expected = self.get_expected_columns()
            column_map = {name: idx + 1 for idx, name in enumerate(expected)}

        if column_name == "Base URL and Accession ID (Use for Hyperlink Only)":
            base_col = col_letter(column_map.get("Base URL (Use for Hyperlink Only)", 2))
            id_col = col_letter(column_map.get("Accession ID", 3))
            return f"=CONCAT({base_col}{row},{id_col}{row})"

        if column_name == "Accession Number":
            url_col = col_letter(column_map.get(
                "Base URL and Accession ID (Use for Hyperlink Only)", 4
            ))
            ident_col = col_letter(column_map.get("Identifier (Use for Hyperlink Only)", 5))
            return f"=HYPERLINK({url_col}{row},{ident_col}{row})"

        if column_name == "Total Number of Formats":
            format_keywords = self.config.get("format_keywords", default={})
            format_names = list(format_keywords.keys())
            if not format_names:
                return ""
            first_col = column_map.get(format_names[0])
            last_col = column_map.get(format_names[-1])
            if first_col and last_col:
                return f"=COUNTIF({col_letter(first_col)}{row}:{col_letter(last_col)}{row},TRUE)"
            return ""

        if column_name == "Total Number of Subject Descriptors":
            num_sd = self.config.get("subject_descriptors", "num_columns", default=9)
            first_sd = column_map.get("Subject Descriptor (#1)")
            last_sd = column_map.get(f"Subject Descriptor (#{num_sd})")
            if first_sd and last_sd:
                return f"=COUNTA({col_letter(first_sd)}{row}:{col_letter(last_sd)}{row})"
            return ""

        if column_name == "Total Number of Issues":
            issue_cols = [
                "Access Issues", "Conservation Issues", "Digital Issues",
                "Documentation and Use Issues", "Other Processing Information",
                "Physical Space Management Issues",
            ]
            positions = [column_map.get(c) for c in issue_cols if column_map.get(c)]
            if positions:
                first = col_letter(min(positions))
                last = col_letter(max(positions))
                return f'=COUNTIF({first}{row}:{last}{row},"*")'
            return ""

        return None

    # -------------------------------------------------------------------------
    # Scoring criteria validation
    # -------------------------------------------------------------------------

    def get_scoring_columns(self) -> list[str]:
        """
        Dynamically build the scoring column list from configured dimensions.

        For each dimension, generates three columns (UWS, Weight, WS)
        plus a Final Accession Score column. Falls back to the hardcoded
        SCORING_COLUMNS list if no criteria are configured in data.yml.
        """
        criteria = self.config.get_data("scoring_criteria", default={})
        dimensions = criteria.get("dimensions", {})

        if not dimensions:
            return list(SCORING_COLUMNS)

        columns = []
        for dim in dimensions.values():
            label = dim.get("label", "Unknown")
            columns.append(f"{label} (UWS)")
            columns.append(f"{label} (Weight)")
            columns.append(f"{label} (WS)")
        columns.append("Final Accession Score")
        return columns

    def validate_scoring_criteria(self) -> list[str]:
        """
        Deprecated shim — delegates to ConfigManager.validate_scoring_criteria.

        This method previously lived here, but it validates config data
        (not spreadsheet structure), so it moved to ConfigManager. Kept
        as a shim so any external callers keep working.
        """
        return self.config.validate_scoring_criteria()

    def validate_processing_queue(self) -> list[str]:
        """
        Deprecated shim — delegates to ConfigManager.validate_processing_queue.

        Passes the current expected column set so grouping-field validation
        still works. This method previously lived here but validates config
        data (not spreadsheet structure), so it moved to ConfigManager.
        """
        return self.config.validate_processing_queue(
            valid_columns=set(self.get_expected_columns())
        )
