"""
Configuration Manager

Handles parsing, validating, and providing access to three files:
  - config.yml: User-facing settings organized in three tiers
  - credentials.yml: Sensitive credentials (never committed to git)
  - data.yml: Wizard-managed data (keyword lists, extent types, etc.)

Provides default values for optional settings and validates that
required settings are present and sensible.
"""

from pathlib import Path
from typing import Any, Optional

import yaml

from sync.utils import validate_config_value


# Default configuration values for config.yml
CONFIG_DEFAULTS: dict[str, Any] = {
    # Tier 1 — Essential
    "archivesspace": {
        "base_url": "",
        "repository_id": 2,
    },
    "output": {
        "format": "excel",
        "spreadsheet_name": "Accession Data and Scores",
    },
    "excel": {
        "target_directory": "",
    },
    "google_sheets": {
        "folder_id": "",
        "spreadsheet_url": "",
        "sharing": [],
    },
    # Tier 2 — Common
    "agents": {
        "donor_role": "source",
    },
    "scheduling": {
        "frequency": "weekly",
        "time": "20:00",
        "buffer_minutes": 60,
    },
    "notifications": {
        "recipient_email": "",
        "format": "plain",
        "digest_mode": False,
    },
    "logging": {
        "level": "standard",
        "directory": "",
        "consolidation_frequency": "weekly",
    },
    "cache": {
        "directory": "",
    },
    "preview": {
        "directory": "",
        "review_timeout_hours": 72,
        "retention": "until_next_run",
    },
    # Tier 3 — Advanced
    "throttling": {
        "archivesspace": 0.5,
        "google_sheets": 1.0,
        "google_drive": 0.5,
        "batch_mode": True,
    },
    "retry": {
        "max_retries": 5,
        "file_lock_retries": 5,
        "file_lock_interval": 60,
    },
    "matching": {
        "fuzzy_enabled": False,
        "fuzzy_threshold": 85,
    },
    "subject_descriptors": {
        "num_columns": 9,
    },
    "logging_advanced": {
        "grace_period_days": 3,
        "retention": "indefinite",
        "archive_review_window_days": 30,
        "storage_warning_threshold_mb": 1024,
    },
    "ui": {
        "show_confirmations": True,
        "tour_completed": False,
    },
}

# Default data values for data.yml
DATA_DEFAULTS: dict[str, Any] = {
    "extent_types": {},
    "format_keywords": {
        "Architectural Drawing(s)": ["architectural drawing", "blueprint", "floor plan"],
        "Artifact(s)": ["artifact", "object", "memorabilia"],
        "Artwork": ["artwork", "painting", "print", "drawing", "sketch"],
        "Audio and/or Visual Recording(s)": [
            "audio", "video", "recording", "cassette", "reel-to-reel",
        ],
        "Botanical Specimen(s)": ["botanical", "plant", "herbarium"],
        "Film (negative, slide, or motion picture)": [
            "film", "negative", "slide", "motion picture", "nitrate", "acetate",
        ],
        "Glass Material(s)": ["glass", "plate negative", "lantern slide"],
        "Photographic Material(s)": [
            "photograph", "photo", "daguerreotype", "tintype", "carte de visite",
        ],
        "Scrapbook(s)": ["scrapbook"],
        "Technical Drawing(s) and Schematic(s)": [
            "technical drawing", "schematic", "diagram", "engineering drawing",
        ],
        "Textile(s)": ["textile", "fabric", "quilt", "clothing", "garment"],
        "Vellum and Parchment": ["vellum", "parchment"],
        "Volume(s)": ["volume", "bound", "ledger", "journal", "diary"],
        "Oversize Material?": ["oversize", "oversized", "flat file"],
    },
    "removable_media_keywords": [
        "floppy disk", "floppy", "3.5 inch disk", "5.25 inch disk", "diskette",
        "CD", "CD-ROM", "CD-R", "CD-RW", "DVD", "DVD-ROM", "DVD-R", "Blu-ray",
        "hard drive", "external hard drive", "flash drive", "USB", "thumb drive",
        "zip disk", "zip drive", "tape", "DAT", "LTO", "data tape",
        "magnetic tape", "memory card", "SD card", "CompactFlash",
        "MiniDisc", "Jaz disk", "SyQuest",
    ],
    "issue_scan_configs": {},
    "documentation_use_issues_options": [
        "Material is actually or possibly on deposit.",
        "Material may or does lack a gift agreement.",
        "Other issues.",
    ],
    "processing_project_types": [
        "Accessioning as Processing",
        "Backlog",
        "Requested",
        "Cataloging",
        "Unknown",
    ],
    "completion_triggers": [],
    # -------------------------------------------------------------------------
    # Scoring Criteria
    # -------------------------------------------------------------------------
    # Defines the weighted scoring model used to prioritize accessions.
    # Each dimension has a label, category (quantitative or strategic),
    # a weight, and either thresholds (for range-based scoring) or
    # mappings (for value-based scoring). Users can customize the default
    # dimensions and define entirely new ones through the wizard or menu.
    #
    # The program generates the scoring criteria spreadsheet/sheet from
    # these values — data.yml is the sole source of truth.
    #
    # Storage settings:
    # - excel_scoring_mode: "embedded_sheet" (default) keeps the scoring
    #   criteria on a sheet inside the main workbook, or "linked_workbook"
    #   which stores it in a separate Excel file referenced by formulas.
    # - scoring_workbook_path: Used only when excel_scoring_mode is
    #   "linked_workbook". The absolute path to the separate workbook.
    "scoring_criteria": {
        "excel_scoring_mode": "embedded_sheet",
        "scoring_workbook_path": "",
        "dimensions": {
            "time_in_backlog": {
                "label": "Time in Backlog",
                "category": "quantitative",
                "weight": 0.5,
                "source_field": "accession_date",
                "scoring_type": "date_range",
                "thresholds": [
                    {"label": "Less than 3 Years", "score": 1, "min_years": 0, "max_years": 3},
                    {"label": "3 - 5 Years", "score": 2, "min_years": 3, "max_years": 5},
                    {"label": "6 - 8 Years", "score": 3, "min_years": 5, "max_years": 8},
                    {"label": "9 or More Years", "score": 4, "min_years": 8, "max_years": None},
                ],
            },
            "priority": {
                "label": "Priority",
                "category": "quantitative",
                "weight": 0.2,
                "source_field": "processing_priority",
                "scoring_type": "value_map",
                "mappings": [
                    {"value": "Not specified", "score": 1},
                    {"value": "Low", "score": 2},
                    {"value": "Medium", "score": 3},
                    {"value": "High", "score": 4},
                ],
            },
            "subject_descriptors": {
                "label": "Subject Descriptors",
                "category": "strategic",
                "weight": 0.3,
                "source_field": "subject_descriptor_count",
                "scoring_type": "count_range",
                "thresholds": [
                    {"label": "1", "score": 1, "min_count": 1, "max_count": 1},
                    {"label": "2", "score": 2, "min_count": 2, "max_count": 2},
                    {"label": "3", "score": 3, "min_count": 3, "max_count": 3},
                    {"label": "4+", "score": 4, "min_count": 4, "max_count": None},
                ],
            },
        },
    },
    # -------------------------------------------------------------------------
    # Processing Queue Configuration
    # -------------------------------------------------------------------------
    # Defines how accessions are grouped into "projects" for prioritized
    # processing. The program generates one or more "Processing Queue" sheets
    # that group accessions by a configurable field (default: Donor Name)
    # and sort them by average final score per group.
    #
    # Each queue has:
    #   - name: Display name (becomes the sheet title)
    #   - status_values: List of Accession Status values to include
    #   - grouping_field: Spreadsheet column to group by (default: "Donor Name")
    #   - view_mode: "indented" (sub-rows) or "flat" (one row per accession)
    #
    # Multiple queues can be configured (e.g., one for general backlog,
    # one for requested accessions). The default is a single queue.
    "processing_queue": {
        "queues": [
            {
                "name": "General Backlog",
                "status_values": ["Backlog - General"],
                "grouping_field": "Donor Name",
                "view_mode": "indented",
            },
        ],
    },
    # -------------------------------------------------------------------------
    # Backlog At a Glance Status Groups
    # -------------------------------------------------------------------------
    # Defines how accession statuses are grouped in the "Backlog At a Glance"
    # summary dashboard. Each group rolls up multiple status values into a
    # single row with accession counts and extent totals.
    "backlog_at_a_glance": {
        "status_groups": [
            {
                "label": "General Backlog",
                "status_values": ["Backlog - General"],
                "show_project_count": True,
            },
            {
                "label": "Cataloging (Not Part of the Backlog)",
                "status_values": ["Cataloging"],
                "show_project_count": False,
            },
            {
                "label": "Requested",
                "status_values": ["Backlog - Requested"],
                "show_project_count": False,
            },
            {
                "label": "In-Progress",
                "status_values": ["In-Progress"],
                "show_project_count": False,
            },
        ],
    },
}


class ConfigError(Exception):
    """Raised when configuration is invalid or incomplete."""
    pass


class ConfigManager:
    """
    Manages loading, validating, and accessing configuration settings
    from config.yml, credentials.yml, and data.yml.
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        credentials_path: Optional[str] = None,
        data_path: Optional[str] = None,
    ):
        """
        Initialize the ConfigManager.

        Args:
            config_path: Path to config.yml. If None, searches standard locations.
            credentials_path: Path to credentials.yml. If None, searches alongside config.
            data_path: Path to data.yml. If None, searches alongside config.
        """
        self.project_root = self._find_project_root()
        self.config_path = self._resolve_path(config_path, "config.yml")
        self.credentials_path = self._resolve_path(credentials_path, "credentials.yml")
        self.data_path = self._resolve_path(data_path, "data.yml")

        self._config: dict = {}
        self._credentials: dict = {}
        self._data: dict = {}
        self._loaded = False

    def _find_project_root(self) -> Path:
        """Find the project root directory by looking for pyproject.toml."""
        current = Path(__file__).resolve().parent.parent
        if (current / "pyproject.toml").exists():
            return current
        cwd = Path.cwd()
        if (cwd / "pyproject.toml").exists():
            return cwd
        return cwd

    def _resolve_path(self, explicit_path: Optional[str], filename: str) -> Path:
        """Resolve the path to a configuration file, checking multiple locations."""
        if explicit_path:
            return Path(explicit_path).resolve()

        cwd_path = Path.cwd() / filename
        if cwd_path.exists():
            return cwd_path

        root_path = self.project_root / filename
        if root_path.exists():
            return root_path

        return root_path

    # -------------------------------------------------------------------------
    # File existence checks
    # -------------------------------------------------------------------------

    def config_exists(self) -> bool:
        """Check whether the config file exists."""
        return self.config_path.exists()

    def credentials_exist(self) -> bool:
        """Check whether the credentials file exists."""
        return self.credentials_path.exists()

    def data_exists(self) -> bool:
        """Check whether the data file exists."""
        return self.data_path.exists()

    # -------------------------------------------------------------------------
    # Loading
    # -------------------------------------------------------------------------

    def load(self) -> None:
        """
        Load and validate all configuration files.

        Raises:
            ConfigError: If config.yml is missing or contains invalid YAML.
        """
        self._config = self._load_yaml(self.config_path, required=True)
        self._apply_defaults(self._config, CONFIG_DEFAULTS)

        if self.credentials_path.exists():
            self._credentials = self._load_yaml(self.credentials_path, required=False)
        else:
            self._credentials = {}

        if self.data_path.exists():
            self._data = self._load_yaml(self.data_path, required=False)
        else:
            self._data = {}

        # --- One-time migration: scoring settings moved from config.yml to data.yml ---
        # Earlier versions stored excel_scoring_mode and scoring_workbook_path
        # under `scoring` in config.yml. They now live under `scoring_criteria`
        # in data.yml alongside the dimensions. Migrate in-memory so existing
        # installations continue to work; the migrated values will be persisted
        # on the next save_config()/save_data() call.
        legacy_scoring = self._config.pop("scoring", None)
        if isinstance(legacy_scoring, dict):
            self._data.setdefault("scoring_criteria", {})
            for key in ("excel_scoring_mode", "scoring_workbook_path"):
                if key in legacy_scoring and key not in self._data["scoring_criteria"]:
                    self._data["scoring_criteria"][key] = legacy_scoring[key]

        self._apply_defaults(self._data, DATA_DEFAULTS)

        self._loaded = True

    def _load_yaml(self, path: Path, required: bool = True) -> dict:
        """
        Load a YAML file and return its contents as a dictionary.

        Raises:
            ConfigError: If the file is missing (when required) or malformed.
        """
        if not path.exists():
            if required:
                raise ConfigError(
                    f"Configuration file not found: {path}\n"
                    f"Run the setup wizard or copy the template to create this file."
                )
            return {}

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                return data if isinstance(data, dict) else {}
        except yaml.YAMLError as e:
            if hasattr(e, "problem_mark"):
                mark = e.problem_mark
                raise ConfigError(
                    f"Invalid YAML syntax in {path.name} at line {mark.line + 1}, "
                    f"column {mark.column + 1}:\n"
                    f"  {e.problem}\n"
                    f"Please check your indentation and formatting."
                )
            raise ConfigError(f"Invalid YAML syntax in {path.name}: {e}")

    def _apply_defaults(self, target: dict, defaults: dict, _path: str = "") -> None:
        """
        Apply default values for any settings not present, modifying target in place.

        Most dict values are deep-merged so users can override individual sub-keys
        while keeping other defaults. However, certain collection-type keys are
        treated as opaque — if the user provides any value, it replaces the default
        entirely rather than merging. This is essential for collections like
        scoring dimensions, where merging would prevent users from removing
        default entries (a user dimension dict like {'a': ..., 'b': ...} would
        otherwise be merged with the three default dimensions, resulting in five).
        """
        # Keys whose dict values should be replaced wholesale, not merged.
        # (Lists are already replaced wholesale by default — only dict values
        # need explicit no-merge handling.)
        NO_MERGE_KEYS = {"scoring_criteria.dimensions"}

        for key, value in defaults.items():
            full_path = f"{_path}.{key}" if _path else key
            if key not in target:
                target[key] = value
            elif (
                isinstance(value, dict)
                and isinstance(target.get(key), dict)
                and full_path not in NO_MERGE_KEYS
            ):
                self._apply_defaults(target[key], value, full_path)
            # If full_path is in NO_MERGE_KEYS and target already has a value,
            # leave target's value untouched (user's config takes priority)

    # -------------------------------------------------------------------------
    # Accessors
    # -------------------------------------------------------------------------

    def get(self, *keys: str, default: Any = None) -> Any:
        """
        Get a configuration value from config.yml or data.yml.
        Checks config.yml first, then falls back to data.yml.

        Example:
            config.get("archivesspace", "base_url")
            config.get("format_keywords")
        """
        if not self._loaded:
            self.load()

        # Try config first
        result = self._traverse(self._config, keys)
        if result is not None:
            return result

        # Try data
        result = self._traverse(self._data, keys)
        if result is not None:
            return result

        return default

    def get_credential(self, *keys: str, default: Any = None) -> Any:
        """Get a credential value from credentials.yml."""
        if not self._loaded:
            self.load()
        result = self._traverse(self._credentials, keys)
        return result if result is not None else default

    def get_data(self, *keys: str, default: Any = None) -> Any:
        """Get a value specifically from data.yml."""
        if not self._loaded:
            self.load()
        result = self._traverse(self._data, keys)
        return result if result is not None else default

    def _traverse(self, source: dict, keys: tuple[str, ...]) -> Any:
        """Traverse a nested dictionary using a sequence of keys."""
        current = source
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    # -------------------------------------------------------------------------
    # Setters
    # -------------------------------------------------------------------------

    def set(self, *keys: str, value: Any) -> None:
        """Set a value in config.yml (in memory)."""
        if not keys:
            return
        current = self._config
        for key in keys[:-1]:
            if key not in current or not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value

    def set_data(self, *keys: str, value: Any) -> None:
        """Set a value in data.yml (in memory)."""
        if not keys:
            return
        current = self._data
        for key in keys[:-1]:
            if key not in current or not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value

    # -------------------------------------------------------------------------
    # Saving
    # -------------------------------------------------------------------------

    def save_config(self) -> None:
        """Save the current configuration to config.yml."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w", encoding="utf-8") as f:
            yaml.dump(self._config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    def save_data(self) -> None:
        """Save the current data to data.yml."""
        self.data_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.data_path, "w", encoding="utf-8") as f:
            # Add header comment
            f.write(
                "# =============================================================================\n"
                "# archivesspace-accession-sync Data File\n"
                "# =============================================================================\n"
                "# This file is managed by the setup wizard and interactive menu.\n"
                "# Direct edits are possible for advanced users, but take care to\n"
                "# preserve proper YAML formatting to avoid syntax errors.\n"
                "# =============================================================================\n\n"
            )
            yaml.dump(self._data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    def save_credentials(self, credentials: dict) -> None:
        """Save credentials to credentials.yml."""
        self.credentials_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.credentials_path, "w", encoding="utf-8") as f:
            yaml.dump(credentials, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        self._credentials = credentials

    # -------------------------------------------------------------------------
    # Validation
    # -------------------------------------------------------------------------

    def validate(self) -> list[str]:
        """
        Validate the current configuration and return a list of issues.

        Returns:
            List of validation error messages. Empty list means valid.
        """
        if not self._loaded:
            self.load()

        issues: list[str] = []

        # Required ArchivesSpace settings
        base_url = self.get("archivesspace", "base_url")
        if not base_url:
            issues.append("ArchivesSpace base URL is not configured.")

        repo_id = self.get("archivesspace", "repository_id")
        if repo_id is not None:
            valid, msg = validate_config_value(
                repo_id, int, min_val=1, field_name="archivesspace.repository_id"
            )
            if not valid:
                issues.append(msg)

        # Output format
        output_format = self.get("output", "format")
        valid, msg = validate_config_value(
            output_format, str, allowed_values=["excel", "google_sheets"],
            field_name="output.format",
        )
        if not valid:
            issues.append(msg)

        # Format-specific settings
        if output_format == "excel":
            target_dir = self.get("excel", "target_directory")
            if not target_dir:
                issues.append("Excel target directory is not configured.")
            elif not Path(target_dir).exists():
                issues.append(f"Excel target directory does not exist: {target_dir}")

        if output_format == "google_sheets":
            auth_method = self.get_credential("google", "auth_method")
            if auth_method == "service_account":
                key_path = self.get_credential("google", "service_account_key_path")
                if not key_path or not Path(key_path).exists():
                    issues.append("Google service account key file not found.")
            elif auth_method == "oauth":
                if not self.get_credential("google", "oauth_client_id"):
                    issues.append("Google OAuth client ID is not configured.")

        # ArchivesSpace credentials
        if not self.get_credential("archivesspace", "username"):
            issues.append("ArchivesSpace username not found in credentials.yml.")
        if not self.get_credential("archivesspace", "password"):
            issues.append("ArchivesSpace password not found in credentials.yml.")

        # Directories
        for dir_name, keys in [
            ("Logging", ("logging", "directory")),
            ("Cache", ("cache", "directory")),
        ]:
            if not self.get(*keys):
                issues.append(f"{dir_name} directory is not configured.")

        # Validate numeric ranges
        numeric_validations = [
            (("throttling", "archivesspace"), float, 0.0, 60.0),
            (("throttling", "google_sheets"), float, 0.0, 60.0),
            (("throttling", "google_drive"), float, 0.0, 60.0),
            (("retry", "max_retries"), int, 0, 20),
            (("retry", "file_lock_retries"), int, 0, 20),
            (("retry", "file_lock_interval"), int, 1, 600),
            (("matching", "fuzzy_threshold"), int, 0, 100),
            (("subject_descriptors", "num_columns"), int, 1, 50),
            (("scheduling", "buffer_minutes"), int, 0, 1440),
            (("preview", "review_timeout_hours"), int, 1, 720),
        ]
        for keys, expected_type, min_val, max_val in numeric_validations:
            val = self.get(*keys)
            if val is not None:
                valid, msg = validate_config_value(
                    val, expected_type, min_val=min_val, max_val=max_val,
                    field_name=".".join(keys),
                )
                if not valid:
                    issues.append(msg)

        # Validate time format
        time_str = self.get("scheduling", "time")
        if time_str:
            try:
                from sync.utils import parse_time_string
                parse_time_string(time_str)
            except ValueError as e:
                issues.append(str(e))

        return issues

    def validate_scoring_criteria(self) -> list[str]:
        """
        Validate the scoring criteria configuration in data.yml.

        Checks for: at least one dimension defined, valid labels and
        weights, weights sum to 1.0 (within tolerance), valid thresholds
        or mappings for each dimension's scoring type, and positive
        integer scores.

        Returns:
            List of validation issue messages. Empty list means valid.
        """
        if not self._loaded:
            self.load()

        issues: list[str] = []

        criteria = self.get_data("scoring_criteria", default={})
        if not criteria:
            issues.append("Scoring criteria not configured in data.yml.")
            return issues

        dimensions = criteria.get("dimensions", {})
        if not dimensions:
            issues.append("No scoring dimensions defined.")
            return issues

        total_weight = 0.0
        for key, dim in dimensions.items():
            label = dim.get("label", key)

            if not dim.get("label"):
                issues.append(f"Dimension '{key}' is missing a label.")

            weight = dim.get("weight", 0)
            if not isinstance(weight, (int, float)) or weight <= 0:
                issues.append(f"Dimension '{label}' has invalid weight: {weight}")
            total_weight += float(weight)

            scoring_type = dim.get("scoring_type", "")
            if scoring_type not in ("date_range", "value_map", "count_range"):
                issues.append(
                    f"Dimension '{label}' has unknown scoring_type: '{scoring_type}'"
                )
                continue

            if scoring_type in ("date_range", "count_range"):
                thresholds = dim.get("thresholds", [])
                if not thresholds:
                    issues.append(f"Dimension '{label}' has no thresholds defined.")
                else:
                    scores = [t.get("score") for t in thresholds]
                    if any(not isinstance(s, (int, float)) or s < 1 for s in scores):
                        issues.append(
                            f"Dimension '{label}' has invalid scores in thresholds."
                        )

            elif scoring_type == "value_map":
                mappings = dim.get("mappings", [])
                if not mappings:
                    issues.append(f"Dimension '{label}' has no mappings defined.")
                else:
                    for m in mappings:
                        if not m.get("value"):
                            issues.append(
                                f"Dimension '{label}' has a mapping without a value."
                            )

        if abs(total_weight - 1.0) > 0.01:
            issues.append(
                f"Dimension weights sum to {total_weight:.4f}, "
                f"but must sum to 1.0."
            )

        return issues

    def validate_processing_queue(
        self, valid_columns: "Optional[set[str]]" = None
    ) -> list[str]:
        """
        Validate the processing queue configuration in data.yml.

        Checks for: each queue has a unique non-empty name, at least one
        status value, a recognized grouping field (if valid_columns is
        provided), and a valid view mode.

        Args:
            valid_columns: If provided, each queue's grouping_field is
                checked against this set. If None, grouping field names
                are checked only for non-emptiness.

        Returns:
            List of validation issue messages. Empty list means valid.
        """
        if not self._loaded:
            self.load()

        issues: list[str] = []

        pq = self.get_data("processing_queue", default={})
        queues = pq.get("queues", [])

        if not queues:
            # Not having queues is acceptable — the sheets simply won't be created
            return issues

        seen_names: set[str] = set()
        for i, queue in enumerate(queues, 1):
            name = queue.get("name", "").strip()
            if not name:
                issues.append(f"Queue {i} is missing a name.")
            elif name in seen_names:
                issues.append(f"Queue name '{name}' is duplicated.")
            else:
                seen_names.add(name)

            statuses = queue.get("status_values", [])
            if not statuses:
                issues.append(f"Queue '{name or i}' has no status values defined.")

            grouping = queue.get("grouping_field", "")
            if not grouping:
                issues.append(
                    f"Queue '{name or i}' is missing a grouping field."
                )
            elif valid_columns is not None and grouping not in valid_columns:
                issues.append(
                    f"Queue '{name or i}' uses grouping field '{grouping}' "
                    f"which is not a known column name."
                )

            view_mode = queue.get("view_mode", "indented")
            if view_mode not in ("indented", "flat"):
                issues.append(
                    f"Queue '{name or i}' has invalid view_mode '{view_mode}' "
                    f"(must be 'indented' or 'flat')."
                )

        return issues

    # -------------------------------------------------------------------------
    # Convenience helpers
    # -------------------------------------------------------------------------

    def get_output_format(self) -> str:
        """Return the configured output format ('excel' or 'google_sheets')."""
        return self.get("output", "format", default="excel")

    def get_spreadsheet_name(self) -> str:
        """Return the configured spreadsheet name."""
        return self.get("output", "spreadsheet_name", default="Accession Data and Scores")

    def get_base_url(self) -> str:
        """Return the configured ArchivesSpace base URL."""
        return self.get("archivesspace", "base_url", default="")

    def get_repository_id(self) -> int:
        """Return the configured ArchivesSpace repository ID."""
        return self.get("archivesspace", "repository_id", default=2)

    def get_repository_uri(self) -> str:
        """Return the full repository URI path."""
        return f"/repositories/{self.get_repository_id()}"

    def is_email_configured(self) -> bool:
        """Check whether email notifications are fully configured."""
        recipient = self.get("notifications", "recipient_email")
        smtp_server = self.get_credential("smtp", "server")
        return bool(recipient and smtp_server)

    def is_digest_mode(self) -> bool:
        """Check whether digest mode is enabled for notifications."""
        return self.get("notifications", "digest_mode", default=False)

    def show_confirmations(self) -> bool:
        """Check whether UI confirmations should be shown."""
        return self.get("ui", "show_confirmations", default=True)

    def tour_completed(self) -> bool:
        """Check whether the guided tour has been completed."""
        return self.get("ui", "tour_completed", default=False)

    def ensure_directories(self) -> None:
        """
        Create all configured directories if they don't exist.
        This is a safe self-repair operation for missing directories.
        """
        directories = [
            self.get("logging", "directory"),
            self.get("cache", "directory"),
            self.get("preview", "directory"),
        ]

        if self.get_output_format() == "excel":
            directories.append(self.get("excel", "target_directory"))

        for dir_path in directories:
            if dir_path:
                Path(dir_path).mkdir(parents=True, exist_ok=True)
