"""
Configuration Manager

Handles parsing, validating, and providing access to the config.yml
and credentials.yml files. Provides default values for optional settings
and validates that required settings are present.
"""

import os
import sys
from pathlib import Path
from typing import Any, Optional

import yaml


# Default configuration values
DEFAULTS = {
    "archivesspace": {
        "base_url": "https://sandbox.archivesspace.org/api",
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
    "agents": {
        "donor_role": "source",
    },
    "extent_types": {},
    "format_keywords": {},
    "removable_media_keywords": [],
    "matching": {
        "fuzzy_enabled": False,
        "fuzzy_threshold": 85,
    },
    "subject_descriptors": {
        "num_columns": 9,
    },
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
    "logging": {
        "level": "standard",
        "directory": "",
        "consolidation_frequency": "weekly",
        "grace_period_days": 3,
        "retention": "indefinite",
        "archive_review_window_days": 30,
        "storage_warning_threshold_mb": 1024,
    },
    "cache": {
        "directory": "",
    },
    "preview": {
        "directory": "",
    },
    "scheduling": {
        "frequency": "weekly",
        "time": "20:00",
    },
    "notifications": {
        "recipient_email": "",
    },
    "scoring": {
        "excel_scoring_mode": "linked_workbook",
        "scoring_workbook_path": "",
    },
}


class ConfigError(Exception):
    """Raised when configuration is invalid or incomplete."""

    pass


class ConfigManager:
    """
    Manages loading, validating, and accessing configuration settings
    from the config.yml and credentials.yml files.
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        credentials_path: Optional[str] = None,
    ):
        """
        Initialize the ConfigManager.

        Args:
            config_path: Path to the config.yml file. If None, looks in the
                         current directory and the project root.
            credentials_path: Path to the credentials.yml file. If None, looks
                              in the same directory as config.yml.
        """
        self.project_root = self._find_project_root()
        self.config_path = self._resolve_path(config_path, "config.yml")
        self.credentials_path = self._resolve_path(credentials_path, "credentials.yml")

        self._config: dict = {}
        self._credentials: dict = {}
        self._loaded = False

    def _find_project_root(self) -> Path:
        """Find the project root directory by looking for pyproject.toml."""
        current = Path(__file__).resolve().parent.parent
        if (current / "pyproject.toml").exists():
            return current
        # Fall back to current working directory
        cwd = Path.cwd()
        if (cwd / "pyproject.toml").exists():
            return cwd
        return cwd

    def _resolve_path(self, explicit_path: Optional[str], filename: str) -> Path:
        """
        Resolve the path to a configuration file.

        Checks, in order:
            1. The explicitly provided path
            2. The current working directory
            3. The project root directory
        """
        if explicit_path:
            return Path(explicit_path).resolve()

        cwd_path = Path.cwd() / filename
        if cwd_path.exists():
            return cwd_path

        root_path = self.project_root / filename
        if root_path.exists():
            return root_path

        # Return the project root path even if it doesn't exist yet
        # (the setup wizard will create it)
        return root_path

    def config_exists(self) -> bool:
        """Check whether the config file exists."""
        return self.config_path.exists()

    def credentials_exist(self) -> bool:
        """Check whether the credentials file exists."""
        return self.credentials_path.exists()

    def load(self) -> None:
        """
        Load and validate configuration and credentials files.

        Raises:
            ConfigError: If a required file is missing or contains invalid YAML.
        """
        self._config = self._load_yaml(self.config_path, required=True)
        self._apply_defaults()

        if self.credentials_path.exists():
            self._credentials = self._load_yaml(self.credentials_path, required=False)
        else:
            self._credentials = {}

        self._loaded = True

    def _load_yaml(self, path: Path, required: bool = True) -> dict:
        """
        Load a YAML file and return its contents as a dictionary.

        Args:
            path: Path to the YAML file.
            required: If True, raise ConfigError when the file is missing.

        Returns:
            Dictionary of the YAML contents, or empty dict if file is
            missing and not required.

        Raises:
            ConfigError: If the file is missing (when required) or contains
                         invalid YAML syntax.
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
            # Provide a user-friendly error message with line information
            if hasattr(e, "problem_mark"):
                mark = e.problem_mark
                raise ConfigError(
                    f"Invalid YAML syntax in {path.name} at line {mark.line + 1}, "
                    f"column {mark.column + 1}:\n"
                    f"  {e.problem}\n"
                    f"Please check your indentation and formatting."
                )
            raise ConfigError(f"Invalid YAML syntax in {path.name}: {e}")

    def _apply_defaults(self) -> None:
        """Apply default values for any settings not present in the config file."""
        self._config = self._deep_merge(DEFAULTS, self._config)

    def _deep_merge(self, defaults: dict, overrides: dict) -> dict:
        """
        Deep merge two dictionaries. Values in overrides take precedence.

        Args:
            defaults: Dictionary of default values.
            overrides: Dictionary of user-provided values.

        Returns:
            Merged dictionary.
        """
        result = defaults.copy()
        for key, value in overrides.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def get(self, *keys: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot-notation-style keys.

        Example:
            config.get("archivesspace", "base_url")
            config.get("throttling", "archivesspace")

        Args:
            *keys: Sequence of keys to traverse the config hierarchy.
            default: Value to return if the key path doesn't exist.

        Returns:
            The configuration value, or the default.
        """
        if not self._loaded:
            self.load()

        current = self._config
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current

    def get_credential(self, *keys: str, default: Any = None) -> Any:
        """
        Get a credential value using dot-notation-style keys.

        Example:
            config.get_credential("archivesspace", "username")

        Args:
            *keys: Sequence of keys to traverse the credentials hierarchy.
            default: Value to return if the key path doesn't exist.

        Returns:
            The credential value, or the default.
        """
        if not self._loaded:
            self.load()

        current = self._credentials
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current

    def set(self, *keys: str, value: Any) -> None:
        """
        Set a configuration value and save to disk.

        Args:
            *keys: Sequence of keys to traverse the config hierarchy.
            value: The value to set.
        """
        if not keys:
            return

        current = self._config
        for key in keys[:-1]:
            if key not in current or not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value

    def save_config(self) -> None:
        """Save the current configuration to the config.yml file."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w", encoding="utf-8") as f:
            yaml.dump(
                self._config,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )

    def save_credentials(self, credentials: dict) -> None:
        """
        Save credentials to the credentials.yml file.

        Args:
            credentials: Dictionary of credentials to save.
        """
        self.credentials_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.credentials_path, "w", encoding="utf-8") as f:
            yaml.dump(
                credentials,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )
        self._credentials = credentials

    def validate(self) -> list[str]:
        """
        Validate the current configuration and return a list of issues.

        Returns:
            List of validation error messages. Empty list means valid.
        """
        if not self._loaded:
            self.load()

        issues = []

        # Check required ArchivesSpace settings
        base_url = self.get("archivesspace", "base_url")
        if not base_url:
            issues.append("ArchivesSpace base URL is not configured.")

        repo_id = self.get("archivesspace", "repository_id")
        if not repo_id:
            issues.append("ArchivesSpace repository ID is not configured.")

        # Check output format
        output_format = self.get("output", "format")
        if output_format not in ("excel", "google_sheets"):
            issues.append(
                f'Output format must be "excel" or "google_sheets", got: "{output_format}"'
            )

        # Check format-specific settings
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
                    issues.append(
                        "Google service account key file not found. "
                        "Check the path in credentials.yml."
                    )
            elif auth_method == "oauth":
                client_id = self.get_credential("google", "oauth_client_id")
                if not client_id:
                    issues.append("Google OAuth client ID is not configured.")

        # Check ArchivesSpace credentials
        as_user = self.get_credential("archivesspace", "username")
        as_pass = self.get_credential("archivesspace", "password")
        if not as_user or not as_pass:
            issues.append(
                "ArchivesSpace username and/or password not found in credentials.yml."
            )

        # Check directory configurations
        log_dir = self.get("logging", "directory")
        if not log_dir:
            issues.append("Logging directory is not configured.")

        cache_dir = self.get("cache", "directory")
        if not cache_dir:
            issues.append("Cache directory is not configured.")

        return issues

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
        """Check whether email notifications are configured."""
        recipient = self.get("notifications", "recipient_email")
        smtp_server = self.get_credential("smtp", "server")
        return bool(recipient and smtp_server)

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
