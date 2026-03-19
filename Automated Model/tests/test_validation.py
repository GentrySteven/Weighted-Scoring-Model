"""Tests for the SpreadsheetValidator module."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.validation import SpreadsheetValidator, REQUIRED_COLUMNS, SYNC_COLUMNS


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def config(temp_dir):
    config_data = {
        "archivesspace": {"base_url": "https://test.archivesspace.org/api", "repository_id": 2},
        "output": {"format": "excel", "spreadsheet_name": "Test"},
        "excel": {"target_directory": str(temp_dir)},
        "logging": {"directory": str(temp_dir / "logs")},
        "cache": {"directory": str(temp_dir / "cache")},
        "preview": {"directory": str(temp_dir / "preview")},
        "format_keywords": {"Photographic Material(s)": ["photograph"]},
        "subject_descriptors": {"num_columns": 3},
    }
    config_path = temp_dir / "config.yml"
    with open(config_path, "w") as f:
        yaml.dump(config_data, f)

    creds_path = temp_dir / "credentials.yml"
    with open(creds_path, "w") as f:
        yaml.dump({"archivesspace": {"username": "t", "password": "t"}}, f)

    cm = ConfigManager(config_path=str(config_path), credentials_path=str(creds_path))
    cm.load()
    return cm


@pytest.fixture
def logger(config):
    return LoggingManager(config)


@pytest.fixture
def validator(config, logger):
    return SpreadsheetValidator(config, logger)


class TestSpreadsheetValidator:
    def test_get_expected_columns_includes_required(self, validator):
        """Test that expected columns include all required columns."""
        expected = validator.get_expected_columns()
        for col in REQUIRED_COLUMNS:
            assert col in expected

    def test_get_expected_columns_includes_sync(self, validator):
        """Test that expected columns include sync columns."""
        expected = validator.get_expected_columns()
        for col in SYNC_COLUMNS:
            assert col in expected

    def test_validate_all_present(self, validator):
        """Test validation passes when all columns are present."""
        headers = validator.get_expected_columns()
        result = validator.validate(headers)
        assert result.is_valid

    def test_validate_missing_column(self, validator):
        """Test validation fails when a required column is missing."""
        headers = validator.get_expected_columns()
        headers.remove("Accession ID")
        result = validator.validate(headers)
        assert not result.is_valid
        assert "Accession ID" in result.missing_columns

    def test_validate_extra_columns_accepted(self, validator):
        """Test that extra user-added columns are accepted."""
        headers = validator.get_expected_columns()
        headers.append("My Custom Column")
        result = validator.validate(headers)
        assert result.is_valid
        assert "My Custom Column" in result.extra_columns

    def test_validate_rearranged_columns(self, validator):
        """Test that rearranged columns are accepted."""
        headers = validator.get_expected_columns()
        headers.reverse()
        result = validator.validate(headers)
        assert result.is_valid

    def test_is_protected_column(self, validator):
        """Test protected column detection."""
        assert validator.is_protected_column("Notes")
        assert validator.is_protected_column("Documentation and Use Issues")
        assert validator.is_protected_column("Final Accession Score")
        assert not validator.is_protected_column("Accession Date")

    def test_is_sync_column(self, validator):
        """Test sync column detection."""
        assert validator.is_sync_column("[Sync] Status")
        assert validator.is_sync_column("[Sync] Accession lock_version")
        assert not validator.is_sync_column("Accession ID")

    def test_get_column_formula(self, validator):
        """Test formula generation for formula columns."""
        formula = validator.get_column_formula(
            "Base URL and Accession ID (Use for Hyperlink Only)", 2
        )
        assert formula == "=CONCAT(B2,C2)"

        formula = validator.get_column_formula("Accession Number", 5)
        assert formula == "=HYPERLINK(D5,E5)"

    def test_non_formula_column_returns_none(self, validator):
        """Test that non-formula columns return None."""
        assert validator.get_column_formula("Accession Date", 2) is None
