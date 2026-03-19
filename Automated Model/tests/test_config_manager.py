"""Tests for the ConfigManager module."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from sync.config_manager import ConfigManager, ConfigError, DEFAULTS


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_config(temp_dir):
    """Create a sample config file."""
    config_data = {
        "archivesspace": {
            "base_url": "https://test.archivesspace.org/api",
            "repository_id": 3,
        },
        "output": {
            "format": "excel",
            "spreadsheet_name": "Test Spreadsheet",
        },
        "excel": {
            "target_directory": str(temp_dir / "output"),
        },
        "logging": {
            "directory": str(temp_dir / "logs"),
        },
        "cache": {
            "directory": str(temp_dir / "cache"),
        },
    }
    config_path = temp_dir / "config.yml"
    with open(config_path, "w") as f:
        yaml.dump(config_data, f)
    return config_path


@pytest.fixture
def sample_credentials(temp_dir):
    """Create a sample credentials file."""
    creds_data = {
        "archivesspace": {
            "username": "test_user",
            "password": "test_pass",
        },
    }
    creds_path = temp_dir / "credentials.yml"
    with open(creds_path, "w") as f:
        yaml.dump(creds_data, f)
    return creds_path


class TestConfigManager:
    def test_load_valid_config(self, sample_config, sample_credentials):
        """Test loading a valid configuration file."""
        cm = ConfigManager(
            config_path=str(sample_config),
            credentials_path=str(sample_credentials),
        )
        cm.load()
        assert cm.get("archivesspace", "base_url") == "https://test.archivesspace.org/api"
        assert cm.get("archivesspace", "repository_id") == 3

    def test_defaults_applied(self, sample_config, sample_credentials):
        """Test that default values are applied for missing settings."""
        cm = ConfigManager(
            config_path=str(sample_config),
            credentials_path=str(sample_credentials),
        )
        cm.load()
        # Throttling not in config, should use defaults
        assert cm.get("throttling", "archivesspace") == 0.5
        assert cm.get("throttling", "google_sheets") == 1.0
        assert cm.get("retry", "max_retries") == 5

    def test_config_not_found(self, temp_dir):
        """Test that missing config file raises ConfigError."""
        cm = ConfigManager(config_path=str(temp_dir / "nonexistent.yml"))
        with pytest.raises(ConfigError, match="Configuration file not found"):
            cm.load()

    def test_invalid_yaml(self, temp_dir):
        """Test that invalid YAML raises ConfigError."""
        bad_yaml = temp_dir / "bad.yml"
        with open(bad_yaml, "w") as f:
            f.write("invalid: yaml: content: [broken")
        cm = ConfigManager(config_path=str(bad_yaml))
        with pytest.raises(ConfigError, match="Invalid YAML"):
            cm.load()

    def test_get_credential(self, sample_config, sample_credentials):
        """Test retrieving credential values."""
        cm = ConfigManager(
            config_path=str(sample_config),
            credentials_path=str(sample_credentials),
        )
        cm.load()
        assert cm.get_credential("archivesspace", "username") == "test_user"
        assert cm.get_credential("archivesspace", "password") == "test_pass"

    def test_get_missing_key(self, sample_config, sample_credentials):
        """Test that missing keys return the default value."""
        cm = ConfigManager(
            config_path=str(sample_config),
            credentials_path=str(sample_credentials),
        )
        cm.load()
        assert cm.get("nonexistent", "key", default="fallback") == "fallback"

    def test_set_value(self, sample_config, sample_credentials):
        """Test setting a configuration value."""
        cm = ConfigManager(
            config_path=str(sample_config),
            credentials_path=str(sample_credentials),
        )
        cm.load()
        cm.set("output", "format", value="google_sheets")
        assert cm.get("output", "format") == "google_sheets"

    def test_config_exists(self, sample_config, temp_dir):
        """Test checking for config file existence."""
        cm = ConfigManager(config_path=str(sample_config))
        assert cm.config_exists()

        cm2 = ConfigManager(config_path=str(temp_dir / "nonexistent.yml"))
        assert not cm2.config_exists()

    def test_get_output_format(self, sample_config, sample_credentials):
        """Test getting the output format."""
        cm = ConfigManager(
            config_path=str(sample_config),
            credentials_path=str(sample_credentials),
        )
        cm.load()
        assert cm.get_output_format() == "excel"

    def test_get_repository_uri(self, sample_config, sample_credentials):
        """Test getting the full repository URI."""
        cm = ConfigManager(
            config_path=str(sample_config),
            credentials_path=str(sample_credentials),
        )
        cm.load()
        assert cm.get_repository_uri() == "/repositories/3"

    def test_validate_missing_credentials(self, sample_config, temp_dir):
        """Test validation with missing credentials."""
        cm = ConfigManager(
            config_path=str(sample_config),
            credentials_path=str(temp_dir / "no_creds.yml"),
        )
        cm.load()
        issues = cm.validate()
        assert any("username" in issue.lower() or "password" in issue.lower() for issue in issues)

    def test_ensure_directories(self, sample_config, sample_credentials, temp_dir):
        """Test that ensure_directories creates missing directories."""
        cm = ConfigManager(
            config_path=str(sample_config),
            credentials_path=str(sample_credentials),
        )
        cm.load()

        # Create the output directory for validation
        output_dir = temp_dir / "output"
        output_dir.mkdir(exist_ok=True)

        cm.ensure_directories()
        assert (temp_dir / "logs").exists()
        assert (temp_dir / "cache").exists()

    def test_deep_merge(self, sample_config, sample_credentials):
        """Test deep merge preserves nested defaults."""
        cm = ConfigManager(
            config_path=str(sample_config),
            credentials_path=str(sample_credentials),
        )
        cm.load()
        # matching settings not in config, should come from defaults
        assert cm.get("matching", "fuzzy_enabled") is False
        assert cm.get("matching", "fuzzy_threshold") == 85
