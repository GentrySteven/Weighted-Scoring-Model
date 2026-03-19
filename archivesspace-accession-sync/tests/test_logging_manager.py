"""Tests for the LoggingManager module."""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def logger(temp_dir):
    config_data = {
        "logging": {
            "level": "verbose",
            "directory": str(temp_dir / "logs"),
            "consolidation_frequency": "weekly",
            "grace_period_days": 3,
            "retention": "indefinite",
            "archive_review_window_days": 30,
            "storage_warning_threshold_mb": 1024,
        },
        "archivesspace": {"base_url": "test", "repository_id": 2},
        "output": {"format": "excel", "spreadsheet_name": "Test"},
        "excel": {"target_directory": str(temp_dir)},
        "cache": {"directory": str(temp_dir / "cache")},
        "preview": {"directory": str(temp_dir / "preview")},
    }
    config_path = temp_dir / "config.yml"
    with open(config_path, "w") as f:
        yaml.dump(config_data, f)
    creds_path = temp_dir / "credentials.yml"
    with open(creds_path, "w") as f:
        yaml.dump({"archivesspace": {"username": "t", "password": "t"}}, f)

    cm = ConfigManager(str(config_path), str(creds_path))
    cm.load()
    return LoggingManager(cm)


class TestLoggingManager:
    def test_start_and_end_run(self, logger, temp_dir):
        """Test that a run produces log files."""
        logger.start_run()
        logger.summary("Test summary entry")
        logger.technical("Test technical entry")
        logger.end_run(success=True)

        log_dir = temp_dir / "logs"
        assert log_dir.exists()
        log_files = list(log_dir.glob("*.log"))
        assert len(log_files) >= 2  # summary + technical

    def test_error_logged_at_all_levels(self, logger, temp_dir):
        """Test that errors are always logged."""
        logger.start_run()
        logger.error("Critical error occurred")
        logger.end_run(success=False)

        log_dir = temp_dir / "logs"
        summary_files = list(log_dir.glob("summary_*.log"))
        assert len(summary_files) == 1

        with open(summary_files[0]) as f:
            content = f.read()
        assert "ERROR" in content

    def test_verbose_only_at_verbose_level(self, temp_dir):
        """Test that verbose entries are only recorded at verbose level."""
        config_data = {
            "logging": {"level": "minimal", "directory": str(temp_dir / "logs2")},
            "archivesspace": {"base_url": "t", "repository_id": 2},
            "output": {"format": "excel", "spreadsheet_name": "T"},
            "excel": {"target_directory": str(temp_dir)},
            "cache": {"directory": str(temp_dir / "cache")},
            "preview": {"directory": str(temp_dir / "preview")},
        }
        config_path = temp_dir / "config2.yml"
        with open(config_path, "w") as f:
            yaml.dump(config_data, f)
        creds_path = temp_dir / "creds2.yml"
        with open(creds_path, "w") as f:
            yaml.dump({"archivesspace": {"username": "t", "password": "t"}}, f)

        cm = ConfigManager(str(config_path), str(creds_path))
        cm.load()
        min_logger = LoggingManager(cm)

        min_logger.start_run()
        min_logger.verbose("This should not appear")
        min_logger.end_run()

        technical_files = list((temp_dir / "logs2").glob("technical_*.log"))
        if technical_files:
            with open(technical_files[0]) as f:
                content = f.read()
            assert "This should not appear" not in content

    def test_get_storage_info(self, logger, temp_dir):
        """Test storage info reporting."""
        logger.start_run()
        logger.summary("Test")
        logger.end_run()

        info = logger.get_storage_info()
        assert "total_mb" in info
        assert "file_count" in info
        assert info["file_count"] > 0

    def test_get_recent_entries(self, logger, temp_dir):
        """Test retrieving recent entries."""
        logger.start_run()
        logger.summary("Entry one")
        logger.summary("Entry two")
        logger.end_run()

        entries = logger.get_recent_entries(count=10)
        assert len(entries) > 0
