"""Tests for the SyncEngine module."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.sync_engine import SyncEngine


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
        "logging": {"directory": str(temp_dir / "logs"), "level": "verbose"},
        "cache": {"directory": str(temp_dir / "cache")},
        "preview": {"directory": str(temp_dir / "preview")},
        "agents": {"donor_role": "source"},
        "extent_types": {
            "linear_feet": {"category": "physical", "conversion_factor": 1.0},
            "cubic_feet": {"category": "physical", "conversion_factor": 1.0},
            "gigabytes": {"category": "digital", "conversion_factor": 1.0},
            "megabytes": {"category": "digital", "conversion_factor": 0.001},
        },
        "format_keywords": {
            "Photographic Material(s)": ["photograph", "photo"],
            "Oversize Material?": ["oversize", "oversized"],
        },
        "matching": {"fuzzy_enabled": False, "fuzzy_threshold": 85},
        "subject_descriptors": {"num_columns": 9},
        "removable_media_keywords": ["floppy disk", "CD", "USB", "flash drive"],
    }
    config_path = temp_dir / "config.yml"
    with open(config_path, "w") as f:
        yaml.dump(config_data, f)

    creds_path = temp_dir / "credentials.yml"
    with open(creds_path, "w") as f:
        yaml.dump({"archivesspace": {"username": "test", "password": "test"}}, f)

    cm = ConfigManager(config_path=str(config_path), credentials_path=str(creds_path))
    cm.load()
    return cm


@pytest.fixture
def logger(config):
    return LoggingManager(config)


@pytest.fixture
def engine(config, logger):
    return SyncEngine(config, logger)


class TestSyncEngine:
    def test_calculate_physical_extent(self, engine):
        """Test physical extent conversion and summing."""
        accession = {
            "extents": [
                {"extent_type": "linear_feet", "number": "3.5"},
                {"extent_type": "cubic_feet", "number": "2.0"},
            ]
        }
        result = engine._calculate_physical_extent(accession)
        assert result == 5.5

    def test_calculate_digital_extent(self, engine):
        """Test digital extent conversion and summing."""
        accession = {
            "extents": [
                {"extent_type": "gigabytes", "number": "10"},
                {"extent_type": "megabytes", "number": "500"},
            ]
        }
        result = engine._calculate_digital_extent(accession)
        assert result == 10.5

    def test_calculate_extent_ignores_wrong_category(self, engine):
        """Test that physical extent calculation ignores digital types."""
        accession = {
            "extents": [
                {"extent_type": "linear_feet", "number": "3.0"},
                {"extent_type": "gigabytes", "number": "10"},
            ]
        }
        physical = engine._calculate_physical_extent(accession)
        digital = engine._calculate_digital_extent(accession)
        assert physical == 3.0
        assert digital == 10.0

    def test_build_identifier(self, engine):
        """Test four-part identifier construction."""
        accession = {"id_0": "2023", "id_1": "001"}
        assert engine._build_identifier(accession) == "2023-001"

        accession2 = {"id_0": "UA", "id_1": "2023", "id_2": "001", "id_3": ""}
        assert engine._build_identifier(accession2) == "UA-2023-001"

    def test_extract_donor_name_single(self, engine):
        """Test extracting a single donor name."""
        detail = {
            "resolved_agents": [
                {
                    "_role": "source",
                    "display_name": {"sort_name": "Smith, John"},
                    "title": "John Smith",
                }
            ]
        }
        assert engine._extract_donor_name(detail) == "Smith, John"

    def test_extract_donor_name_multiple(self, engine):
        """Test extracting multiple donor names."""
        detail = {
            "resolved_agents": [
                {"_role": "source", "display_name": {"sort_name": "Smith, John"}, "title": ""},
                {"_role": "source", "display_name": {"sort_name": "Doe, Jane"}, "title": ""},
                {"_role": "creator", "display_name": {"sort_name": "Nobody"}, "title": ""},
            ]
        }
        result = engine._extract_donor_name(detail)
        assert result == "Smith, John; Doe, Jane"

    def test_match_keywords_exact(self, engine):
        """Test case-insensitive partial keyword matching."""
        assert engine._match_keywords("contains photograph material", ["photograph"])
        assert engine._match_keywords("OVERSIZE material here", ["oversize"])
        assert not engine._match_keywords("no matching content", ["photograph"])

    def test_detect_formats(self, engine):
        """Test format detection via keyword scanning."""
        accession = {
            "content_description": "Collection contains photographs and oversized maps.",
            "condition_description": "",
            "inventory": "",
            "extents": [],
        }
        detail = {"resolved_top_containers": []}

        results = engine._detect_formats(accession, detail)
        assert results.get("Photographic Material(s)") is True
        assert results.get("Oversize Material?") is True

    def test_evaluate_digital_issues_absent_object(self, engine):
        """Test digital issue detection: extent present, no digital object."""
        accession = {
            "extents": [{"extent_type": "gigabytes", "number": "5"}],
            "content_description": "",
            "inventory": "",
        }
        detail = {"resolved_digital_objects": []}

        result = engine._evaluate_digital_issues(accession, detail)
        assert "Digital object potentially or actually absent" in result

    def test_evaluate_digital_issues_removable_media(self, engine):
        """Test digital issue detection: removable media without transfer."""
        accession = {
            "extents": [],
            "content_description": "Collection includes floppy disk and CD materials.",
            "inventory": "",
        }
        detail = {"resolved_digital_objects": []}

        result = engine._evaluate_digital_issues(accession, detail)
        assert "Removable media" in result

    def test_evaluate_access_issues_restricted(self, engine):
        """Test access issue evaluation for restricted accessions."""
        accession = {
            "access_restrictions": True,
            "access_restrictions_note": "Closed until 2030.",
        }
        result = engine._evaluate_access_issues(accession)
        assert "Closed" in result or "2030" in result or len(result) > 0

    def test_evaluate_access_issues_unrestricted(self, engine):
        """Test access issue evaluation for unrestricted accessions."""
        accession = {
            "access_restrictions": False,
            "access_restrictions_note": "",
        }
        result = engine._evaluate_access_issues(accession)
        assert result == ""

    def test_summarize_short_text(self, engine):
        """Test that short text is returned as-is."""
        text = "Material is closed."
        assert engine._summarize_text(text) == "Material is closed."

    def test_format_agent_display_simple(self, engine):
        """Test simple agent display formatting."""
        agent = {
            "display_name": {"sort_name": "University of Iowa"},
            "title": "University of Iowa",
            "_terms": [],
            "names": [],
        }
        assert engine._format_agent_display(agent) == "University of Iowa"

    def test_format_agent_display_with_terms(self, engine):
        """Test agent display with term subdivisions."""
        agent = {
            "display_name": {"sort_name": "University of Maryland"},
            "title": "University of Maryland",
            "_terms": [{"term": "Faculty"}, {"term": "History Department"}],
            "names": [],
        }
        result = engine._format_agent_display(agent)
        assert result == "University of Maryland — Faculty — History Department"

    def test_get_sorted_lock_versions(self, engine):
        """Test sorted lock_version concatenation."""
        records = [
            {"lock_version": 5},
            {"lock_version": 2},
            {"lock_version": 8},
        ]
        result = engine._get_sorted_lock_versions(records)
        assert result == "2;5;8"

    def test_detect_changes_new_accession(self, engine):
        """Test detection of a new accession."""
        current_data = [
            {"accession": {"uri": "/repositories/2/accessions/1", "lock_version": 0}}
        ]
        cached_data = {}
        spreadsheet_data = []

        changes = engine.detect_changes(current_data, cached_data, spreadsheet_data)
        assert len(changes["new"]) == 1
        assert len(changes["updated"]) == 0
        assert len(changes["deleted"]) == 0

    def test_detect_changes_deleted_accession(self, engine):
        """Test detection of a deleted accession."""
        current_data = []
        cached_data = {"1": {"accession_lock_version": 0}}
        spreadsheet_data = [{"accession_id": 1}]

        changes = engine.detect_changes(current_data, cached_data, spreadsheet_data)
        assert len(changes["new"]) == 0
        assert len(changes["deleted"]) == 1

    def test_check_completion_triggered(self, engine):
        """Test completion detection when status changes."""
        accession = {
            "collection_management": {"processing_status": "completed"},
        }
        result = engine.check_completion(accession, "in_progress", ["completed"])
        assert result is not None
        assert "20" in result  # Should contain a year

    def test_check_completion_not_triggered(self, engine):
        """Test completion detection when status hasn't changed."""
        accession = {
            "collection_management": {"processing_status": "in_progress"},
        }
        result = engine.check_completion(accession, "in_progress", ["completed"])
        assert result is None
