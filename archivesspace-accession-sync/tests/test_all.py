"""
Comprehensive test suite for archivesspace-accession-sync.

Tests organized by module, covering:
- ConfigManager (config.yml + data.yml + credentials.yml)
- SyncEngine (change detection, extent conversion, keyword matching, etc.)
- SpreadsheetValidator (column detection, formulas, protection)
- LoggingManager (log creation, consolidation, storage)
- BackupManager (backup creation, folder management)
- Utilities (col_letter, config validation, time parsing)
"""

import json
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path

import yaml


# =========================================================================
# Test helpers
# =========================================================================

def setup_config(tmpdir, config_overrides=None, creds_overrides=None, data_overrides=None):
    """Create config, credentials, and data files for testing."""
    from sync.config_manager import ConfigManager

    config_data = {
        "archivesspace": {"base_url": "https://test.archivesspace.org/api", "repository_id": 2},
        "output": {"format": "excel", "spreadsheet_name": "Test Sheet"},
        "excel": {"target_directory": str(tmpdir)},
        "logging": {"level": "verbose", "directory": str(tmpdir / "logs")},
        "logging_advanced": {"grace_period_days": 3, "retention": "indefinite",
                             "archive_review_window_days": 30, "storage_warning_threshold_mb": 1024},
        "cache": {"directory": str(tmpdir / "cache")},
        "preview": {"directory": str(tmpdir / "preview"), "review_timeout_hours": 72},
        "agents": {"donor_role": "source"},
        "matching": {"fuzzy_enabled": False, "fuzzy_threshold": 85},
        "subject_descriptors": {"num_columns": 9},
        "throttling": {"archivesspace": 0.5, "google_sheets": 1.0, "google_drive": 0.5, "batch_mode": True},
        "retry": {"max_retries": 5, "file_lock_retries": 5, "file_lock_interval": 60},
        "scheduling": {"frequency": "weekly", "time": "20:00", "buffer_minutes": 60},
        "notifications": {"recipient_email": "", "format": "plain", "digest_mode": False},
        "ui": {"show_confirmations": True, "tour_completed": False},
    }
    if config_overrides:
        config_data.update(config_overrides)

    creds_data = {"archivesspace": {"username": "test", "password": "test"}}
    if creds_overrides:
        creds_data.update(creds_overrides)

    data_data = {
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
        "removable_media_keywords": ["floppy disk", "CD", "USB", "flash drive"],
        "documentation_use_issues_options": ["Option A", "Option B"],
        "processing_project_types": ["Backlog", "Requested"],
        "completion_triggers": ["completed"],
    }
    if data_overrides:
        data_data.update(data_overrides)

    config_path = tmpdir / "config.yml"
    creds_path = tmpdir / "credentials.yml"
    data_path = tmpdir / "data.yml"

    with open(config_path, "w") as f:
        yaml.dump(config_data, f)
    with open(creds_path, "w") as f:
        yaml.dump(creds_data, f)
    with open(data_path, "w") as f:
        yaml.dump(data_data, f)

    cm = ConfigManager(str(config_path), str(creds_path), str(data_path))
    cm.load()
    return cm


def run_tests():
    """Run all tests and report results."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    passed = 0
    failed = 0
    errors = []

    def check(name, condition):
        nonlocal passed, failed
        if condition:
            passed += 1
            print(f"  PASS: {name}")
        else:
            failed += 1
            errors.append(name)
            print(f"  FAIL: {name}")

    tmpdir = Path(tempfile.mkdtemp())

    # =====================================================================
    # UTILITIES TESTS
    # =====================================================================
    print("\n=== Utilities ===")
    from sync.utils import col_letter, col_index, validate_config_value, parse_time_string

    check("col_letter A", col_letter(1) == "A")
    check("col_letter Z", col_letter(26) == "Z")
    check("col_letter AA", col_letter(27) == "AA")
    check("col_letter AK", col_letter(37) == "AK")
    check("col_letter BA", col_letter(53) == "BA")

    check("col_index A", col_index("A") == 1)
    check("col_index Z", col_index("Z") == 26)
    check("col_index AA", col_index("AA") == 27)

    valid, _ = validate_config_value(5, int, min_val=1, max_val=10)
    check("validate int in range", valid)
    valid, _ = validate_config_value(15, int, min_val=1, max_val=10)
    check("validate int out of range", not valid)
    valid, _ = validate_config_value("excel", str, allowed_values=["excel", "google_sheets"])
    check("validate allowed value", valid)
    valid, _ = validate_config_value("pdf", str, allowed_values=["excel", "google_sheets"])
    check("validate disallowed value", not valid)

    h, m = parse_time_string("20:00")
    check("parse time valid", h == 20 and m == 0)
    try:
        parse_time_string("25:00")
        check("parse time invalid hour", False)
    except ValueError:
        check("parse time invalid hour", True)

    # =====================================================================
    # CONFIG MANAGER TESTS
    # =====================================================================
    print("\n=== ConfigManager ===")
    from sync.config_manager import ConfigManager, ConfigError

    cm = setup_config(tmpdir)
    check("load config", cm.get("archivesspace", "base_url") == "https://test.archivesspace.org/api")
    check("config defaults applied", cm.get("throttling", "archivesspace") == 0.5)
    check("credentials loaded", cm.get_credential("archivesspace", "username") == "test")
    check("data loaded", cm.get_data("extent_types", "linear_feet") is not None)
    check("data format_keywords", "Photographic Material(s)" in cm.get("format_keywords", default={}))
    check("repository URI", cm.get_repository_uri() == "/repositories/2")
    check("output format", cm.get_output_format() == "excel")
    check("missing key default", cm.get("nonexistent", default="fallback") == "fallback")

    cm.set("output", "format", value="google_sheets")
    check("set value", cm.get("output", "format") == "google_sheets")
    cm.set("output", "format", value="excel")

    cm.set_data("completion_triggers", value=["completed", "processed"])
    check("set data value", cm.get_data("completion_triggers") == ["completed", "processed"])

    check("email not configured", not cm.is_email_configured())
    check("show confirmations", cm.show_confirmations())
    check("tour not completed", not cm.tour_completed())

    cm.ensure_directories()
    check("directories created", (tmpdir / "logs").exists() and (tmpdir / "cache").exists())

    # Missing config
    try:
        bad_cm = ConfigManager(str(tmpdir / "nope.yml"))
        bad_cm.load()
        check("missing config error", False)
    except ConfigError:
        check("missing config error", True)

    # Validation
    issues = cm.validate()
    check("validation finds no critical issues", not any("base_url" in i.lower() for i in issues))

    # =====================================================================
    # SYNC ENGINE TESTS
    # =====================================================================
    print("\n=== SyncEngine ===")
    from sync.logging_manager import LoggingManager
    from sync.sync_engine import SyncEngine, UnknownExtentTypeError

    logger = LoggingManager(cm)
    engine = SyncEngine(cm, logger)

    # Extent calculations
    acc = {"extents": [
        {"extent_type": "linear_feet", "number": "3.5"},
        {"extent_type": "cubic_feet", "number": "2.0"},
    ]}
    check("physical extent sum", engine._calculate_physical_extent(acc) == 5.5)

    acc2 = {"extents": [
        {"extent_type": "gigabytes", "number": "10"},
        {"extent_type": "megabytes", "number": "500"},
    ]}
    check("digital extent sum", engine._calculate_digital_extent(acc2) == 10.5)

    # Unknown extent type
    acc_unknown = {"extents": [{"extent_type": "unknown_type", "number": "5"}]}
    try:
        engine._calculate_physical_extent(acc_unknown, acc_id=42)
        check("unknown extent raises error", False)
    except UnknownExtentTypeError as e:
        check("unknown extent raises error", "unknown_type" in str(e) and "42" in str(e))

    # Identifier
    check("identifier 2-part", engine._build_identifier({"id_0": "2023", "id_1": "001"}) == "2023-001")
    check("identifier 3-part", engine._build_identifier({"id_0": "UA", "id_1": "2023", "id_2": "001", "id_3": ""}) == "UA-2023-001")

    # Donor name
    detail = {"resolved_agents": [
        {"_role": "source", "display_name": {"sort_name": "Smith, John"}, "title": ""},
        {"_role": "source", "display_name": {"sort_name": "Doe, Jane"}, "title": ""},
        {"_role": "creator", "display_name": {"sort_name": "Nobody"}, "title": ""},
    ]}
    check("donor name multi", engine._extract_donor_name(detail) == "Smith, John; Doe, Jane")

    # Keywords
    check("keyword match found", engine._match_keywords("contains photograph material", ["photograph"]))
    check("keyword case insensitive", engine._match_keywords("OVERSIZE material", ["oversize"]))
    check("keyword no match", not engine._match_keywords("no matching content", ["photograph"]))

    # Format detection
    acc3 = {"content_description": "Contains photographs and oversized maps.", "condition_description": "", "inventory": "", "extents": []}
    results = engine._detect_formats(acc3, {"resolved_top_containers": []})
    check("detect photograph", results.get("Photographic Material(s)") is True)
    check("detect oversize", results.get("Oversize Material?") is True)

    # Digital issues
    acc4 = {"extents": [{"extent_type": "gigabytes", "number": "5"}], "content_description": "", "inventory": ""}
    check("digital issue absent", "Digital object potentially" in engine._evaluate_digital_issues(acc4, {"resolved_digital_objects": []}))

    acc5 = {"extents": [], "content_description": "Includes floppy disk and CD.", "inventory": ""}
    check("digital issue removable", "Removable media" in engine._evaluate_digital_issues(acc5, {"resolved_digital_objects": []}))

    # Access issues
    check("access restricted", len(engine._evaluate_access_issues({"access_restrictions": True, "access_restrictions_note": "Closed until 2030."})) > 0)
    check("access unrestricted", engine._evaluate_access_issues({"access_restrictions": False, "access_restrictions_note": ""}) == "")

    # Agent display
    agent = {"display_name": {"sort_name": "Univ of Maryland"}, "title": "", "_terms": [{"term": "Faculty"}, {"term": "History"}], "names": []}
    check("agent display terms", engine._format_agent_display(agent) == "Univ of Maryland — Faculty — History")

    # Lock versions
    check("sorted lock_versions", engine._get_sorted_lock_versions([{"lock_version": 5}, {"lock_version": 2}, {"lock_version": 8}]) == "2;5;8")

    # Change detection
    changes = engine.detect_changes(
        [{"accession": {"uri": "/repositories/2/accessions/1", "lock_version": 0}}], {}, []
    )
    check("new accession detected", len(changes["new"]) == 1)

    changes2 = engine.detect_changes(
        [], {"1": {"accession_lock_version": 0}}, [{"accession_id": 1}]
    )
    check("deleted accession detected", len(changes2["deleted"]) == 1)

    # Completion
    check("completion triggered", engine.check_completion(
        {"collection_management": {"processing_status": "completed"}}, "in_progress", ["completed"]
    ) is not None)
    check("completion not triggered", engine.check_completion(
        {"collection_management": {"processing_status": "in_progress"}}, "in_progress", ["completed"]
    ) is None)

    # Summarization
    check("short text passthrough", engine._summarize_text("Material is closed.") == "Material is closed.")

    # Overflow tracking
    check("overflow initially empty", len(engine.get_overflow_accessions()) == 0)

    # =====================================================================
    # VALIDATION TESTS
    # =====================================================================
    print("\n=== Validation ===")
    from sync.validation import SpreadsheetValidator, REQUIRED_COLUMNS, SYNC_COLUMNS, SCORING_COLUMNS

    validator = SpreadsheetValidator(cm, logger)
    expected = validator.get_expected_columns()

    check("expected includes required", all(c in expected for c in REQUIRED_COLUMNS))
    check("expected includes sync", all(c in expected for c in SYNC_COLUMNS))

    result = validator.validate(expected)
    check("all present valid", result.is_valid)

    missing = [h for h in expected if h != "Accession ID"]
    result2 = validator.validate(missing)
    check("missing column invalid", not result2.is_valid)
    check("missing column identified", "Accession ID" in result2.missing_columns)

    extra = expected + ["Custom Col"]
    result3 = validator.validate(extra)
    check("extra columns valid", result3.is_valid)
    check("extra columns identified", "Custom Col" in result3.extra_columns)

    check("rearranged valid", validator.validate(list(reversed(expected))).is_valid)

    check("Notes protected", validator.is_protected_column("Notes"))
    check("Score protected", validator.is_protected_column("Final Accession Score"))
    check("Date not protected", not validator.is_protected_column("Accession Date"))
    check("Month Completed protected normally", validator.is_protected_column("Month Completed"))
    check("Month Completed writable on completion", not validator.is_protected_column("Month Completed", is_completion_event=True))

    check("sync column detected", validator.is_sync_column("[Sync] Status"))
    check("non-sync column", not validator.is_sync_column("Accession ID"))

    # Dynamic formula generation
    col_map = {name: idx + 1 for idx, name in enumerate(expected)}
    formula = validator.get_column_formula("Base URL and Accession ID (Use for Hyperlink Only)", 2, col_map)
    check("CONCAT formula generated", formula is not None and "CONCAT" in formula)
    formula2 = validator.get_column_formula("Accession Number", 5, col_map)
    check("HYPERLINK formula generated", formula2 is not None and "HYPERLINK" in formula2)
    check("non-formula returns None", validator.get_column_formula("Accession Date", 2, col_map) is None)

    # =====================================================================
    # LOGGING TESTS
    # =====================================================================
    print("\n=== LoggingManager ===")

    log_logger = LoggingManager(cm)
    log_logger.start_run()
    log_logger.summary("Test summary")
    log_logger.technical("Test technical")
    log_logger.verbose("Test verbose")
    log_logger.error("Test error")
    log_logger.warning("Test warning")
    log_logger.end_run(success=True)

    log_dir = tmpdir / "logs"
    check("log dir created", log_dir.exists())
    check("log files created", len(list(log_dir.glob("*.log"))) >= 2)

    summary_files = list(log_dir.glob("summary_*.log"))
    check("summary file exists", len(summary_files) >= 1)
    with open(summary_files[0]) as f:
        content = f.read()
    check("summary has header", "=== Run:" in content)
    check("summary has entry", "Test summary" in content)
    check("error in summary", "ERROR" in content)

    info = log_logger.get_storage_info()
    check("storage info valid", info["file_count"] > 0)

    status = log_logger.get_last_run_status()
    check("last run status found", status is not None)

    entries = log_logger.get_recent_entries(10)
    check("recent entries found", len(entries) > 0)

    # =====================================================================
    # BACKUP TESTS
    # =====================================================================
    print("\n=== BackupManager ===")
    from sync.backup import BackupManager

    bm = BackupManager(cm, logger)
    test_file = tmpdir / "TestSheet.xlsx"
    test_file.write_text("test data")

    backups = []
    for i in range(4):
        b = bm.create_backup(test_file)
        backups.append(b)
        check(f"backup {i+1} created", b is not None and b.exists())
        time.sleep(1.1)

    check("all unique names", len(set(b.name for b in backups if b)) == 4)

    backup_folder = tmpdir / "[Backups] TestSheet"
    check("backup folder created", backup_folder.exists())

    check("non-existent returns None", bm.create_backup(tmpdir / "nope.xlsx") is None)

    # =====================================================================
    # NOTIFICATIONS TESTS
    # =====================================================================
    print("\n=== NotificationManager ===")
    from sync.notifications import NotificationManager

    nm = NotificationManager(cm, logger)
    check("email not enabled", not nm.is_enabled())
    check("send returns false", not nm.send("Test", "Body"))
    # Verify _pending_retry is initialized (was a bug)
    nm.retry_pending()  # Should not raise AttributeError
    check("retry_pending safe when empty", True)

    # =====================================================================
    # SUMMARY
    # =====================================================================
    print(f"\n{'='*60}")
    print(f"  {passed} passed, {failed} failed out of {passed + failed} tests.")
    if errors:
        print(f"  Failed: {', '.join(errors)}")
    print(f"{'='*60}\n")

    return failed == 0


if __name__ == "__main__":
    import sys
    success = run_tests()
    sys.exit(0 if success else 1)
