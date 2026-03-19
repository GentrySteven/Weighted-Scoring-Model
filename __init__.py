"""
CLI Entry Point

Main entry point for the archivesspace-accession-sync tool.
Handles command-line argument parsing and routes to either the
interactive menu or direct execution mode.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from sync import __version__
from sync.config_manager import ConfigManager, ConfigError
from sync.logging_manager import LoggingManager
from sync.archivesspace import ArchivesSpaceClient, ArchivesSpaceError
from sync.sync_engine import SyncEngine
from sync.validation import SpreadsheetValidator
from sync.backup import BackupManager
from sync.notifications import NotificationManager
from sync.scheduler import Scheduler
from sync.menu import Menu


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Synchronize ArchivesSpace accession metadata to Excel or Google Sheets.",
        prog="accession-sync",
    )
    parser.add_argument(
        "--target",
        choices=["excel", "google_sheets"],
        help="Output format (overrides config file)",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Run in automatic mode (non-interactive, for scheduled runs)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to the spreadsheet",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config.yml",
    )
    parser.add_argument(
        "--credentials",
        type=str,
        default=None,
        help="Path to credentials.yml",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"archivesspace-accession-sync v{__version__}",
    )

    return parser.parse_args()


def check_for_updates(config: ConfigManager, logger: LoggingManager) -> None:
    """Check for available updates on GitHub."""
    try:
        import urllib.request
        import json as json_mod

        url = "https://api.github.com/repos/YOUR_USERNAME/archivesspace-accession-sync/releases/latest"
        req = urllib.request.Request(url, headers={"User-Agent": "accession-sync"})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json_mod.loads(response.read().decode())
            latest_version = data.get("tag_name", "").lstrip("v")

            if latest_version and latest_version != __version__:
                # Check if critical (look for keywords in release notes)
                body = data.get("body", "").lower()
                is_critical = any(
                    word in body for word in ["security", "critical", "vulnerability", "urgent"]
                )

                if is_critical:
                    print(f"\n  ⚠ CRITICAL UPDATE AVAILABLE: v{latest_version}")
                    print("  This update includes security patches. Please update promptly.")
                    print("  Run 'Check for updates' from the menu to apply.\n")
                else:
                    print(f"\n  Update available: v{latest_version} (current: v{__version__})")

    except Exception:
        # Silently fail - update check is not critical
        pass


def run_sync(
    config: ConfigManager,
    logger: LoggingManager,
    dry_run: bool = False,
    auto_mode: bool = False,
) -> bool:
    """
    Execute a sync operation.

    Args:
        config: ConfigManager instance.
        logger: LoggingManager instance.
        dry_run: If True, preview changes without writing.
        auto_mode: If True, suppress interactive prompts.

    Returns:
        True if sync completed successfully.
    """
    notifications = NotificationManager(config, logger)
    backup = BackupManager(config, logger)
    sync_engine = SyncEngine(config, logger)

    logger.start_run()

    try:
        # Step 1: Connect to ArchivesSpace
        print("  Connecting to ArchivesSpace...")
        as_client = ArchivesSpaceClient(config, logger)
        if not as_client.connect():
            raise ArchivesSpaceError("Failed to connect to ArchivesSpace.")

        # Step 2: Initialize spreadsheet manager
        output_format = config.get_output_format()
        if output_format == "excel":
            from sync.excel import ExcelManager
            spreadsheet = ExcelManager(config, logger)

            # Check file lock
            if spreadsheet.file_exists():
                is_locked, lock_info = spreadsheet.is_file_locked()
                if is_locked:
                    if auto_mode:
                        raise Exception(f"Excel file is locked. {lock_info}")
                    if not spreadsheet.wait_for_unlock():
                        logger.error("Excel file remained locked after all retries.")
                        _save_staging_data(config, logger, None)
                        raise Exception("Excel file locked - data saved to staging file.")

        elif output_format == "google_sheets":
            from sync.google_sheets import GoogleSheetsManager
            spreadsheet = GoogleSheetsManager(config, logger)
            if not spreadsheet.authenticate():
                raise Exception("Failed to authenticate with Google APIs.")

        # Step 3: Check if spreadsheet exists or create it
        validator = SpreadsheetValidator(config, logger)
        expected_headers = validator.get_expected_columns()

        if output_format == "excel":
            if not spreadsheet.file_exists():
                print("  Creating new Excel spreadsheet...")
                spreadsheet.create_spreadsheet(expected_headers)
            else:
                # Validate structure
                headers, _ = spreadsheet.read_data()
                result = validator.validate(headers)
                if not result.is_valid:
                    if auto_mode:
                        raise Exception(
                            f"Spreadsheet validation failed: {', '.join(result.errors)}"
                        )
                    _handle_validation_failure(result, spreadsheet, validator, config, logger, backup)

        elif output_format == "google_sheets":
            if not spreadsheet.spreadsheet_exists():
                print("  Creating new Google Sheet...")
                spreadsheet.create_spreadsheet(expected_headers)
            else:
                headers, _ = spreadsheet.read_data()
                result = validator.validate(headers)
                if not result.is_valid:
                    if auto_mode:
                        raise Exception(
                            f"Spreadsheet validation failed: {', '.join(result.errors)}"
                        )
                    _handle_validation_failure(result, spreadsheet, validator, config, logger, backup)

            # Verify permissions
            spreadsheet.verify_permissions()

        # Step 4: Read current spreadsheet data
        print("  Reading current spreadsheet data...")
        headers, current_rows = spreadsheet.read_data()

        # Step 5: Load cache
        cache_data = _load_cache(config)

        # Step 6: Retrieve accession data from ArchivesSpace
        print("  Retrieving accession data from ArchivesSpace...")
        all_ids = as_client.get_all_accession_ids()

        # Two-step retrieval: check which accessions need full detail
        accession_details = []
        for acc_id in all_ids:
            cached_entry = cache_data.get(str(acc_id), {})
            # For now, retrieve all - optimization with lock_version comparison
            # can be added once cache is populated
            detail = as_client.get_accession_full_detail(acc_id)
            accession_details.append(detail)

        # Step 7: Detect changes
        print("  Detecting changes...")
        changes = sync_engine.detect_changes(accession_details, cache_data, current_rows)

        # Step 8: Build row data for changed accessions
        base_url = config.get_base_url()
        new_rows = []
        for detail in changes["new"]:
            row_data = sync_engine.build_row_data(detail, base_url)
            row_data["_is_new"] = True
            new_rows.append(row_data)

        updated_rows = []
        for detail in changes["updated"]:
            row_data = sync_engine.build_row_data(detail, base_url)
            row_data["_changes"] = detail.get("_changes", [])
            updated_rows.append(row_data)

        # Step 9: Report summary
        summary = (
            f"Sync summary: {len(changes['new'])} new, "
            f"{len(changes['updated'])} updated, "
            f"{len(changes['deleted'])} deleted, "
            f"{len(changes['unchanged'])} unchanged."
        )
        print(f"\n  {summary}")
        logger.summary(summary)

        # Step 10: Handle dry run
        if dry_run:
            print("\n  DRY RUN - No changes written to the spreadsheet.")
            # TODO: Create preview spreadsheet
            # TODO: If not auto_mode, prompt to proceed

            if auto_mode:
                # Send preview notification
                notifications.notify_preview_ready("Preview spreadsheet created.")

            logger.end_run(success=True)
            notifications.notify_sync_success(summary)
            return True

        # Step 11: Write changes
        if new_rows or updated_rows or changes["deleted"]:
            print("  Writing changes to spreadsheet...")

            # Delete removed accessions
            for del_id in changes["deleted"]:
                if output_format == "excel":
                    row_idx = spreadsheet.find_row_by_accession_id(del_id)
                    if row_idx:
                        spreadsheet.delete_row(row_idx)
                        logger.technical(f"Deleted row for accession {del_id}")

            # Update existing rows
            for row_data in updated_rows:
                acc_id = row_data.get("Accession ID")
                if output_format == "excel":
                    row_idx = spreadsheet.find_row_by_accession_id(acc_id)
                    if row_idx:
                        spreadsheet.update_row(headers, row_data, row_idx)

            # Append new rows
            if new_rows:
                if output_format == "excel":
                    _, existing = spreadsheet.read_data()
                    start_row = len(existing) + 2
                    spreadsheet.write_rows(headers, new_rows, start_row=start_row)

            # Check for completion triggers
            completion_triggers = config.get("completion_triggers", default=[])
            if completion_triggers:
                for detail in changes["updated"]:
                    accession = detail.get("accession", {})
                    acc_id = sync_engine._extract_id(accession)
                    cached = cache_data.get(str(acc_id), {})
                    # Get cached status from the spreadsheet data
                    cached_status = ""
                    for row in current_rows:
                        if row.get("accession_id") == acc_id:
                            cached_status = row.get("Accession Status", "")
                            break

                    month_completed = sync_engine.check_completion(
                        accession, cached_status, completion_triggers
                    )
                    if month_completed:
                        logger.summary(
                            f"Accession {acc_id} marked as completed: {month_completed}"
                        )

            print("  Changes written successfully.")
        else:
            print("  No changes to write.")

        # Step 12: Update cache
        print("  Updating cache...")
        new_cache = {}
        for detail in accession_details:
            accession = detail.get("accession", {})
            acc_id = str(sync_engine._extract_id(accession))
            new_cache[acc_id] = sync_engine.build_cache_entry(detail)

        _save_cache(config, new_cache)

        # Step 13: Complete
        logger.end_run(success=True)
        notifications.notify_sync_success(summary)
        notifications.retry_pending()
        print("\n  Sync completed successfully.")
        return True

    except Exception as e:
        logger.error(str(e))
        logger.end_run(success=False)
        notifications.notify_sync_failure(str(e))
        notifications.retry_pending()
        print(f"\n  Sync failed: {e}")
        return False


def _handle_validation_failure(result, spreadsheet, validator, config, logger, backup):
    """Handle a spreadsheet validation failure interactively."""
    print("\n  Spreadsheet validation failed:")
    for error in result.errors:
        print(f"    - {error}")

    choice = Menu.prompt_yes_no("Would you like to rebuild the spreadsheet?", default=False)
    if not choice:
        logger.error("User declined rebuild. Exiting.")
        raise Exception("Spreadsheet validation failed. User declined rebuild.")

    # Warn about data loss
    print("\n  WARNING: Rebuilding will delete all existing data and custom columns.")
    print("  A backup will be created automatically.")

    proceed = Menu.prompt_yes_no("Proceed with rebuild?", default=False)
    if not proceed:
        logger.error("User cancelled rebuild. Exiting.")
        raise Exception("Spreadsheet validation failed. User cancelled rebuild.")

    # Create backup
    if hasattr(spreadsheet, 'get_file_path'):
        backup.create_backup(spreadsheet.get_file_path())
    print("  Backup created.")

    # Clear and rebuild
    spreadsheet.clear_data()
    print("  Spreadsheet rebuilt.")


def _load_cache(config: ConfigManager) -> dict:
    """Load the accession data cache from disk."""
    cache_dir = config.get("cache", "directory", default="")
    if not cache_dir:
        return {}

    cache_path = Path(cache_dir) / "accession_cache.json"
    if not cache_path.exists():
        return {}

    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def _save_cache(config: ConfigManager, cache_data: dict) -> None:
    """Save the accession data cache to disk."""
    cache_dir = config.get("cache", "directory", default="")
    if not cache_dir:
        return

    cache_path = Path(cache_dir) / "accession_cache.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, indent=2)


def _save_staging_data(config: ConfigManager, logger: LoggingManager, data) -> None:
    """Save retrieved data to a staging file for retry on next run."""
    if data is None:
        return

    log_dir = config.get("logging", "directory", default="")
    if not log_dir:
        return

    date_str = datetime.now().strftime("%Y-%m-%d")
    staging_path = Path(log_dir) / f"staging_sync_{date_str}.json"

    try:
        with open(staging_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.summary(f"Staging data saved to: {staging_path}")
    except (IOError, TypeError) as e:
        logger.error(f"Failed to save staging data: {e}")


def run_interactive(config: ConfigManager, logger: LoggingManager) -> None:
    """Launch the interactive menu."""
    menu = Menu(config, logger)

    # Register actions
    menu.register_action("sync", lambda: run_sync(config, logger))
    menu.register_action("dry_run", lambda: run_sync(config, logger, dry_run=True))
    menu.register_action(
        "verify_config",
        lambda: _action_verify_config(config),
    )
    menu.register_action(
        "last_sync_status",
        lambda: _action_last_sync_status(logger),
    )
    menu.register_action(
        "view_logs",
        lambda: _action_view_logs(logger),
    )
    menu.register_action(
        "schedule_create",
        lambda: _action_schedule_create(config, logger),
    )
    menu.register_action(
        "schedule_modify",
        lambda: _action_schedule_modify(config, logger),
    )
    menu.register_action(
        "schedule_remove",
        lambda: _action_schedule_remove(config, logger),
    )
    menu.register_action(
        "check_updates",
        lambda: check_for_updates(config, logger),
    )

    menu.display()


def _action_verify_config(config: ConfigManager) -> None:
    """Verify and display current configuration status."""
    print("  Verifying configuration...\n")
    issues = config.validate()
    if issues:
        print("  Configuration issues found:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print("  Configuration is valid.")

    print(f"\n  Output format: {config.get_output_format()}")
    print(f"  ArchivesSpace URL: {config.get_base_url()}")
    print(f"  Repository ID: {config.get_repository_id()}")
    print(f"  Spreadsheet name: {config.get_spreadsheet_name()}")
    print(f"  Email notifications: {'enabled' if config.is_email_configured() else 'disabled'}")


def _action_last_sync_status(logger: LoggingManager) -> None:
    """Display the status of the most recent sync."""
    status = logger.get_last_run_status()
    if not status:
        print("  No sync runs found.")
        return

    print(f"  Last sync: {status['timestamp'].strftime('%B %d, %Y %I:%M %p')}")
    print(f"  Log file: {status['file']}\n")
    for entry in status["entries"][:10]:
        print(f"    {entry}")


def _action_view_logs(logger: LoggingManager) -> None:
    """Display recent log entries."""
    entries = logger.get_recent_entries(count=30)
    if not entries:
        print("  No log entries found.")
        return

    print("  Recent log entries:\n")
    for entry in entries:
        print(f"    {entry.rstrip()}")


def _action_schedule_create(config: ConfigManager, logger: LoggingManager) -> None:
    """Create a new scheduled job through the menu."""
    scheduler = Scheduler(config, logger)

    if scheduler.job_exists():
        print("  A scheduled job already exists. Remove it first or use 'Modify'.")
        return

    freq_idx = Menu.prompt_choice(
        "How often should the sync run?",
        ["Daily (recommended for active repositories)", "Weekly", "Monthly"],
    )
    frequency = ["daily", "weekly", "monthly"][freq_idx]

    time_str = Menu.prompt_text(
        "What time should it run? (24-hour format, e.g., 20:00)",
        default="20:00",
    )

    target = config.get_output_format()

    dry_run_choice = Menu.prompt_yes_no("Schedule a dry run instead of a full sync?", default=False)

    if scheduler.create_job(frequency, time_str, target, dry_run_choice):
        print(f"\n  Scheduled job created: {frequency} at {time_str}")
    else:
        print("\n  Failed to create scheduled job.")


def _action_schedule_modify(config: ConfigManager, logger: LoggingManager) -> None:
    """Modify the existing scheduled job."""
    scheduler = Scheduler(config, logger)

    if not scheduler.job_exists():
        print("  No scheduled job exists. Create one first.")
        return

    freq_idx = Menu.prompt_choice(
        "New frequency?",
        ["Daily", "Weekly", "Monthly", "Keep current"],
    )
    frequency = ["daily", "weekly", "monthly", None][freq_idx]

    time_str = Menu.prompt_text("New time? (Enter to keep current)", default="")
    time_str = time_str or None

    if scheduler.modify_job(frequency=frequency, time_str=time_str):
        print("\n  Scheduled job modified.")
    else:
        print("\n  Failed to modify scheduled job.")


def _action_schedule_remove(config: ConfigManager, logger: LoggingManager) -> None:
    """Remove the existing scheduled job."""
    scheduler = Scheduler(config, logger)

    if not scheduler.job_exists():
        print("  No scheduled job exists.")
        return

    if Menu.prompt_yes_no("Are you sure you want to remove the scheduled job?"):
        if scheduler.remove_job():
            print("\n  Scheduled job removed.")
        else:
            print("\n  Failed to remove scheduled job.")


def main():
    """Main entry point."""
    args = parse_args()

    # Initialize configuration
    config = ConfigManager(
        config_path=args.config,
        credentials_path=args.credentials,
    )

    # Check if first run (no config exists)
    if not config.config_exists():
        print("\n  Welcome to archivesspace-accession-sync!\n")
        print("  No configuration file found. You can either:")
        print("  1. Run the guided setup wizard")
        print("  2. Create template files for manual configuration\n")

        choice = Menu.prompt_choice("Choose an option:", ["Guided setup wizard", "Create template files"])

        if choice == 0:
            # TODO: Implement setup wizard
            print("\n  The setup wizard will be implemented in a future version.")
            print("  For now, template files have been created.")
            _create_templates(config)
        else:
            _create_templates(config)

        print(f"\n  Config file: {config.config_path}")
        print(f"  Credentials file: {config.credentials_path}")
        print("  Edit these files and run the tool again.\n")
        return

    # Load configuration
    try:
        config.load()
    except ConfigError as e:
        print(f"\n  Configuration error: {e}\n")
        sys.exit(1)

    # Override output format if specified via CLI
    if args.target:
        config.set("output", "format", value=args.target)

    # Ensure directories exist
    config.ensure_directories()

    # Initialize logger
    logger = LoggingManager(config)

    # Check for updates on startup
    check_for_updates(config, logger)

    # Route to appropriate mode
    if args.auto:
        # Automatic mode (non-interactive)
        success = run_sync(config, logger, dry_run=args.dry_run, auto_mode=True)
        sys.exit(0 if success else 1)
    elif args.target or args.dry_run:
        # Direct execution with flags
        success = run_sync(config, logger, dry_run=args.dry_run)
        sys.exit(0 if success else 1)
    else:
        # Interactive menu
        run_interactive(config, logger)


def _create_templates(config: ConfigManager) -> None:
    """Create template config and credentials files."""
    import shutil

    template_dir = Path(__file__).resolve().parent.parent

    # Copy config template
    config_template = template_dir / "config.yml"
    if config_template.exists() and not config.config_path.exists():
        shutil.copy2(str(config_template), str(config.config_path))

    # Copy credentials template
    creds_template = template_dir / "credentials_example.yml"
    if creds_template.exists() and not config.credentials_path.exists():
        shutil.copy2(str(creds_template), str(config.credentials_path))

    print("  Template files created.")


if __name__ == "__main__":
    main()
