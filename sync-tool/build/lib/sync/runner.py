"""
Sync Runner

Contains `run_sync` — the core orchestration function that executes a
complete synchronization pipeline from ArchivesSpace to the target
spreadsheet — along with its private helpers for cache management,
staging, preview handling, and spreadsheet initialization.

The pipeline has 15 steps and two-phase failure handling: retrieval
failures halt immediately (nothing written), while write failures save
data to a staging file so the next run can retry without re-fetching
from ArchivesSpace.

These functions were previously in sync/cli.py but were extracted here
to keep that module focused on entry points and argument parsing.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from sync.backup import BackupManager
from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.menu import Menu
from sync.notifications import NotificationManager
from sync.sync_engine import SyncEngine, UnknownExtentTypeError
from sync.validation import SpreadsheetValidator


def run_sync(
    config: ConfigManager,
    logger: LoggingManager,
    dry_run: bool = False,
    auto_mode: bool = False,
) -> bool:
    """
    Execute a sync operation — the core orchestration function.

    Pipeline (15 steps):
      1. Check for staging data from a previously interrupted write
      2. Check for unreviewed dry run preview (blocks sync if pending)
      3. Connect to ArchivesSpace via ArchivesSnake
      4. Initialize the spreadsheet manager (Excel or Google Sheets)
      5. Validate spreadsheet structure (column names, not positions)
      6. Load the accession data cache from the previous run
      7. Retrieve all accessions from ArchivesSpace (paginated)
      8. Detect changes by comparing current data against the cache
      9. Build row data for new and updated accessions
     10. Print summary to terminal and log
     11. Handle dry run (create preview, optionally approve)
     12. Write changes to the spreadsheet
     13. Update supporting sheets (Backlog, Processing Projects)
     14. Update the cache (only after successful write)
     15. Send notifications and clean up

    Two-phase failure handling:
      - If retrieval (steps 3-8) fails: halt immediately, nothing written
      - If write (step 12) fails: save data to a staging file for retry

    Args:
        config: Loaded ConfigManager instance.
        logger: LoggingManager instance.
        dry_run: If True, create a preview without writing.
        auto_mode: If True, running from a scheduled job (non-interactive).

    Returns:
        True if sync completed successfully.
    """
    notifications = NotificationManager(config, logger)
    backup = BackupManager(config, logger)
    sync_engine = SyncEngine(config, logger)

    logger.start_run()
    had_changes = False

    try:
        # Step 1: Check for pending staging data
        staging_data = _load_staging_data(config)
        if staging_data:
            logger.summary("Found pending staging data from a previous interrupted run.")
            print("  Found pending staging data. Attempting to complete previous write...")
            _apply_staging_data(config, logger, staging_data)
            _clear_staging_data(config)

        # Step 2: Check for pending preview (if sync job, not dry run)
        if not dry_run:
            preview_status = _check_pending_preview(config, logger, auto_mode)
            if preview_status == "blocked":
                logger.summary("Sync blocked by unreviewed preview.")
                notifications.notify_sync_failure(
                    "Sync blocked: an unreviewed dry run preview exists.",
                    phase="pre-check",
                )
                logger.end_run(success=False)
                return False

        # Step 3: Connect to ArchivesSpace (deferred import)
        print("  Connecting to ArchivesSpace...")
        try:
            from sync.archivesspace import ArchivesSpaceClient
        except ImportError:
            raise RuntimeError(
                "ArchivesSnake is not installed. Install it with:\n"
                "  pip install ArchivesSnake"
            )

        as_client = ArchivesSpaceClient(config, logger)
        if not as_client.connect():
            raise RuntimeError("Failed to connect to ArchivesSpace.")

        # Step 4: Initialize spreadsheet manager (deferred import)
        output_format = config.get_output_format()
        spreadsheet = _init_spreadsheet(config, logger, output_format, auto_mode, backup)

        # Step 5: Validate and get headers
        validator = SpreadsheetValidator(config, logger)
        expected_headers = validator.get_expected_columns()
        headers, current_rows = _ensure_spreadsheet(
            spreadsheet, validator, expected_headers, output_format,
            config, logger, backup, auto_mode,
        )

        # Step 5b: Validate scoring criteria configuration
        # Non-fatal — warns about issues but does not halt the sync.
        # Broken criteria still allow the sync to complete; only the
        # scoring formulas in the spreadsheet will be affected.
        scoring_issues = validator.validate_scoring_criteria()
        if scoring_issues:
            logger.warning(
                f"Scoring criteria has {len(scoring_issues)} validation issue(s):"
            )
            for issue in scoring_issues:
                logger.warning(f"  - {issue}")
                print(f"  WARNING: {issue}")
            print("  Run 'View scoring criteria' or 'Edit scoring criteria' to fix.")

        # Step 5c: Validate processing queue configuration
        # Non-fatal — broken queue config skips queue sheet generation
        # but does not halt the sync.
        queue_issues = validator.validate_processing_queue()
        if queue_issues:
            logger.warning(
                f"Processing queue has {len(queue_issues)} validation issue(s):"
            )
            for issue in queue_issues:
                logger.warning(f"  - {issue}")
                print(f"  WARNING: {issue}")
            print("  Run 'View processing queues' or 'Edit processing queues' to fix.")

        # Step 6: Load cache
        cache_data = _load_cache(config)

        # Step 7: Retrieve from ArchivesSpace
        # First get all IDs (fast), then retrieve full details for each
        # (slow — each accession requires multiple API calls to resolve
        # linked agents, subjects, classifications, etc.)
        print("  Retrieving accession data from ArchivesSpace...")
        all_ids = as_client.get_all_accession_ids()

        from sync.progress import progress_bar

        accession_details: list[dict] = []
        for acc_id in progress_bar(all_ids, desc="Retrieving full details", unit="accessions"):
            try:
                detail = as_client.get_accession_full_detail(acc_id)
                accession_details.append(detail)
            except Exception as e:
                logger.warning(f"Failed to retrieve accession {acc_id}: {e}")

        # Step 8: Detect changes
        print("  Detecting changes...")
        changes = sync_engine.detect_changes(accession_details, cache_data, current_rows)

        # Step 9: Build row data
        # Transform raw accession data into spreadsheet row dicts,
        # including extent conversion, format detection, and issue evaluation
        base_url = config.get_base_url()
        new_rows = []
        all_new = changes["new"]
        if all_new:
            for detail in progress_bar(all_new, desc="Building new rows", unit="rows"):
                row_data = sync_engine.build_row_data(detail, base_url)
                row_data["_is_new"] = True
                new_rows.append(row_data)

        updated_rows = []
        all_updated = changes["updated"]
        if all_updated:
            for detail in progress_bar(all_updated, desc="Building updated rows", unit="rows"):
                row_data = sync_engine.build_row_data(detail, base_url)
                row_data["_changes"] = detail.get("_changes", [])
                updated_rows.append(row_data)

        had_changes = bool(new_rows or updated_rows or changes["deleted"])

        # Step 10: Summary
        summary = (
            f"{len(changes['new'])} new, {len(changes['updated'])} updated, "
            f"{len(changes['deleted'])} deleted, {len(changes['unchanged'])} unchanged."
        )
        print(f"\n  Sync summary: {summary}")
        logger.summary(f"Sync summary: {summary}")

        # Step 11: Handle dry run
        if dry_run:
            print("\n  DRY RUN — No changes written.")

            # Create preview spreadsheet
            preview_location = _create_preview(
                config, logger, output_format, headers,
                new_rows, updated_rows, changes, current_rows,
            )

            if auto_mode:
                # Create flag file for approval workflow
                _create_preview_flag(config)
                notifications.notify_preview_ready(
                    preview_location,
                    summary,
                    _get_timeout_deadline(config),
                )
            else:
                # Manual mode: prompt to proceed
                print(f"\n  Preview saved to: {preview_location}")
                if Menu.prompt_yes_no("Proceed with sync based on this preview?", default=False):
                    dry_run = False
                    # Fall through to Step 12 (write changes)
                else:
                    print("  Preview retained for review.")
                    logger.end_run(success=True)
                    return True

            if dry_run:
                logger.end_run(success=True)
                return True

        # Step 12 & 13: Write changes and update supporting sheets.
        # Both steps are wrapped in a single transaction so that for the
        # Excel backend the workbook is loaded once and saved once. For
        # the Google Sheets backend, transaction() is a no-op. If any
        # exception is raised inside this block, the transaction exits
        # without saving (Excel) and the caller's except-handler stages
        # the data for a retry on the next run.
        if had_changes:
            print("  Writing changes...")
            try:
                with spreadsheet.transaction():
                    _write_changes(
                        spreadsheet, headers, changes, new_rows, updated_rows,
                        current_rows, sync_engine, cache_data, config, logger, output_format,
                    )
                    print("  Updating supporting sheets...")
                    from sync.supporting_sheets import update_supporting_sheets
                    update_supporting_sheets(
                        spreadsheet, sync_engine, config, logger, output_format,
                    )
                print("  Changes written successfully.")

                # Clean up preview after successful sync
                _cleanup_preview(config, logger)

            except Exception as e:
                # Two-phase failure: save staging data so the next run
                # can complete the write without re-fetching from ArchivesSpace
                logger.error(f"Write failed: {e}")
                _save_staging_data(config, logger, {
                    "new_rows": new_rows,
                    "updated_rows": updated_rows,
                    "deleted": changes["deleted"],
                })
                raise
        else:
            print("  No changes to write.")
            # Even when there are no accession changes, refresh the
            # supporting sheets so trend data stays current.
            print("  Updating supporting sheets...")
            with spreadsheet.transaction():
                from sync.supporting_sheets import update_supporting_sheets
                update_supporting_sheets(
                    spreadsheet, sync_engine, config, logger, output_format,
                )

        # Step 13: Notify about subject descriptor overflow
        for acc_id, total, max_cols in sync_engine.get_overflow_accessions():
            notifications.notify_subject_descriptor_overflow(acc_id, total, max_cols)

        # Step 14: Update cache
        print("  Updating cache...")
        new_cache = {}
        for detail in accession_details:
            accession = detail.get("accession", {})
            aid = str(sync_engine._extract_id(accession))
            new_cache[aid] = sync_engine.build_cache_entry(detail)
        _save_cache(config, new_cache)
        logger.technical(f"Cache updated: {len(new_cache)} entries, "
                         f"size: {_get_cache_size(config)}")

        # Step 15: Complete
        logger.end_run(success=True)
        notifications.notify_sync_success(summary, had_changes=had_changes)
        notifications.retry_pending()
        print("\n  Sync completed successfully.")
        return True

    except UnknownExtentTypeError as e:
        logger.error(str(e))
        if auto_mode:
            notifications.notify_sync_failure(str(e), phase="retrieval")
        else:
            print(f"\n  {e}")
            print("  Please categorize this extent type in the setup wizard or config.")
        logger.end_run(success=False)
        return False

    except Exception as e:
        logger.error(str(e))
        logger.end_run(success=False)
        notifications.notify_sync_failure(str(e), phase="unknown")
        notifications.retry_pending()
        print(f"\n  Sync failed: {e}")
        return False


# -------------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------------

def _init_spreadsheet(config, logger, output_format, auto_mode, backup):
    """Initialize the appropriate spreadsheet manager."""
    if output_format == "excel":
        from sync.excel import ExcelManager
        spreadsheet = ExcelManager(config, logger)
        if spreadsheet.file_exists():
            is_locked, lock_info = spreadsheet.is_file_locked()
            if is_locked:
                if auto_mode:
                    raise RuntimeError(f"Excel file is locked. {lock_info}")
                if not spreadsheet.wait_for_unlock():
                    raise RuntimeError("Excel file remained locked after all retries.")
        return spreadsheet

    elif output_format == "google_sheets":
        from sync.google_sheets import GoogleSheetsManager
        spreadsheet = GoogleSheetsManager(config, logger)
        if not spreadsheet.authenticate():
            raise RuntimeError("Failed to authenticate with Google APIs.")
        return spreadsheet

    raise RuntimeError(f"Unknown output format: {output_format}")


def _ensure_spreadsheet(spreadsheet, validator, expected_headers, output_format, config, logger, backup, auto_mode):
    """Ensure spreadsheet exists and is valid. Create if missing."""
    if output_format == "excel":
        if not spreadsheet.file_exists():
            print("  Creating new Excel spreadsheet...")
            spreadsheet.create_spreadsheet(expected_headers)
        headers, rows = spreadsheet.read_data()
        result = validator.validate(headers)
        if not result.is_valid:
            if auto_mode:
                raise RuntimeError(f"Validation failed: {', '.join(result.errors)}")
            _handle_validation_failure(result, spreadsheet, config, logger, backup)
            headers, rows = spreadsheet.read_data()
        return headers, rows

    elif output_format == "google_sheets":
        if not spreadsheet.spreadsheet_exists():
            print("  Creating new Google Sheet...")
            spreadsheet.create_spreadsheet(expected_headers)
        headers, rows = spreadsheet.read_data()
        result = validator.validate(headers)
        if not result.is_valid:
            if auto_mode:
                raise RuntimeError(f"Validation failed: {', '.join(result.errors)}")
            _handle_validation_failure(result, spreadsheet, config, logger, backup)
            headers, rows = spreadsheet.read_data()
        spreadsheet.verify_permissions()
        return headers, rows

    return expected_headers, []


def _write_changes(spreadsheet, headers, changes, new_rows, updated_rows, current_rows, sync_engine, cache_data, config, logger, output_format):
    """Write all changes to the spreadsheet."""
    from sync.progress import progress_bar

    completion_triggers = config.get("completion_triggers", default=[])

    # Delete removed accessions (process in reverse order to preserve row indices)
    deleted = sorted(changes["deleted"], reverse=True)
    if deleted:
        for del_id in progress_bar(deleted, desc="Deleting removed accessions", unit="rows"):
            row_idx = spreadsheet.find_row_by_accession_id(del_id)
            if row_idx:
                spreadsheet.delete_row(row_idx)
                logger.technical(f"Deleted row for accession {del_id}")

    # Update existing rows
    if updated_rows:
        for row_data in progress_bar(updated_rows, desc="Updating existing rows", unit="rows"):
            acc_id = row_data.get("Accession ID")

            # Check for completion event: did processing_status change
            # to a value that indicates the accession is now completed?
            is_completion = False
            if completion_triggers:
                cached_status = ""
                for row in current_rows:
                    if row.get("accession_id") == acc_id:
                        cached_status = row.get("Accession Status", "")
                        break
                month_completed = sync_engine.check_completion(
                    {"collection_management": {"processing_status": row_data.get("Accession Status", "")}},
                    cached_status,
                    completion_triggers,
                )
                if month_completed:
                    row_data["Month Completed"] = month_completed
                    is_completion = True
                    logger.summary(f"Accession {acc_id} marked completed: {month_completed}")

            row_idx = spreadsheet.find_row_by_accession_id(acc_id)
            if row_idx:
                spreadsheet.update_row(
                    headers, row_data, row_idx,
                    is_completion_event=is_completion,
                )

    # Append new rows
    if new_rows:
        print("  Appending new rows...")
        _, existing = spreadsheet.read_data()
        start_row = len(existing) + 2
        spreadsheet.write_rows(headers, new_rows, start_row=start_row)


def _handle_validation_failure(result, spreadsheet, config, logger, backup):
    """Handle a spreadsheet validation failure interactively."""
    print("\n  Spreadsheet validation failed:")
    for error in result.errors:
        print(f"    - {error}")
    if not Menu.prompt_yes_no("Rebuild the spreadsheet?", default=False):
        raise RuntimeError("Validation failed. User declined rebuild.")

    print("\n  WARNING: Rebuilding will delete all data and custom columns.")
    print("  A backup will be created automatically.")
    if not Menu.prompt_yes_no("Proceed?", default=False):
        raise RuntimeError("Validation failed. User cancelled rebuild.")

    file_ref = spreadsheet.get_file_path()
    if file_ref:
        backup.create_backup(file_ref)
    spreadsheet.clear_data()
    print("  Spreadsheet rebuilt.")


def _check_pending_preview(config, logger, auto_mode) -> str:
    """Check for an unreviewed preview. Returns 'blocked', 'clear', or 'timeout'."""
    preview_dir = Path(config.get("preview", "directory", default="") or "")
    if not preview_dir.exists():
        return "clear"

    flag_files = list(preview_dir.glob("preview_pending_review.flag"))
    if not flag_files:
        return "clear"

    # Check for approval
    approved = list(preview_dir.glob("preview_approved.flag"))
    if approved:
        for f in approved:
            f.unlink(missing_ok=True)
        for f in flag_files:
            f.unlink(missing_ok=True)
        return "clear"

    # Check timeout
    for flag in flag_files:
        try:
            mtime = datetime.fromtimestamp(flag.stat().st_mtime)
            timeout_hours = config.get("preview", "review_timeout_hours", default=72)
            if (datetime.now() - mtime).total_seconds() > timeout_hours * 3600:
                logger.summary(f"Preview review period expired ({timeout_hours}h). Proceeding.")
                for f in flag_files:
                    f.unlink(missing_ok=True)
                return "timeout"
        except (OSError, IOError):
            continue

    if auto_mode:
        return "blocked"

    # Manual mode: inform user
    print("\n  A pending dry run preview has not been reviewed.")
    print("  You can review it from the interactive menu.")
    return "clear"


def _get_timeout_deadline(config) -> str:
    """Get the formatted deadline for preview review timeout."""
    timeout_hours = config.get("preview", "review_timeout_hours", default=72)
    from datetime import timedelta
    deadline = datetime.now() + timedelta(hours=timeout_hours)
    return deadline.strftime("%B %d, %Y at %I:%M %p")


# -------------------------------------------------------------------------
# Cache and staging file management
# -------------------------------------------------------------------------

def _load_cache(config: ConfigManager) -> dict:
    """Load the accession data cache."""
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
    """Save the accession data cache."""
    cache_dir = config.get("cache", "directory", default="")
    if not cache_dir:
        return
    cache_path = Path(cache_dir) / "accession_cache.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, indent=2)


def _get_cache_size(config: ConfigManager) -> str:
    """Get human-readable cache file size."""
    cache_dir = config.get("cache", "directory", default="")
    if not cache_dir:
        return "N/A"
    cache_path = Path(cache_dir) / "accession_cache.json"
    if not cache_path.exists():
        return "N/A"
    size = cache_path.stat().st_size
    if size < 1024:
        return f"{size} bytes"
    elif size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size / (1024 * 1024):.1f} MB"


def _load_staging_data(config: ConfigManager) -> Optional[dict]:
    """Load staging data from a previous interrupted run."""
    log_dir = config.get("logging", "directory", default="")
    if not log_dir:
        return None
    log_path = Path(log_dir)
    if not log_path.exists():
        return None
    staging_files = sorted(log_path.glob("staging_sync_*.json"), reverse=True)
    if not staging_files:
        return None
    try:
        with open(staging_files[0], "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def _save_staging_data(config: ConfigManager, logger: LoggingManager, data: Optional[dict]) -> None:
    """Save data to a staging file for retry on next run."""
    if data is None:
        return
    log_dir = config.get("logging", "directory", default="")
    if not log_dir:
        return
    date_str = datetime.now().strftime("%Y-%m-%d")
    staging_path = Path(log_dir) / f"staging_sync_{date_str}.json"
    try:
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        with open(staging_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        logger.summary(f"Staging data saved to: {staging_path}")
    except (IOError, TypeError) as e:
        logger.error(f"Failed to save staging data: {e}")


def _clear_staging_data(config: ConfigManager) -> None:
    """Remove staging files after successful processing."""
    log_dir = config.get("logging", "directory", default="")
    if not log_dir:
        return
    for staging_file in Path(log_dir).glob("staging_sync_*.json"):
        staging_file.unlink(missing_ok=True)


def _apply_staging_data(
    config: ConfigManager, logger: LoggingManager, staging_data: dict
) -> None:
    """
    Apply previously staged data to the spreadsheet.

    This handles the case where a previous run successfully retrieved data
    from ArchivesSpace but failed during the write phase.
    """
    output_format = config.get_output_format()
    try:
        spreadsheet = _init_spreadsheet(config, logger, output_format, False, BackupManager(config, logger))

        if output_format == "excel":
            headers, existing = spreadsheet.read_data()
        elif output_format == "google_sheets":
            headers, existing = spreadsheet.read_data()
        else:
            return

        new_rows = staging_data.get("new_rows", [])
        updated_rows = staging_data.get("updated_rows", [])
        deleted_ids = staging_data.get("deleted", [])

        # Apply deletions
        for del_id in reversed(sorted(deleted_ids)):
            row_idx = spreadsheet.find_row_by_accession_id(del_id)
            if row_idx:
                spreadsheet.delete_row(row_idx)

        # Apply updates
        for row_data in updated_rows:
            acc_id = row_data.get("Accession ID")
            row_idx = spreadsheet.find_row_by_accession_id(acc_id)
            if row_idx:
                spreadsheet.update_row(headers, row_data, row_idx)

        # Apply new rows
        if new_rows:
            _, current = spreadsheet.read_data()
            start_row = len(current) + 2
            spreadsheet.write_rows(headers, new_rows, start_row=start_row)

        logger.summary(f"Staging data applied: {len(new_rows)} new, {len(updated_rows)} updated, {len(deleted_ids)} deleted.")
        print(f"  Staged data applied successfully.")

    except Exception as e:
        logger.error(f"Failed to apply staging data: {e}")
        print(f"  Warning: Could not apply staged data: {e}")
        print("  The staging file will be retained for the next attempt.")


def _create_preview(
    config: ConfigManager, logger: LoggingManager, output_format: str,
    headers: list[str], new_rows: list[dict], updated_rows: list[dict],
    changes: dict, current_rows: list[dict],
) -> str:
    """
    Create a preview spreadsheet showing proposed changes.

    Returns the path or URL of the preview file.
    """
    date_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    spreadsheet_name = config.get_spreadsheet_name()
    preview_name = f"[Preview] {spreadsheet_name} - {date_str}"

    if output_format == "excel":
        preview_dir = Path(config.get("preview", "directory", default="") or "")
        preview_dir.mkdir(parents=True, exist_ok=True)
        preview_path = preview_dir / f"{preview_name}.xlsx"

        try:
            from sync.excel import ExcelManager
            import shutil

            # Copy the current spreadsheet as the base for the preview
            source_path = Path(config.get("excel", "target_directory", default="")) / f"{spreadsheet_name}.xlsx"
            if source_path.exists():
                shutil.copy2(str(source_path), str(preview_path))
            else:
                # Create a fresh preview
                from sync.validation import SpreadsheetValidator
                validator = SpreadsheetValidator(config, logger)
                # Create a temporary ExcelManager pointing to the preview
                original_dir = config.get("excel", "target_directory")
                config.set("excel", "target_directory", value=str(preview_dir))
                temp_mgr = ExcelManager(config, logger)
                temp_mgr.spreadsheet_name = preview_name
                temp_mgr.file_path = preview_path
                temp_mgr.create_spreadsheet(headers)
                config.set("excel", "target_directory", value=original_dir)

            logger.summary(f"Preview spreadsheet created: {preview_path.name}")
            return str(preview_path)

        except Exception as e:
            logger.warning(f"Could not create preview spreadsheet: {e}")
            return str(preview_path)

    elif output_format == "google_sheets":
        # For Google Sheets, create a copy in the same folder
        try:
            from sync.google_sheets import GoogleSheetsManager
            gs = GoogleSheetsManager(config, logger)
            gs.authenticate()

            spreadsheet_id = gs.get_spreadsheet_id()
            folder_id = config.get("google_sheets", "folder_id", default="")
            drive_service = gs.get_drive_service()

            if spreadsheet_id and drive_service:
                copy_metadata = {"name": preview_name}
                if folder_id:
                    copy_metadata["parents"] = [folder_id]

                import time
                time.sleep(0.5)
                backup = drive_service.files().copy(
                    fileId=spreadsheet_id, body=copy_metadata
                ).execute()

                preview_id = backup.get("id", "")
                preview_url = f"https://docs.google.com/spreadsheets/d/{preview_id}"

                # Apply sharing permissions to preview
                gs._spreadsheet_id = preview_id
                gs.verify_permissions()

                logger.summary(f"Preview Google Sheet created: {preview_name}")
                return preview_url

        except Exception as e:
            logger.warning(f"Could not create Google Sheets preview: {e}")

    return "Preview creation failed — check logs."


def _create_preview_flag(config: ConfigManager) -> None:
    """Create a flag file indicating a preview is pending review."""
    preview_dir = Path(config.get("preview", "directory", default="") or "")
    preview_dir.mkdir(parents=True, exist_ok=True)
    flag_path = preview_dir / "preview_pending_review.flag"
    flag_path.write_text(
        f"Preview created: {datetime.now().isoformat()}\n"
        f"To approve: rename this file to 'preview_approved.flag'\n"
        f"To dismiss: delete this file\n"
    )


def _cleanup_preview(config: ConfigManager, logger: LoggingManager) -> None:
    """Clean up preview files and flags after a successful sync."""
    preview_dir = Path(config.get("preview", "directory", default="") or "")
    if not preview_dir.exists():
        return

    # Remove flag files
    for flag in preview_dir.glob("*.flag"):
        flag.unlink(missing_ok=True)

    # Remove preview spreadsheets based on retention policy
    retention = config.get("preview", "retention", default="until_next_run")
    if retention == "until_next_run":
        for preview in preview_dir.glob("[Preview]*"):
            preview.unlink(missing_ok=True)
            logger.technical(f"Cleaned up preview: {preview.name}")

