"""
CLI Entry Point

Main entry point for the archivesspace-accession-sync tool.
Handles CLI argument parsing, routes to interactive menu or direct
execution, and delegates the actual sync pipeline to sync.runner.
Action handlers for the interactive menu live in sync.actions, and
supporting-sheet computation lives in sync.supporting_sheets.

All external library imports are deferred to the functions that need
them to prevent import failures from blocking the setup wizard.
"""

import argparse
from pathlib import Path

from sync import __version__
from sync.config_manager import ConfigManager, ConfigError
from sync.logging_manager import LoggingManager
from sync.menu import Menu
from sync.runner import run_sync


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Synchronize ArchivesSpace accession metadata to Excel or Google Sheets.",
        prog="accession-sync",
    )
    parser.add_argument("--target", choices=["excel", "google_sheets"], help="Output format")
    parser.add_argument("--auto", action="store_true", help="Automatic mode (non-interactive)")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes only")
    parser.add_argument("--config", type=str, default=None, help="Path to config.yml")
    parser.add_argument("--credentials", type=str, default=None, help="Path to credentials.yml")
    parser.add_argument("--data", type=str, default=None, help="Path to data.yml")
    parser.add_argument("--version", action="version", version=f"%(prog)s v{__version__}")
    return parser.parse_args()


def check_for_updates(config: ConfigManager, logger: LoggingManager) -> None:
    """Check GitHub for available updates on startup. Non-blocking."""
    try:
        from sync.updater import Updater
        updater = Updater(config, logger)
        updater.check_and_notify_on_startup()
    except Exception:
        pass  # Update check is not critical


# -------------------------------------------------------------------------
# Interactive menu
# -------------------------------------------------------------------------

def run_interactive(config: ConfigManager, logger: LoggingManager) -> None:
    """Launch the interactive menu."""
    # Action handlers live in sync.actions; imported here to avoid
    # circular-import hazards at module load time.
    from sync.actions import (
        _action_clear_cache, _action_consolidate_logs,
        _action_edit_processing_queues, _action_edit_scoring_criteria,
        _action_last_sync_status, _action_manage_dropdowns,
        _action_manage_extents, _action_manage_previews,
        _action_manage_schedule, _action_manage_subject_list,
        _action_manage_triggers, _action_manage_vocabularies,
        _action_reconfigure, _action_review_preview,
        _action_verify_config, _action_view_backups,
        _action_view_cache, _action_view_log_storage,
        _action_view_logs, _action_view_processing_queues,
        _action_view_scoring_criteria, _has_pending_preview,
    )

    menu = Menu(config, logger)
    scheduler_mod = None
    try:
        from sync.scheduler import Scheduler
        scheduler_mod = Scheduler(config, logger)
    except Exception:
        pass

    # Sync operations
    menu.register_action("sync", lambda: run_sync(config, logger))
    menu.register_action("dry_run", lambda: run_sync(config, logger, dry_run=True))
    menu.register_action("review_preview", lambda: _action_review_preview(config, logger))

    # Scheduling
    if scheduler_mod:
        menu.register_action("schedule_sync", lambda: _action_manage_schedule(config, logger, scheduler_mod, dry_run=False))
        menu.register_action("schedule_dry_run", lambda: _action_manage_schedule(config, logger, scheduler_mod, dry_run=True))

    # Data & vocabulary management (scanning framework)
    from sync.scanning import ScanningFramework
    scanner = ScanningFramework(config, logger)
    menu.register_action("scan_formats", lambda: scanner.scan_menu("formats"))
    menu.register_action("scan_subjects", lambda: scanner.scan_menu("subjects"))
    menu.register_action("scan_issues", lambda: scanner.scan_menu("issues"))
    menu.register_action("manage_extents", lambda: _action_manage_extents(config, logger))
    menu.register_action("manage_vocabs", lambda: _action_manage_vocabularies(config, logger))
    menu.register_action("manage_dropdowns", lambda: _action_manage_dropdowns(config))
    menu.register_action("manage_triggers", lambda: _action_manage_triggers(config, logger))
    menu.register_action("manage_subjects", lambda: _action_manage_subject_list(config, logger))
    menu.register_action("view_scoring", lambda: _action_view_scoring_criteria(config))
    menu.register_action("edit_scoring", lambda: _action_edit_scoring_criteria(config, logger))
    menu.register_action("view_queues", lambda: _action_view_processing_queues(config))
    menu.register_action("edit_queues", lambda: _action_edit_processing_queues(config, logger))

    # Administration
    from sync.updater import Updater
    updater = Updater(config, logger)
    menu.register_action("verify_config", lambda: _action_verify_config(config))
    menu.register_action("last_sync", lambda: _action_last_sync_status(logger))
    menu.register_action("view_logs", lambda: _action_view_logs(logger))
    menu.register_action("check_updates", lambda: updater.run_update_interactive())
    menu.register_action("view_log_storage", lambda: _action_view_log_storage(logger))
    menu.register_action("consolidate_logs", lambda: _action_consolidate_logs(logger))
    menu.register_action("reconfigure", lambda: _action_reconfigure(config))

    # File management
    menu.register_action("manage_previews", lambda: _action_manage_previews(config, logger))
    menu.register_action("view_backups", lambda: _action_view_backups(config))
    menu.register_action("view_cache", lambda: _action_view_cache(config))
    menu.register_action("clear_cache", lambda: _action_clear_cache(config, logger))

    # Conditional visibility for pending preview
    menu.register_conditional(
        "review_preview",
        lambda: _has_pending_preview(config),
    )

    # Offer tour on first run
    if not config.tour_completed():
        print("\n  Welcome! It looks like this is your first time using the menu.")
        if Menu.prompt_yes_no("Would you like a guided tour of the menu options?"):
            menu._run_tour()

    menu.display()


# -------------------------------------------------------------------------
# Main entry point
# -------------------------------------------------------------------------

def main():
    """Main entry point."""
    args = parse_args()

    config = ConfigManager(
        config_path=args.config,
        credentials_path=args.credentials,
        data_path=getattr(args, "data", None),
    )

    # First run detection
    if not config.config_exists():
        print("\n  Welcome to archivesspace-accession-sync!\n")
        print("  No configuration file found. You can either:")
        print("  1. Run the guided setup wizard")
        print("  2. Create template files for manual configuration\n")

        choice = Menu.prompt_choice("Choose:", ["Guided setup wizard", "Create template files"])
        if choice == 0:
            # Create minimal templates first so wizard can load them
            _create_templates(config)
            config.load()

            from sync.wizard import SetupWizard
            wizard = SetupWizard(config)
            if wizard.run(rerun=False):
                config.load()  # Reload after wizard changes
            else:
                print("\n  Wizard was not completed. Run the tool again to resume.")
                return
        else:
            _create_templates(config)
            print(f"\n  Config: {config.config_path}")
            print(f"  Credentials: {config.credentials_path}")
            print(f"  Data: {config.data_path}")
            print("  Edit these files and run again.\n")
            return

    # Load configuration
    try:
        config.load()
    except ConfigError as e:
        print(f"\n  Configuration error: {e}\n")
        sys.exit(1)

    if args.target:
        config.set("output", "format", value=args.target)

    config.ensure_directories()
    logger = LoggingManager(config)

    check_for_updates(config, logger)

    # Route to mode
    if args.auto:
        success = run_sync(config, logger, dry_run=args.dry_run, auto_mode=True)
        sys.exit(0 if success else 1)
    elif args.target or args.dry_run:
        success = run_sync(config, logger, dry_run=args.dry_run)
        sys.exit(0 if success else 1)
    else:
        run_interactive(config, logger)


def _create_templates(config: ConfigManager) -> None:
    """Create template configuration files."""
    import shutil

    template_dir = Path(__file__).resolve().parent.parent

    for src_name, dest_path in [
        ("config.yml", config.config_path),
        ("credentials_example.yml", config.credentials_path),
        ("data_example.yml", config.data_path),
    ]:
        src = template_dir / src_name
        if src.exists() and not dest_path.exists():
            shutil.copy2(str(src), str(dest_path))

    print("  Template files created.")


if __name__ == "__main__":
    main()
