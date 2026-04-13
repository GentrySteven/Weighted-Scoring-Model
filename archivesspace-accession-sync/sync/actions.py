"""
Menu Action Handlers

Contains the `_action_*` functions that are bound to interactive menu
options. These handle the user-facing workflows for viewing, editing,
and managing configuration, data, logs, schedules, and other
subsystems.

Each action handler accepts a `ConfigManager` and (where needed) a
`LoggingManager`, and interacts with the user through the `Menu`
helpers. Handlers are registered in `sync.cli.run_interactive` via
`menu.register_action()`.

These were previously defined in sync/cli.py but were extracted here
to keep that module focused on entry points and argument parsing.
"""

from datetime import datetime
from pathlib import Path

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.menu import Menu


def _action_verify_config(config: ConfigManager) -> None:
    """Verify and display configuration status."""
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
    print(f"  Email: {'enabled' if config.is_email_configured() else 'disabled'}")


def _action_last_sync_status(logger: LoggingManager) -> None:
    status = logger.get_last_run_status()
    if not status:
        print("  No sync runs found.")
        return
    print(f"  Last sync: {status['timestamp'].strftime('%B %d, %Y %I:%M %p')}")
    for entry in status["entries"][:10]:
        print(f"    {entry}")


def _action_view_logs(logger: LoggingManager) -> None:
    entries = logger.get_recent_entries(count=30)
    if not entries:
        print("  No log entries found.")
        return
    print("  Recent log entries:\n")
    for entry in entries:
        print(f"    {entry.rstrip()}")


def _action_view_log_storage(logger: LoggingManager) -> None:
    info = logger.get_storage_info()
    print(f"  Total log storage: {info['total_mb']} MB")
    print(f"  Number of log files: {info['file_count']}")
    print(f"  Threshold: {info['threshold_mb']} MB")
    if info.get("exceeds_threshold"):
        print("  ⚠ Storage exceeds threshold!")


def _action_consolidate_logs(logger: LoggingManager) -> None:
    """Manually trigger log consolidation."""
    print("  Running log consolidation...")
    logger._check_consolidation()
    print("  Consolidation complete.")

    # Check and process retention
    expired = logger.check_retention()
    if expired:
        print(f"  Found {len(expired)} log(s) exceeding retention period.")
        logger.archive_logs(expired)
        print(f"  Archived {len(expired)} log(s).")

    deleted = logger.clean_archive()
    if deleted:
        print(f"  Permanently deleted {len(deleted)} archived log(s).")


def _action_review_preview(config: ConfigManager, logger: LoggingManager) -> None:
    """Review a pending dry run preview and approve or dismiss."""
    preview_dir = Path(config.get("preview", "directory", default="") or "")
    if not preview_dir.exists():
        print("  No preview directory found.")
        return

    previews = list(preview_dir.glob("[Preview]*"))
    flags = list(preview_dir.glob("preview_pending_review.flag"))

    if not previews and not flags:
        print("  No pending preview found.")
        return

    if previews:
        print(f"  Pending preview: {previews[0].name}")
        print(f"  Location: {previews[0]}")

    choice = Menu.prompt_choice(
        "What would you like to do?",
        ["Approve and run sync now", "Dismiss (clear the preview)", "Cancel (leave for later)"],
    )

    if choice == 0:
        # Approve: remove flag, run sync
        for f in flags:
            f.unlink(missing_ok=True)
        print("\n  Preview approved. Running sync...")
        run_sync(config, logger)
    elif choice == 1:
        # Dismiss
        for f in flags:
            f.unlink(missing_ok=True)
        for p in previews:
            p.unlink(missing_ok=True)
        print("  Preview dismissed.")
    else:
        print("  Preview retained for later review.")


def _action_manage_schedule(
    config: ConfigManager, logger: LoggingManager,
    scheduler, dry_run: bool = False,
) -> None:
    """Manage a scheduled job (create, modify, or remove)."""
    job_type = "dry run" if dry_run else "sync"
    info = scheduler.get_job_info(dry_run=dry_run)

    if info:
        print(f"  Current {job_type} job: {info.get('schedule', 'unknown')} at {info.get('time', 'unknown')}")
        choice = Menu.prompt_choice(
            f"Manage {job_type} schedule:",
            ["Modify", "Remove", "Cancel"],
        )
        if choice == 0:
            freq_idx = Menu.prompt_choice("Frequency:", ["Daily", "Weekly", "Monthly"])
            freq = ["daily", "weekly", "monthly"][freq_idx]
            time_str = Menu.prompt_text("Time (HH:MM):", default="20:00")
            if scheduler.modify_job(frequency=freq, time_str=time_str, dry_run=dry_run):
                print(f"\n  {job_type.title()} schedule modified.")
        elif choice == 1:
            if scheduler.remove_job(dry_run=dry_run):
                print(f"\n  {job_type.title()} schedule removed.")
    else:
        print(f"  No {job_type} schedule configured.")
        if Menu.prompt_yes_no(f"Create a {job_type} schedule?"):
            freq_idx = Menu.prompt_choice(
                "Frequency (evening hours recommended):",
                ["Daily", "Weekly", "Monthly"],
            )
            freq = ["daily", "weekly", "monthly"][freq_idx]
            time_str = Menu.prompt_text("Time (HH:MM):", default="20:00")
            target = config.get_output_format()
            if scheduler.create_job(freq, time_str, target, dry_run=dry_run):
                print(f"\n  {job_type.title()} schedule created: {freq} at {time_str}")


def _action_manage_previews(config: ConfigManager, logger: LoggingManager) -> None:
    """View and manage preview files."""
    preview_dir = Path(config.get("preview", "directory", default="") or "")
    if not preview_dir.exists():
        print("  No preview directory found.")
        return

    previews = sorted(preview_dir.glob("[Preview]*"))
    flags = list(preview_dir.glob("*.flag"))

    if not previews and not flags:
        print("  No preview files found.")
        return

    print(f"  Preview files ({len(previews)}):")
    for p in previews:
        size = p.stat().st_size / 1024
        print(f"    {p.name} ({size:.1f} KB)")

    if flags:
        print(f"\n  Flag files ({len(flags)}):")
        for f in flags:
            print(f"    {f.name}")

    if Menu.prompt_yes_no("Clear all preview files?", default=False):
        for p in previews:
            p.unlink(missing_ok=True)
        for f in flags:
            f.unlink(missing_ok=True)
        print("  All preview files cleared.")


def _action_view_backups(config: ConfigManager) -> None:
    """View existing backup files."""
    output_format = config.get_output_format()

    if output_format == "excel":
        target_dir = Path(config.get("excel", "target_directory", default="") or "")
        spreadsheet_name = config.get_spreadsheet_name()

        # Check alongside original
        backups = sorted(target_dir.glob(f"[Backup]*{spreadsheet_name}*"))

        # Check in backup folder
        backup_folder = target_dir / f"[Backups] {spreadsheet_name}"
        if backup_folder.exists():
            backups.extend(sorted(backup_folder.glob("*")))

        if not backups:
            print("  No backups found.")
            return

        print(f"  Found {len(backups)} backup(s):")
        for b in backups:
            size = b.stat().st_size / 1024
            print(f"    {b.name} ({size:.1f} KB)")

    elif output_format == "google_sheets":
        print("  Google Sheets backup listing requires Drive API access.")
        print("  Check your Google Drive backup folder directly.")


def _action_view_cache(config: ConfigManager) -> None:
    """View cache file status."""
    cache_dir = config.get("cache", "directory", default="")
    if not cache_dir:
        print("  No cache directory configured.")
        return

    cache_path = Path(cache_dir) / "accession_cache.json"
    if not cache_path.exists():
        print("  No cache file found. Run a sync to create one.")
        return

    size = cache_path.stat().st_size
    from datetime import datetime as dt
    mtime = dt.fromtimestamp(cache_path.stat().st_mtime)

    if size < 1024:
        size_str = f"{size} bytes"
    elif size < 1024 * 1024:
        size_str = f"{size / 1024:.1f} KB"
    else:
        size_str = f"{size / (1024 * 1024):.1f} MB"

    # Count entries
    try:
        with open(cache_path) as f:
            data = json.load(f)
        num_entries = len(data)
    except Exception:
        num_entries = "unknown"

    print(f"  Cache file: {cache_path.name}")
    print(f"  Size: {size_str}")
    print(f"  Last updated: {mtime.strftime('%B %d, %Y %I:%M %p')}")
    print(f"  Accession entries: {num_entries}")


def _action_clear_cache(config: ConfigManager, logger: LoggingManager) -> None:
    """Clear the accession data cache."""
    cache_dir = config.get("cache", "directory", default="")
    if not cache_dir:
        print("  No cache directory configured.")
        return

    cache_path = Path(cache_dir) / "accession_cache.json"
    if not cache_path.exists():
        print("  No cache file to clear.")
        return

    if Menu.prompt_yes_no("Clear the cache? The next sync will re-retrieve all data."):
        cache_path.unlink()
        logger.summary("Cache cleared by user.")
        print("  Cache cleared. Next sync will perform a full retrieval.")


def _has_pending_preview(config: ConfigManager) -> bool:
    """Check if there's a pending preview for the menu conditional."""
    preview_dir = Path(config.get("preview", "directory", default="") or "")
    if not preview_dir.exists():
        return False
    return bool(list(preview_dir.glob("preview_pending_review.flag")))


def _action_reconfigure(config: ConfigManager) -> None:
    """Launch the setup wizard in re-run mode."""
    from sync.wizard import SetupWizard
    wizard = SetupWizard(config)
    wizard.run(rerun=True)
    config.load()
    print("\n  Configuration updated. Changes take effect on the next sync.")


def _action_manage_extents(config: ConfigManager, logger: LoggingManager) -> None:
    """View and manage extent type categorizations."""
    extent_types = config.get_data("extent_types", default={}) or {}

    if not extent_types:
        print("  No extent types configured.")
        print("  Run the setup wizard or scan your repository to configure them.")
        return

    print(f"\n  Configured extent types ({len(extent_types)}):")
    print(f"  {'─' * 55}")
    print(f"  {'Type':<25s} {'Category':<12s} {'Factor'}")
    print(f"  {'─' * 55}")
    for ext_type, info in sorted(extent_types.items()):
        cat = info.get("category", "?")
        factor = info.get("conversion_factor", "?")
        print(f"  {ext_type:<25s} {cat:<12s} {factor}")

    if Menu.prompt_yes_no("\nModify an extent type?", default=False):
        type_name = Menu.prompt_text("Extent type to modify:")
        if type_name in extent_types:
            cat_idx = Menu.prompt_choice("Category:", ["Physical", "Digital"])
            category = "physical" if cat_idx == 0 else "digital"
            unit = "linear feet" if category == "physical" else "gigabytes"
            factor_str = Menu.prompt_text(f"Conversion factor (1 {type_name} = ? {unit}):")
            try:
                factor = float(factor_str)
            except ValueError:
                factor = 1.0
            extent_types[type_name] = {"category": category, "conversion_factor": factor}
            config.set_data("extent_types", value=extent_types)
            config.save_data()
            print(f"  Updated: {type_name} = {category}, factor {factor}")
        else:
            print(f"  Type '{type_name}' not found.")


def _action_manage_vocabularies(config: ConfigManager, logger: LoggingManager) -> None:
    """View and manage structured vocabularies for issue columns."""
    vocab_keys = {
        "Access Issues": "access_issues_vocabulary",
        "Conservation Issues": "conservation_issues_vocabulary",
        "Digital Issues": "digital_issues_vocabulary",
        "Other Processing Information": "other_processing_options",
        "Physical Space Management Issues": "physical_space_options",
    }

    columns = list(vocab_keys.keys())
    choice = Menu.prompt_choice("Which column's vocabulary?", columns)
    column = columns[choice]
    key = vocab_keys[column]

    terms = config.get_data(key, default=[]) or []

    print(f"\n  Vocabulary for '{column}' ({len(terms)} terms):")
    for idx, term in enumerate(terms, 1):
        print(f"    {idx}. {term}")

    action = Menu.prompt_choice(
        "Action:",
        ["Add a term", "Remove a term", "Run a scan to discover terms", "Done"],
    )

    if action == 0:
        new_term = Menu.prompt_text("New term:")
        if new_term and new_term not in terms:
            terms.append(new_term)
            config.set_data(key, value=terms)
            config.save_data()
            print(f"  Added: {new_term}")

    elif action == 1:
        if terms:
            del_idx = Menu.prompt_choice("Remove which term?", terms)
            removed = terms.pop(del_idx)
            config.set_data(key, value=terms)
            config.save_data()
            print(f"  Removed: {removed}")

    elif action == 2:
        from sync.scanning import ScanningFramework
        scanner = ScanningFramework(config, logger)
        result = scanner.run_guided_scan(target_column=column)
        if result and result.term_counts:
            approved = scanner.present_results(result)
            if approved:
                merge = Menu.prompt_yes_no("Merge with existing terms?")
                scanner.apply_results_to_vocabulary(approved, column, merge)


def _action_manage_dropdowns(config: ConfigManager) -> None:
    """Manage dropdown options for manual selection columns."""
    dropdown_keys = {
        "Documentation and Use Issues": "documentation_use_issues_options",
        "Kind of Processing Project": "processing_project_types",
    }

    columns = list(dropdown_keys.keys())
    choice = Menu.prompt_choice("Which dropdown?", columns)
    column = columns[choice]
    key = dropdown_keys[column]

    options = config.get_data(key, default=[]) or []

    print(f"\n  Options for '{column}' ({len(options)}):")
    for idx, opt in enumerate(options, 1):
        print(f"    {idx}. {opt}")

    action = Menu.prompt_choice("Action:", ["Add an option", "Remove an option", "Done"])

    if action == 0:
        new_opt = Menu.prompt_text("New option:")
        if new_opt and new_opt not in options:
            options.append(new_opt)
            config.set_data(key, value=options)
            config.save_data()
            print(f"  Added: {new_opt}")

    elif action == 1:
        if options:
            del_idx = Menu.prompt_choice("Remove which option?", options)
            removed = options.pop(del_idx)
            config.set_data(key, value=options)
            config.save_data()
            print(f"  Removed: {removed}")


def _action_manage_triggers(config: ConfigManager, logger: LoggingManager) -> None:
    """Manage completion trigger values."""
    triggers = config.get_data("completion_triggers", default=[]) or []

    print(f"\n  Completion triggers ({len(triggers)}):")
    if triggers:
        for t in triggers:
            print(f"    - {t}")
    else:
        print("    None configured.")

    # Try to fetch available statuses
    print("\n  Checking ArchivesSpace for available statuses...")
    statuses: list[str] = []
    try:
        from sync.archivesspace import ArchivesSpaceClient
        client = ArchivesSpaceClient(config, logger)
        if client.connect():
            statuses = client.get_processing_statuses()
    except Exception:
        pass

    if statuses:
        print("  Available statuses:")
        for s in statuses:
            marker = " [trigger]" if s in triggers else ""
            print(f"    {s}{marker}")

        if Menu.prompt_yes_no("Update triggers from this list?"):
            new_triggers: list[str] = []
            for status in statuses:
                is_trigger = status in triggers
                if Menu.prompt_yes_no(f"  '{status}' means completed?", default=is_trigger):
                    new_triggers.append(status)
            config.set_data("completion_triggers", value=new_triggers)
            config.save_data()
            print(f"  Updated: {len(new_triggers)} trigger(s) configured.")
    else:
        action = Menu.prompt_choice("Action:", ["Add a trigger value", "Remove a trigger", "Done"])
        if action == 0:
            new_val = Menu.prompt_text("Status value:")
            if new_val and new_val not in triggers:
                triggers.append(new_val)
                config.set_data("completion_triggers", value=triggers)
                config.save_data()
        elif action == 1 and triggers:
            del_idx = Menu.prompt_choice("Remove which trigger?", triggers)
            triggers.pop(del_idx)
            config.set_data("completion_triggers", value=triggers)
            config.save_data()


def _action_manage_subject_list(config: ConfigManager, logger: LoggingManager) -> None:
    """Manage the approved subject descriptors list."""
    print("\n  The approved subject descriptors list determines which subjects")
    print("  and agents appear in the Subject Descriptor columns.")
    print("\n  This list is stored in a hidden sheet within the spreadsheet")
    print("  and can be updated by scanning your repository.")

    choice = Menu.prompt_choice(
        "Action:",
        [
            "Scan repository for subjects and agents",
            "View current configuration",
            "Cancel",
        ],
    )

    if choice == 0:
        from sync.scanning import ScanningFramework
        scanner = ScanningFramework(config, logger)
        scanner.scan_menu("subjects")

    elif choice == 1:
        num_cols = config.get("subject_descriptors", "num_columns", default=9)
        print(f"\n  Subject Descriptor columns: {num_cols}")
        print("  Approved list: managed in the spreadsheet's hidden sheet.")
        print("  Run a scan to discover and approve subjects from your repository.")


def _action_view_scoring_criteria(config: ConfigManager) -> None:
    """Display the current scoring criteria configuration."""
    criteria = config.get_data("scoring_criteria", default={})
    dimensions = criteria.get("dimensions", {})

    print("\n  === Current Scoring Criteria ===\n")

    if not dimensions:
        print("  No scoring criteria configured.")
        print("  Run option 15 (Edit scoring criteria) or re-run the setup wizard.")
        return

    total_weight = 0.0
    max_score = 0.0

    for key, dim in dimensions.items():
        label = dim.get("label", key)
        category = dim.get("category", "N/A")
        weight = dim.get("weight", 0)
        scoring_type = dim.get("scoring_type", "")
        total_weight += weight

        print(f"  {label}")
        print(f"    Category: {category}")
        print(f"    Weight:   {weight} ({weight * 100:.0f}%)")
        print(f"    Type:     {scoring_type}")

        if scoring_type in ("date_range", "count_range"):
            thresholds = dim.get("thresholds", [])
            print(f"    Levels:")
            dim_max = 0
            for t in thresholds:
                score = t.get("score", 0)
                dim_max = max(dim_max, score)
                print(f"      {score}: {t.get('label', '')}")
            max_score += dim_max * weight
        elif scoring_type == "value_map":
            mappings = dim.get("mappings", [])
            print(f"    Mappings:")
            dim_max = 0
            for m in mappings:
                score = m.get("score", 0)
                dim_max = max(dim_max, score)
                print(f"      '{m.get('value', '')}' → {score}")
            max_score += dim_max * weight
        print()

    print(f"  Total weight: {total_weight:.2f} (must equal 1.0)")
    print(f"  Score range:  ~1.0 to {max_score:.2f}")

    # Validation
    from sync.validation import SpreadsheetValidator
    from sync.logging_manager import LoggingManager
    validator = SpreadsheetValidator(config, LoggingManager(config))
    issues = validator.validate_scoring_criteria()
    if issues:
        print("\n  Validation issues:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print("\n  Configuration is valid.")


def _action_edit_scoring_criteria(config: ConfigManager, logger: LoggingManager) -> None:
    """Edit the scoring criteria using the wizard's scoring phase."""
    print("\n  Editing scoring criteria...")
    print("  Note: Changes will take effect on the next sync.")
    print("  The scoring criteria sheet will be regenerated automatically.\n")

    from sync.wizard import SetupWizard
    wizard = SetupWizard(config)
    wizard._phase_scoring(rerun=True)
    config.save_data()
    print("\n  Scoring criteria saved.")


def _action_view_processing_queues(config: ConfigManager) -> None:
    """Display the current processing queue and Backlog At a Glance configuration."""
    pq = config.get_data("processing_queue", default={})
    bag = config.get_data("backlog_at_a_glance", default={})

    print("\n  === Processing Queue Configuration ===\n")

    queues = pq.get("queues", [])
    if not queues:
        print("  No processing queues configured.")
    else:
        for i, queue in enumerate(queues, 1):
            print(f"  Queue {i}: {queue.get('name', 'Unnamed')}")
            statuses = queue.get("status_values", [])
            print(f"    Status filter: {', '.join(statuses) if statuses else '(none)'}")
            print(f"    Grouping field: {queue.get('grouping_field', 'Donor Name')}")
            print(f"    View mode: {queue.get('view_mode', 'indented')}")
            print()

    print("  === Backlog At a Glance Status Groups ===\n")
    status_groups = bag.get("status_groups", [])
    if not status_groups:
        print("  No status groups configured.")
    else:
        for group in status_groups:
            label = group.get("label", "Unnamed")
            statuses = group.get("status_values", [])
            shows_projects = group.get("show_project_count", False)
            project_marker = " (with project count)" if shows_projects else ""
            print(f"    {label}{project_marker}")
            print(f"      Includes: {', '.join(statuses) if statuses else '(none)'}")
        print()


def _action_edit_processing_queues(
    config: ConfigManager, logger: LoggingManager,
) -> None:
    """
    Edit processing queue configuration.

    Walks the user through:
    - Adding/removing/modifying queues
    - Setting status filters, grouping field, view mode for each queue
    - Configuring Backlog At a Glance status groups
    """
    from sync.menu import Menu

    print("\n  === Edit Processing Queues ===\n")

    pq = config.get_data("processing_queue", default={})
    queues = list(pq.get("queues", []))

    while True:
        # Display current queues
        print("\n  Current queues:")
        if not queues:
            print("    (none)")
        else:
            for i, q in enumerate(queues, 1):
                print(
                    f"    {i}. {q.get('name', 'Unnamed')} "
                    f"(statuses: {', '.join(q.get('status_values', []))})"
                )

        choice = Menu.prompt_choice(
            "\n  Action:",
            [
                "Add a new queue",
                "Modify an existing queue",
                "Remove a queue",
                "Edit Backlog At a Glance status groups",
                "Save and exit",
            ],
        )

        if choice == 0:
            # Add new queue
            name = Menu.prompt_text("\n  Queue name:", default="New Queue")
            statuses_str = Menu.prompt_text(
                "  Accession statuses (comma-separated):",
                default="Backlog - General",
            )
            statuses = [s.strip() for s in statuses_str.split(",") if s.strip()]
            grouping = Menu.prompt_text(
                "  Grouping field (column name):", default="Donor Name"
            )
            view = Menu.prompt_choice(
                "  View mode:", ["Indented (sub-rows)", "Flat (one row per accession)"]
            )
            queues.append({
                "name": name,
                "status_values": statuses,
                "grouping_field": grouping,
                "view_mode": "indented" if view == 0 else "flat",
            })
            print(f"\n  Added queue '{name}'.")

        elif choice == 1 and queues:
            # Modify existing
            idx = Menu.prompt_choice(
                "  Which queue?", [q.get("name", "?") for q in queues]
            )
            q = queues[idx]
            q["name"] = Menu.prompt_text("  Queue name:", default=q.get("name", ""))
            statuses_str = Menu.prompt_text(
                "  Statuses (comma-separated):",
                default=", ".join(q.get("status_values", [])),
            )
            q["status_values"] = [s.strip() for s in statuses_str.split(",") if s.strip()]
            q["grouping_field"] = Menu.prompt_text(
                "  Grouping field:", default=q.get("grouping_field", "Donor Name")
            )
            view = Menu.prompt_choice(
                "  View mode:", ["Indented", "Flat"]
            )
            q["view_mode"] = "indented" if view == 0 else "flat"
            print(f"\n  Updated queue '{q['name']}'.")

        elif choice == 2 and queues:
            # Remove
            idx = Menu.prompt_choice(
                "  Which queue to remove?", [q.get("name", "?") for q in queues]
            )
            removed = queues.pop(idx)
            print(f"\n  Removed queue '{removed.get('name', '?')}'.")

        elif choice == 3:
            # Edit Backlog At a Glance status groups
            _edit_backlog_at_a_glance_groups(config)

        elif choice == 4:
            # Save and exit
            pq["queues"] = queues
            config.set_data("processing_queue", value=pq)
            config.save_data()
            print("\n  Processing queue configuration saved.")
            print("  Changes will take effect on the next sync.")
            return


def _edit_backlog_at_a_glance_groups(config: ConfigManager) -> None:
    """Sub-handler for editing Backlog At a Glance status groups."""
    from sync.menu import Menu

    bag = config.get_data("backlog_at_a_glance", default={})
    groups = list(bag.get("status_groups", []))

    while True:
        print("\n  Current status groups:")
        for i, g in enumerate(groups, 1):
            shows = " *projects shown" if g.get("show_project_count") else ""
            print(f"    {i}. {g.get('label', '?')}{shows}")

        choice = Menu.prompt_choice(
            "\n  Action:",
            [
                "Add a status group",
                "Modify an existing group",
                "Remove a group",
                "Save and return",
            ],
        )

        if choice == 0:
            label = Menu.prompt_text("\n  Group label:", default="New Group")
            statuses_str = Menu.prompt_text(
                "  Statuses (comma-separated):", default=""
            )
            statuses = [s.strip() for s in statuses_str.split(",") if s.strip()]
            show_projects = Menu.prompt_yes_no(
                "  Show 'Processing Projects Remaining' count?", default=False
            )
            groups.append({
                "label": label,
                "status_values": statuses,
                "show_project_count": show_projects,
            })

        elif choice == 1 and groups:
            idx = Menu.prompt_choice(
                "  Which group?", [g.get("label", "?") for g in groups]
            )
            g = groups[idx]
            g["label"] = Menu.prompt_text("  Label:", default=g.get("label", ""))
            statuses_str = Menu.prompt_text(
                "  Statuses (comma-separated):",
                default=", ".join(g.get("status_values", [])),
            )
            g["status_values"] = [s.strip() for s in statuses_str.split(",") if s.strip()]
            g["show_project_count"] = Menu.prompt_yes_no(
                "  Show project count?", default=g.get("show_project_count", False)
            )

        elif choice == 2 and groups:
            idx = Menu.prompt_choice(
                "  Which group to remove?", [g.get("label", "?") for g in groups]
            )
            groups.pop(idx)

        elif choice == 3:
            bag["status_groups"] = groups
            config.set_data("backlog_at_a_glance", value=bag)
            config.save_data()
            print("\n  Status groups saved.")
            return

