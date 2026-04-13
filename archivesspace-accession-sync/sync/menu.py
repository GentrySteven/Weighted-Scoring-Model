"""
Interactive Menu Module

Provides a persistent, format-adaptive interactive menu with:
- Sub-menu categories for organized navigation
- Help system (inline labels, help command, context-sensitive descriptions)
- [Info] tags on read-only options
- Guided first-use tour
- Configurable confirmations for action options
"""

from typing import Callable

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager


# Menu structure: category -> list of (key, label, description, action_key, info_only)
MENU_STRUCTURE = {
    "Sync Operations": [
        ("1", "Sync", "Run a full synchronization with ArchivesSpace", "sync", False),
        ("2", "Dry run", "Preview changes without writing to the spreadsheet", "dry_run", False),
        ("3", "Review pending preview", "Review and approve or dismiss a pending dry run", "review_preview", False),
    ],
    "Scheduling": [
        ("4", "Manage sync schedule", "Create, modify, or remove the sync schedule", "schedule_sync", False),
        ("5", "Manage dry run schedule", "Create, modify, or remove the dry run schedule", "schedule_dry_run", False),
    ],
    "Data & Vocabulary Management": [
        ("6", "Scan for format keywords", "Detect material types from repository data", "scan_formats", False),
        ("7", "Scan for subject descriptors", "Find subjects and agents on accession records", "scan_subjects", False),
        ("8", "Scan for issue terms", "Build structured vocabularies for issue columns", "scan_issues", False),
        ("9", "Manage extent conversions", "Update physical/digital categories and factors", "manage_extents", False),
        ("10", "Manage vocabularies", "Edit structured vocabularies for issue columns", "manage_vocabs", False),
        ("11", "Manage dropdown options", "Configure dropdown options for manual columns", "manage_dropdowns", False),
        ("12", "Manage completion triggers", "Set which statuses indicate processing is complete", "manage_triggers", False),
        ("13", "Manage subject descriptors", "Curate the approved subject descriptors list", "manage_subjects", False),
        ("14", "View scoring criteria [Info]", "View current scoring dimensions, thresholds, and weights", "view_scoring", True),
        ("15", "Edit scoring criteria", "Modify scoring thresholds, weights, or add dimensions", "edit_scoring", False),
        ("16", "View processing queues [Info]", "View configured processing queues and status groups", "view_queues", True),
        ("17", "Edit processing queues", "Add, remove, or modify processing queues and grouping", "edit_queues", False),
    ],
    "File & Storage Management": [
        ("18", "Manage preview files", "View, delete, or clear preview spreadsheets", "manage_previews", False),
        ("19", "View backup history", "See existing backups", "view_backups", True),
        ("20", "View cache status", "Check cache file size and last update", "view_cache", True),
        ("21", "Clear cache", "Delete the cached accession data", "clear_cache", False),
    ],
    "Logging": [
        ("22", "View recent log entries", "Browse the most recent log output", "view_logs", True),
        ("23", "View log storage info", "Check total log size and file count", "view_log_storage", True),
        ("24", "Trigger log consolidation", "Manually consolidate log files now", "consolidate_logs", False),
    ],
    "Administration": [
        ("25", "Reconfigure settings", "Re-run the setup wizard", "reconfigure", False),
        ("26", "Verify configuration", "Check that all settings are valid", "verify_config", True),
        ("27", "Last sync status", "View the result of the most recent sync", "last_sync", True),
        ("28", "Check for updates", "Check GitHub for a newer version", "check_updates", True),
        ("29", "Show menu tour", "Re-run the guided tour of all menu options", "show_tour", False),
    ],
}

# Help text for each action (detailed descriptions for the help command)
HELP_TEXT = {
    "sync": (
        "Runs a full synchronization: connects to ArchivesSpace, retrieves\n"
        "  accession data, compares against the spreadsheet, and writes changes.\n"
        "  New accessions are added, changed accessions are refreshed, and\n"
        "  deleted accessions are removed."
    ),
    "dry_run": (
        "Performs all the steps of a full sync except the final write.\n"
        "  Creates a preview spreadsheet showing what would change.\n"
        "  You can then approve or dismiss the preview."
    ),
    "review_preview": (
        "If a dry run has created a pending preview, this option lets you\n"
        "  view it and choose to approve (run the sync) or dismiss it."
    ),
    "verify_config": (
        "Checks all configuration settings for validity — ArchivesSpace\n"
        "  connection details, output format settings, directory paths,\n"
        "  and credential files. Reports any issues found."
    ),
}


class Menu:
    """
    Persistent interactive menu with sub-categories, help, and tour support.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        self.config = config
        self.logger = logger
        self.output_format = config.get_output_format()
        self.show_confirms = config.show_confirmations()
        self._actions: dict[str, Callable] = {}
        self._conditional_visible: dict[str, Callable[[], bool]] = {}

    def register_action(self, key: str, action: Callable) -> None:
        """Register a callable action for a menu option."""
        self._actions[key] = action

    def register_conditional(self, key: str, condition: Callable[[], bool]) -> None:
        """Register a visibility condition for a menu option."""
        self._conditional_visible[key] = condition

    def display(self) -> None:
        """Display the main menu in a persistent loop."""
        while True:
            self._print_header()
            self._print_categories()

            choice = input("\n  Enter choice (or 'help', 'tour', 'q' to quit): ").strip()

            if choice.lower() in ("q", "quit", "exit"):
                print("\n  Goodbye!\n")
                break

            if choice.lower() in ("help", "?"):
                self._show_help()
                continue

            if choice.lower().startswith("help "):
                self._show_help_for(choice[5:].strip())
                continue

            if choice.lower() == "tour":
                self._run_tour()
                continue

            self._handle_choice(choice)

    def _print_header(self) -> None:
        """Print the menu header."""
        format_label = "Excel" if self.output_format == "excel" else "Google Sheets"
        print("\n" + "=" * 60)
        print("  archivesspace-accession-sync")
        print(f"  Output format: {format_label}")
        print("=" * 60)

    def _print_categories(self) -> None:
        """Print menu options organized by category."""
        for category, options in MENU_STRUCTURE.items():
            visible_options = [
                opt for opt in options
                if self._is_visible(opt[3])
            ]
            if not visible_options:
                continue

            print(f"\n  {category}")
            print("  " + "─" * 40)
            for key, label, desc, action_key, info_only in visible_options:
                tag = " [Info]" if info_only else ""
                print(f"  {key:>3}. {label}{tag} — {desc}")

        print(f"\n    q. Quit")

    def _is_visible(self, action_key: str) -> bool:
        """Check if a menu option should be visible."""
        if action_key in self._conditional_visible:
            return self._conditional_visible[action_key]()
        return True

    def _handle_choice(self, choice: str) -> None:
        """Route a menu choice to the appropriate action."""
        # Find the action key for this choice number
        action_key = None
        action_info_only = False
        for _category, options in MENU_STRUCTURE.items():
            for key, _label, _desc, ak, info_only in options:
                if key == choice:
                    action_key = ak
                    action_info_only = info_only
                    break

        if not action_key:
            print(f"\n  Invalid choice: {choice}")
            input("\n  Press Enter to continue...")
            return

        if action_key not in self._actions:
            print(f"\n  This feature is not yet implemented.")
            input("\n  Press Enter to continue...")
            return

        # Show confirmation for non-info actions if enabled
        if not action_info_only and self.show_confirms:
            desc = HELP_TEXT.get(action_key, "")
            if desc:
                print(f"\n  {desc}")
                if not self.prompt_yes_no("Proceed?"):
                    input("\n  Press Enter to continue...")
                    return

        print()
        try:
            self._actions[action_key]()
        except KeyboardInterrupt:
            print("\n\n  Action cancelled.")
        except Exception as e:
            print(f"\n  Error: {e}")
            self.logger.error(str(e))

        input("\n  Press Enter to return to the menu...")

    def _show_help(self) -> None:
        """Show complete help for all options."""
        print("\n  " + "=" * 56)
        print("  HELP — All Menu Options")
        print("  " + "=" * 56)

        for category, options in MENU_STRUCTURE.items():
            print(f"\n  {category}")
            print("  " + "─" * 40)
            for key, label, desc, action_key, info_only in options:
                tag = " [Info]" if info_only else ""
                print(f"\n  {key}. {label}{tag}")
                detailed = HELP_TEXT.get(action_key, desc)
                for line in detailed.split("\n"):
                    print(f"     {line.strip()}")

        print("\n  Type 'help <number>' for details on a specific option.")
        input("\n  Press Enter to continue...")

    def _show_help_for(self, query: str) -> None:
        """Show help for a specific option."""
        for _category, options in MENU_STRUCTURE.items():
            for key, label, desc, action_key, info_only in options:
                if key == query or action_key == query or label.lower().startswith(query.lower()):
                    tag = " [Info]" if info_only else ""
                    print(f"\n  {key}. {label}{tag}")
                    detailed = HELP_TEXT.get(action_key, desc)
                    for line in detailed.split("\n"):
                        print(f"     {line.strip()}")
                    input("\n  Press Enter to continue...")
                    return

        print(f"\n  No help found for: {query}")
        input("\n  Press Enter to continue...")

    def _run_tour(self) -> None:
        """Run the guided tour of all menu options."""
        print("\n  " + "=" * 56)
        print("  GUIDED TOUR — archivesspace-accession-sync")
        print("  " + "=" * 56)
        print("\n  This tour will walk you through each menu category and")
        print("  explain what each option does. You can skip any section.")

        # Check wizard completion
        if not self.config.config_exists():
            print("\n  NOTE: The setup wizard has not been completed yet.")
            print("  It's recommended to complete setup before exploring the menu.")
            if self.prompt_yes_no("Run the setup wizard now?"):
                if "reconfigure" in self._actions:
                    self._actions["reconfigure"]()
                return

        phases = [
            ("Phase 1 — Getting Started", ["verify_config", "reconfigure", "manage_extents", "manage_triggers"]),
            ("Phase 2 — Building Vocabularies", ["scan_formats", "scan_subjects", "scan_issues", "manage_vocabs", "manage_dropdowns", "manage_subjects"]),
            ("Phase 3 — Running Your First Sync", ["dry_run", "review_preview", "sync", "last_sync"]),
            ("Phase 4 — Ongoing Operations", ["schedule_sync", "schedule_dry_run", "check_updates"]),
            ("Phase 5 — Monitoring & Maintenance", ["view_logs", "view_log_storage", "consolidate_logs", "view_backups", "view_cache", "clear_cache", "manage_previews"]),
        ]

        for phase_name, action_keys in phases:
            print(f"\n  {'=' * 56}")
            print(f"  {phase_name}")
            print(f"  {'=' * 56}")

            if not self.prompt_yes_no("Continue with this section?"):
                continue

            for action_key in action_keys:
                # Find the option details
                for _category, options in MENU_STRUCTURE.items():
                    for key, label, desc, ak, info_only in options:
                        if ak == action_key:
                            tag = " [Info]" if info_only else ""
                            print(f"\n  Option {key}: {label}{tag}")
                            detailed = HELP_TEXT.get(ak, desc)
                            print(f"  {detailed}")

                            if ak in self._actions and not info_only:
                                if self.prompt_yes_no("Try this now?", default=False):
                                    try:
                                        self._actions[ak]()
                                    except Exception as e:
                                        print(f"  Error: {e}")

                            input("  Press Enter to continue...")
                            break

        print("\n  Tour complete! You can re-run it anytime by typing 'tour'.")
        self.config.set("ui", "tour_completed", value=True)
        self.config.save_config()
        input("\n  Press Enter to return to the menu...")

    # -------------------------------------------------------------------------
    # Static prompt helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def prompt_yes_no(question: str, default: bool = True) -> bool:
        """Prompt for a yes/no answer."""
        suffix = " [Y/n]: " if default else " [y/N]: "
        while True:
            answer = input(f"  {question}{suffix}").strip().lower()
            if not answer:
                return default
            if answer in ("y", "yes"):
                return True
            if answer in ("n", "no"):
                return False
            print("  Please enter 'y' or 'n'.")

    @staticmethod
    def prompt_choice(question: str, options: list[str]) -> int:
        """Prompt to choose from a list. Returns 0-indexed selection."""
        print(f"\n  {question}")
        for idx, option in enumerate(options, 1):
            print(f"    {idx}. {option}")
        while True:
            try:
                choice = int(input("\n  Enter your choice: ").strip())
                if 1 <= choice <= len(options):
                    return choice - 1
            except ValueError:
                pass
            print(f"  Please enter a number between 1 and {len(options)}.")

    @staticmethod
    def prompt_text(question: str, default: str = "") -> str:
        """Prompt for text input with optional default."""
        if default:
            answer = input(f"  {question} [{default}]: ").strip()
            return answer if answer else default
        return input(f"  {question}: ").strip()
