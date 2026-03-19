"""
Interactive Menu Module

Provides a persistent, format-adaptive interactive menu for the tool.
Shows only options relevant to the configured output format.
"""

import sys
from typing import Callable, Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager


class Menu:
    """
    Persistent interactive menu that adapts to the configured output format.
    Returns to the main screen after each action.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        self.config = config
        self.logger = logger
        self.output_format = config.get_output_format()
        self._actions: dict[str, Callable] = {}

    def register_action(self, key: str, action: Callable) -> None:
        """Register a callable action for a menu option."""
        self._actions[key] = action

    def display(self) -> None:
        """Display the main menu and handle user interaction in a persistent loop."""
        while True:
            self._print_header()
            self._print_options()

            choice = input("\n  Enter your choice: ").strip()

            if choice.lower() in ("q", "quit", "exit"):
                print("\n  Goodbye!\n")
                break

            self._handle_choice(choice)

    def _print_header(self) -> None:
        """Print the menu header."""
        format_label = "Excel" if self.output_format == "excel" else "Google Sheets"
        print("\n" + "=" * 60)
        print("  archivesspace-accession-sync")
        print(f"  Output format: {format_label}")
        print("=" * 60)

    def _print_options(self) -> None:
        """Print menu options based on the configured output format."""
        format_name = "Excel" if self.output_format == "excel" else "Google Sheets"

        print("\n  Sync Operations")
        print("  ─────────────────────────────────────")
        print(f"  1. Sync to {format_name}")
        print("  2. Dry run (preview changes)")

        print("\n  Scheduling")
        print("  ─────────────────────────────────────")
        print("  3. Set up a new scheduled job")
        print("  4. Modify existing scheduled job")
        print("  5. Remove scheduled job")

        print("\n  Scanning & Vocabulary")
        print("  ─────────────────────────────────────")
        print("  6. Scan repository for format keywords")
        print("  7. Scan repository for subject descriptors")
        print("  8. Scan repository for issue terms")

        print("\n  Administration")
        print("  ─────────────────────────────────────")
        print("  9.  Reconfigure settings")
        print("  10. Verify current configuration")
        print("  11. Check status of last sync")
        print("  12. View recent log entries")
        print("  13. Check for updates")

        print("\n  q. Quit")

    def _handle_choice(self, choice: str) -> None:
        """Route a menu choice to the appropriate action."""
        action_map = {
            "1": "sync",
            "2": "dry_run",
            "3": "schedule_create",
            "4": "schedule_modify",
            "5": "schedule_remove",
            "6": "scan_formats",
            "7": "scan_subjects",
            "8": "scan_issues",
            "9": "reconfigure",
            "10": "verify_config",
            "11": "last_sync_status",
            "12": "view_logs",
            "13": "check_updates",
        }

        action_key = action_map.get(choice)
        if action_key and action_key in self._actions:
            print()
            try:
                self._actions[action_key]()
            except KeyboardInterrupt:
                print("\n\n  Action cancelled.")
            except Exception as e:
                print(f"\n  Error: {e}")
                self.logger.error(str(e))
        elif action_key:
            print(f"\n  Action '{action_key}' is not yet implemented.")
        else:
            print(f"\n  Invalid choice: {choice}")

        input("\n  Press Enter to return to the menu...")

    @staticmethod
    def prompt_yes_no(question: str, default: bool = True) -> bool:
        """
        Prompt the user for a yes/no answer.

        Args:
            question: The question to ask.
            default: Default answer if user just presses Enter.

        Returns:
            True for yes, False for no.
        """
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
        """
        Prompt the user to choose from a list of options.

        Args:
            question: The question to ask.
            options: List of option strings.

        Returns:
            0-indexed selection.
        """
        print(f"\n  {question}")
        for idx, option in enumerate(options, 1):
            print(f"    {idx}. {option}")

        while True:
            try:
                choice = int(input("\n  Enter your choice: ").strip())
                if 1 <= choice <= len(options):
                    return choice - 1
                print(f"  Please enter a number between 1 and {len(options)}.")
            except ValueError:
                print("  Please enter a valid number.")

    @staticmethod
    def prompt_text(question: str, default: str = "") -> str:
        """
        Prompt the user for text input.

        Args:
            question: The question to ask.
            default: Default value shown in brackets.

        Returns:
            The user's input, or the default if empty.
        """
        if default:
            answer = input(f"  {question} [{default}]: ").strip()
            return answer if answer else default
        else:
            return input(f"  {question}: ").strip()
