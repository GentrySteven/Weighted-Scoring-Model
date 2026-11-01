"""
Updater Module

Handles version checking against GitHub releases and automatic
update execution. Distinguishes between regular and critical updates.
Falls back to displaying manual commands if automatic execution fails.
"""

import json
import platform
import subprocess
import sys
import urllib.request
from typing import Optional

from sync import __version__
from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.menu import Menu


GITHUB_REPO = "GentrySteven/Weighted-Scoring-Model"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"


class UpdateInfo:
    """Information about an available update."""

    def __init__(
        self,
        current_version: str,
        latest_version: str,
        is_critical: bool,
        release_notes: str,
        release_url: str,
    ):
        self.current_version = current_version
        self.latest_version = latest_version
        self.is_critical = is_critical
        self.release_notes = release_notes
        self.release_url = release_url
        self.is_newer = self._compare_versions()

    def _compare_versions(self) -> bool:
        """Check if the latest version is newer than the current version."""
        try:
            current_parts = [int(p) for p in self.current_version.split(".")]
            latest_parts = [int(p) for p in self.latest_version.split(".")]
            return latest_parts > current_parts
        except (ValueError, AttributeError):
            return self.latest_version != self.current_version


class Updater:
    """
    Manages update checking and execution.

    Checks the GitHub API for the latest release, compares versions,
    and can automatically execute update commands with user confirmation.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        self.config = config
        self.logger = logger
        self.python_path = sys.executable
        self.project_root = config.project_root

    def check_for_updates(self, silent: bool = False) -> Optional[UpdateInfo]:
        """
        Check GitHub for available updates.

        Args:
            silent: If True, only log results without printing to terminal.
                    Used for startup checks.

        Returns:
            UpdateInfo if an update is available, None otherwise.
        """
        try:
            req = urllib.request.Request(
                GITHUB_API_URL,
                headers={"User-Agent": "archivesspace-accession-sync"},
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())

            latest_version = data.get("tag_name", "").lstrip("v")
            if not latest_version:
                return None

            release_notes = data.get("body", "")
            release_url = data.get("html_url", "")

            # Determine if critical
            notes_lower = release_notes.lower()
            is_critical = any(
                keyword in notes_lower
                for keyword in ["security", "critical", "vulnerability", "urgent", "cve"]
            )

            info = UpdateInfo(
                current_version=__version__,
                latest_version=latest_version,
                is_critical=is_critical,
                release_notes=release_notes,
                release_url=release_url,
            )

            if info.is_newer:
                self.logger.technical(
                    f"Update available: v{latest_version} "
                    f"(current: v{__version__}, critical: {is_critical})"
                )

                if not silent:
                    self._display_update_notice(info)

                return info

            if not silent:
                print(f"\n  You are running the latest version (v{__version__}).")

            return None

        except urllib.error.URLError as e:
            if not silent:
                print(f"\n  Could not check for updates: network error.")
                self.logger.technical(f"Update check network error: {e}")
            return None

        except Exception as e:
            if not silent:
                print(f"\n  Could not check for updates: {e}")
                self.logger.technical(f"Update check error: {e}")
            return None

    def check_and_notify_on_startup(self) -> None:
        """
        Perform a silent update check on startup.
        Only prints a notice if an update is available.
        """
        info = self.check_for_updates(silent=True)
        if info and info.is_newer:
            self._display_update_notice(info)

            # Send email for critical updates
            if info.is_critical:
                try:
                    from sync.notifications import NotificationManager
                    nm = NotificationManager(self.config, self.logger)
                    nm.notify_critical_update(info.latest_version)
                except Exception:
                    pass

    def run_update_interactive(self) -> bool:
        """
        Check for updates interactively and offer to apply them.

        Returns:
            True if an update was successfully applied.
        """
        print("\n  Checking for updates...")
        info = self.check_for_updates(silent=False)

        if not info or not info.is_newer:
            return False

        # Show release notes summary
        if info.release_notes:
            print(f"\n  Release notes (summary):")
            # Show first 5 lines of release notes
            lines = info.release_notes.strip().split("\n")
            for line in lines[:5]:
                print(f"    {line}")
            if len(lines) > 5:
                print(f"    ... ({len(lines) - 5} more lines)")
            if info.release_url:
                print(f"\n  Full release notes: {info.release_url}")

        # Offer to update
        if Menu.prompt_yes_no("\n  Apply this update now?", default=True):
            return self._execute_update(info)

        # Show manual commands as alternative
        print("\n  To update manually, run these commands:")
        self._print_manual_commands()
        return False

    def _display_update_notice(self, info: UpdateInfo) -> None:
        """Display an update notice in the terminal."""
        if info.is_critical:
            print(f"\n  {'!' * 56}")
            print(f"  ⚠ CRITICAL UPDATE AVAILABLE: v{info.latest_version}")
            print(f"  This update includes security patches.")
            print(f"  Please update promptly.")
            print(f"  {'!' * 56}")
        else:
            print(f"\n  Update available: v{info.latest_version} (current: v{__version__})")
            print(f"  Select 'Check for updates' from the menu to apply.")

    def _execute_update(self, info: UpdateInfo) -> bool:
        """
        Attempt to execute the update automatically.

        Steps:
        1. git pull to get the latest code
        2. pip install to update dependencies
        3. Verify the new version

        Falls back to manual commands on any failure.
        """
        print("\n  Applying update...")

        # Step 1: git pull
        print("  Step 1/3: Pulling latest code...")
        success, output = self._run_command(
            ["git", "pull", "origin", "main"],
            cwd=str(self.project_root),
        )

        if not success:
            print(f"  ⚠ git pull failed.")
            print(f"  Output: {output}")
            print("\n  Falling back to manual update instructions:")
            self._print_manual_commands()
            return False

        print(f"    {output.strip()}")

        # Step 2: pip install
        print("  Step 2/3: Updating dependencies...")
        output_format = self.config.get_output_format()
        extras = "excel" if output_format == "excel" else "google"
        pip_cmd = [self.python_path, "-m", "pip", "install", f".[{extras}]"]

        success, output = self._run_command(pip_cmd, cwd=str(self.project_root))

        if not success:
            print(f"  ⚠ pip install failed.")
            print(f"  Output: {output}")
            print("\n  The code has been updated but dependencies may need manual attention:")
            self._print_pip_command(extras)
            return False

        # Show only the last few lines of pip output
        pip_lines = output.strip().split("\n")
        for line in pip_lines[-3:]:
            print(f"    {line}")

        # Step 3: Verify
        print("  Step 3/3: Verifying update...")
        success, output = self._run_command(
            [self.python_path, "-c", "from sync import __version__; print(__version__)"],
            cwd=str(self.project_root),
        )

        if success:
            new_version = output.strip()
            if new_version == info.latest_version:
                print(f"\n  Update successful! Now running v{new_version}.")
                self.logger.summary(f"Updated from v{info.current_version} to v{new_version}.")
                print("  Please restart the tool for changes to take full effect.")
                return True
            else:
                print(f"  Version after update: v{new_version}")
                print(f"  Expected: v{info.latest_version}")
                print("  The update may require a restart to fully apply.")
                return True
        else:
            print("  Could not verify the update, but files were updated.")
            print("  Restart the tool to apply changes.")
            return True

    def _run_command(
        self, cmd: list[str], cwd: Optional[str] = None
    ) -> tuple[bool, str]:
        """
        Run a shell command and return (success, output).

        Args:
            cmd: Command and arguments.
            cwd: Working directory.

        Returns:
            Tuple of (success, combined stdout+stderr output).
        """
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
                cwd=cwd,
            )
            output = result.stdout + result.stderr
            return result.returncode == 0, output

        except subprocess.TimeoutExpired:
            return False, "Command timed out after 120 seconds."

        except FileNotFoundError:
            cmd_name = cmd[0] if cmd else "unknown"
            return False, f"Command not found: {cmd_name}"

        except Exception as e:
            return False, str(e)

    def _print_manual_commands(self) -> None:
        """Print manual update commands for the user's platform."""
        output_format = self.config.get_output_format()
        extras = "excel" if output_format == "excel" else "google"

        print(f"\n  Run these commands in your terminal:")
        print(f"  {'─' * 45}")

        system = platform.system()
        if system == "Windows":
            print(f"    cd {self.project_root}")
            print(f"    git pull origin main")
            print(f"    pip install .[{extras}]")
        else:
            print(f"    cd {self.project_root}")
            print(f"    git pull origin main")
            print(f"    pip install .[{extras}]")

        print(f"\n  Then restart the tool.")

    def _print_pip_command(self, extras: str) -> None:
        """Print just the pip install command."""
        print(f"\n  Run this command:")
        print(f"    {self.python_path} -m pip install .[{extras}]")
