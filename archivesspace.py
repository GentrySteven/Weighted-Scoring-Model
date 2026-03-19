"""
Scheduler Module

Handles platform-aware scheduling of automatic sync runs.
Detects the operating system and creates/modifies/removes:
- cron jobs on Linux/macOS
- Task Scheduler entries on Windows
"""

import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager


class SchedulerError(Exception):
    """Raised when a scheduling operation fails."""
    pass


class Scheduler:
    """
    Manages scheduled sync jobs across platforms.

    Supports one active scheduled job at a time.
    """

    TASK_NAME = "ArchivesSpaceAccessionSync"

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        self.config = config
        self.logger = logger
        self.system = platform.system()
        self.script_path = Path(sys.argv[0]).resolve()
        self.python_path = Path(sys.executable).resolve()

    def get_platform(self) -> str:
        """Return the current platform name."""
        return self.system

    def create_job(
        self,
        frequency: str = "weekly",
        time_str: str = "20:00",
        target: str = "excel",
        dry_run: bool = False,
    ) -> bool:
        """
        Create a new scheduled job.

        Args:
            frequency: "daily", "weekly", or "monthly"
            time_str: Time in 24-hour format (e.g., "20:00")
            target: Output format ("excel" or "google_sheets")
            dry_run: If True, schedule a dry run instead of a full sync.

        Returns:
            True if the job was created successfully.
        """
        # Remove any existing job first
        self.remove_job()

        try:
            hour, minute = time_str.split(":")
            hour = int(hour)
            minute = int(minute)
        except (ValueError, AttributeError):
            raise SchedulerError(f"Invalid time format: {time_str}. Use HH:MM (24-hour).")

        flag = "--dry-run" if dry_run else ""
        command = f'"{self.python_path}" "{self.script_path}" --target {target} --auto {flag}'.strip()

        try:
            if self.system in ("Linux", "Darwin"):
                return self._create_cron_job(frequency, hour, minute, command)
            elif self.system == "Windows":
                return self._create_windows_task(frequency, hour, minute, command)
            else:
                raise SchedulerError(f"Unsupported platform: {self.system}")

        except Exception as e:
            self.logger.error(f"Failed to create scheduled job: {e}")
            return False

    def modify_job(
        self,
        frequency: Optional[str] = None,
        time_str: Optional[str] = None,
        target: Optional[str] = None,
        dry_run: Optional[bool] = None,
    ) -> bool:
        """
        Modify the existing scheduled job.

        Any parameter set to None retains the current value.
        """
        current = self.get_job_info()
        if not current:
            self.logger.error("No existing scheduled job to modify.")
            return False

        freq = frequency or current.get("frequency", "weekly")
        time_val = time_str or current.get("time", "20:00")
        tgt = target or current.get("target", "excel")
        dr = dry_run if dry_run is not None else current.get("dry_run", False)

        return self.create_job(frequency=freq, time_str=time_val, target=tgt, dry_run=dr)

    def remove_job(self) -> bool:
        """
        Remove the existing scheduled job.

        Returns:
            True if the job was removed (or didn't exist).
        """
        try:
            if self.system in ("Linux", "Darwin"):
                return self._remove_cron_job()
            elif self.system == "Windows":
                return self._remove_windows_task()
            return True
        except Exception as e:
            self.logger.warning(f"Failed to remove scheduled job: {e}")
            return False

    def get_job_info(self) -> Optional[dict]:
        """
        Get information about the current scheduled job.

        Returns:
            Dictionary with job details, or None if no job exists.
        """
        try:
            if self.system in ("Linux", "Darwin"):
                return self._get_cron_job_info()
            elif self.system == "Windows":
                return self._get_windows_task_info()
            return None
        except Exception:
            return None

    def job_exists(self) -> bool:
        """Check whether a scheduled job currently exists."""
        return self.get_job_info() is not None

    # -------------------------------------------------------------------------
    # Cron (Linux/macOS)
    # -------------------------------------------------------------------------

    def _create_cron_job(self, frequency: str, hour: int, minute: int, command: str) -> bool:
        """Create a cron job."""
        cron_schedule = self._frequency_to_cron(frequency, hour, minute)
        cron_line = f"{cron_schedule} {command} # {self.TASK_NAME}"

        # Get existing crontab
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True
        )
        existing = result.stdout if result.returncode == 0 else ""

        # Remove any existing job for this tool
        lines = [
            line for line in existing.split("\n")
            if self.TASK_NAME not in line and line.strip()
        ]

        # Add new job
        lines.append(cron_line)
        new_crontab = "\n".join(lines) + "\n"

        # Install new crontab
        process = subprocess.Popen(
            ["crontab", "-"], stdin=subprocess.PIPE, text=True
        )
        process.communicate(input=new_crontab)

        if process.returncode == 0:
            self.logger.summary(
                f"Scheduled job created: {frequency} at {hour:02d}:{minute:02d}"
            )
            return True

        return False

    def _remove_cron_job(self) -> bool:
        """Remove the cron job for this tool."""
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True
        )
        if result.returncode != 0:
            return True

        lines = [
            line for line in result.stdout.split("\n")
            if self.TASK_NAME not in line and line.strip()
        ]

        new_crontab = "\n".join(lines) + "\n" if lines else ""

        if new_crontab.strip():
            process = subprocess.Popen(
                ["crontab", "-"], stdin=subprocess.PIPE, text=True
            )
            process.communicate(input=new_crontab)
        else:
            subprocess.run(["crontab", "-r"], capture_output=True)

        self.logger.summary("Scheduled job removed.")
        return True

    def _get_cron_job_info(self) -> Optional[dict]:
        """Get information about the current cron job."""
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True
        )
        if result.returncode != 0:
            return None

        for line in result.stdout.split("\n"):
            if self.TASK_NAME in line:
                parts = line.split()
                if len(parts) >= 5:
                    return {
                        "schedule": " ".join(parts[:5]),
                        "command": " ".join(parts[5:]).replace(f"# {self.TASK_NAME}", "").strip(),
                        "platform": "cron",
                    }

        return None

    def _frequency_to_cron(self, frequency: str, hour: int, minute: int) -> str:
        """Convert a frequency string to a cron schedule."""
        if frequency == "daily":
            return f"{minute} {hour} * * *"
        elif frequency == "weekly":
            return f"{minute} {hour} * * 0"  # Sunday
        elif frequency == "monthly":
            return f"{minute} {hour} 1 * *"  # 1st of month
        else:
            raise SchedulerError(f"Invalid frequency: {frequency}")

    # -------------------------------------------------------------------------
    # Task Scheduler (Windows)
    # -------------------------------------------------------------------------

    def _create_windows_task(
        self, frequency: str, hour: int, minute: int, command: str
    ) -> bool:
        """Create a Windows Task Scheduler task."""
        schedule_type = {
            "daily": "DAILY",
            "weekly": "WEEKLY",
            "monthly": "MONTHLY",
        }.get(frequency, "WEEKLY")

        time_formatted = f"{hour:02d}:{minute:02d}"

        args = [
            "schtasks", "/Create",
            "/TN", self.TASK_NAME,
            "/TR", command,
            "/SC", schedule_type,
            "/ST", time_formatted,
            "/F",  # Force overwrite
        ]

        if frequency == "weekly":
            args.extend(["/D", "SUN"])
        elif frequency == "monthly":
            args.extend(["/D", "1"])

        result = subprocess.run(args, capture_output=True, text=True)

        if result.returncode == 0:
            self.logger.summary(
                f"Windows scheduled task created: {frequency} at {time_formatted}"
            )
            return True

        self.logger.error(f"Failed to create Windows task: {result.stderr}")
        return False

    def _remove_windows_task(self) -> bool:
        """Remove the Windows Task Scheduler task."""
        result = subprocess.run(
            ["schtasks", "/Delete", "/TN", self.TASK_NAME, "/F"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            self.logger.summary("Windows scheduled task removed.")
        return True

    def _get_windows_task_info(self) -> Optional[dict]:
        """Get information about the current Windows task."""
        result = subprocess.run(
            ["schtasks", "/Query", "/TN", self.TASK_NAME, "/FO", "LIST"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            return None

        info = {}
        for line in result.stdout.split("\n"):
            if ":" in line:
                key, _, value = line.partition(":")
                info[key.strip()] = value.strip()

        return {
            "schedule": info.get("Schedule Type", ""),
            "time": info.get("Start Time", ""),
            "command": info.get("Task To Run", ""),
            "platform": "task_scheduler",
        }
