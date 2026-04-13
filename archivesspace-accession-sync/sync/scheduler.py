"""
Scheduler Module

Platform-aware scheduling supporting two simultaneous jobs:
one sync and one dry run. Includes timing buffer validation.

Platform detection:
- Linux/macOS: Uses crontab entries with comment-based identification
- Windows: Uses Windows Task Scheduler via schtasks.exe

Each job type (sync vs dry run) gets a distinct task name so they can
be created, modified, and removed independently. The timing buffer
check warns (but doesn't block) if the two jobs are scheduled too
close together, since large repositories may have runs that overlap.
"""

import platform
import subprocess
import sys
from pathlib import Path
from typing import Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.utils import parse_time_string


class SchedulerError(Exception):
    """Raised when a scheduling operation fails."""
    pass


class Scheduler:
    """
    Manages scheduled sync and dry run jobs across platforms.
    Supports one sync job and one dry run job simultaneously.
    """

    TASK_NAME_SYNC = "ArchivesSpaceAccessionSync"
    TASK_NAME_DRY_RUN = "ArchivesSpaceAccessionSyncDryRun"

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        self.config = config
        self.logger = logger
        self.system = platform.system()
        self.script_path = Path(sys.argv[0]).resolve()
        self.python_path = Path(sys.executable).resolve()
        self.buffer_minutes = config.get("scheduling", "buffer_minutes", default=60)

    def _task_name(self, dry_run: bool = False) -> str:
        """Get the task name for the specified job type."""
        return self.TASK_NAME_DRY_RUN if dry_run else self.TASK_NAME_SYNC

    def create_job(
        self, frequency: str = "weekly", time_str: str = "20:00",
        target: str = "excel", dry_run: bool = False,
    ) -> bool:
        """Create a new scheduled job."""
        # Remove existing job of this type
        self.remove_job(dry_run=dry_run)

        hour, minute = parse_time_string(time_str)

        # Check timing buffer against the other job
        self._check_timing_buffer(hour, minute, dry_run)

        flag = "--dry-run" if dry_run else ""
        command = (
            f'"{self.python_path}" "{self.script_path}" '
            f'--target {target} --auto {flag}'.strip()
        )

        task_name = self._task_name(dry_run)
        job_type = "dry run" if dry_run else "sync"

        try:
            if self.system in ("Linux", "Darwin"):
                success = self._create_cron_job(frequency, hour, minute, command, task_name)
            elif self.system == "Windows":
                success = self._create_windows_task(
                    frequency, hour, minute, command, task_name
                )
            else:
                raise SchedulerError(f"Unsupported platform: {self.system}")

            if success:
                self.logger.summary(
                    f"Scheduled {job_type} job created: {frequency} at {hour:02d}:{minute:02d}"
                )
            return success

        except SchedulerError:
            raise
        except Exception as e:
            self.logger.error(f"Failed to create scheduled job: {e}")
            return False

    def modify_job(
        self, frequency: Optional[str] = None, time_str: Optional[str] = None,
        target: Optional[str] = None, dry_run: bool = False,
    ) -> bool:
        """Modify an existing scheduled job. None values retain current settings."""
        current = self.get_job_info(dry_run=dry_run)
        if not current:
            self.logger.error("No existing scheduled job to modify.")
            return False

        freq = frequency or current.get("frequency", "weekly")
        time_val = time_str or current.get("time", "20:00")
        tgt = target or current.get("target", "excel")

        return self.create_job(frequency=freq, time_str=time_val, target=tgt, dry_run=dry_run)

    def remove_job(self, dry_run: bool = False) -> bool:
        """Remove the specified scheduled job."""
        task_name = self._task_name(dry_run)
        try:
            if self.system in ("Linux", "Darwin"):
                return self._remove_cron_job(task_name)
            elif self.system == "Windows":
                return self._remove_windows_task(task_name)
            return True
        except Exception as e:
            self.logger.warning(f"Failed to remove scheduled job: {e}")
            return False

    def get_job_info(self, dry_run: bool = False) -> Optional[dict]:
        """Get information about a scheduled job."""
        task_name = self._task_name(dry_run)
        try:
            if self.system in ("Linux", "Darwin"):
                return self._get_cron_job_info(task_name)
            elif self.system == "Windows":
                return self._get_windows_task_info(task_name)
            return None
        except Exception:
            return None

    def job_exists(self, dry_run: bool = False) -> bool:
        """Check whether a scheduled job exists."""
        return self.get_job_info(dry_run=dry_run) is not None

    def get_both_jobs_info(self) -> dict[str, Optional[dict]]:
        """Get info on both sync and dry run jobs."""
        return {
            "sync": self.get_job_info(dry_run=False),
            "dry_run": self.get_job_info(dry_run=True),
        }

    def _check_timing_buffer(self, hour: int, minute: int, is_dry_run: bool) -> None:
        """
        Check that the proposed job time has sufficient buffer from the other job.
        Warns but does not block if buffer is insufficient.
        """
        other_info = self.get_job_info(dry_run=not is_dry_run)
        if not other_info or "time" not in other_info:
            return

        try:
            other_hour, other_minute = parse_time_string(other_info["time"])
        except ValueError:
            return

        proposed_minutes = hour * 60 + minute
        other_minutes = other_hour * 60 + other_minute
        gap = abs(proposed_minutes - other_minutes)
        # Account for wrapping around midnight
        gap = min(gap, 1440 - gap)

        if gap < self.buffer_minutes:
            self.logger.warning(
                f"The scheduled jobs are only {gap} minutes apart "
                f"(recommended buffer: {self.buffer_minutes} minutes). "
                f"Runs may overlap for large repositories."
            )

    # -------------------------------------------------------------------------
    # Cron (Linux/macOS)
    # -------------------------------------------------------------------------

    def _create_cron_job(
        self, frequency: str, hour: int, minute: int, command: str, task_name: str
    ) -> bool:
        """
        Create a cron job by appending to the user's crontab.

        Jobs are identified by a trailing comment (# TaskName) so they
        can be found, modified, and removed independently without
        affecting other cron entries.
        """
        cron_schedule = self._frequency_to_cron(frequency, hour, minute)
        # Append task name as a comment for identification
        cron_line = f"{cron_schedule} {command} # {task_name}"

        # Read existing crontab, filter out any old entry for this task
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        existing = result.stdout if result.returncode == 0 else ""

        lines = [l for l in existing.split("\n") if task_name not in l and l.strip()]
        lines.append(cron_line)

        # Write the updated crontab via stdin pipe
        process = subprocess.Popen(["crontab", "-"], stdin=subprocess.PIPE, text=True)
        process.communicate(input="\n".join(lines) + "\n")
        return process.returncode == 0

    def _remove_cron_job(self, task_name: str) -> bool:
        """Remove a cron job by filtering out lines containing the task name."""
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if result.returncode != 0:
            return True  # No crontab = nothing to remove
        lines = [l for l in result.stdout.split("\n") if task_name not in l and l.strip()]
        new_crontab = "\n".join(lines) + "\n" if lines else ""
        if new_crontab.strip():
            process = subprocess.Popen(["crontab", "-"], stdin=subprocess.PIPE, text=True)
            process.communicate(input=new_crontab)
        else:
            # No entries left, remove the entire crontab
            subprocess.run(["crontab", "-r"], capture_output=True)
        return True

    def _get_cron_job_info(self, task_name: str) -> Optional[dict]:
        """Parse crontab to find info about a specific job."""
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if result.returncode != 0:
            return None
        for line in result.stdout.split("\n"):
            if task_name in line:
                parts = line.split()
                if len(parts) >= 5:
                    return {
                        "schedule": " ".join(parts[:5]),
                        "command": " ".join(parts[5:]).replace(f"# {task_name}", "").strip(),
                        "time": f"{parts[1]}:{parts[0]}",
                        "platform": "cron",
                    }
        return None

    def _frequency_to_cron(self, frequency: str, hour: int, minute: int) -> str:
        if frequency == "daily":
            return f"{minute} {hour} * * *"
        elif frequency == "weekly":
            return f"{minute} {hour} * * 0"
        elif frequency == "monthly":
            return f"{minute} {hour} 1 * *"
        raise SchedulerError(f"Invalid frequency: {frequency}")

    # -------------------------------------------------------------------------
    # Task Scheduler (Windows)
    # -------------------------------------------------------------------------

    def _create_windows_task(
        self, frequency: str, hour: int, minute: int, command: str, task_name: str
    ) -> bool:
        """
        Create a Windows Task Scheduler entry via schtasks.exe.

        Uses the /F flag to force-overwrite any existing task with the
        same name. Weekly tasks run on Sundays, monthly on the 1st.
        """
        schedule_type = {"daily": "DAILY", "weekly": "WEEKLY", "monthly": "MONTHLY"}.get(
            frequency, "WEEKLY"
        )
        args = [
            "schtasks", "/Create", "/TN", task_name, "/TR", command,
            "/SC", schedule_type, "/ST", f"{hour:02d}:{minute:02d}", "/F",
        ]
        if frequency == "weekly":
            args.extend(["/D", "SUN"])  # Run on Sundays
        elif frequency == "monthly":
            args.extend(["/D", "1"])  # Run on the 1st

        result = subprocess.run(args, capture_output=True, text=True)
        return result.returncode == 0

    def _remove_windows_task(self, task_name: str) -> bool:
        """Remove a Windows Task Scheduler entry. /F suppresses confirmation."""
        subprocess.run(
            ["schtasks", "/Delete", "/TN", task_name, "/F"],
            capture_output=True, text=True,
        )
        return True

    def _get_windows_task_info(self, task_name: str) -> Optional[dict]:
        """Query Task Scheduler for info about a specific task."""
        result = subprocess.run(
            ["schtasks", "/Query", "/TN", task_name, "/FO", "LIST"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            return None
        info: dict[str, str] = {}
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
