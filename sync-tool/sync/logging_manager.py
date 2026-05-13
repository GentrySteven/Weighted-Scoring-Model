"""
Logging Manager

Handles creation, consolidation, retention, and viewing of log files.
Produces two types of logs per run:
  - Summary log: Human-readable entries (tagged [SUMMARY])
  - Technical log: Detailed entries for troubleshooting (tagged [TECHNICAL])

Log consolidation chain:
  Individual run files (summary_YYYY-MM-DD_HHMMSS.log)
    -> consolidated_YYYY-WNN.log  (weekly, after grace period)
    -> consolidated_YYYY-MM.log   (monthly, after the month ends)
    -> consolidated_YYYY.log      (yearly, after the year ends — final tier)

At each tier, source files are merged into a single file and then deleted.
The grace period (default 3 days) prevents consolidation of very recent logs
that the user might still be actively reviewing.

Log retention lifecycle:
  Active logs -> Expired (past retention period) -> Archived -> Permanently deleted
  The archive review window gives the user time to rescue logs before deletion.
"""

import re
import shutil
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

from sync.config_manager import ConfigManager


class LogEntry:
    """Represents a single log entry with type, timestamp, and message."""

    def __init__(self, entry_type: str, message: str, timestamp: Optional[datetime] = None):
        self.entry_type = entry_type
        self.timestamp = timestamp or datetime.now()
        self.message = message

    def format(self) -> str:
        """Format the log entry as a string for writing to a file."""
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M")
        return f"[{self.entry_type}] [{ts}] {self.message}"


class LoggingManager:
    """
    Manages all logging operations for the sync tool.
    """

    def __init__(self, config: ConfigManager):
        self.config = config
        self.log_dir = Path(config.get("logging", "directory", default="") or "")
        self.level = config.get("logging", "level", default="standard")
        self.consolidation_freq = config.get(
            "logging", "consolidation_frequency", default="weekly"
        )
        self.grace_period_days = config.get("logging_advanced", "grace_period_days", default=3)
        self.retention = config.get("logging_advanced", "retention", default="indefinite")
        self.archive_review_days = config.get(
            "logging_advanced", "archive_review_window_days", default=30
        )
        self.storage_threshold_mb = config.get(
            "logging_advanced", "storage_warning_threshold_mb", default=1024
        )

        self._entries: list[LogEntry] = []
        self._run_start: Optional[datetime] = None
        self._run_id: str = ""

    def start_run(self) -> None:
        """Mark the start of a new sync run."""
        self._run_start = datetime.now()
        self._run_id = self._run_start.strftime("%Y%m%d_%H%M%S")
        self._entries = []
        self.summary(f"Sync run started at {self._run_start.strftime('%B %d, %Y %I:%M %p')}")

    def summary(self, message: str) -> None:
        """Add a summary-level log entry (human-readable)."""
        self._entries.append(LogEntry("SUMMARY", message))

    def technical(self, message: str) -> None:
        """Add a technical-level log entry (detailed troubleshooting)."""
        if self.level in ("standard", "verbose"):
            self._entries.append(LogEntry("TECHNICAL", message))

    def verbose(self, message: str) -> None:
        """Add a verbose-level log entry (only recorded when level is 'verbose')."""
        if self.level == "verbose":
            self._entries.append(LogEntry("TECHNICAL", message))

    def error(self, message: str) -> None:
        """Log an error. Always logged regardless of level."""
        self._entries.append(LogEntry("SUMMARY", f"ERROR: {message}"))
        self._entries.append(LogEntry("TECHNICAL", f"ERROR: {message}"))

    def warning(self, message: str) -> None:
        """Log a warning. Always logged regardless of level."""
        self._entries.append(LogEntry("SUMMARY", f"WARNING: {message}"))

    def end_run(self, success: bool = True) -> None:
        """Mark the end of a sync run and write log files."""
        status = "completed successfully" if success else "completed with errors"
        self.summary(f"Sync run {status}")
        self._write_run_logs()
        self._check_consolidation()
        self._check_storage()

    def _write_run_logs(self) -> None:
        """Write the current run's entries to individual log files."""
        if not self.log_dir or not self._run_start:
            return

        self.log_dir.mkdir(parents=True, exist_ok=True)

        date_str = self._run_start.strftime("%Y-%m-%d")
        time_str = self._run_start.strftime("%H%M%S")

        summary_path = self.log_dir / f"summary_{date_str}_{time_str}.log"
        summary_entries = [e for e in self._entries if e.entry_type == "SUMMARY"]
        self._write_entries_to_file(summary_path, summary_entries)

        technical_path = self.log_dir / f"technical_{date_str}_{time_str}.log"
        technical_entries = [e for e in self._entries if e.entry_type == "TECHNICAL"]
        self._write_entries_to_file(technical_path, technical_entries)

    def _write_entries_to_file(self, path: Path, entries: list[LogEntry]) -> None:
        """Write a list of log entries to a file."""
        with open(path, "w", encoding="utf-8") as f:
            run_header = self._run_start.strftime("%B %d, %Y %I:%M %p")
            f.write(f"=== Run: {run_header} ===\n")
            for entry in entries:
                f.write(entry.format() + "\n")

    def _check_consolidation(self) -> None:
        """Check if any logs are due for consolidation."""
        if self.consolidation_freq == "none":
            return
        self._consolidate_daily_to_weekly()
        self._consolidate_weekly_to_monthly()
        self._consolidate_monthly_to_yearly()

    def _consolidate_daily_to_weekly(self) -> None:
        """
        Consolidate daily log files into weekly files.

        Only consolidates logs from completed weeks (before the current
        week's start) that have passed the grace period. Groups logs
        by ISO week number and merges them into consolidated_YYYY-WNN.log.
        """
        if self.consolidation_freq not in ("weekly", "monthly"):
            return

        today = datetime.now().date()
        # Monday of the current week — logs from this week are too recent
        current_week_start = today - timedelta(days=today.weekday())
        daily_logs = self._find_daily_logs()
        weeks_to_consolidate: dict[str, list[Path]] = {}

        for log_path in daily_logs:
            log_date = self._extract_date_from_filename(log_path.name)
            if log_date and log_date < current_week_start:
                # Only consolidate after the grace period has passed,
                # giving the user time to review recent logs
                days_since = (today - log_date).days
                if days_since > self.grace_period_days:
                    # Group by ISO week: YYYY-WNN
                    week_start = log_date - timedelta(days=log_date.weekday())
                    week_key = f"{week_start.year}-W{week_start.isocalendar()[1]:02d}"
                    if week_key not in weeks_to_consolidate:
                        weeks_to_consolidate[week_key] = []
                    weeks_to_consolidate[week_key].append(log_path)

        # Merge each week's logs into a single consolidated file
        for week_key, logs in weeks_to_consolidate.items():
            if logs:
                consolidated_path = self.log_dir / f"consolidated_{week_key}.log"
                self._merge_logs(consolidated_path, logs)
                # Remove the original daily files after successful merge
                for log_path in logs:
                    log_path.unlink(missing_ok=True)

    def _consolidate_weekly_to_monthly(self) -> None:
        """
        Consolidate weekly log files into monthly files.

        Finds all consolidated_YYYY-WNN.log files from completed months
        and merges them into consolidated_YYYY-MM.log. Uses the ISO
        week-to-date mapping to determine which month a week belongs to.
        """
        today = datetime.now().date()
        current_month = today.strftime("%Y-%m")
        weekly_logs = sorted(self.log_dir.glob("consolidated_*-W*.log"))
        months_to_consolidate: dict[str, list[Path]] = {}

        for log_path in weekly_logs:
            # Parse the week identifier from the filename
            name = log_path.stem.replace("consolidated_", "")
            parts = name.split("-W")
            if len(parts) == 2:
                try:
                    year = int(parts[0])
                    week = int(parts[1])
                    # Convert ISO week to a date to determine the month
                    week_date = date.fromisocalendar(year, week, 1)
                    month_key = week_date.strftime("%Y-%m")
                    # Only consolidate completed months (not the current month)
                    if month_key < current_month:
                        if month_key not in months_to_consolidate:
                            months_to_consolidate[month_key] = []
                        months_to_consolidate[month_key].append(log_path)
                except (ValueError, IndexError):
                    continue

        for month_key, logs in months_to_consolidate.items():
            if logs:
                consolidated_path = self.log_dir / f"consolidated_{month_key}.log"
                self._merge_logs(consolidated_path, logs)
                for log_path in logs:
                    log_path.unlink(missing_ok=True)

    def _consolidate_monthly_to_yearly(self) -> None:
        """
        Consolidate monthly log files into yearly files (final tier).

        This is the last consolidation step. Yearly files are never
        further consolidated — they represent the permanent archive.
        Only consolidates completed years (not the current year).
        """
        today = datetime.now().date()
        current_year = str(today.year)
        monthly_logs = sorted(self.log_dir.glob("consolidated_????-??.log"))
        years_to_consolidate: dict[str, list[Path]] = {}

        for log_path in monthly_logs:
            month_key = log_path.stem.replace("consolidated_", "")
            year = month_key[:4]
            # Only consolidate completed years
            if year < current_year:
                if year not in years_to_consolidate:
                    years_to_consolidate[year] = []
                years_to_consolidate[year].append(log_path)

        for year, logs in years_to_consolidate.items():
            if logs:
                consolidated_path = self.log_dir / f"consolidated_{year}.log"
                self._merge_logs(consolidated_path, logs)
                for log_path in logs:
                    log_path.unlink(missing_ok=True)

    def _merge_logs(self, output_path: Path, source_logs: list[Path]) -> None:
        """Merge multiple log files into a single consolidated file."""
        all_lines: list[str] = []
        for log_path in sorted(source_logs):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    all_lines.extend(f.readlines())
            except (IOError, OSError):
                continue

        with open(output_path, "w", encoding="utf-8") as f:
            for line in all_lines:
                f.write(line)

    def _find_daily_logs(self) -> list[Path]:
        """Find all individual daily summary and technical log files."""
        if not self.log_dir.exists():
            return []
        patterns = ["summary_*.log", "technical_*.log"]
        logs: list[Path] = []
        for pattern in patterns:
            logs.extend(self.log_dir.glob(pattern))
        return [p for p in logs if not p.name.startswith("consolidated_")]

    def _extract_date_from_filename(self, filename: str) -> Optional[date]:
        """Extract a date from a log filename like 'summary_2026-03-13_200000.log'."""
        match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
        if match:
            try:
                return datetime.strptime(match.group(1), "%Y-%m-%d").date()
            except ValueError:
                return None
        return None

    def _check_storage(self) -> None:
        """Check total log storage and warn if threshold is exceeded."""
        if not self.log_dir.exists():
            return
        total_bytes = sum(f.stat().st_size for f in self.log_dir.rglob("*") if f.is_file())
        total_mb = total_bytes / (1024 * 1024)
        if total_mb > self.storage_threshold_mb:
            self.warning(
                f"Log storage ({total_mb:.1f} MB) exceeds the configured "
                f"threshold ({self.storage_threshold_mb} MB)."
            )

    def check_retention(self) -> list[Path]:
        """Check for logs that have exceeded the retention period."""
        if self.retention == "indefinite":
            return []
        try:
            retention_days = int(self.retention)
        except (ValueError, TypeError):
            return []
        if not self.log_dir.exists():
            return []

        cutoff = datetime.now() - timedelta(days=retention_days)
        expired: list[Path] = []
        for log_path in self.log_dir.glob("consolidated_*.log"):
            try:
                mtime = datetime.fromtimestamp(log_path.stat().st_mtime)
                if mtime < cutoff:
                    expired.append(log_path)
            except (OSError, IOError):
                continue
        return expired

    def archive_logs(self, logs: list[Path]) -> None:
        """Move expired logs to the archive directory."""
        archive_dir = self.log_dir / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        for log_path in logs:
            dest = archive_dir / log_path.name
            shutil.move(str(log_path), str(dest))
            self.summary(f"Archived log: {log_path.name}")

    def clean_archive(self) -> list[Path]:
        """Remove archived logs that have exceeded the review window."""
        archive_dir = self.log_dir / "archive"
        if not archive_dir.exists():
            return []
        cutoff = datetime.now() - timedelta(days=self.archive_review_days)
        deleted: list[Path] = []
        for log_path in archive_dir.glob("*.log"):
            try:
                mtime = datetime.fromtimestamp(log_path.stat().st_mtime)
                if mtime < cutoff:
                    log_path.unlink()
                    deleted.append(log_path)
            except (OSError, IOError):
                continue
        return deleted

    def get_approaching_deletion(self) -> list[tuple[Path, int]]:
        """Find archived logs within 7 days of permanent deletion."""
        archive_dir = self.log_dir / "archive"
        if not archive_dir.exists():
            return []
        approaching: list[tuple[Path, int]] = []
        for log_path in archive_dir.glob("*.log"):
            try:
                mtime = datetime.fromtimestamp(log_path.stat().st_mtime)
                deletion_date = mtime + timedelta(days=self.archive_review_days)
                days_remaining = (deletion_date - datetime.now()).days
                if 0 < days_remaining <= 7:
                    approaching.append((log_path, days_remaining))
            except (OSError, IOError):
                continue
        return approaching

    def get_recent_entries(self, count: int = 50) -> list[str]:
        """Retrieve the most recent log entries for display."""
        if not self.log_dir or not self.log_dir.exists():
            return ["No log directory configured."]
        all_logs = sorted(
            self.log_dir.glob("*.log"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        entries: list[str] = []
        for log_path in all_logs:
            if len(entries) >= count:
                break
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    entries.extend(f.readlines())
            except (IOError, OSError):
                continue
        return entries[:count]

    def get_last_run_status(self) -> Optional[dict]:
        """Get the status of the most recent sync run."""
        if not self.log_dir or not self.log_dir.exists():
            return None
        summary_logs = sorted(
            self.log_dir.glob("summary_*.log"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not summary_logs:
            return None
        latest = summary_logs[0]
        try:
            with open(latest, "r", encoding="utf-8") as f:
                lines = f.readlines()
            return {
                "file": latest.name,
                "timestamp": datetime.fromtimestamp(latest.stat().st_mtime),
                "entries": [line.strip() for line in lines if line.strip()],
            }
        except (IOError, OSError):
            return None

    def get_storage_info(self) -> dict:
        """Get information about log storage usage."""
        if not self.log_dir or not self.log_dir.exists():
            return {"total_mb": 0, "file_count": 0, "threshold_mb": self.storage_threshold_mb}
        files = list(self.log_dir.rglob("*"))
        file_count = sum(1 for f in files if f.is_file())
        total_bytes = sum(f.stat().st_size for f in files if f.is_file())
        total_mb = total_bytes / (1024 * 1024)
        return {
            "total_mb": round(total_mb, 2),
            "file_count": file_count,
            "threshold_mb": self.storage_threshold_mb,
            "exceeds_threshold": total_mb > self.storage_threshold_mb,
        }
