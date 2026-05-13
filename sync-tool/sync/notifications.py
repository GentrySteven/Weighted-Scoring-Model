"""
Notifications Module

Handles email notifications via SMTP. Gracefully falls back to
logging-only when email is not configured. Supports plain text
(default) and HTML formats, with digest mode to suppress
notifications for uneventful runs.
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager


class NotificationManager:
    """
    Manages email notifications for sync operations.

    Email is entirely optional. If SMTP credentials are not configured,
    all notification methods complete silently without error.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        """
        Initialize the NotificationManager.

        Args:
            config: ConfigManager instance with notification settings.
            logger: LoggingManager instance.
        """
        self.config = config
        self.logger = logger
        self.recipient = config.get("notifications", "recipient_email", default="")
        self.email_format = config.get("notifications", "format", default="plain")
        self.digest_mode = config.get("notifications", "digest_mode", default=False)
        self.smtp_server = config.get_credential("smtp", "server", default="")
        self.smtp_port = config.get_credential("smtp", "port", default=587)
        self.smtp_username = config.get_credential("smtp", "username", default="")
        self.smtp_password = config.get_credential("smtp", "password", default="")
        self._enabled = bool(self.recipient and self.smtp_server)
        self._pending_retry: Optional[tuple[str, str]] = None

        if not self._enabled:
            self.logger.technical("Email notifications are not configured.")

    def is_enabled(self) -> bool:
        """Check whether email notifications are configured."""
        return self._enabled

    def send(self, subject: str, body: str, retry_at_end: bool = False) -> bool:
        """
        Send an email notification.

        Args:
            subject: Email subject line.
            body: Email body text.
            retry_at_end: If True and sending fails, queue for retry.

        Returns:
            True if sent successfully, False otherwise.
        """
        if not self._enabled:
            return False

        try:
            msg = MIMEMultipart()
            msg["From"] = self.smtp_username
            msg["To"] = self.recipient
            msg["Subject"] = f"[archivesspace-accession-sync] {subject}"

            content_type = "html" if self.email_format == "html" else "plain"
            msg.attach(MIMEText(body, content_type))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)

            self.logger.technical(f"Email notification sent: {subject}")
            return True

        except Exception as e:
            self.logger.warning(f"Failed to send email notification: {e}")
            if retry_at_end:
                self._pending_retry = (subject, body)
            return False

    def notify_sync_success(self, summary: str, had_changes: bool = True) -> bool:
        """
        Send a success notification after a completed sync.

        Respects digest mode: if enabled and no changes occurred, suppresses the email.

        Args:
            summary: Aggregate summary of what changed.
            had_changes: Whether any accessions were added, updated, or deleted.

        Returns:
            True if notification was sent successfully.
        """
        if self.digest_mode and not had_changes:
            self.logger.technical("Digest mode: suppressing notification for uneventful sync.")
            return False

        body = (
            "The archivesspace-accession-sync tool has completed a sync run.\n\n"
            f"Summary:\n{summary}\n\n"
            "Check the log files for full details including individual accession changes."
        )
        return self.send("Sync completed successfully", body, retry_at_end=True)

    def notify_sync_failure(self, error_message: str, phase: str = "unknown") -> bool:
        """
        Send a failure notification when a sync encounters an error.

        Args:
            error_message: Description of what went wrong.
            phase: Which phase failed ("retrieval" or "write").

        Returns:
            True if notification was sent successfully.
        """
        body = (
            "The archivesspace-accession-sync tool encountered an error during sync.\n\n"
            f"Phase: {phase}\n"
            f"Error:\n{error_message}\n\n"
            "Check the log files for full details.\n"
            "You may need to run the tool manually to investigate."
        )
        return self.send("Sync completed with errors", body, retry_at_end=True)

    def notify_preview_ready(
        self, preview_location: str, summary: str, timeout_deadline: str
    ) -> bool:
        """
        Send a notification that a dry run preview is ready for review.

        Args:
            preview_location: Path or URL to the preview spreadsheet.
            summary: Aggregate summary of proposed changes.
            timeout_deadline: When the preview will expire.

        Returns:
            True if notification was sent successfully.
        """
        body = (
            "A scheduled dry run has completed and a preview spreadsheet is available.\n\n"
            f"Summary of proposed changes:\n{summary}\n\n"
            f"Preview location: {preview_location}\n\n"
            "To approve the sync, you can either:\n"
            "  1. Open the tool and select 'Review pending preview' from the menu\n"
            "  2. Rename the flag file from 'preview_pending_review.flag' to "
            "'preview_approved.flag'\n\n"
            f"If not reviewed, the sync will proceed automatically on {timeout_deadline}."
        )
        return self.send("Dry run preview ready for review", body)

    def notify_timeout_sync(
        self, preview_date: str, timeout_hours: int, summary: str
    ) -> bool:
        """
        Notify the user that a sync was auto-executed due to timeout.

        Args:
            preview_date: When the preview was originally generated.
            timeout_hours: The configured timeout period.
            summary: Aggregate summary of what was committed.

        Returns:
            True if notification was sent successfully.
        """
        body = (
            "A sync was automatically executed because the preview review period expired.\n\n"
            f"Preview was generated on: {preview_date}\n"
            f"Configured timeout period: {timeout_hours} hours\n"
            f"The preview was not reviewed or acknowledged within this window.\n\n"
            f"Changes committed:\n{summary}\n\n"
            "Review the annotated preview file and log for full details.\n"
            f"To adjust the timeout period, change 'preview.review_timeout_hours' "
            f"in config.yml (currently set to {timeout_hours} hours)."
        )
        return self.send(
            "Sync auto-executed — preview review period expired", body
        )

    def notify_validation_failure(self, errors: list[str]) -> bool:
        """
        Notify the user that a sync was skipped due to validation failure.

        Args:
            errors: List of validation error descriptions.

        Returns:
            True if notification was sent successfully.
        """
        error_list = "\n".join(f"  - {e}" for e in errors)
        body = (
            "A scheduled sync was skipped because the spreadsheet failed validation.\n\n"
            f"Issues found:\n{error_list}\n\n"
            "The sync was skipped to avoid data corruption.\n"
            "Please run the tool manually to investigate and repair the spreadsheet."
        )
        return self.send("Sync skipped — validation failure", body)

    def notify_critical_update(self, version: str) -> bool:
        """Send a notification about a critical update (security patch)."""
        body = (
            f"A critical update (v{version}) is available for archivesspace-accession-sync.\n\n"
            "Critical updates include security patches and should be applied promptly.\n\n"
            "To update, run the tool and select 'Check for updates' from the menu,\n"
            "or run the following commands:\n\n"
            "  git pull\n"
            "  pip install .[excel]  # or .[google]\n"
        )
        return self.send(f"Critical update available: v{version}", body)

    def notify_approaching_deletion(self, files: list[tuple[str, int]]) -> bool:
        """Notify user about log files approaching permanent deletion."""
        if not files:
            return False

        file_list = "\n".join(
            f"  - {name} ({days} days remaining)" for name, days in files
        )
        body = (
            "The following archived log files will be permanently deleted soon:\n\n"
            f"{file_list}\n\n"
            "If you need to retain these logs, move them out of the archive\n"
            "directory before they are deleted."
        )
        return self.send("Archived logs approaching permanent deletion", body)

    def notify_subject_descriptor_overflow(
        self, accession_id: int, total: int, max_columns: int
    ) -> bool:
        """Notify user about subject descriptor overflow on an accession."""
        body = (
            f"Accession {accession_id} has {total} matching subject descriptors,\n"
            f"but only {max_columns} Subject Descriptor columns are configured.\n\n"
            f"{total - max_columns} descriptor(s) could not be captured.\n\n"
            "To capture all descriptors, increase the number of Subject Descriptor\n"
            "columns in config.yml under subject_descriptors.num_columns."
        )
        return self.send(
            f"Subject descriptor overflow on accession {accession_id}", body
        )

    def retry_pending(self) -> None:
        """Retry any pending notifications that failed earlier in the run."""
        if self._pending_retry:
            subject, body = self._pending_retry
            self.send(subject, body)
            self._pending_retry = None

    def send_test_email(self) -> bool:
        """Send a test email to verify SMTP configuration."""
        return self.send(
            "Test notification",
            "This is a test email from archivesspace-accession-sync.\n"
            "If you receive this message, email notifications are configured correctly.",
        )
