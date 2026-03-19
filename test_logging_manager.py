"""
Notifications Module

Handles email notifications via SMTP. Gracefully falls back to
logging-only when email is not configured. Supports notifications
for sync success, failure, and approaching log deletions.
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
        self.smtp_server = config.get_credential("smtp", "server", default="")
        self.smtp_port = config.get_credential("smtp", "port", default=587)
        self.smtp_username = config.get_credential("smtp", "username", default="")
        self.smtp_password = config.get_credential("smtp", "password", default="")
        self._enabled = bool(self.recipient and self.smtp_server)

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
            retry_at_end: If True and sending fails, the notification
                          will be queued for retry at the end of the run.

        Returns:
            True if email was sent successfully, False otherwise.
        """
        if not self._enabled:
            return False

        try:
            msg = MIMEMultipart()
            msg["From"] = self.smtp_username
            msg["To"] = self.recipient
            msg["Subject"] = f"[archivesspace-accession-sync] {subject}"

            msg.attach(MIMEText(body, "plain"))

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

    def notify_sync_success(self, summary: str) -> bool:
        """
        Send a success notification after a completed sync.

        Args:
            summary: Human-readable summary of what changed.

        Returns:
            True if notification was sent successfully.
        """
        subject = "Sync completed successfully"
        body = (
            "The archivesspace-accession-sync tool has completed a sync run.\n\n"
            f"Summary:\n{summary}\n\n"
            "Check the log files for full details."
        )
        return self.send(subject, body, retry_at_end=True)

    def notify_sync_failure(self, error_message: str) -> bool:
        """
        Send a failure notification when a sync encounters an error.

        Args:
            error_message: Description of what went wrong.

        Returns:
            True if notification was sent successfully.
        """
        subject = "Sync completed with errors"
        body = (
            "The archivesspace-accession-sync tool encountered an error during sync.\n\n"
            f"Error:\n{error_message}\n\n"
            "Check the log files for full details.\n"
            "You may need to run the tool manually to investigate."
        )
        return self.send(subject, body, retry_at_end=True)

    def notify_critical_update(self, version: str) -> bool:
        """
        Send a notification about a critical update (security patch).

        Args:
            version: The version number of the available update.

        Returns:
            True if notification was sent successfully.
        """
        subject = f"Critical update available: v{version}"
        body = (
            f"A critical update (v{version}) is available for archivesspace-accession-sync.\n\n"
            "Critical updates include security patches and should be applied promptly.\n\n"
            "To update, run the tool and select 'Check for updates' from the menu,\n"
            "or run the following commands:\n\n"
            "  git pull\n"
            "  pip install .[excel]  # or .[google]\n"
        )
        return self.send(subject, body)

    def notify_preview_ready(self, preview_location: str) -> bool:
        """
        Send a notification that a dry run preview is ready for review.

        Args:
            preview_location: Path or URL to the preview spreadsheet.

        Returns:
            True if notification was sent successfully.
        """
        subject = "Dry run preview ready for review"
        body = (
            "A scheduled dry run has completed and a preview spreadsheet is available.\n\n"
            f"Preview location: {preview_location}\n\n"
            "Review the preview and run the tool manually to confirm the sync\n"
            "if the changes look correct."
        )
        return self.send(subject, body)

    def notify_approaching_deletion(self, files: list[tuple[str, int]]) -> bool:
        """
        Notify user about log files approaching permanent deletion.

        Args:
            files: List of tuples (filename, days_remaining).

        Returns:
            True if notification was sent successfully.
        """
        if not files:
            return False

        subject = "Archived logs approaching permanent deletion"
        file_list = "\n".join(
            f"  - {name} ({days} days remaining)" for name, days in files
        )
        body = (
            "The following archived log files will be permanently deleted soon:\n\n"
            f"{file_list}\n\n"
            "If you need to retain these logs, move them out of the archive\n"
            "directory before they are deleted."
        )
        return self.send(subject, body)

    def notify_subject_descriptor_overflow(
        self, accession_id: int, total: int, max_columns: int
    ) -> bool:
        """
        Notify user about subject descriptor overflow on an accession.

        Args:
            accession_id: The accession ID with overflow.
            total: Total number of matching descriptors.
            max_columns: Maximum columns configured.

        Returns:
            True if notification was sent successfully.
        """
        subject = f"Subject descriptor overflow on accession {accession_id}"
        body = (
            f"Accession {accession_id} has {total} matching subject descriptors,\n"
            f"but only {max_columns} Subject Descriptor columns are configured.\n\n"
            f"{total - max_columns} descriptor(s) could not be captured.\n\n"
            "To capture all descriptors, increase the number of Subject Descriptor\n"
            "columns in config.yml under subject_descriptors.num_columns."
        )
        return self.send(subject, body)

    def retry_pending(self) -> None:
        """Retry any pending notifications that failed earlier in the run."""
        if hasattr(self, "_pending_retry") and self._pending_retry:
            subject, body = self._pending_retry
            self.send(subject, body)
            self._pending_retry = None

    def send_test_email(self) -> bool:
        """
        Send a test email to verify SMTP configuration.

        Returns:
            True if test email was sent successfully.
        """
        return self.send(
            "Test notification",
            "This is a test email from archivesspace-accession-sync.\n"
            "If you receive this message, email notifications are configured correctly.",
        )
