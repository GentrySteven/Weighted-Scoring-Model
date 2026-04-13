# Security Guide

This guide covers security considerations for the archivesspace-accession-sync tool.

## Credential Storage

The tool stores credentials in `credentials.yml`, which contains ArchivesSpace passwords, Google API keys, and SMTP credentials in plain text. This is a common approach for local configuration files, but requires careful handling.

### Recommended Practices

**File permissions:** On Linux/macOS, restrict the credentials file so only your user can read it:

```bash
chmod 600 credentials.yml
```

On Windows, right-click the file > Properties > Security, and ensure only your user account has access.

**Never commit to Git:** The `.gitignore` file excludes `credentials.yml` from version control. Before pushing to GitHub, verify this file is not staged:

```bash
git status
```

If you accidentally commit credentials, change all affected passwords immediately — removing the file from Git history does not guarantee the credentials haven't been captured.

**Google service account keys:** The JSON key file for Google service accounts provides full access to any Google resources shared with that account. Store it in a secure location with restricted permissions. Do not place it inside the project directory if the directory is synced to cloud storage.

## ArchivesSpace Credentials

**Use a dedicated account:** Consider creating a dedicated ArchivesSpace user account for the sync tool rather than using a personal account. This account should have read-only API access to the repository — it does not need write permissions since the tool only reads from ArchivesSpace.

**Password rotation:** Change the ArchivesSpace password periodically and update `credentials.yml` accordingly. The setup wizard can be re-run to update credentials.

## Google API Security

**Service account scope:** The service account only needs access to the specific Google Drive folder configured in `config.yml`. Do not share your entire Drive with the service account.

**OAuth token storage:** OAuth tokens are stored locally and can be refreshed automatically. If you suspect a token has been compromised, revoke it through the Google Cloud Console and re-authorize.

**Audit sharing:** The tool re-verifies sharing permissions on each run. Periodically review who has access to the spreadsheet through Google Drive's sharing settings.

## SMTP Credentials

**Use app-specific passwords:** If your email provider supports them (Gmail, Outlook), use an app-specific password rather than your main account password. This limits the scope of access if the credential is compromised.

**TLS encryption:** The tool uses STARTTLS for SMTP connections, encrypting credentials in transit. Do not configure an SMTP server that doesn't support TLS.

## Network Security

**HTTPS for ArchivesSpace:** Use HTTPS URLs for your ArchivesSpace API endpoint whenever possible. Self-hosted instances on `http://localhost` are acceptable since traffic stays on the local machine.

**Firewall considerations:** The tool needs outbound access to your ArchivesSpace instance and (if using Google Sheets) to Google APIs. No inbound access is required.

## Data Sensitivity

**Accession data:** Accession records may contain sensitive information about donors, restrictions, and collection contents. The spreadsheet and cache files should be treated with the same sensitivity as the ArchivesSpace records they derive from.

**Log files:** Logs contain accession identifiers and operational details but not full accession content or credentials. They are safe for sharing when troubleshooting but may reveal institutional workflows.

**Preview files:** Preview spreadsheets contain the same data as the main spreadsheet and should be treated with equal sensitivity.

## Reporting Security Issues

If you discover a security vulnerability in this tool, please contact the maintainer directly rather than opening a public GitHub issue. See [CONTRIBUTING.md](../CONTRIBUTING.md) for contact information.
