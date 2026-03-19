# Google Cloud Setup Guide

This guide walks you through setting up a Google Cloud project, enabling the required APIs, and configuring authentication for the archivesspace-accession-sync tool.

## Overview

The tool needs access to two Google APIs:

- **Google Sheets API**: To create, read, and write spreadsheet data.
- **Google Drive API**: To manage file location, sharing permissions, and backups.

Both APIs are accessed through a single Google Cloud project. You only need to set this up once.

## Prerequisites

- A Google account (personal or institutional Google Workspace)
- Access to create a Google Cloud project (you may need to request this from your IT department — see the section on Institutional Accounts below)

## Step 1: Create a Google Cloud Project

1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Click the project selector dropdown at the top of the page.
3. Click "New Project."
4. Enter a project name (e.g., "ArchivesSpace Accession Sync").
5. Click "Create."

### A Note About Billing

Google will prompt you to enable billing on your project. **Under normal usage of the Sheets and Drive APIs, no charges will be incurred.** Google provides generous free-tier usage limits that far exceed what this tool requires. Enabling billing is simply a requirement for activating API access.

If your account is managed by your institution, you may need to request billing access from your IT department.

## Step 2: Enable the APIs

1. In the Google Cloud Console, navigate to "APIs & Services" > "Library."
2. Search for "Google Sheets API" and click on it.
3. Click "Enable."
4. Return to the Library and search for "Google Drive API."
5. Click "Enable."

## Step 3: Configure Authentication

The tool supports two authentication methods. Choose the one that best fits your institution's needs.

### Option A: Service Account (Recommended)

A service account is a special Google account that represents an application rather than a person. This is the recommended approach because it allows the tool to run automatically without requiring a browser-based login.

1. In the Google Cloud Console, navigate to "APIs & Services" > "Credentials."
2. Click "Create Credentials" > "Service Account."
3. Enter a name (e.g., "accession-sync-service") and click "Create and Continue."
4. For the role, select "Editor" and click "Continue."
5. Click "Done."
6. Click on the newly created service account email address.
7. Go to the "Keys" tab.
8. Click "Add Key" > "Create new key."
9. Select "JSON" and click "Create."
10. A JSON key file will be downloaded. **Store this file securely** — it provides access to your Google APIs.
11. Note the service account's email address (it looks like `name@project-id.iam.gserviceaccount.com`).

**Important:** You must share your Google Drive folder with the service account's email address. The service account needs "Editor" access to the folder where the spreadsheet will be created.

To share the folder:
1. Open Google Drive and navigate to the target folder.
2. Right-click the folder and select "Share."
3. Enter the service account's email address.
4. Set the permission to "Editor."
5. Click "Send."

### Option B: OAuth (Fallback)

If your institution restricts service account creation, you can use OAuth authentication instead. This requires a one-time browser-based authorization but can then run automatically using a stored refresh token.

1. In the Google Cloud Console, navigate to "APIs & Services" > "Credentials."
2. Click "Create Credentials" > "OAuth client ID."
3. If prompted, configure the OAuth consent screen first:
   - Choose "Internal" (for Google Workspace) or "External."
   - Fill in the required fields (app name, support email).
   - Add the scopes: `https://www.googleapis.com/auth/spreadsheets` and `https://www.googleapis.com/auth/drive`.
4. For application type, select "Desktop app."
5. Click "Create."
6. Note the Client ID and Client Secret.

When you run the tool for the first time with OAuth, it will open a browser window asking you to authorize access. After authorization, a token is stored locally for future runs.

**Note:** OAuth tokens expire periodically. If the token cannot be automatically refreshed, the tool will prompt you to re-authorize during a manual run.

## Step 4: Configure the Tool

Add your credentials to `credentials.yml`:

**For Service Account:**
```yaml
google:
  auth_method: "service_account"
  service_account_key_path: "/path/to/your/service-account-key.json"
```

**For OAuth:**
```yaml
google:
  auth_method: "oauth"
  oauth_client_id: "your_client_id"
  oauth_client_secret: "your_client_secret"
  oauth_token_path: "/path/to/stored/token.json"
```

## Institutional Accounts

If you are working under an institutional Google Workspace account, you may not have permission to create Cloud projects or service accounts. Here is suggested language you can use when requesting access from your IT department:

> We are implementing an archival processing tool that needs to read and write data to Google Sheets. This requires a Google Cloud project with the Google Sheets API and Google Drive API enabled. The APIs are free under normal usage. We would need either a service account (preferred for automated operation) or OAuth credentials configured for a desktop application. The tool only accesses spreadsheets and folders that are explicitly shared with it.

## Troubleshooting

- **"Access Not Configured" error**: Make sure both APIs are enabled in your Cloud project.
- **"Permission denied" error with service account**: Ensure the target Google Drive folder is shared with the service account's email address.
- **OAuth token expired**: Run the tool manually to trigger the re-authorization flow.
- **Billing required**: Enable billing on your Cloud project. No charges will be incurred under normal usage.
