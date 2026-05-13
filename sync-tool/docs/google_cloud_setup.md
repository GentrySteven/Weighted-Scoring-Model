# Google Cloud Setup Guide

See the main README for an overview. This guide provides detailed steps.

## Step 1: Create a Google Cloud Project
1. Go to [console.cloud.google.com](https://console.cloud.google.com/)
2. Click "New Project" and name it (e.g., "ArchivesSpace Accession Sync")

**Note on billing:** Enabling billing is required but no charges are incurred under normal usage.

## Step 2: Enable APIs
1. Navigate to APIs & Services > Library
2. Enable "Google Sheets API" and "Google Drive API"

## Step 3: Authentication

### Option A: Service Account (recommended)
1. Go to APIs & Services > Credentials > Create Credentials > Service Account
2. Download the JSON key file
3. Share your Drive folder with the service account email

### Option B: OAuth (fallback)
1. Go to APIs & Services > Credentials > Create Credentials > OAuth client ID
2. Configure consent screen, select "Desktop app"
3. Note the Client ID and Secret

## Institutional Accounts
If you need IT permission, suggested language:
> We need a Google Cloud project with Sheets and Drive APIs enabled for an archival processing tool. The APIs are free under normal usage.
