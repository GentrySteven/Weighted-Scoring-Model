[README.md](https://github.com/user-attachments/files/26107049/README.md)
# archivesspace-accession-sync

A Python-based tool for synchronizing accession metadata from ArchivesSpace to Microsoft Excel or Google Sheets. Designed for archivists and records professionals, this tool serves as the data foundation for a weighted scoring model that helps prioritize backlogged accessions for processing.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [First-Run Setup](#first-run-setup)
- [Usage](#usage)
- [Spreadsheet Structure](#spreadsheet-structure)
- [Synchronization Logic](#synchronization-logic)
- [Scheduling Automatic Runs](#scheduling-automatic-runs)
- [Dry Run Mode](#dry-run-mode)
- [Logging](#logging)
- [Backups](#backups)
- [Error Handling](#error-handling)
- [Updating the Tool](#updating-the-tool)
- [Branch Strategy](#branch-strategy)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

---

## Overview

The **archivesspace-accession-sync** tool connects to an ArchivesSpace instance, retrieves accession metadata, and populates a spreadsheet (Microsoft Excel or Google Sheets) that serves as a flat-file database and back-end for a multi-spreadsheet weighted scoring model. This scoring model helps archivists prioritize which unprocessed ("backlogged") accessions should be processed first.

The tool is designed to be usable by archivists with varying levels of technical experience. It provides an interactive menu for day-to-day operations, a guided setup wizard for first-time configuration, and comprehensive logging to support troubleshooting.

### How It Works

1. The tool connects to your ArchivesSpace instance using [ArchivesSnake](https://github.com/archivesspace-labs/ArchivesSnake), the community-standard Python library for the ArchivesSpace API.
2. It retrieves accession records and extracts specific metadata fields (extents, linked agents, subjects, classifications, and more).
3. It writes the data to a Microsoft Excel spreadsheet (.xlsx) or a Google Sheet, depending on your institution's preference.
4. On subsequent runs, it compares stored tracking data against current ArchivesSpace records and updates only what has changed — adding new accessions, refreshing outdated fields, and removing accessions that have been deleted or suppressed in ArchivesSpace.

### Who This Tool Is For

- **Archivists and processing teams** managing accession backlogs
- **Repository managers** seeking data-driven approaches to processing prioritization
- **Archival institutions** using ArchivesSpace as their collections management system

---

## Features

- **Dual output format support**: Export to Microsoft Excel (.xlsx) or Google Sheets, selected during setup
- **Intelligent synchronization**: Detects new, updated, and deleted accessions using lock_version tracking and linked record composition monitoring
- **Configurable keyword detection**: Scans accession fields for material format keywords with case-insensitive partial matching and optional fuzzy matching via [RapidFuzz](https://github.com/maxbachmann/RapidFuzz)
- **Extent conversion**: Automatically converts various extent types to standardized linear feet (physical) or gigabytes (digital) using user-defined conversion factors
- **Subject descriptor filtering**: Matches linked subjects and agents against a user-curated approved list
- **Issue detection**: Identifies access, conservation, digital, and other processing issues through configurable field scanning and rule-based evaluation
- **Interactive menu**: Persistent menu interface for all operations, accessible to non-technical users
- **Guided setup wizard**: Step-by-step configuration with connection testing
- **Dry run mode**: Preview changes before committing, with a temporary preview spreadsheet
- **Scheduled synchronization**: Automated runs via cron (Linux/macOS) or Task Scheduler (Windows)
- **Email notifications**: Optional SMTP-based alerts for successful and failed operations
- **Comprehensive logging**: Dual-format logs (human-readable summary and detailed technical) with hierarchical consolidation
- **Automatic backups**: Backups created before destructive operations, with folder management after three backups
- **Version checking**: Startup check against the latest GitHub release, with update notifications distinguishing regular and critical updates

---

## Architecture

The tool is organized as a modular Python package. Each module has a single, well-defined responsibility.

### Project Structure

```
archivesspace-accession-sync/
├── accession_sync.py              # Main entry point (interactive menu, CLI flags)
├── pyproject.toml                 # Package configuration, dependencies, versioning
├── config.yml                     # Non-sensitive configuration (template with defaults)
├── credentials_example.yml        # Example secrets file with dummy values
├── .gitignore                     # Excludes secrets, logs, backups, cache
├── README.md                      # This file
├── LICENSE                        # MIT License
├── CONTRIBUTING.md                # Contribution guidelines
├── CODE_OF_CONDUCT.md             # Community code of conduct
├── CHANGELOG.md                   # Version history
│
├── sync/                          # Core package
│   ├── __init__.py
│   ├── archivesspace.py           # ArchivesSnake authentication, accession querying,
│   │                              #   lock_version retrieval
│   ├── excel.py                   # Excel creation, reading, writing, formatting,
│   │                              #   auto-sizing via openpyxl
│   ├── google_sheets.py           # Google Sheets/Drive API operations, authentication
│   │                              #   (service account and OAuth), sharing, folder management
│   ├── sync_engine.py             # Core comparison logic, new/updated/deleted detection,
│   │                              #   format-independent
│   ├── validation.py              # Spreadsheet structure checking, column detection by name,
│   │                              #   repair and rebuild logic
│   ├── backup.py                  # Backup creation, backup folder management,
│   │                              #   three-backup threshold
│   ├── logging_manager.py         # Log creation, consolidation, retention, archiving,
│   │                              #   storage monitoring, log viewing
│   ├── scheduler.py               # OS detection, cron/Task Scheduler
│   │                              #   creation/modification/removal
│   ├── notifications.py           # SMTP email sending with graceful fallback
│   ├── config_manager.py          # Config and secrets file parsing, validation, defaults
│   └── menu.py                    # Interactive menu logic, format-adaptive display,
│                                  #   persistent navigation
│
├── tests/                         # Test suite
│   ├── __init__.py
│   ├── test_archivesspace.py
│   ├── test_excel.py
│   ├── test_google_sheets.py
│   ├── test_sync_engine.py
│   ├── test_validation.py
│   ├── test_backup.py
│   ├── test_logging_manager.py
│   ├── test_scheduler.py
│   ├── test_notifications.py
│   ├── test_config_manager.py
│   └── test_menu.py
│
├── docs/                          # Detailed documentation
│   ├── google_cloud_setup.md      # Google Cloud project and service account setup
│   ├── platform_installation.md   # Platform-specific Python installation guides
│   ├── extent_conversion.md       # Configuring extent type conversion factors
│   ├── keyword_detection.md       # Configuring format and issue keyword lists
│   └── scoring_formulas.md        # Scoring formula setup for Excel and Google Sheets
│
└── examples/                      # Example files
    ├── config_university.yml      # Sample config for a university archives
    ├── config_small_archive.yml   # Sample config for a small archive
    ├── credentials_example.yml    # Sample secrets file
    └── sample_log_output.log      # Example log output
```

### Entry Point Behavior

The tool uses a **hybrid entry point** approach:

- **Without flags**: Launches the interactive menu, adapted to show only options relevant to your configured output format (Excel or Google Sheets)
- **With flags**: Supports direct execution for power users (e.g., `python accession_sync.py --target excel --auto`)

---

## Requirements

- **Python 3.10** or higher
- **ArchivesSpace instance** with API access
- **For Google Sheets**: A Google Cloud project with the Google Sheets API and Google Drive API enabled (see [Google Cloud Setup](docs/google_cloud_setup.md))
- **For Excel**: A local directory with write permissions for the spreadsheet file

### Platform Support

The tool runs on Windows, macOS, and Linux. See [Platform Installation Guide](docs/platform_installation.md) for operating-system-specific instructions on installing Python.

### Terminal

- **Windows**: Use Command Prompt or PowerShell
- **macOS**: Use Terminal (Applications > Utilities > Terminal)
- **Linux**: Use your distribution's terminal emulator

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/archivesspace-accession-sync.git
cd archivesspace-accession-sync
```

### 2. Set Up a Virtual Environment (Recommended)

A virtual environment keeps this tool's dependencies separate from other Python projects on your machine. This prevents version conflicts and keeps your system clean.

**macOS / Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows (Command Prompt):**
```cmd
python -m venv venv
venv\Scripts\activate
```

**Windows (PowerShell):**
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

When the virtual environment is active, you will see `(venv)` at the beginning of your terminal prompt. To deactivate it later, type `deactivate`.

### 3. Install the Tool

Install the base package plus the dependencies for your chosen output format.

**For Excel users:**
```bash
pip install .[excel]
```
This command tells Python: "Install the archivesspace-accession-sync tool along with the additional libraries needed for creating and updating Excel spreadsheets."

**For Google Sheets users:**
```bash
pip install .[google]
```
This command tells Python: "Install the archivesspace-accession-sync tool along with the additional libraries needed for connecting to Google Sheets and Google Drive."

**For both formats:**
```bash
pip install .[excel,google]
```

### 4. Create Your Secrets File

Copy the example secrets file and fill in your credentials:

```bash
cp credentials_example.yml credentials.yml
```

Then open `credentials.yml` in a text editor and replace the placeholder values with your actual credentials. See [Configuration](#configuration) for details on each field.

> **Important**: Never commit `credentials.yml` to version control. It is already listed in `.gitignore` to prevent accidental exposure of your passwords and API keys.

---

## Configuration

The tool uses two YAML files for configuration. YAML is a human-readable format that uses indentation to represent structure — similar to how an outline uses indentation to show hierarchy.

### config.yml — General Settings

This file contains all non-sensitive operational settings. A template with sensible defaults and explanatory comments is included in the repository. Key sections include:

**ArchivesSpace Connection**
```yaml
archivesspace:
  # The base URL of your ArchivesSpace API.
  # This is typically NOT the same as the staff interface URL.
  # Common patterns:
  #   - Hosted/cloud: https://your-instance.archivesspace.org/api
  #   - Self-hosted: http://localhost:8089
  # Replace this URL with your institution's ArchivesSpace API endpoint.
  base_url: "https://sandbox.archivesspace.org/api"

  # The numeric ID of the repository to sync.
  # Find this in the URL when viewing your repository in the staff interface.
  repository_id: 2
```

**Output Format**
```yaml
output:
  # Choose "excel" or "google_sheets"
  format: "excel"

  # The name of the spreadsheet (used for both Excel and Google Sheets)
  # Default: "Accession Data and Scores"
  spreadsheet_name: "Accession Data and Scores"
```

**Excel Settings**
```yaml
excel:
  # Full path to the directory where the Excel file should be saved.
  # Windows example: "C:\\Users\\YourName\\Documents\\accession-sync"
  # macOS/Linux example: "/home/yourname/documents/accession-sync"
  target_directory: ""
```

**Google Sheets Settings**
```yaml
google_sheets:
  # The Google Drive folder ID where the spreadsheet should be created.
  # Find this in the URL when viewing the folder in Google Drive:
  # https://drive.google.com/drive/folders/THIS_IS_THE_FOLDER_ID
  folder_id: ""

  # Sharing permissions applied when the spreadsheet is created
  # and re-verified on each subsequent run.
  sharing:
    - email: "colleague@university.edu"
      role: "writer"       # Options: reader, commenter, writer
      notify: true         # Send a Google sharing notification email
```

**Linked Agent Configuration**
```yaml
agents:
  # The agent role to extract for the Donor Name column.
  # Default: "source". Other common values: "creator"
  donor_role: "source"
```

**Throttling**
```yaml
throttling:
  # Delay in seconds between API requests. Increase these values if your
  # ArchivesSpace server is shared or under heavy load.
  archivesspace: 0.5      # Default: 0.5 seconds
  google_sheets: 1.0      # Default: 1.0 seconds
  google_drive: 0.5       # Default: 0.5 seconds

  # Use batch mode for Google Sheets API calls (recommended).
  # Batch mode sends all changes in a single API call rather than one at a time.
  batch_mode: true
```

**Network Retry Settings**
```yaml
retry:
  # Number of retry attempts for failed network requests.
  max_retries: 5           # Default: 5
  # Excel file lock retry settings.
  file_lock_retries: 5     # Default: 5
  file_lock_interval: 60   # Seconds between retries. Default: 60
```

**Logging**
```yaml
logging:
  # Log detail level: "minimal", "standard", or "verbose"
  level: "standard"

  # Directory where log files are stored.
  # Windows example: "C:\\Users\\YourName\\Documents\\accession-sync\\logs"
  # macOS/Linux example: "/home/yourname/documents/accession-sync/logs"
  directory: ""

  # How frequently individual logs are consolidated.
  # Options: "weekly", "monthly", or "none"
  consolidation_frequency: "weekly"

  # How long to keep individual log files after consolidation (in days).
  grace_period_days: 3

  # How long to retain logs. Use "indefinite" or a number of days.
  retention: "indefinite"

  # How long archived logs remain before permanent deletion (in days).
  archive_review_window_days: 30

  # Warn when total log storage exceeds this threshold (in MB).
  storage_warning_threshold_mb: 1024
```

**Cache**
```yaml
cache:
  # Directory where the accession data cache is stored.
  directory: ""
```

**Scheduling**
```yaml
scheduling:
  # Recommended frequencies: "daily", "weekly", or "monthly"
  # Recommended time: evening hours to avoid impacting daytime work.
  frequency: "weekly"
  time: "20:00"            # 8:00 PM in 24-hour format
```

**Email Notifications (Optional)**
```yaml
notifications:
  # Email address to receive sync notifications.
  # Leave blank to disable email notifications.
  recipient_email: ""
```

**Fuzzy Matching (Optional)**
```yaml
matching:
  # Enable fuzzy matching for keyword detection.
  fuzzy_enabled: false
  # Similarity threshold (0-100). Higher = stricter matching.
  fuzzy_threshold: 85
```

### credentials.yml — Secrets

This file contains sensitive credentials and should **never** be committed to version control.

```yaml
archivesspace:
  username: "your_username"
  password: "your_password"

# For Google Sheets — include ONE of the following sections:

# Option A: Service account (recommended for automated/scheduled runs)
google:
  service_account_key_path: "/path/to/your/service-account-key.json"

# Option B: OAuth (for institutions where service accounts are restricted)
# google:
#   oauth_client_id: "your_client_id"
#   oauth_client_secret: "your_client_secret"
#   oauth_token_path: "/path/to/stored/token.json"

# For email notifications (optional)
smtp:
  server: "smtp.your-institution.edu"
  port: 587
  username: "your_email@institution.edu"
  password: "your_email_password"
```

For detailed instructions on setting up Google Cloud credentials, see [Google Cloud Setup](docs/google_cloud_setup.md).

> **Note on billing**: Setting up a Google Cloud project requires enabling billing on your Google account. Under normal usage of the Sheets and Drive APIs, **no charges are incurred** — Google provides generous free-tier usage limits. If your account is managed by your institution, you may need to request billing access from your IT department. See the [Google Cloud Setup guide](docs/google_cloud_setup.md) for suggested language to use in that request.

---

## First-Run Setup

When you run the tool for the first time, it will detect that no configuration exists and offer two paths:

1. **Guided Setup Wizard** (recommended for most users): A step-by-step process that walks you through every configuration option, tests your connections, and generates your config and secrets files.

2. **Manual Configuration**: The tool generates template files (`config.yml` and `credentials_example.yml`) that you can fill out with a text editor.

### What the Guided Wizard Covers

1. **Output format selection**: Choose between Excel and Google Sheets.
2. **ArchivesSpace connection**: Enter your API URL, repository ID, and credentials. The wizard tests the connection and displays a sample accession from your repository to confirm everything is working.
3. **Google API setup** (if Google Sheets selected): Enter your service account or OAuth credentials. The wizard tests the Google API connection.
4. **Excel directory** (if Excel selected): Specify where the spreadsheet should be saved.
5. **Extent type configuration**: The wizard retrieves all extent types from your repository and asks you to categorize each as physical or digital, with conversion factors to linear feet or gigabytes. Common types are pre-categorized as suggestions.
6. **Format keyword configuration**: The wizard offers suggested keyword lists for 14 material format types and optionally scans your repository's accession records to suggest additional terms based on your institution's actual descriptive vocabulary.
7. **Subject descriptor setup**: The wizard optionally scans subjects and linked agents (with the role "Subject") across your accession records and presents them for approval.
8. **Issue column configuration**: Configure keyword lists, scanning parameters, and structured vocabularies for each issue column.
9. **Scoring formula setup**: Choose between linked workbook references or embedded criteria sheets for the Excel scoring formulas.
10. **Email notification preferences**: Optionally configure SMTP settings, with a test email sent to verify.
11. **Scheduling preferences**: Choose a sync frequency and time, and the wizard creates the scheduled job.
12. **Directory creation**: The wizard creates all necessary directories (logs, cache, preview temp location, etc.).

The wizard can be re-run at any time from the interactive menu under "Reconfigure settings." When re-running, all fields are pre-populated with current values so you only need to change what's different.

---

## Usage

### Interactive Menu

Run the tool without any flags to launch the interactive menu:

```bash
python accession_sync.py
```

The menu adapts to your configured output format, showing only relevant options. Available options include:

**Sync Operations**
- Sync to Excel / Google Sheets
- Dry run (preview changes)

**Scheduling**
- Set up a new scheduled job
- Modify an existing scheduled job
- Remove an existing scheduled job

**Administration**
- Reconfigure settings (re-run setup wizard)
- Verify current configuration
- Check status of the last sync
- View recent log entries
- Check for updates
- Repository scanning (re-scan for format keywords, subject descriptors, or issue terms)

The menu is persistent — after completing an action, it returns to the main screen so you can perform multiple tasks in one session.

### Command-Line Flags

Power users can bypass the interactive menu with direct flags:

```bash
# Sync to Excel
python accession_sync.py --target excel

# Sync to Google Sheets
python accession_sync.py --target google_sheets

# Dry run (preview without writing changes)
python accession_sync.py --target excel --dry-run

# Automatic mode (non-interactive, for scheduled runs)
python accession_sync.py --target excel --auto
```

---

## Spreadsheet Structure

The spreadsheet is named **"Accession Data and Scores"** by default (configurable). Each row represents a single accession record. Columns are organized into functional groups.

### Data Columns (Populated from ArchivesSpace)

| Column | Header | Source | Notes |
|--------|--------|--------|-------|
| A | Accession Status | `collection_management.processing_status` | |
| B | Base URL (Use for Hyperlink Only) | Constructed from config | ArchivesSpace instance URL |
| C | Accession ID | Accession URI | Internal database ID (primary key for sync) |
| D | Base URL and Accession ID (Use for Hyperlink Only) | Formula: `=CONCAT(B2,C2)` | Direct link URL |
| E | Identifier (Use for Hyperlink Only) | `id_0`, `id_1`, `id_2`, `id_3` | Four-part accession identifier |
| F | Accession Number | Formula: `=HYPERLINK(D2,E2)` | Clickable link to ArchivesSpace record |
| G | Donor Name | `linked_agents` (configurable role, default: "source") | Multiple agents separated by semicolons |
| H | Accession Date | `accession_date` | |
| I | Priority | `collection_management.processing_priority` | |
| J | Classification | `classifications` (linked) | Multiple classifications separated by semicolons |
| K | Accession Extent - Physical (Linear Feet) | `extents` sub-records | Converted and summed using user-defined factors |
| L | Accession Extent - Digital (GB) | `extents` sub-records | Converted and summed using user-defined factors |

### Format Detection Columns (Auto-populated)

Columns M through Z (14 columns by default, configurable) detect the presence of specific material formats by scanning the `content_description`, `condition_description`, `inventory`, extent `extent_type`, and linked top container `type` and `container_type` fields for configured keywords.

Default format columns: Architectural Drawing(s), Artifact(s), Artwork, Audio and/or Visual Recording(s), Botanical Specimen(s), Film (negative, slide, or motion picture), Glass Material(s), Photographic Material(s), Scrapbook(s), Technical Drawing(s) and Schematic(s), Textile(s), Vellum and Parchment, Volume(s), Oversize Material?

Cell values are checkboxes (TRUE/FALSE). The **Total Number of Formats** column uses a formula to count all TRUE values.

Users can add new format columns or disable existing ones through the config file.

### Subject Descriptor Columns (Auto-populated)

Nine columns by default (configurable) that are populated by matching linked subjects and agents (with role "Subject") against a user-curated approved list maintained in a hidden sheet within the spreadsheet.

Subject values display as the subject's title from ArchivesSpace. Agent values display as the sort name followed by emdashes and any term subdivisions (e.g., "University of Maryland — Faculty — History Department").

The **Total Number of Subject Descriptors** column uses a `=COUNTA()` formula that dynamically adjusts based on the number of configured columns.

### Issue Columns (Mixed: Auto-populated and Manual)

| Column | Header | Behavior |
|--------|--------|----------|
| AM-equivalent | Access Issues | Auto-populated from `access_restrictions` / `access_restrictions_note` with rule-based summarization |
| AN-equivalent | Conservation Issues | Auto-populated from `condition_description` with keyword matching |
| AO-equivalent | Digital Issues | Auto-populated via rule-based evaluation of extents, digital objects, and content descriptions |
| AP-equivalent | Documentation and Use Issues | Manual dropdown (single selection) |
| AQ-equivalent | Other Processing Information | Auto-populated via user-configured keyword matching |
| AR-equivalent | Physical Space Management Issues | Manual dropdown (single selection) |

The **Total Number of Issues** column uses a `=COUNTIF()` formula that dynamically adjusts based on the number of configured issue columns.

All issue columns support a configurable scanning framework that allows users to define which fields to scan, what terms to look for, and which matching approach to use. Scans can be run during setup and on-demand from the interactive menu. Multiple named scan configurations can be saved and reused.

### Scoring Columns (Formula-based, Protected)

| Column Group | Description |
|-------------|-------------|
| Unweighted Scores (UWS) | Time in Backlog, Priority, Subject Descriptors — nested IF/AND formulas referencing scoring criteria |
| Weights | Time in Backlog, Priority, Subject Descriptors — imported from a separate scoring criteria spreadsheet |
| Weighted Scores (WS) | Products of UWS × Weight for each scoring dimension |
| Final Accession Score | SUM of all weighted scores |

For Google Sheets, scoring criteria are imported via `IMPORTRANGE` from a separate spreadsheet. For Excel, two options are available (selected during setup):
- **Option A**: Linked workbook references to a separate Excel file containing scoring criteria
- **Option B**: Scoring criteria embedded in a dedicated sheet within the same workbook

### Local Tracking Columns (Manual, Protected)

| Column | Header | Description |
|--------|--------|-------------|
| BF-equivalent | Notes | Free-text field for user notes |
| BG-equivalent | Month Completed | Auto-populated when processing status changes to a completion value; displays as "March 2026" format |
| BH-equivalent | Kind of Processing Project | Dropdown with defaults: Accessioning as Processing, Backlog, Requested, Cataloging, Unknown |

### Sync Tracking Columns (Grouped at End, Color-coded)

These columns are grouped at the far right of the spreadsheet with `[Sync]` prefixed headers and color-coded backgrounds. They should not be manually edited.

**Sub-record lock_version tracking:**
- [Sync] Accession lock_version
- [Sync] Collection Management lock_version
- [Sync] Extents lock_version

**Linked record composition and content tracking (two columns per type):**
- [Sync] Linked Agents IDs / [Sync] Linked Agents Values
- [Sync] Subjects IDs / [Sync] Subjects Values
- [Sync] Classifications IDs / [Sync] Classifications Values
- [Sync] Digital Objects IDs / [Sync] Digital Objects Values
- [Sync] Top Containers IDs / [Sync] Top Containers Values

**Status:**
- [Sync] Status — human-readable indicator (e.g., "Up to date", "New", "Updated — extents changed, subjects changed")

### Hidden Sheets

The spreadsheet contains several hidden sheets (which can be unhidden by the user) that store structured vocabulary lists and approved terms:

- Approved subject descriptors list
- Access issues structured vocabulary
- Conservation issues structured vocabulary
- Digital issues structured vocabulary
- Documentation and use issues options
- Other processing information options
- Physical space management issues options

---

## Synchronization Logic

### Core Principles

- **ArchivesSpace is the authoritative source of truth.** The spreadsheet reflects ArchivesSpace data; it does not push changes back to ArchivesSpace.
- **Data integrity is the top priority.** The spreadsheet is never left in a silently corrupted or misleading state.
- **The cache is sacred.** Only a fully successful sync updates the cache.
- **Fail gracefully, not silently.** When something goes wrong, the user always knows about it.
- **Preserve work when safe.** Staging files and caching avoid repeating work unnecessarily, but never at the cost of data consistency.
- **Empower the user.** At decision points, the tool presents options rather than acting unilaterally.
- **Log everything.** Every error, retry, warning, and recovery action is logged.

### What Happens During a Sync

1. **Pre-write validation**: The tool verifies it can access the spreadsheet before starting. For Excel, it checks that the file isn't locked. For Google Sheets, it confirms the spreadsheet exists and permissions are valid.
2. **Retrieve accession data**: The tool queries ArchivesSpace, using a two-step approach — a lightweight paginated listing first, then full detail calls only for new or changed accessions.
3. **Compare against cache**: Lock_versions and linked record compositions are compared against the cached data from the last successful sync.
4. **Determine changes**: Accessions are categorized as new, updated, unchanged, or deleted/suppressed.
5. **Write changes**: New rows are added, updated rows receive a full refresh of all mapped fields, deleted accession rows are removed.
6. **Update tracking columns**: Lock_versions, composition IDs, display values, and sync status are updated.
7. **Update cache**: The cache file is overwritten with the current state (only after a fully successful sync).

### Sync Status Values

- **Up to date** — No changes detected since the last sync.
- **New** — Accession was added to the spreadsheet for the first time.
- **Updated — [specifics]** — One or more fields were refreshed, with details about what changed (e.g., "Updated — extents changed, subjects changed").

### Protected Columns

The following column types are **never overwritten** during sync operations:

- Spreadsheet formulas (CONCAT, HYPERLINK, COUNTIF, COUNTA, scoring formulas)
- Manually assigned values (Documentation and Use Issues, Physical Space Management Issues, Notes, Kind of Processing Project)
- Scoring columns (UWS, Weights, Weighted Scores, Final Accession Score)

---

## Scheduling Automatic Runs

The tool can create, modify, and remove scheduled jobs through the interactive menu. It automatically detects your operating system and uses the appropriate scheduler:

- **Linux / macOS**: cron
- **Windows**: Task Scheduler

### Recommended Settings

- **Frequency**: Daily, weekly, or monthly (configured by you)
- **Time**: Evening hours, when ArchivesSpace server load is typically lower

### Automatic Mode Behavior

When running automatically (via the `--auto` flag), the tool operates non-interactively:

- Skips any action requiring user input (such as spreadsheet rebuilds)
- Logs all issues for later review
- Sends email notifications if configured (for both successes and failures)
- If a structural validation failure is detected, the tool logs the problem, notifies the user, and skips the sync — it never performs a destructive action unattended

---

## Dry Run Mode

Dry run mode performs the full sync process — connecting to ArchivesSpace, retrieving data, comparing changes — without writing to the actual spreadsheet.

### What It Produces

- A **brief terminal summary** with accession-level detail (what would be added, updated, deleted)
- A **temporary preview spreadsheet** showing the full proposed state of the data, stored in a temporary location

### After the Dry Run

In manual mode, the tool prompts you: "Would you like to proceed with this sync?" If confirmed, the actual sync runs immediately using the already-retrieved data, and the preview file is deleted upon successful completion. If declined, the preview file is retained until the next run.

### Scheduled Dry Runs

Dry runs can be scheduled. A scheduled dry run generates the preview spreadsheet and sends an email notification alerting you that a preview is ready for review. You then run the tool manually to review and confirm.

### Preview Spreadsheet Sharing

For Google Sheets, the preview spreadsheet inherits the same sharing permissions as the actual spreadsheet, so collaborators can also review the preview.

---

## Logging

The tool produces two types of logs per run:

- **Summary log**: Human-readable entries describing what happened (e.g., "150 accessions checked, 3 updated, 1 new")
- **Technical log**: Detailed entries for troubleshooting (e.g., "lock_version mismatch on /repositories/2/accessions/47 — old: 3, new: 5")

### Log Levels

Configurable in `config.yml`:
- **minimal**: Only errors and significant events
- **standard**: Errors, warnings, and routine operation summaries
- **verbose**: Everything, including individual accession check results

### Consolidation

Individual run logs are automatically consolidated over time to keep the log directory manageable:

- **Daily logs** → consolidated into **weekly** files
- **Weekly logs** → consolidated into **monthly** files
- **Monthly logs** → consolidated into **yearly** files (final tier)

When consolidating, both log types (summary and technical) are merged into a single file. Entries are interleaved chronologically and prefixed with `[SUMMARY]` or `[TECHNICAL]` tags. Each run's entries are preceded by a header indicating the date and time of the original run:

```
=== Run: March 13, 2026 8:00 PM ===
[SUMMARY] [2026-03-13 20:00] 150 accessions checked, 3 updated, 1 new
[TECHNICAL] [2026-03-13 20:00] lock_version mismatch on /repositories/2/accessions/47 — old: 3, new: 5
[TECHNICAL] [2026-03-13 20:00] Full row refresh for accession 2023-001
[SUMMARY] [2026-03-13 20:00] Sync completed successfully
```

Original log files are retained for 3 days (configurable) after consolidation before deletion. Lower-tier consolidated logs are deleted when they are merged into the next tier.

### Retention

- **Default**: Indefinite retention
- **Configurable**: Set a retention period in `config.yml`
- **Deletion process**: Logs removed due to retention policy are moved to `logs/archive/` for 30 days (configurable) before permanent deletion, with user notification as the deletion date approaches
- **Storage monitoring**: A warning is displayed (terminal and email) when total log storage exceeds 1 GB (configurable)

### Viewing Logs

Use the interactive menu option "View recent log entries" to browse logs without navigating the file system.

---

## Backups

Backups are created automatically before destructive operations (such as spreadsheet rebuilds).

### Backup Behavior

- Backups are named with a `[Backup]` prefix and date (e.g., `[Backup] Accession Data and Scores - 2026-03-12`)
- For **Excel**: Backup files are saved in the same directory as the original spreadsheet
- For **Google Sheets**: Backup copies are created in the same Google Drive folder
- All backups are retained — the tool never automatically deletes backups

### Backup Folder Management

After three backups have been created, the tool creates a dedicated folder named `[Backups] Accession Data and Scores` (using the spreadsheet's name) in the same parent location. All existing backups are moved into this folder, and all subsequent backups are created directly in this folder.

On each run that requires a backup, the tool checks whether the backup folder exists. If it does, the backup goes there. If it doesn't (for example, if someone deleted it), the tool resumes storing backups alongside the original until the three-backup threshold is reached again.

---

## Error Handling

### Network Failures

- **Exponential backoff**: Retries with delays of 1, 2, 4, 8, and 16 seconds
- **5 retries by default** (configurable)
- **Two-phase failure handling**:
  - During data retrieval: Halt, log the error, notify the user. Cache remains untouched.
  - During spreadsheet write: Save retrieved data to a JSON staging file in the log directory, attempt to complete the write on the next run.

### Rate Limiting

- **Built-in throttling**: 0.5 seconds between ArchivesSpace requests, 1.0 second for Google Sheets, 0.5 seconds for Google Drive (all configurable)
- **HTTP 429 detection**: The tool pauses and waits for the rate limit to reset
- **Batch mode**: Google Sheets changes are collected and written in a single API call to minimize request count (configurable)

### Spreadsheet Access

- **Excel file locks**: 5 retries at 60-second intervals (configurable), with terminal alerts identifying the locking process and user when possible. Falls back to staging file if all retries fail.
- **Google Sheets deletion**: Halts, notifies user, presents options to investigate, recreate, or re-apply permissions. URL updates require user confirmation with an explanation of what the update entails.

### Authentication

- **ArchivesSpace token expiration**: Automatic re-authentication (up to 3 attempts per run)
- **Google service account failure**: Halt, log, notify user
- **Google OAuth token expiration**: Automatic refresh if possible; in automatic mode, halt and notify user to re-authorize manually

### Configuration Errors at Runtime

- **Critical errors** (invalid repository, inaccessible output location): Halt, log, notify with remediation steps
- **Non-critical errors** (SMTP failure, missing log directory): Warn, log, continue with sync
- **Self-repair**: Missing directories are recreated automatically; anything involving data requires user confirmation

---

## Updating the Tool

The tool uses [semantic versioning](https://semver.org/) (e.g., `1.0.0`):
- **First number** (major): Breaking changes that may require configuration updates
- **Second number** (minor): New features, backward-compatible
- **Third number** (patch): Bug fixes and security patches

### Automatic Update Checks

On each startup, the tool compares your installed version against the latest release on GitHub. If an update is available:
- **Regular updates** display a brief notice
- **Critical updates** (security patches) display a prominent warning

### Updating

Use the interactive menu option "Check for updates" to check for and apply updates. The tool will attempt to run the update commands automatically with your confirmation. If the automatic update fails, the tool displays the exact commands to run manually.

**Manual update commands:**

```bash
git pull
pip install .[excel]     # or .[google] or .[excel,google]
```

---

## Branch Strategy

This project uses a two-branch strategy:

- **`main`**: Contains only tested, stable releases. Each release is tagged with a version number (e.g., `v1.0.0`). **Use this branch for production.**
- **`develop`**: Contains in-progress work and is the target for pull requests. Code is merged into `main` only after testing and review.

If you are contributing, please base your work on the `develop` branch. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## Contributing

Contributions of all kinds are welcome — bug reports, bug fixes, new features, and documentation improvements. See [CONTRIBUTING.md](CONTRIBUTING.md) for full details on how to contribute, including:

- How to fork the repository and submit pull requests
- Coding recommendations (we recommend but do not require `black` formatting and docstrings)
- How to report bugs or request features
- How to contact the maintainer directly if you prefer not to use GitHub

---

## License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and redistribute this tool for any purpose, including commercial use, with no restrictions beyond including the original license text.

---

## Acknowledgments

This tool was developed to support archival processing prioritization at the University of Iowa. It relies on the following open-source projects:

- [ArchivesSnake](https://github.com/archivesspace-labs/ArchivesSnake) — Python library for the ArchivesSpace API
- [openpyxl](https://openpyxl.readthedocs.io/) — Python library for reading and writing Excel files
- [RapidFuzz](https://github.com/maxbachmann/RapidFuzz) — Fast fuzzy string matching
- [Google API Python Client](https://github.com/googleapis/google-api-python-client) — Google Sheets and Drive API access

---

## Additional Documentation

Detailed guides are available in the `docs/` directory:

- [Google Cloud Setup](docs/google_cloud_setup.md) — Step-by-step guide to creating a Google Cloud project, enabling APIs, and configuring authentication (service account and OAuth)
- [Platform Installation](docs/platform_installation.md) — Installing Python 3.10+ on Windows, macOS, and Linux
- [Extent Conversion](docs/extent_conversion.md) — Configuring physical and digital extent type conversion factors
- [Keyword Detection](docs/keyword_detection.md) — Configuring format and issue keyword lists, fuzzy matching, and the scanning framework
- [Scoring Formulas](docs/scoring_formulas.md) — Setting up scoring criteria for Excel (linked workbooks vs. embedded sheets) and Google Sheets (IMPORTRANGE)

---

## Remaining Design Topics

The following design topics have been identified but not yet finalized. They will be addressed in future development:

1. **Scheduling and dry run conflict** — Whether to allow one sync job and one dry run job simultaneously, or maintain the single-job limit
2. **Preview file location** — Designating a specific temporary directory for dry run preview files
3. **Preview cleanup timing** — Ensuring preview files are not deleted before the user has reviewed them in automatic mode
4. **Email notification content** — Defining what information sync notification emails contain
5. **Interactive menu completeness** — Reviewing all menu options given features added during design
6. **Config file organization** — Structuring the config file for usability with the large number of settings
7. **First-run wizard scope** — Confirming the wizard creates all necessary directories and handles all setup tasks
8. **Testing framework and scope** — Selecting pytest and defining coverage goals
9. **Scoring columns protection** — Confirming detailed sync behavior for all protected column types

These topics are tracked and will be resolved before the `v1.0.0` release.
