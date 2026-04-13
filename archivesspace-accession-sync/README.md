# archivesspace-accession-sync

A Python tool for synchronizing ArchivesSpace accession metadata to Microsoft Excel or Google Sheets for processing prioritization.

## Overview

This tool connects to an ArchivesSpace instance, retrieves accession metadata, and populates a spreadsheet that serves as the data foundation for a multi-spreadsheet weighted scoring model. It helps archivists prioritize which unprocessed ("backlogged") accessions should be processed first.

**Key features:** Dual output format support (Excel/Google Sheets), intelligent sync with lock_version tracking, configurable keyword detection with optional fuzzy matching, extent conversion, subject descriptor filtering, issue detection, interactive menu with guided tour, scheduled sync and dry run jobs, email notifications, progress bars with time estimates, comprehensive logging, automatic backups.

## Architecture

The tool is a modular Python package. Configuration is split into three files:

- **`config.yml`** — User-editable settings in three tiers (Essential, Common, Advanced)
- **`credentials.yml`** — Sensitive credentials (never committed to git)
- **`data.yml`** — Wizard-managed data (keyword lists, extent types, scan configs)

### Project Structure

```
archivesspace-accession-sync/
├── accession_sync.py          # Main entry point
├── pyproject.toml             # Package config with optional dependency groups
├── config.yml                 # Three-tier configuration template
├── credentials_example.yml    # Credentials template (copy to credentials.yml)
├── data_example.yml           # Data template (copy to data.yml)
├── sync/                      # Core package (18 modules)
│   ├── cli.py                 # Entry point, argument parsing, sync orchestration
│   ├── config_manager.py      # Config + data + credentials loading and validation
│   ├── archivesspace.py       # ArchivesSnake API client with throttling/retry
│   ├── sync_engine.py         # Change detection, extent conversion, keyword matching
│   ├── validation.py          # Column-name-based structure checking, dynamic formulas
│   ├── excel.py               # Excel operations via openpyxl
│   ├── google_sheets.py       # Google Sheets/Drive API operations
│   ├── backup.py              # Automatic backups with folder management
│   ├── logging_manager.py     # Dual-format logging with consolidation
│   ├── scheduler.py           # Platform-aware cron/Task Scheduler (two-job support)
│   ├── notifications.py       # SMTP email with digest mode
│   ├── menu.py                # Interactive menu with sub-categories and help
│   ├── wizard.py              # 18-phase setup wizard with save/resume
│   ├── scanning.py            # Configurable scanning framework for vocabularies
│   ├── updater.py             # Automatic update checking and execution
│   ├── progress.py            # Progress bars (tqdm with built-in fallback)
│   └── utils.py               # Shared utilities
├── tests/                     # Test suite (pytest)
│   ├── fixtures/              # Realistic ArchivesSpace JSON fixtures
│   └── test_all.py            # Comprehensive tests (90 assertions)
├── docs/                      # Detailed guides (linked from README)
├── examples/                  # Sample configs and log output
└── .github/                   # CI workflow, issue/PR templates
```

## Requirements

- **Python 3.10+**
- **ArchivesSpace instance** with API access
- **For Excel:** openpyxl (`pip install .[excel]`)
- **For Google Sheets:** Google API libraries (`pip install .[google]`)

## Installation

```bash
git clone https://github.com/GentrySteven/Weighted-Scoring-Model.git
cd Weighted-Scoring-Model

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # macOS/Linux
# or: venv\Scripts\activate  # Windows

# Install for Excel users:
pip install .[excel]

# Install for Google Sheets users:
pip install .[google]

# Optional enhancements (can be combined, e.g., pip install .[excel,progress,matching]):
pip install .[progress]   # Rich progress bars with time estimates (tqdm)
pip install .[matching]   # Fuzzy keyword matching (RapidFuzz)
```

See [Platform Installation Guide](docs/platform_installation.md) for OS-specific Python setup.

## Configuration

### config.yml (Three Tiers)

**Tier 1 — Essential:** ArchivesSpace URL, repository ID, output format, target directory.
**Tier 2 — Common:** Scheduling, notifications, logging level, scoring config.
**Tier 3 — Advanced:** Throttling, retry settings, fuzzy matching, log retention.

### credentials.yml (Secrets)

ArchivesSpace username/password, Google API credentials, SMTP settings.

### data.yml (Wizard-Managed)

Extent type mappings, format keyword lists, removable media keywords, issue scan configs, dropdown options, completion triggers. Managed through the setup wizard and interactive menu. Advanced users can edit directly with care.

## First-Run Setup

The tool detects a first run and offers a guided setup wizard (18 phases) or manual template creation. The wizard covers ArchivesSpace connection testing, extent type configuration, keyword setup with repository scanning, and scheduling. Progress is saved at a granular level so you can resume later. Non-essential phases can be skipped.

## Usage

### Interactive Menu
```bash
python accession_sync.py
```
The menu adapts to your output format and organizes 25 options into sub-categories: Sync Operations, Scheduling, Data & Vocabulary Management, File & Storage Management, Logging, and Administration. A help system (`help` command), guided tour (`tour` command), and `[Info]` tags on read-only options make it approachable for all experience levels.

### Command-Line Flags
```bash
python accession_sync.py --target excel           # Direct sync
python accession_sync.py --target excel --dry-run  # Preview changes
python accession_sync.py --target excel --auto     # Automatic mode
```

## Synchronization Logic

**Principles:** ArchivesSpace is the source of truth. Data integrity is the top priority. The cache is only updated after fully successful syncs. Failures are logged, never silent. Users are empowered at decision points.

**Process:** Pre-write validation → ArchivesSpace retrieval (two-step: paginated listing + full detail for changed records) → Change detection via lock_version and composition tracking → Write changes → Update cache.

**Sync Status Values:** "Up to date", "New", "Updated — [specifics]" (e.g., "Updated — extents changed, subjects changed"). Note: deleted accessions are removed from the spreadsheet entirely and logged — the status "Deleted" appears only in the log files, not in the spreadsheet.

**Protected columns** (never overwritten): Scoring formulas, manually assigned values (Notes, Documentation Issues, Physical Space Issues, Kind of Processing Project). Month Completed is only written during completion events.

## Scoring Model

The tool generates a **weighted scoring model** that helps archivists prioritize which backlogged accessions to process first. Each accession receives a score based on configurable dimensions, and the scores can be sorted to identify high-priority records.

### Default Dimensions

The tool ships with three scoring dimensions, each scored on a 1-4 scale:

**1. Time in Backlog** (default weight: 0.5)
A quantitative dimension that measures how long an accession has been waiting for processing. Older accessions receive higher scores. The default thresholds are:

| Length of Time in Backlog | Score |
|---|---|
| Less than 3 years | 1 |
| 3 - 5 years | 2 |
| 6 - 8 years | 3 |
| 9 or more years | 4 |

The date boundaries use dynamic `TODAY()` formulas, so they always reflect the current date relative to each accession's accession date.

**2. Priority** (default weight: 0.2)
A quantitative dimension mapped from ArchivesSpace's `processing_priority` field on the collection management sub-record:

| Priority Value | Score |
|---|---|
| Not specified | 1 |
| Low | 2 |
| Medium | 3 |
| High | 4 |

**3. Subject Descriptors** (default weight: 0.3)
A strategic alignment dimension that scores accessions based on the number of approved subject descriptors they contain. The premise is that accessions touching more curated subject areas have broader strategic relevance:

| Number of Descriptors | Score |
|---|---|
| 1 | 1 |
| 2 | 2 |
| 3 | 3 |
| 4+ | 4 |

### Final Score Calculation

For each dimension, the spreadsheet computes:
- **UWS (Unweighted Score):** the raw score (1-4) based on the accession's data
- **Weight:** the dimension's configured weight (0.0 - 1.0)
- **WS (Weighted Score):** UWS × Weight

The **Final Accession Score** is the sum of all weighted scores. With default weights, the formula is:

```
Final Score = (Time in Backlog UWS × 0.5)
            + (Priority UWS × 0.2)
            + (Subject Descriptors UWS × 0.3)
```

The score range is **1.0 to 4.0**. An accession that has been in the backlog for 9+ years, is marked High priority, and has 4+ subject descriptors receives the maximum score of 4.0.

### Customization

All scoring criteria are user-configurable through the setup wizard (Phase 12) or the interactive menu (option 15: "Edit scoring criteria"). Users can:

- **Adjust thresholds** within each dimension (e.g., change the year breakpoints for Time in Backlog from 3/5/8 to 2/4/7)
- **Modify priority value mappings** to match their ArchivesSpace instance's priority labels
- **Change weights** for each dimension (validated to sum to 1.0)
- **Add custom dimensions** with three scoring types: value map (text → score), count range (number → score), or date range (years → score)

When weights don't sum to 1.0, the wizard offers automatic normalization. Custom dimensions are automatically protected during sync and integrated into the spreadsheet's scoring columns.

### Storage and Synchronization

Scoring criteria are stored in `data.yml` as the **single source of truth**. The program generates the "Scoring Criteria - DO NOT MOVE" sheet from this configuration whenever the spreadsheet is created or rebuilt. This means:

- Users cannot edit the scoring criteria sheet directly — changes must go through the wizard or menu, which validates inputs
- If the criteria sheet is lost, deleted, or corrupted, the program regenerates it from `data.yml`
- The configuration is text-based and can be version-controlled, backed up, or shared between institutions

For **Excel**, the scoring criteria can be embedded as a sheet in the same workbook (default) or stored in a separate linked workbook. For **Google Sheets**, the scoring criteria are embedded as a sheet within the same Google Sheet; users who prefer the original IMPORTRANGE multi-spreadsheet architecture can copy the contents to a separate Google Sheet manually.

### Validation

On every sync, the program validates the scoring criteria configuration. Issues are reported as warnings (not fatal errors) so that broken criteria don't prevent the sync from completing. Validation checks include:

- At least one dimension is defined
- Each dimension has a valid label, weight, and scoring type
- Weights sum to 1.0 (within tolerance of 0.01)
- Each dimension has valid thresholds or mappings
- Scores are positive integers

Use the menu option "View scoring criteria" to see the current configuration and any validation issues at any time.

## Processing Queue

The tool generates **processing queue sheets** that group accessions into "projects" and prioritize them by score. Each queue answers the question: "Which projects should I work on next?" This replaces the multi-spreadsheet pivot table architecture of the original Processing Scoring Model with a simpler approach: the program computes the queues in Python during sync and writes the results as static rows.

### How It Works

On every sync, the tool:

1. Filters accessions by configurable status values (e.g., "Backlog - General")
2. Groups them by a configurable field (default: Donor Name) — all accessions sharing the same value are treated as one "project"
3. Computes per-project metrics: accession count, total physical/digital extent, total formats, total subject descriptors, total issues, and average final accession score
4. Sorts projects by average score descending (highest priority first), with alphabetical tiebreaking
5. Writes the results to a "Queue - [Name]" sheet in the workbook

Multiple queues can be configured (e.g., one for general backlog, one for requested accessions), each generating its own sheet.

### View Modes

**Indented view (default):** One header row per project with project totals, followed by indented sub-rows showing each constituent accession. This matches the visual feel of a pivot table.

**Flat view:** One row per accession, with the project name and average score repeated in the leftmost columns. Easier to filter and sort manually.

### Backlog At a Glance

A snapshot dashboard sheet that summarizes accession counts and extent totals across configurable status groups (default: General Backlog, Cataloging, Requested, In-Progress, plus a TOTAL row). The "General Backlog" group also displays a "Processing Projects Remaining" count showing how many distinct projects exist in the backlog.

### Configuration

Processing queue and Backlog At a Glance settings live in `data.yml` and are managed through menu options 16 ("View processing queues") and 17 ("Edit processing queues"). The interactive editor supports:

- Adding, modifying, and removing queues
- Changing each queue's status filter, grouping field, and view mode
- Editing Backlog At a Glance status groups and which ones display project counts

The default configuration creates a single queue called "General Backlog" that filters by status "Backlog - General", groups by Donor Name, and uses indented view mode.

### Validation

On every sync, the tool validates that each queue has a unique name, at least one status value, a recognized grouping field (must match a column in the main accession sheet), and a valid view mode. Validation issues are reported as warnings without halting the sync — broken queue configurations simply skip the affected sheets.

### Sheet Location

All processing queue sheets and the Backlog At a Glance dashboard live in the same workbook as the main "Accession Data and Scores" sheet, alongside the existing "Backlog Change Over Time" and "Processing Projects Over Time" supporting sheets. There is no separate "Processing Scoring Model" file.

## Visualizations

The tool generates a **"Visualizations" sheet** in the main workbook containing eight native chart objects that replicate the dashboards from the original standalone visualizations Google Sheet. Charts refresh from computed data tables on every sync — no manual pivot-table refresh is needed.

### Charts

1. **Top 10 Subject Descriptors in Backlog** (pie chart) — shows the most-common subject descriptors across accessions currently in the backlog, identifying where collection strengths lie.
2. **Physical and Digital Backlog Over Time** (grouped columns) — end-of-month backlog totals in linear feet and gigabytes, month by month.
3. **Growth or Reduction in Backlog Over Time** (grouped columns) — month-over-month deltas for both physical and digital extent. Positive values are growth; negative values show processing that exceeded new accessions that month.
4. **Accessioning vs. Processing per Month (count)** (grouped columns) — new accessions acquired each month vs. accessions completed.
5. **Accessioning vs. Processing per Month (physical extent)** (grouped columns) — the same comparison measured in linear feet rather than accession count.
6. **Accessions Completed per Month (by status group)** (grouped columns) — counts of completed work, broken out by the configured Backlog At a Glance status groups (default: General Backlog, Cataloging, Requested, In-Progress).
7. **Physical Extent Completed per Month (by status group)** (grouped columns) — linear-foot totals of completed work, per status group.
8. **Digital Extent Completed per Month (by status group)** (grouped columns) — gigabyte totals of completed work, per status group.

### How It Works

The program computes three derived data tables on every sync and writes them to hidden sheets:

- **Viz - Monthly Change** — monthly backlog totals, month-over-month deltas, and monthly accessioning/completion counts and extents. Feeds charts 2, 3, 4, 5.
- **Viz - Completion by Status** — monthly completion metrics broken out by status group. Feeds charts 6, 7, 8.
- **Viz - Subject Counts** — descriptor-level counts across the backlog. Feeds chart 1 (top 10 by count).

Chart objects on the visible Visualizations sheet reference these hidden tables. Because the program rebuilds the tables and chart objects on every sync, the charts always reflect current data without any user intervention.

### Status Group Configuration

Charts 6-8 use the same status groups as the Backlog At a Glance dashboard. Changes to `backlog_at_a_glance.status_groups` in `data.yml` (via menu option 17) automatically propagate to the visualizations — no separate configuration for charts.

### Excel vs. Google Sheets

- **Excel:** Native `BarChart` and `PieChart` objects via openpyxl. Charts are rebuilt on every sync so data ranges extend as new months are added.
- **Google Sheets:** Native chart objects created via the Sheets API's `addChart` request on first run. On subsequent runs, the underlying data tables are refreshed and Google Sheets automatically re-renders the charts. Charts are not duplicated across runs — the program detects existing charts and only creates missing ones.

### Charts Dropped from the Original

None. All eight charts from the original Visualizations Google Sheet were preserved. Some data-source references were adapted: the original's "Identity Descriptors" field is now called "Subject Descriptors" (same data, renamed during earlier work), and the hidden data tables are computed in Python rather than populated via `IMPORTRANGE`.

## Scheduling

Supports **two simultaneous jobs**: one sync and one dry run. Platform-aware (cron on Linux/macOS, Task Scheduler on Windows). Configurable timing buffer (default 60 minutes) with conflict prevention.

**Preview approval workflow:** Scheduled dry runs create a preview with a flag file. The sync job checks for unreviewed previews and blocks until approved (via menu or file-based trigger) or the configurable timeout expires (default 72 hours). Timeout-executed syncs send explicit email notification.

## Notifications

Optional SMTP email in plain text (default) or HTML. Digest mode suppresses uneventful sync notifications. Notification scenarios: successful sync (aggregate summary), failed sync (error details and phase), preview ready (with approval instructions and timeout deadline), timeout-executed sync, validation failure, critical update, approaching log deletion, subject descriptor overflow.

## Logging

Dual-format per run (human-readable summary + detailed technical). Consolidation: daily → weekly → monthly → yearly. Entries interleaved chronologically with `[SUMMARY]`/`[TECHNICAL]` tags and run headers. Configurable retention with archive review window before permanent deletion.

## Error Handling

- **Network:** Exponential backoff (1, 2, 4, 8, 16s), 5 retries default
- **Two-phase failure:** Retrieval failure = halt; write failure = save to staging file for next run
- **Rate limiting:** Built-in throttling + HTTP 429 detection with pause
- **Unknown extent types:** Halt and prompt for categorization
- **File locks:** 5 retries at 60s intervals with process identification
- **Authentication:** Automatic re-auth for ArchivesSpace (3 attempts); OAuth refresh for Google

## Updating

Startup version check against GitHub releases. Regular updates show a brief notice; critical updates (security) show a prominent warning and send email. "Check for updates" menu option can run updates automatically.

## Branch Strategy

- **`main`**: Stable releases only, tagged with version numbers
- **`develop`**: Active development, target for pull requests

## Testing

pytest with tiered coverage: 90% for core modules (sync_engine, validation, config_manager), 75% overall guideline. GitHub Actions CI runs tests and informational linting (black + ruff) on push/PR to develop.

## Contributing

All contributions welcome. Fork → branch from `develop` → PR. `black` formatting and docstrings recommended, not required. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [Security Guide](docs/security.md) for credential storage best practices, file permissions, Google API security, and data sensitivity considerations.

## License

[MIT](LICENSE)
