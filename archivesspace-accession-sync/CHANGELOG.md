# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure and architecture
- Configuration management with YAML config and credentials files
- ArchivesSpace API integration via ArchivesSnake
- Excel spreadsheet creation and synchronization via openpyxl
- Google Sheets and Drive API integration with service account and OAuth support
- Intelligent sync engine with lock_version tracking and linked record composition monitoring
- Spreadsheet validation with column-name-based detection
- Configurable keyword detection for material format identification
- Fuzzy matching support via RapidFuzz
- Extent type conversion with user-defined factors
- Subject descriptor filtering against curated approved lists
- Issue detection (access, conservation, digital, documentation, other, physical space)
- Configurable scanning framework for building structured vocabularies
- Interactive persistent menu with format-adaptive display
- Guided first-run setup wizard with connection testing
- Command-line flag support for power users
- Dry run mode with preview spreadsheet generation
- Platform-aware scheduling (cron for Linux/macOS, Task Scheduler for Windows)
- SMTP email notifications (optional) for success and failure
- Dual-format logging (human-readable summary and detailed technical)
- Hierarchical log consolidation (daily → weekly → monthly → yearly)
- Configurable log retention with archive review window
- Automatic backup creation with folder management
- Network retry with exponential backoff
- Two-phase failure handling with JSON staging files
- Request throttling for ArchivesSpace, Google Sheets, and Google Drive APIs
- Batch mode for Google Sheets API operations
- Accession data caching to minimize API calls
- Version checking against GitHub releases
- Comprehensive README and documentation
- Test suite with pytest
