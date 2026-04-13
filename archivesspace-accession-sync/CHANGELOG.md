# Changelog

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure and modular architecture
- Configuration management with three-tier config.yml and wizard-managed data.yml
- ArchivesSpace API integration via ArchivesSnake with throttling and retry
- Excel spreadsheet operations via openpyxl with auto-sizing and data validation
- Google Sheets/Drive API with service account and OAuth authentication
- Sync engine with lock_version tracking and linked record composition monitoring
- Spreadsheet validation with column-name-based detection and dynamic formulas
- Keyword detection for material formats with optional fuzzy matching (RapidFuzz)
- Extent type conversion with unknown type detection
- Subject descriptor filtering against approved lists with overflow notification
- Issue detection (access, conservation, digital, documentation, other, physical)
- Rule-based text summarization with sentence extraction fallback
- Interactive menu with sub-categories, help system, and guided tour
- Dual-job scheduling (sync + dry run) with timing buffer validation
- Preview approval workflow with flag files and configurable timeout
- SMTP email notifications with digest mode and HTML option
- Dual-format logging with hierarchical consolidation
- Automatic backup creation with folder management
- Network retry with exponential backoff and two-phase failure handling
- JSON staging files for interrupted write recovery
- Accession data caching with cache integrity protection
- Supporting sheet computation (Backlog Change Over Time, Processing Projects)
- GitHub Actions CI with pytest and informational linting
- Comprehensive README, documentation, and test fixtures
