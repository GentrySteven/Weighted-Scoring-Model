"""
Setup Wizard

Guided setup experience with 18 phases covering all configuration.
Supports:
- Granular save/resume (progress tracked within phases)
- Phase skipping with importance messaging
- Pre-populated values on re-run
- Two re-run modes: full sequential or phase selection with dependency awareness
- Connection testing with sample accession display
"""

import platform
import sys
from pathlib import Path
from typing import Any, Callable, Optional

import yaml

from sync import __version__
from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.menu import Menu


# Phase metadata: (phase_num, name, description, essential, dependencies)
PHASES = [
    (1, "Welcome & Prerequisites", "Check system requirements", True, []),
    (2, "Output Format", "Choose Excel or Google Sheets", True, []),
    (3, "ArchivesSpace Connection", "Connect to your ArchivesSpace instance", True, []),
    (4, "Google API Setup", "Configure Google Sheets authentication", True, [2]),
    (5, "Excel Setup", "Configure Excel file location", True, [2]),
    (6, "Extent Type Configuration", "Categorize physical and digital extents", False, [3]),
    (7, "Format Keyword Configuration", "Set up material format detection", False, [3]),
    (8, "Subject Descriptor Configuration", "Build the approved subjects list", False, [3]),
    (9, "Issue Column Configuration", "Configure issue detection and vocabularies", False, [3]),
    (10, "Linked Agent Role", "Set which agent role to use for Donor Name", False, []),
    (11, "Completion Triggers", "Define which statuses mean 'completed'", False, [3]),
    (12, "Scoring Configuration", "Configure scoring dimensions, thresholds, and weights", False, [2]),
    (13, "Email Notifications", "Configure SMTP for email alerts", False, []),
    (14, "Scheduling", "Set up automatic sync and dry run schedules", False, [2, 3]),
    (15, "Directory Creation", "Create log, cache, and preview directories", True, []),
    (16, "Spreadsheet Creation", "Create the initial spreadsheet", True, [2, 3, 15]),
    (17, "First Sync", "Run the initial synchronization", False, [16]),
    (18, "Tour Offer", "Introduction to the interactive menu", False, []),
]

# Common extent type suggestions
EXTENT_SUGGESTIONS = {
    "linear_feet": {"category": "physical", "conversion_factor": 1.0},
    "linear_foot": {"category": "physical", "conversion_factor": 1.0},
    "linear feet": {"category": "physical", "conversion_factor": 1.0},
    "cubic_feet": {"category": "physical", "conversion_factor": 1.0},
    "cubic feet": {"category": "physical", "conversion_factor": 1.0},
    "linear_inches": {"category": "physical", "conversion_factor": 0.0833},
    "items": {"category": "physical", "conversion_factor": 0.01},
    "volumes": {"category": "physical", "conversion_factor": 0.1},
    "gigabytes": {"category": "digital", "conversion_factor": 1.0},
    "megabytes": {"category": "digital", "conversion_factor": 0.001},
    "terabytes": {"category": "digital", "conversion_factor": 1000.0},
    "kilobytes": {"category": "digital", "conversion_factor": 0.000001},
}


class WizardProgress:
    """Tracks wizard progress at a granular level for save/resume."""

    def __init__(self, progress_path: Path):
        self.path = progress_path
        self.data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """Load progress from disk."""
        if self.path.exists():
            try:
                with open(self.path, "r") as f:
                    self.data = yaml.safe_load(f) or {}
            except Exception:
                self.data = {}

    def save(self) -> None:
        """Save progress to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            yaml.dump(self.data, f, default_flow_style=False)

    def get_last_phase(self) -> int:
        """Get the last completed phase number."""
        return self.data.get("last_completed_phase", 0)

    def get_step_in_phase(self, phase: int) -> int:
        """Get the last completed step within a phase."""
        return self.data.get(f"phase_{phase}_step", 0)

    def complete_step(self, phase: int, step: int) -> None:
        """Mark a step within a phase as complete."""
        self.data[f"phase_{phase}_step"] = step
        self.save()

    def complete_phase(self, phase: int) -> None:
        """Mark an entire phase as complete."""
        self.data["last_completed_phase"] = phase
        self.data[f"phase_{phase}_complete"] = True
        self.save()

    def is_phase_complete(self, phase: int) -> bool:
        """Check if a phase has been completed."""
        return self.data.get(f"phase_{phase}_complete", False)

    def store(self, key: str, value: Any) -> None:
        """Store partial data collected during a phase."""
        self.data[key] = value
        self.save()

    def retrieve(self, key: str, default: Any = None) -> Any:
        """Retrieve stored partial data."""
        return self.data.get(key, default)

    def clear(self) -> None:
        """Clear all progress (wizard completed or reset)."""
        self.data = {}
        if self.path.exists():
            self.path.unlink()

    def has_progress(self) -> bool:
        """Check if there's any saved progress."""
        return bool(self.data) and self.data.get("last_completed_phase", 0) > 0


class SetupWizard:
    """
    Guided setup wizard with 18 phases.

    Handles first-time setup and re-configuration with two modes:
    - Full sequential: walks through all phases in order
    - Phase selection: user picks specific phases, with dependency checking
    """

    def __init__(self, config: ConfigManager):
        self.config = config
        self.progress = WizardProgress(config.project_root / "wizard_progress.yml")
        self._logger: Optional[LoggingManager] = None

    def run(self, rerun: bool = False) -> bool:
        """
        Run the setup wizard.

        Args:
            rerun: If True, this is a re-run (existing config exists).

        Returns:
            True if wizard completed successfully.
        """
        self._print_banner()

        if rerun:
            return self._run_rerun()

        # Check for saved progress
        if self.progress.has_progress():
            last = self.progress.get_last_phase()
            print(f"\n  Found saved progress (completed through phase {last}).")
            choice = Menu.prompt_choice(
                "What would you like to do?",
                ["Resume where you left off", "Start over from the beginning"],
            )
            if choice == 1:
                self.progress.clear()

        return self._run_sequential()

    def _run_rerun(self) -> bool:
        """Handle re-run mode with two options."""
        choice = Menu.prompt_choice(
            "Reconfiguration mode:",
            ["Run the full wizard (review all settings)", "Select specific phases to reconfigure"],
        )

        if choice == 0:
            return self._run_sequential(rerun=True)
        else:
            return self._run_phase_selection()

    def _run_sequential(self, rerun: bool = False) -> bool:
        """
        Run all phases sequentially.

        On first run, starts from phase 1 (or resumes from saved progress).
        On re-run, starts from phase 1 with pre-populated current values.

        Format-specific phases (Google API, Excel Setup, Scoring) are
        automatically skipped based on the selected output format.
        Non-essential phases can be skipped by the user with an importance note.
        """
        # Resume from the next phase after the last completed one
        start_phase = self.progress.get_last_phase() + 1 if not rerun else 1

        for phase_num, name, desc, essential, deps in PHASES:
            # Skip phases we've already completed (resume scenario)
            if phase_num < start_phase:
                continue

            # Skip format-specific phases that don't apply:
            # Phase 4 (Google API) only runs for Google Sheets users
            # Phase 5 (Excel Setup) only runs for Excel users
            if phase_num == 4 and self.config.get_output_format() != "google_sheets":
                self.progress.complete_phase(phase_num)
                continue
            if phase_num == 5 and self.config.get_output_format() != "excel":
                self.progress.complete_phase(phase_num)
                continue

            print(f"\n  {'=' * 56}")
            print(f"  Phase {phase_num}/18: {name}")
            print(f"  {desc}")
            print(f"  {'=' * 56}")

            if not essential:
                print(f"\n  This phase is optional but recommended for full functionality.")
                if not Menu.prompt_yes_no("Complete this phase now?"):
                    print(f"  Skipped. You can configure this later from the menu.")
                    self.progress.complete_phase(phase_num)
                    continue

            try:
                success = self._run_phase(phase_num, rerun)
                if success:
                    self.progress.complete_phase(phase_num)
                else:
                    print(f"\n  Phase {phase_num} was not completed.")
                    if Menu.prompt_yes_no("Save progress and exit?"):
                        self.progress.save()
                        print("  Progress saved. Run the tool again to resume.")
                        return False
            except KeyboardInterrupt:
                print("\n\n  Wizard interrupted.")
                self.progress.save()
                print("  Progress saved. Run the tool again to resume.")
                return False

        # Wizard complete
        self.progress.clear()
        self.config.save_config()
        self.config.save_data()
        print(f"\n  {'=' * 56}")
        print("  Setup complete!")
        print(f"  {'=' * 56}")
        return True

    def _run_phase_selection(self) -> bool:
        """Run selected phases with dependency awareness."""
        print("\n  Available phases:")
        for phase_num, name, desc, essential, deps in PHASES:
            status = "✓" if self.progress.is_phase_complete(phase_num) else " "
            print(f"    [{status}] {phase_num:2d}. {name} — {desc}")

        phases_str = Menu.prompt_text(
            "Enter phase numbers to run (comma-separated, e.g., '3,6,13')"
        )

        try:
            selected = [int(p.strip()) for p in phases_str.split(",")]
        except ValueError:
            print("  Invalid input. Please enter numbers separated by commas.")
            return False

        # Check dependencies for each selected phase.
        # A phase's dependencies are defined in the PHASES tuple (index 4).
        # For example, Phase 6 (Extent Types) depends on Phase 3 (ArchivesSpace)
        # because it needs to scan the repository. If a dependency hasn't
        # been completed and isn't already in the selected list, we warn
        # the user and offer to add the prerequisite phases automatically.
        for phase_num in selected:
            phase_info = next((p for p in PHASES if p[0] == phase_num), None)
            if not phase_info:
                print(f"  Phase {phase_num} does not exist.")
                return False

            deps = phase_info[4]
            # Find dependencies that are neither completed nor selected
            missing_deps = [
                d for d in deps
                if not self.progress.is_phase_complete(d) and d not in selected
            ]

            if missing_deps:
                dep_names = [
                    next(p[1] for p in PHASES if p[0] == d)
                    for d in missing_deps
                ]
                print(f"\n  Phase {phase_num} ({phase_info[1]}) depends on:")
                for d, name in zip(missing_deps, dep_names):
                    print(f"    Phase {d}: {name}")

                if Menu.prompt_yes_no("Add these prerequisite phases?"):
                    for d in missing_deps:
                        if d not in selected:
                            selected.append(d)
                    selected.sort()

        # Run selected phases
        for phase_num in sorted(selected):
            phase_info = next((p for p in PHASES if p[0] == phase_num), None)
            if not phase_info:
                continue

            print(f"\n  {'=' * 56}")
            print(f"  Phase {phase_num}: {phase_info[1]}")
            print(f"  {'=' * 56}")

            success = self._run_phase(phase_num, rerun=True)
            if success:
                self.progress.complete_phase(phase_num)

        self.config.save_config()
        self.config.save_data()
        print("\n  Selected phases complete.")
        return True

    # -------------------------------------------------------------------------
    # Individual phase implementations
    # -------------------------------------------------------------------------

    def _run_phase(self, phase_num: int, rerun: bool = False) -> bool:
        """Dispatch to the appropriate phase handler."""
        handlers: dict[int, Callable] = {
            1: self._phase_welcome,
            2: self._phase_output_format,
            3: self._phase_archivesspace,
            4: self._phase_google_api,
            5: self._phase_excel,
            6: self._phase_extent_types,
            7: self._phase_format_keywords,
            8: self._phase_subject_descriptors,
            9: self._phase_issue_columns,
            10: self._phase_agent_role,
            11: self._phase_completion_triggers,
            12: self._phase_scoring,
            13: self._phase_email,
            14: self._phase_scheduling,
            15: self._phase_directories,
            16: self._phase_spreadsheet,
            17: self._phase_first_sync,
            18: self._phase_tour,
        }

        handler = handlers.get(phase_num)
        if handler:
            return handler(rerun)
        return True

    def _phase_welcome(self, rerun: bool = False) -> bool:
        """Phase 1: Welcome and prerequisites check."""
        print(f"\n  Welcome to archivesspace-accession-sync v{__version__}!")
        print(f"\n  System: {platform.system()} {platform.release()}")
        print(f"  Python: {sys.version.split()[0]}")

        # Check Python version
        if sys.version_info < (3, 10):
            print("\n  ⚠ Python 3.10 or higher is required.")
            print(f"  You have Python {sys.version.split()[0]}.")
            print("  Please upgrade Python and try again.")
            return False

        print("  Python version: OK")
        self.progress.complete_step(1, 1)

        # Check key dependencies
        deps = [
            ("yaml", "pyyaml", True),
            ("openpyxl", "openpyxl (Excel support)", False),
        ]

        for mod_name, display_name, required in deps:
            try:
                __import__(mod_name)
                print(f"  {display_name}: installed")
            except ImportError:
                if required:
                    print(f"  ⚠ {display_name}: NOT installed (required)")
                    return False
                else:
                    print(f"  {display_name}: not installed (optional)")

        self.progress.complete_step(1, 2)
        print("\n  Prerequisites check passed.")
        return True

    def _phase_output_format(self, rerun: bool = False) -> bool:
        """Phase 2: Output format selection."""
        current = self.config.get_output_format() if rerun else "excel"

        print("\n  Choose your output format:")
        print("  Excel is recommended for single-user workflows.")
        print("  Google Sheets is recommended for collaborative workflows.")

        choice = Menu.prompt_choice(
            "Output format:",
            [
                f"Microsoft Excel (.xlsx){' [current]' if current == 'excel' else ''}",
                f"Google Sheets{' [current]' if current == 'google_sheets' else ''}",
            ],
        )

        fmt = "excel" if choice == 0 else "google_sheets"
        self.config.set("output", "format", value=fmt)

        name = self.config.get_spreadsheet_name() if rerun else "Accession Data and Scores"
        new_name = Menu.prompt_text("Spreadsheet name:", default=name)
        self.config.set("output", "spreadsheet_name", value=new_name)

        self.progress.complete_step(2, 1)
        print(f"\n  Format: {'Excel' if fmt == 'excel' else 'Google Sheets'}")
        print(f"  Spreadsheet name: {new_name}")
        return True

    def _phase_archivesspace(self, rerun: bool = False) -> bool:
        """Phase 3: ArchivesSpace connection with sample accession test."""
        current_url = self.config.get_base_url() if rerun else ""
        current_repo = self.config.get_repository_id() if rerun else 2

        print("\n  Enter your ArchivesSpace API connection details.")
        print("  The API URL is typically different from the staff interface URL.")
        print("  Common patterns:")
        print("    Hosted: https://your-instance.archivesspace.org/api")
        print("    Self-hosted: http://localhost:8089")

        url = Menu.prompt_text("ArchivesSpace API URL:", default=current_url)
        self.config.set("archivesspace", "base_url", value=url)
        self.progress.complete_step(3, 1)

        repo_id = Menu.prompt_text("Repository ID:", default=str(current_repo))
        try:
            repo_id = int(repo_id)
        except ValueError:
            print("  Repository ID must be a number.")
            return False
        self.config.set("archivesspace", "repository_id", value=repo_id)
        self.progress.complete_step(3, 2)

        current_user = self.config.get_credential("archivesspace", "username") or ""
        username = Menu.prompt_text("ArchivesSpace username:", default=current_user)
        password = Menu.prompt_text("ArchivesSpace password:")

        self.config.save_credentials({
            "archivesspace": {"username": username, "password": password},
            **{k: v for k, v in self.config._credentials.items() if k != "archivesspace"},
        })
        self.progress.complete_step(3, 3)

        # Test connection
        print("\n  Testing connection...")
        try:
            from sync.archivesspace import ArchivesSpaceClient
            if not self._logger:
                self._logger = LoggingManager(self.config)
            client = ArchivesSpaceClient(self.config, self._logger)

            if client.connect():
                print("  Connection successful!")

                # Retrieve sample accession
                sample = client.get_sample_accession()
                if sample:
                    title = sample.get("title", "Untitled")
                    identifier = client.extract_identifier(sample)
                    acc_date = sample.get("accession_date", "No date")
                    print(f"\n  Sample accession from your repository:")
                    print(f"    Title: {title}")
                    print(f"    Identifier: {identifier}")
                    print(f"    Date: {acc_date}")
                else:
                    print("  Connected, but no accessions found in the repository.")
                self.progress.complete_step(3, 4)
                return True
            else:
                print("  ⚠ Connection failed.")
                if Menu.prompt_yes_no("Retry this step?"):
                    return self._phase_archivesspace(rerun)
                return False

        except Exception as e:
            print(f"  ⚠ Connection error: {e}")
            if Menu.prompt_yes_no("Retry this step?"):
                return self._phase_archivesspace(rerun)
            return False

    def _phase_google_api(self, rerun: bool = False) -> bool:
        """Phase 4: Google API setup."""
        print("\n  Configure Google API authentication.")
        print("  See docs/google_cloud_setup.md for detailed instructions.")

        choice = Menu.prompt_choice(
            "Authentication method:",
            ["Service account (recommended for automated runs)", "OAuth (for restricted environments)"],
        )

        if choice == 0:
            key_path = Menu.prompt_text(
                "Path to service account JSON key file:",
                default=self.config.get_credential("google", "service_account_key_path") or "",
            )
            if not Path(key_path).exists():
                print(f"  ⚠ File not found: {key_path}")
                if not Menu.prompt_yes_no("Continue anyway?"):
                    return False

            creds = dict(self.config._credentials)
            creds["google"] = {"auth_method": "service_account", "service_account_key_path": key_path}
            self.config.save_credentials(creds)

        elif choice == 1:
            client_id = Menu.prompt_text(
                "OAuth Client ID:",
                default=self.config.get_credential("google", "oauth_client_id") or "",
            )
            client_secret = Menu.prompt_text("OAuth Client Secret:")
            token_path = Menu.prompt_text(
                "Token storage path:",
                default=str(self.config.project_root / "token.json"),
            )

            creds = dict(self.config._credentials)
            creds["google"] = {
                "auth_method": "oauth",
                "oauth_client_id": client_id,
                "oauth_client_secret": client_secret,
                "oauth_token_path": token_path,
            }
            self.config.save_credentials(creds)

        self.progress.complete_step(4, 1)

        # Google Drive folder
        folder_id = Menu.prompt_text(
            "Google Drive folder ID (from folder URL):",
            default=self.config.get("google_sheets", "folder_id") or "",
        )
        self.config.set("google_sheets", "folder_id", value=folder_id)
        self.progress.complete_step(4, 2)

        # Sharing permissions
        print("\n  Configure sharing permissions for the spreadsheet.")
        sharing: list[dict] = self.config.get("google_sheets", "sharing", default=[]) or []

        while True:
            if sharing:
                print("  Current sharing:")
                for s in sharing:
                    print(f"    {s['email']} ({s['role']})")

            if not Menu.prompt_yes_no("Add a sharing permission?", default=not sharing):
                break

            email = Menu.prompt_text("Email address:")
            role_idx = Menu.prompt_choice("Role:", ["Reader", "Commenter", "Writer"])
            role = ["reader", "commenter", "writer"][role_idx]
            notify = Menu.prompt_yes_no("Send notification email?")
            sharing.append({"email": email, "role": role, "notify": notify})

        self.config.set("google_sheets", "sharing", value=sharing)
        self.progress.complete_step(4, 3)

        # Test Google connection
        print("\n  Testing Google API connection...")
        try:
            from sync.google_sheets import GoogleSheetsManager
            if not self._logger:
                self._logger = LoggingManager(self.config)
            gs = GoogleSheetsManager(self.config, self._logger)
            if gs.authenticate():
                print("  Google API connection successful!")
                return True
            else:
                print("  ⚠ Google API connection failed.")
                if Menu.prompt_yes_no("Retry?"):
                    return self._phase_google_api(rerun)
                return False
        except Exception as e:
            print(f"  ⚠ Google API error: {e}")
            if Menu.prompt_yes_no("Continue anyway? (You can fix this later)"):
                return True
            return False

    def _phase_excel(self, rerun: bool = False) -> bool:
        """Phase 5: Excel file location."""
        current = self.config.get("excel", "target_directory") or ""

        print("\n  Specify where the Excel file should be saved.")

        target_dir = Menu.prompt_text(
            "Target directory (full path):",
            default=current,
        )

        path = Path(target_dir)
        if not path.exists():
            if Menu.prompt_yes_no(f"Directory doesn't exist. Create it?"):
                path.mkdir(parents=True, exist_ok=True)
                print(f"  Created: {path}")
            else:
                return False

        self.config.set("excel", "target_directory", value=str(path))
        return True

    def _phase_extent_types(self, rerun: bool = False) -> bool:
        """Phase 6: Extent type categorization."""
        print("\n  The tool needs to know how to categorize extent types in your repository.")
        print("  Each type will be classified as 'physical' or 'digital' with a conversion factor.")

        # Try to scan repository
        extent_types: list[str] = []
        try:
            from sync.archivesspace import ArchivesSpaceClient
            if not self._logger:
                self._logger = LoggingManager(self.config)
            client = ArchivesSpaceClient(self.config, self._logger)
            if client.connect():
                print("  Scanning repository for extent types...")
                extent_types = client.get_extent_types()
                print(f"  Found {len(extent_types)} unique extent type(s).")
        except Exception as e:
            print(f"  Could not scan repository: {e}")
            print("  You can configure extent types manually.")

        # Use cached scan results if available
        if not extent_types:
            cached = self.progress.retrieve("extent_types_scan")
            if cached:
                extent_types = cached

        if extent_types:
            self.progress.store("extent_types_scan", extent_types)

        configured: dict[str, dict] = self.config.get_data("extent_types", default={}) or {}

        for i, ext_type in enumerate(extent_types):
            existing = configured.get(ext_type) or EXTENT_SUGGESTIONS.get(ext_type)

            if existing:
                category = existing.get("category", "physical")
                factor = existing.get("conversion_factor", 1.0)
                print(f"\n  [{i+1}/{len(extent_types)}] {ext_type}")
                print(f"    Suggested: {category} (factor: {factor})")

                if Menu.prompt_yes_no("Accept this suggestion?"):
                    configured[ext_type] = existing
                    self.progress.complete_step(6, i + 1)
                    continue

            print(f"\n  [{i+1}/{len(extent_types)}] {ext_type}")
            cat_idx = Menu.prompt_choice("Category:", ["Physical", "Digital"])
            category = "physical" if cat_idx == 0 else "digital"
            unit = "linear feet" if category == "physical" else "gigabytes"

            factor_str = Menu.prompt_text(
                f"Conversion factor (1 {ext_type} = ? {unit}):",
                default="1.0",
            )
            try:
                factor = float(factor_str)
            except ValueError:
                factor = 1.0

            configured[ext_type] = {"category": category, "conversion_factor": factor}
            self.progress.complete_step(6, i + 1)

        self.config.set_data("extent_types", value=configured)
        print(f"\n  Configured {len(configured)} extent type(s).")
        return True

    def _phase_format_keywords(self, rerun: bool = False) -> bool:
        """Phase 7: Format keyword configuration."""
        print("\n  The tool detects material formats by scanning accession fields for keywords.")
        print("  Default keyword lists are provided for 14 format types.")

        current = self.config.get_data("format_keywords", default={})

        if current:
            print(f"\n  Currently configured formats: {len(current)}")
            for name, keywords in current.items():
                print(f"    {name}: {len(keywords)} keyword(s)")

        if Menu.prompt_yes_no("Accept the current/default keyword lists?"):
            return True

        # Offer repository scanning
        if Menu.prompt_yes_no("Scan your repository for additional terms?", default=False):
            try:
                from sync.archivesspace import ArchivesSpaceClient
                if not self._logger:
                    self._logger = LoggingManager(self.config)
                client = ArchivesSpaceClient(self.config, self._logger)
                if client.connect():
                    print("  Scanning content_description, condition_description, inventory...")
                    terms = client.scan_fields_for_terms(
                        ["content_description", "condition_description", "inventory"]
                    )
                    top_terms = list(terms.items())[:50]
                    print(f"  Found {len(terms)} unique terms. Top 50:")
                    for term, count in top_terms:
                        print(f"    {term}: {count} occurrence(s)")
                    print("\n  Review these terms and add relevant ones to your keyword lists")
                    print("  through the interactive menu later.")
            except Exception as e:
                print(f"  Scan error: {e}")

        return True

    def _phase_subject_descriptors(self, rerun: bool = False) -> bool:
        """Phase 8: Subject descriptor configuration."""
        current_num = self.config.get("subject_descriptors", "num_columns", default=9)

        print(f"\n  Subject Descriptor columns: currently {current_num}")
        num_str = Menu.prompt_text("Number of Subject Descriptor columns:", default=str(current_num))
        try:
            num = int(num_str)
        except ValueError:
            num = 9
        self.config.set("subject_descriptors", "num_columns", value=num)
        self.progress.complete_step(8, 1)

        if Menu.prompt_yes_no("Scan repository for subjects and agents to build the approved list?", default=False):
            try:
                from sync.archivesspace import ArchivesSpaceClient
                if not self._logger:
                    self._logger = LoggingManager(self.config)
                client = ArchivesSpaceClient(self.config, self._logger)
                if client.connect():
                    results = client.scan_subjects_and_agents()
                    subjects = results.get("subjects", [])
                    agents = results.get("agents", [])
                    print(f"  Found {len(subjects)} subjects and {len(agents)} agents (role: Subject)")
                    print("  These can be curated through the interactive menu.")
            except Exception as e:
                print(f"  Scan error: {e}")

        return True

    def _phase_issue_columns(self, rerun: bool = False) -> bool:
        """Phase 9: Issue column configuration."""
        print("\n  The tool has six issue columns, each with different detection approaches.")
        print("  Detailed configuration can be done through the interactive menu.")
        print("  For now, the defaults will be applied.")

        if Menu.prompt_yes_no("Review the default issue column settings?", default=False):
            print("\n  Access Issues: Checks access_restrictions field")
            print("  Conservation Issues: Scans condition_description")
            print("  Digital Issues: Rule-based (digital extent without digital object)")
            print("  Documentation Issues: Manual dropdown (3 defaults)")
            print("  Other Processing Info: Auto-populated via keyword matching")
            print("  Physical Space Issues: Manual dropdown")

        return True

    def _phase_agent_role(self, rerun: bool = False) -> bool:
        """Phase 10: Linked agent role."""
        current = self.config.get("agents", "donor_role", default="source")
        print(f"\n  The Donor Name column extracts names from linked agents.")
        print(f"  Current role filter: {current}")

        role = Menu.prompt_text("Agent role to use for Donor Name:", default=current)
        self.config.set("agents", "donor_role", value=role)
        return True

    def _phase_completion_triggers(self, rerun: bool = False) -> bool:
        """Phase 11: Completion trigger values."""
        print("\n  When an accession's processing status changes to a 'completed' value,")
        print("  the Month Completed field is automatically populated.")

        current = self.config.get_data("completion_triggers", default=[])

        # Try to get available statuses
        statuses: list[str] = []
        try:
            from sync.archivesspace import ArchivesSpaceClient
            if not self._logger:
                self._logger = LoggingManager(self.config)
            client = ArchivesSpaceClient(self.config, self._logger)
            if client.connect():
                statuses = client.get_processing_statuses()
                if statuses:
                    print(f"\n  Available processing statuses in your ArchivesSpace:")
                    for s in statuses:
                        marker = " [selected]" if s in current else ""
                        print(f"    {s}{marker}")
        except Exception:
            pass

        if statuses:
            triggers: list[str] = []
            print("\n  Select which statuses indicate completion:")
            for status in statuses:
                is_current = status in current
                if Menu.prompt_yes_no(f"  '{status}' means completed?", default=is_current):
                    triggers.append(status)
            self.config.set_data("completion_triggers", value=triggers)
        else:
            print("  Enter completion status values (comma-separated):")
            val = Menu.prompt_text("Completion triggers:", default=",".join(current))
            triggers = [t.strip() for t in val.split(",") if t.strip()]
            self.config.set_data("completion_triggers", value=triggers)

        return True

    def _phase_scoring(self, rerun: bool = False) -> bool:
        """
        Phase 12: Scoring criteria configuration.

        Walks the user through customizing the three default scoring
        dimensions (Time in Backlog, Priority, Subject Descriptors),
        adjusting their weights, and optionally adding custom dimensions.

        For Excel users, also asks how scoring criteria are referenced
        (linked workbook vs embedded sheet in the same workbook).

        All scoring criteria are stored in data.yml; the program generates
        the scoring criteria spreadsheet/sheet from these values.
        """
        print("\n  The scoring model assigns each accession a score based on")
        print("  configurable dimensions. The default model ships with three")
        print("  dimensions, each scored on a 1-4 scale:\n")
        print("    1. Time in Backlog   — how long the accession has been waiting")
        print("    2. Priority          — processing priority from ArchivesSpace")
        print("    3. Subject Descriptors — strategic alignment via subject count\n")
        print("  You can customize thresholds and weights, or add new dimensions.")

        # Load current criteria from data.yml (or defaults)
        criteria = self.config.get_data(
            "scoring_criteria", default={}
        )
        if not criteria or "dimensions" not in criteria:
            from sync.config_manager import DATA_DEFAULTS
            criteria = DATA_DEFAULTS.get("scoring_criteria", {})

        dimensions = criteria.get("dimensions", {})

        # --- Review/customize each default dimension ---

        if Menu.prompt_yes_no("\n  Review and customize scoring dimensions?", default=True):
            dimensions = self._configure_scoring_dimensions(dimensions)

        # --- Weights ---

        print("\n  --- Scoring Weights ---")
        print("  Weights determine each dimension's influence on the final score.")
        print("  They must sum to 1.0 (100%).\n")

        total_weight = sum(d.get("weight", 0) for d in dimensions.values())
        print(f"  Current weights (total: {total_weight}):")
        for key, dim in dimensions.items():
            print(f"    {dim['label']}: {dim['weight']}")

        if Menu.prompt_yes_no("\n  Adjust weights?", default=False):
            dimensions = self._configure_scoring_weights(dimensions)

        # --- Custom dimensions ---

        if Menu.prompt_yes_no("\n  Add a custom scoring dimension?", default=False):
            dimensions = self._add_custom_dimension(dimensions)

        # --- Excel scoring reference mode ---

        if self.config.get_output_format() == "excel":
            print("\n  --- Excel Scoring Reference ---")
            print("  For Excel, the scoring criteria can be stored as:")
            print("    1. A sheet within the same workbook (recommended)")
            print("    2. A separate linked workbook")

            choice = Menu.prompt_choice(
                "Where should the scoring criteria sheet be?",
                [
                    "Embedded sheet (within the same workbook)",
                    "Linked workbook (separate Excel file)",
                ],
            )

            mode = "embedded_sheet" if choice == 0 else "linked_workbook"
            criteria["excel_scoring_mode"] = mode

            if mode == "linked_workbook":
                path = Menu.prompt_text(
                    "Path to scoring criteria workbook:",
                    default=criteria.get("scoring_workbook_path", "") or "",
                )
                criteria["scoring_workbook_path"] = path

        # Save to data.yml
        criteria["dimensions"] = dimensions
        self.config.set_data("scoring_criteria", value=criteria)

        # Summary
        print("\n  Scoring configuration saved:")
        for key, dim in dimensions.items():
            n_levels = len(dim.get("thresholds", dim.get("mappings", [])))
            print(f"    {dim['label']}: {n_levels} levels, weight {dim['weight']}")
        total = sum(d["weight"] for d in dimensions.values())
        print(f"    Total weight: {total}")
        print(f"    Score range: {total:.1f} to {total * max(max(t.get('score', 1) for t in d.get('thresholds', d.get('mappings', [{'score': 1}]))) for d in dimensions.values()):.1f}")

        return True

    def _configure_scoring_dimensions(self, dimensions: dict) -> dict:
        """Walk through each dimension and allow customization."""

        for key in list(dimensions.keys()):
            dim = dimensions[key]
            scoring_type = dim.get("scoring_type", "")

            print(f"\n  --- {dim['label']} ---")
            print(f"  Category: {dim.get('category', 'N/A')}")
            print(f"  Weight: {dim['weight']}")
            print(f"  Scoring type: {scoring_type}")

            if scoring_type == "date_range":
                thresholds = dim.get("thresholds", [])
                print("  Current thresholds:")
                for t in thresholds:
                    max_str = f"{t['max_years']} years" if t.get("max_years") else "no limit"
                    print(f"    Score {t['score']}: {t['label']} ({t['min_years']}-{max_str})")

                if Menu.prompt_yes_no("  Customize these thresholds?", default=False):
                    dim["thresholds"] = self._configure_date_thresholds(thresholds)

            elif scoring_type == "value_map":
                mappings = dim.get("mappings", [])
                print("  Current mappings:")
                for m in mappings:
                    print(f"    '{m['value']}' → Score {m['score']}")

                if Menu.prompt_yes_no("  Customize these mappings?", default=False):
                    dim["mappings"] = self._configure_value_mappings(mappings)

            elif scoring_type == "count_range":
                thresholds = dim.get("thresholds", [])
                print("  Current thresholds:")
                for t in thresholds:
                    max_str = str(t["max_count"]) if t.get("max_count") else "+"
                    if t["min_count"] == t.get("max_count"):
                        print(f"    Score {t['score']}: {t['label']} (exactly {t['min_count']})")
                    else:
                        print(f"    Score {t['score']}: {t['label']} ({t['min_count']}{max_str})")

                if Menu.prompt_yes_no("  Customize these thresholds?", default=False):
                    dim["thresholds"] = self._configure_count_thresholds(thresholds)

            dimensions[key] = dim

        return dimensions

    def _configure_date_thresholds(self, current: list[dict]) -> list[dict]:
        """Configure date-range thresholds for Time in Backlog."""
        print("\n  Enter year breakpoints (comma-separated). These define the")
        print("  boundaries between score levels. The default is: 3,5,8")
        print("  which creates: <3 years=1, 3-5=2, 5-8=3, 8+=4\n")

        default_breaks = ",".join(
            str(t.get("max_years", "")) for t in current if t.get("max_years")
        )
        breaks_str = Menu.prompt_text(
            "Year breakpoints:", default=default_breaks
        )

        try:
            breaks = sorted(int(b.strip()) for b in breaks_str.split(",") if b.strip())
        except ValueError:
            print("  Invalid input. Keeping current thresholds.")
            return current

        # Build thresholds from breakpoints
        thresholds = []
        prev = 0
        for i, bp in enumerate(breaks):
            if prev == 0:
                label = f"Less than {bp} Years"
            else:
                label = f"{prev} - {bp - 1} Years" if bp - prev > 1 else f"{prev} Years"
            thresholds.append({
                "label": label, "score": i + 1,
                "min_years": prev, "max_years": bp,
            })
            prev = bp

        # Final open-ended threshold
        thresholds.append({
            "label": f"{prev} or More Years", "score": len(breaks) + 1,
            "min_years": prev, "max_years": None,
        })

        print(f"  Created {len(thresholds)} threshold levels.")
        return thresholds

    def _configure_value_mappings(self, current: list[dict]) -> list[dict]:
        """Configure value-to-score mappings for Priority."""
        print("\n  Enter each priority value and its score.")
        print("  Type 'done' when finished.\n")

        mappings = []
        score = 1
        for m in current:
            val = Menu.prompt_text(
                f"  Value for score {score}",
                default=m.get("value", ""),
            )
            if val.lower() == "done":
                break
            mappings.append({"value": val, "score": score})
            score += 1

        # Allow adding more
        while True:
            val = Menu.prompt_text(
                f"  Value for score {score} (or 'done')",
                default="done",
            )
            if val.lower() == "done":
                break
            mappings.append({"value": val, "score": score})
            score += 1

        if not mappings:
            print("  No mappings entered. Keeping current.")
            return current

        return mappings

    def _configure_count_thresholds(self, current: list[dict]) -> list[dict]:
        """Configure count-range thresholds for Subject Descriptors."""
        print("\n  Enter count breakpoints (comma-separated). These define the")
        print("  boundaries between score levels. The default is: 1,2,3,4")
        print("  which creates: 1=1, 2=2, 3=3, 4+=4\n")

        # Build default string from current thresholds
        default_breaks = ",".join(str(t["min_count"]) for t in current)
        breaks_str = Menu.prompt_text(
            "Count breakpoints:", default=default_breaks
        )

        try:
            breaks = sorted(int(b.strip()) for b in breaks_str.split(",") if b.strip())
        except ValueError:
            print("  Invalid input. Keeping current thresholds.")
            return current

        thresholds = []
        for i, bp in enumerate(breaks):
            is_last = (i == len(breaks) - 1)
            if is_last:
                # Last level is open-ended
                thresholds.append({
                    "label": f"{bp}+", "score": i + 1,
                    "min_count": bp, "max_count": None,
                })
            else:
                next_bp = breaks[i + 1]
                if next_bp - bp == 1:
                    thresholds.append({
                        "label": str(bp), "score": i + 1,
                        "min_count": bp, "max_count": bp,
                    })
                else:
                    thresholds.append({
                        "label": f"{bp}-{next_bp - 1}", "score": i + 1,
                        "min_count": bp, "max_count": next_bp - 1,
                    })

        if not thresholds:
            print("  No thresholds entered. Keeping current.")
            return current

        print(f"  Created {len(thresholds)} threshold levels.")
        return thresholds

    def _configure_scoring_weights(self, dimensions: dict) -> dict:
        """Configure weights with validation that they sum to 1.0."""
        while True:
            for key, dim in dimensions.items():
                weight_str = Menu.prompt_text(
                    f"  Weight for {dim['label']}",
                    default=str(dim["weight"]),
                )
                try:
                    weight = float(weight_str)
                    if 0 < weight < 1:
                        dim["weight"] = round(weight, 4)
                    else:
                        print(f"  Weight must be between 0 and 1. Keeping {dim['weight']}.")
                except ValueError:
                    print(f"  Invalid number. Keeping {dim['weight']}.")

            total = round(sum(d["weight"] for d in dimensions.values()), 4)
            if abs(total - 1.0) < 0.001:
                print(f"  Weights sum to {total}. Accepted.")
                break
            else:
                print(f"\n  Weights sum to {total}, but must sum to 1.0.")
                if not Menu.prompt_yes_no("  Re-enter weights?", default=True):
                    # Normalize to 1.0
                    for dim in dimensions.values():
                        dim["weight"] = round(dim["weight"] / total, 4)
                    print("  Weights auto-normalized to 1.0.")
                    break

        return dimensions

    def _add_custom_dimension(self, dimensions: dict) -> dict:
        """Add a user-defined custom scoring dimension."""
        print("\n  --- Add Custom Dimension ---")
        print("  Custom dimensions can use one of three scoring types:")
        print("    1. Value map — maps specific text values to scores")
        print("    2. Count range — scores based on a numeric count")
        print("    3. Date range — scores based on a date field\n")

        label = Menu.prompt_text("  Dimension label (e.g., 'Physical Extent'):")
        if not label:
            return dimensions

        # Generate a key from the label
        key = label.lower().replace(" ", "_").replace("-", "_")
        key = "".join(c for c in key if c.isalnum() or c == "_")

        category = Menu.prompt_choice(
            "  Category:",
            ["Quantitative", "Strategic"],
        )
        cat_str = "quantitative" if category == 0 else "strategic"

        scoring_type = Menu.prompt_choice(
            "  Scoring type:",
            ["Value map (text → score)", "Count range (number → score)", "Date range (years → score)"],
        )
        type_str = ["value_map", "count_range", "date_range"][scoring_type]

        source = Menu.prompt_text(
            "  Source field (ArchivesSpace field name or 'manual'):",
            default="manual",
        )

        dim: dict = {
            "label": label,
            "category": cat_str,
            "weight": 0.0,
            "source_field": source,
            "scoring_type": type_str,
        }

        if type_str == "value_map":
            dim["mappings"] = self._configure_value_mappings([])
        elif type_str == "count_range":
            dim["thresholds"] = self._configure_count_thresholds([])
        elif type_str == "date_range":
            dim["thresholds"] = self._configure_date_thresholds([])

        dimensions[key] = dim
        print(f"\n  Added dimension '{label}'. Remember to adjust weights so they sum to 1.0.")

        # Offer to adjust weights now
        if Menu.prompt_yes_no("  Adjust weights now?", default=True):
            dimensions = self._configure_scoring_weights(dimensions)

        return dimensions

    def _phase_email(self, rerun: bool = False) -> bool:
        """Phase 13: Email notification configuration."""
        print("\n  Email notifications are optional. The tool works without them.")

        if not Menu.prompt_yes_no("Configure email notifications?", default=False):
            return True

        recipient = Menu.prompt_text(
            "Recipient email:",
            default=self.config.get("notifications", "recipient_email") or "",
        )
        self.config.set("notifications", "recipient_email", value=recipient)
        self.progress.complete_step(13, 1)

        server = Menu.prompt_text(
            "SMTP server:",
            default=self.config.get_credential("smtp", "server") or "",
        )
        port = Menu.prompt_text("SMTP port:", default="587")
        username = Menu.prompt_text("SMTP username (your email):")
        password = Menu.prompt_text("SMTP password:")

        creds = dict(self.config._credentials)
        creds["smtp"] = {
            "server": server,
            "port": int(port),
            "username": username,
            "password": password,
        }
        self.config.save_credentials(creds)
        self.progress.complete_step(13, 2)

        # Format and digest
        fmt_choice = Menu.prompt_choice("Email format:", ["Plain text (default)", "HTML"])
        self.config.set("notifications", "format", value="plain" if fmt_choice == 0 else "html")

        digest = Menu.prompt_yes_no("Digest mode? (Only email when changes occur)", default=False)
        self.config.set("notifications", "digest_mode", value=digest)
        self.progress.complete_step(13, 3)

        # Test email
        if Menu.prompt_yes_no("Send a test email?"):
            try:
                if not self._logger:
                    self._logger = LoggingManager(self.config)
                from sync.notifications import NotificationManager
                nm = NotificationManager(self.config, self._logger)
                if nm.send_test_email():
                    print("  Test email sent! Check your inbox.")
                else:
                    print("  ⚠ Test email failed. Check your SMTP settings.")
                    if Menu.prompt_yes_no("Retry this step?"):
                        return self._phase_email(rerun)
            except Exception as e:
                print(f"  ⚠ Error: {e}")

        return True

    def _phase_scheduling(self, rerun: bool = False) -> bool:
        """Phase 14: Scheduling configuration."""
        print("\n  Set up automatic sync and/or dry run schedules.")
        print("  Recommended: evening hours to minimize server impact.")

        if not Menu.prompt_yes_no("Set up a sync schedule?"):
            return True

        freq_idx = Menu.prompt_choice(
            "Sync frequency:",
            ["Daily (recommended for active repos)", "Weekly", "Monthly"],
        )
        freq = ["daily", "weekly", "monthly"][freq_idx]
        time_str = Menu.prompt_text("Sync time (HH:MM, 24-hour):", default="20:00")

        self.config.set("scheduling", "frequency", value=freq)
        self.config.set("scheduling", "time", value=time_str)
        self.progress.complete_step(14, 1)

        # Create the scheduled job
        try:
            from sync.scheduler import Scheduler
            if not self._logger:
                self._logger = LoggingManager(self.config)
            scheduler = Scheduler(self.config, self._logger)
            target = self.config.get_output_format()
            if scheduler.create_job(freq, time_str, target):
                print(f"  Sync schedule created: {freq} at {time_str}")
        except Exception as e:
            print(f"  ⚠ Could not create schedule: {e}")
            print("  You can set this up later from the menu.")

        # Dry run schedule
        if Menu.prompt_yes_no("Also set up a dry run schedule?", default=False):
            dr_freq_idx = Menu.prompt_choice("Dry run frequency:", ["Daily", "Weekly", "Monthly"])
            dr_freq = ["daily", "weekly", "monthly"][dr_freq_idx]
            dr_time = Menu.prompt_text("Dry run time (HH:MM):", default="19:00")

            try:
                if scheduler.create_job(dr_freq, dr_time, target, dry_run=True):
                    print(f"  Dry run schedule created: {dr_freq} at {dr_time}")
            except Exception as e:
                print(f"  ⚠ Could not create dry run schedule: {e}")

        return True

    def _phase_directories(self, rerun: bool = False) -> bool:
        """Phase 15: Create all necessary directories."""
        base = self.config.project_root

        log_dir = self.config.get("logging", "directory")
        if not log_dir:
            log_dir = str(base / "logs")
            self.config.set("logging", "directory", value=log_dir)

        cache_dir = self.config.get("cache", "directory")
        if not cache_dir:
            cache_dir = str(base / "cache")
            self.config.set("cache", "directory", value=cache_dir)

        preview_dir = self.config.get("preview", "directory")
        if not preview_dir:
            preview_dir = str(base / "preview")
            self.config.set("preview", "directory", value=preview_dir)

        print("\n  Creating directories:")
        for name, path in [("Logs", log_dir), ("Cache", cache_dir), ("Preview", preview_dir)]:
            Path(path).mkdir(parents=True, exist_ok=True)
            print(f"    {name}: {path}")

        self.config.ensure_directories()
        return True

    def _phase_spreadsheet(self, rerun: bool = False) -> bool:
        """Phase 16: Create the initial spreadsheet."""
        print("\n  Creating the initial spreadsheet with the configured structure...")

        try:
            if not self._logger:
                self._logger = LoggingManager(self.config)
            from sync.validation import SpreadsheetValidator
            validator = SpreadsheetValidator(self.config, self._logger)
            headers = validator.get_expected_columns()

            output_format = self.config.get_output_format()

            if output_format == "excel":
                from sync.excel import ExcelManager
                em = ExcelManager(self.config, self._logger)
                if em.file_exists() and not rerun:
                    print(f"  Spreadsheet already exists: {em.get_file_path()}")
                    return True
                path = em.create_spreadsheet(headers)
                print(f"  Spreadsheet created: {path}")

            elif output_format == "google_sheets":
                from sync.google_sheets import GoogleSheetsManager
                gs = GoogleSheetsManager(self.config, self._logger)
                if not gs.authenticate():
                    print("  ⚠ Could not authenticate with Google APIs.")
                    return False
                url = gs.create_spreadsheet(headers)
                print(f"  Google Sheet created: {url}")

            return True

        except Exception as e:
            print(f"  ⚠ Error creating spreadsheet: {e}")
            return False

    def _phase_first_sync(self, rerun: bool = False) -> bool:
        """Phase 17: Offer to run the first sync."""
        if not Menu.prompt_yes_no("Run the first sync now?", default=True):
            print("  You can sync later from the interactive menu or command line.")
            return True

        try:
            from sync.cli import run_sync
            if not self._logger:
                self._logger = LoggingManager(self.config)
            return run_sync(self.config, self._logger)
        except Exception as e:
            print(f"  ⚠ Sync error: {e}")
            print("  You can try again from the menu.")
            return True

    def _phase_tour(self, rerun: bool = False) -> bool:
        """Phase 18: Offer the guided tour."""
        self.config.set("ui", "tour_completed", value=False)

        if Menu.prompt_yes_no("Would you like a guided tour of the interactive menu?"):
            print("\n  The tour will start when you enter the interactive menu.")
            print("  You can also start it anytime by typing 'tour'.")
        else:
            self.config.set("ui", "tour_completed", value=True)
            print("  You can start the tour anytime by typing 'tour' in the menu.")

        return True

    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------

    def _print_banner(self) -> None:
        """Print the wizard welcome banner."""
        print("\n" + "=" * 60)
        print("  archivesspace-accession-sync — Setup Wizard")
        print("=" * 60)
