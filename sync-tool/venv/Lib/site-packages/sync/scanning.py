"""
Configurable Scanning Framework

Allows users to define custom scans of accession fields to build
keyword lists and structured vocabularies. Supports:
- Guided mode: step-by-step wizard interface
- Advanced mode: direct config file editing
- Open scans (discover all terms) and targeted scans (check for specific terms)
- Multiple named scan configurations that can be saved and reused
- Results feed into structured vocabulary sheets
"""

from collections import Counter
from typing import Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager
from sync.menu import Menu


# Fields available for scanning, organized by source
SCANNABLE_FIELDS = {
    "Accession Record": [
        ("content_description", "Content Description"),
        ("condition_description", "Condition Description"),
        ("inventory", "Inventory"),
        ("access_restrictions_note", "Access Restrictions Note"),
        ("use_restrictions_note", "Use Restrictions Note"),
        ("general_note", "General Note"),
        ("acquisition_type", "Acquisition Type"),
        ("provenance", "Provenance"),
        ("title", "Title"),
    ],
    "Extents Sub-record": [
        ("extents.extent_type", "Extent Type"),
        ("extents.container_summary", "Container Summary"),
    ],
    "Linked Records": [
        ("subjects.title", "Subject Titles"),
        ("agents.title", "Agent Names"),
        ("agents.sort_name", "Agent Sort Names"),
        ("classifications.title", "Classification Titles"),
        ("top_containers.type", "Top Container Type"),
        ("top_containers.container_type", "Top Container Type (detailed)"),
    ],
}

# Matching approach options
MATCHING_APPROACHES = [
    ("partial", "Case-insensitive partial matching (default)"),
    ("exact", "Exact match only (case-insensitive)"),
    ("fuzzy", "Fuzzy matching with configurable threshold"),
]


class ScanConfiguration:
    """Represents a saved scan configuration."""

    def __init__(
        self,
        name: str,
        fields: list[str],
        approach: str = "partial",
        target_terms: Optional[list[str]] = None,
        fuzzy_threshold: int = 85,
        target_column: str = "",
    ):
        """
        Initialize a scan configuration.

        Args:
            name: Human-readable name for this configuration.
            fields: List of field identifiers to scan.
            approach: Matching approach ('partial', 'exact', 'fuzzy').
            target_terms: Specific terms to look for (None for open scan).
            fuzzy_threshold: Similarity threshold for fuzzy matching (0-100).
            target_column: Which issue/format column this scan is for.
        """
        self.name = name
        self.fields = fields
        self.approach = approach
        self.target_terms = target_terms
        self.fuzzy_threshold = fuzzy_threshold
        self.target_column = target_column

    def to_dict(self) -> dict:
        """Serialize to a dictionary for storage in data.yml."""
        return {
            "name": self.name,
            "fields": self.fields,
            "approach": self.approach,
            "target_terms": self.target_terms,
            "fuzzy_threshold": self.fuzzy_threshold,
            "target_column": self.target_column,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ScanConfiguration":
        """Deserialize from a dictionary."""
        return cls(
            name=data.get("name", "Untitled"),
            fields=data.get("fields", []),
            approach=data.get("approach", "partial"),
            target_terms=data.get("target_terms"),
            fuzzy_threshold=data.get("fuzzy_threshold", 85),
            target_column=data.get("target_column", ""),
        )


class ScanResult:
    """Results from a scan operation."""

    def __init__(self):
        self.term_counts: Counter = Counter()
        self.matched_accessions: dict[str, list[int]] = {}  # term -> list of accession IDs
        self.total_accessions_scanned: int = 0
        self.fields_scanned: list[str] = []

    def add_term(self, term: str, accession_id: int) -> None:
        """Record a term occurrence."""
        self.term_counts[term] += 1
        if term not in self.matched_accessions:
            self.matched_accessions[term] = []
        if accession_id not in self.matched_accessions[term]:
            self.matched_accessions[term].append(accession_id)

    def get_top_terms(self, limit: int = 50) -> list[tuple[str, int]]:
        """Get the most common terms."""
        return self.term_counts.most_common(limit)

    def get_terms_above_threshold(self, min_count: int = 2) -> list[tuple[str, int]]:
        """Get terms appearing at least min_count times."""
        return [(t, c) for t, c in self.term_counts.most_common() if c >= min_count]


class ScanningFramework:
    """
    Manages the configurable scanning framework.

    Provides both guided (interactive) and advanced (config-based) modes
    for defining and executing scans.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        self.config = config
        self.logger = logger

    # -------------------------------------------------------------------------
    # Guided Mode (Interactive UI)
    # -------------------------------------------------------------------------

    def run_guided_scan(self, target_column: str = "") -> Optional[ScanResult]:
        """
        Launch the guided scanning interface.

        Walks the user through selecting fields, choosing a matching approach,
        and optionally providing target terms.

        Args:
            target_column: The column this scan is for (e.g., "Access Issues").

        Returns:
            ScanResult if the scan was executed, None if cancelled.
        """
        print(f"\n  {'=' * 50}")
        print("  Configurable Scanning Framework — Guided Mode")
        if target_column:
            print(f"  Target column: {target_column}")
        print(f"  {'=' * 50}")

        # Step 1: Select fields to scan
        fields = self._select_fields()
        if not fields:
            print("  No fields selected. Scan cancelled.")
            return None

        # Step 2: Choose scan type
        scan_type = Menu.prompt_choice(
            "What kind of scan?",
            [
                "Open scan — discover all terms in the selected fields",
                "Targeted scan — check for specific terms you provide",
                "Combined — discover terms AND check for specific ones",
            ],
        )

        # Step 3: Get target terms if needed
        target_terms: Optional[list[str]] = None
        if scan_type in (1, 2):
            target_terms = self._get_target_terms()

        # Step 4: Choose matching approach
        approach_idx = Menu.prompt_choice(
            "Matching approach:",
            [desc for _, desc in MATCHING_APPROACHES],
        )
        approach = MATCHING_APPROACHES[approach_idx][0]

        fuzzy_threshold = 85
        if approach == "fuzzy":
            threshold_str = Menu.prompt_text(
                "Fuzzy matching threshold (0-100, higher = stricter):",
                default="85",
            )
            try:
                fuzzy_threshold = int(threshold_str)
            except ValueError:
                fuzzy_threshold = 85

        # Step 5: Save configuration?
        config_name = ""
        if Menu.prompt_yes_no("Save this scan configuration for reuse?", default=False):
            config_name = Menu.prompt_text("Configuration name:")
            scan_config = ScanConfiguration(
                name=config_name,
                fields=fields,
                approach=approach,
                target_terms=target_terms,
                fuzzy_threshold=fuzzy_threshold,
                target_column=target_column,
            )
            self._save_configuration(scan_config)
            print(f"  Configuration '{config_name}' saved.")

        # Step 6: Execute the scan
        print("\n  Executing scan...")
        return self._execute_scan(fields, approach, target_terms, fuzzy_threshold)

    def run_saved_scan(self, config_name: str) -> Optional[ScanResult]:
        """
        Run a previously saved scan configuration.

        Args:
            config_name: Name of the saved configuration.

        Returns:
            ScanResult or None if configuration not found.
        """
        scan_config = self._load_configuration(config_name)
        if not scan_config:
            print(f"  Configuration '{config_name}' not found.")
            return None

        print(f"\n  Running saved scan: {scan_config.name}")
        print(f"  Fields: {', '.join(scan_config.fields)}")
        print(f"  Approach: {scan_config.approach}")
        if scan_config.target_terms:
            print(f"  Target terms: {len(scan_config.target_terms)}")

        return self._execute_scan(
            scan_config.fields,
            scan_config.approach,
            scan_config.target_terms,
            scan_config.fuzzy_threshold,
        )

    # -------------------------------------------------------------------------
    # Field Selection
    # -------------------------------------------------------------------------

    def _select_fields(self) -> list[str]:
        """Interactively select which fields to scan."""
        print("\n  Select fields to scan:")
        print("  (Enter numbers separated by commas, or 'all' for all fields)\n")

        all_fields: list[tuple[str, str, str]] = []  # (id, display_name, category)
        idx = 1
        for category, fields in SCANNABLE_FIELDS.items():
            print(f"  {category}:")
            for field_id, display_name in fields:
                print(f"    {idx:2d}. {display_name} ({field_id})")
                all_fields.append((field_id, display_name, category))
                idx += 1
            print()

        selection = Menu.prompt_text("Select fields (numbers or 'all'):")

        if selection.lower() == "all":
            return [f[0] for f in all_fields]

        try:
            indices = [int(s.strip()) for s in selection.split(",")]
            selected = []
            for i in indices:
                if 1 <= i <= len(all_fields):
                    selected.append(all_fields[i - 1][0])
            return selected
        except ValueError:
            print("  Invalid selection.")
            return []

    def _get_target_terms(self) -> list[str]:
        """Get target terms from the user."""
        print("\n  Enter terms to search for (one per line, empty line to finish):")
        terms: list[str] = []
        while True:
            term = input("    > ").strip()
            if not term:
                break
            terms.append(term)

        if not terms:
            print("  No terms entered. Enter as comma-separated instead:")
            text = Menu.prompt_text("Terms (comma-separated):")
            terms = [t.strip() for t in text.split(",") if t.strip()]

        print(f"  {len(terms)} target term(s) entered.")
        return terms

    # -------------------------------------------------------------------------
    # Scan Execution
    # -------------------------------------------------------------------------

    def _execute_scan(
        self,
        fields: list[str],
        approach: str,
        target_terms: Optional[list[str]],
        fuzzy_threshold: int = 85,
    ) -> ScanResult:
        """
        Execute a scan across accession records.

        Args:
            fields: Field identifiers to scan.
            approach: Matching approach.
            target_terms: Terms to look for (None for open scan).
            fuzzy_threshold: Threshold for fuzzy matching.

        Returns:
            ScanResult with discovered terms and frequencies.
        """
        result = ScanResult()
        result.fields_scanned = fields

        # Get accession data
        try:
            from sync.archivesspace import ArchivesSpaceClient
            client = ArchivesSpaceClient(self.config, self.logger)
            if not client.connect():
                self.logger.error("Could not connect to ArchivesSpace for scan.")
                return result

            accessions = client.get_all_accessions()
            result.total_accessions_scanned = len(accessions)

        except Exception as e:
            self.logger.error(f"Scan failed: {e}")
            print(f"  Error connecting to ArchivesSpace: {e}")
            return result

        # Process each accession with progress feedback
        from sync.progress import progress_bar

        for acc in progress_bar(accessions, desc="Scanning accessions", unit="accessions"):
            acc_id = 0
            uri = acc.get("uri", "")
            if uri:
                try:
                    acc_id = int(uri.split("/")[-1])
                except (ValueError, IndexError):
                    pass

            # Extract text from selected fields
            texts = self._extract_field_texts(acc, fields)

            if target_terms:
                # Targeted scan: check for specific terms
                for term in target_terms:
                    for text in texts:
                        if self._matches(text, term, approach, fuzzy_threshold):
                            result.add_term(term, acc_id)
                            break  # Only count once per accession per term

            # Open scan: discover all terms
            if target_terms is None or len(target_terms) == 0:
                for text in texts:
                    words = self._tokenize(text)
                    for word in words:
                        result.add_term(word, acc_id)

        self.logger.summary(
            f"Scan complete: {result.total_accessions_scanned} accessions, "
            f"{len(result.term_counts)} unique terms found."
        )

        return result

    def _extract_field_texts(self, accession: dict, fields: list[str]) -> list[str]:
        """Extract text content from the specified fields of an accession."""
        texts: list[str] = []

        for field_id in fields:
            if "." in field_id:
                # Nested field (e.g., "extents.extent_type")
                parts = field_id.split(".", 1)
                parent = parts[0]
                child = parts[1]

                if parent == "extents":
                    for extent in accession.get("extents", []):
                        val = extent.get(child, "")
                        if val:
                            texts.append(str(val))
                elif parent == "subjects":
                    for subj in accession.get("subjects", []):
                        # Subjects are references; we'd need to resolve them
                        # For scanning, use what's available in the accession record
                        val = subj.get(child, "")
                        if val:
                            texts.append(str(val))
                elif parent == "agents":
                    for agent in accession.get("linked_agents", []):
                        val = agent.get(child, "")
                        if val:
                            texts.append(str(val))
                elif parent == "classifications":
                    for cls in accession.get("classifications", []):
                        val = cls.get(child, "")
                        if val:
                            texts.append(str(val))
                elif parent == "top_containers":
                    for inst in accession.get("instances", []):
                        sc = inst.get("sub_container", {})
                        tc = sc.get("top_container", {})
                        val = tc.get(child, "")
                        if val:
                            texts.append(str(val))
            else:
                # Direct field on the accession
                val = accession.get(field_id, "")
                if val and isinstance(val, str):
                    texts.append(val)

        return texts

    def _tokenize(self, text: str) -> list[str]:
        """
        Split text into meaningful tokens for open scanning.

        Filters out very short words and common stop words.
        """
        import re

        stop_words = {
            "the", "and", "for", "are", "but", "not", "you", "all",
            "can", "had", "her", "was", "one", "our", "out", "has",
            "this", "that", "with", "from", "they", "been", "have",
            "will", "each", "make", "like", "some", "into", "than",
            "its", "also", "these", "other", "which", "their",
            "about", "would", "there", "could", "more", "very",
        }

        words = re.findall(r"[a-zA-Z]{3,}", text.lower())
        return [w for w in words if w not in stop_words]

    def _matches(
        self, text: str, term: str, approach: str, fuzzy_threshold: int
    ) -> bool:
        """Check if a term matches in the text using the specified approach."""
        text_lower = text.lower()
        term_lower = term.lower()

        if approach == "exact":
            # Exact word match (case-insensitive)
            import re
            return bool(re.search(r"\b" + re.escape(term_lower) + r"\b", text_lower))

        elif approach == "partial":
            return term_lower in text_lower

        elif approach == "fuzzy":
            try:
                from rapidfuzz import fuzz
                # Check against each word
                for word in text_lower.split():
                    score = fuzz.partial_ratio(term_lower, word)
                    if score >= fuzzy_threshold:
                        return True
                # Also check as substring
                if term_lower in text_lower:
                    return True
            except ImportError:
                # Fall back to partial matching
                return term_lower in text_lower

        return False

    # -------------------------------------------------------------------------
    # Configuration Management
    # -------------------------------------------------------------------------

    def _save_configuration(self, scan_config: ScanConfiguration) -> None:
        """Save a scan configuration to data.yml."""
        configs = self.config.get_data("issue_scan_configs", default={}) or {}
        configs[scan_config.name] = scan_config.to_dict()
        self.config.set_data("issue_scan_configs", value=configs)
        self.config.save_data()

    def _load_configuration(self, name: str) -> Optional[ScanConfiguration]:
        """Load a scan configuration from data.yml."""
        configs = self.config.get_data("issue_scan_configs", default={}) or {}
        if name in configs:
            return ScanConfiguration.from_dict(configs[name])
        return None

    def get_saved_configurations(self) -> list[str]:
        """Get names of all saved scan configurations."""
        configs = self.config.get_data("issue_scan_configs", default={}) or {}
        return list(configs.keys())

    def delete_configuration(self, name: str) -> bool:
        """Delete a saved scan configuration."""
        configs = self.config.get_data("issue_scan_configs", default={}) or {}
        if name in configs:
            del configs[name]
            self.config.set_data("issue_scan_configs", value=configs)
            self.config.save_data()
            return True
        return False

    # -------------------------------------------------------------------------
    # Results Presentation and Vocabulary Building
    # -------------------------------------------------------------------------

    def present_results(self, result: ScanResult) -> list[str]:
        """
        Present scan results to the user and let them select terms
        to add to a structured vocabulary.

        Args:
            result: The scan results to present.

        Returns:
            List of terms approved by the user.
        """
        if not result.term_counts:
            print("\n  No terms found in the scan.")
            return []

        print(f"\n  Scan Results")
        print(f"  {'─' * 50}")
        print(f"  Accessions scanned: {result.total_accessions_scanned}")
        print(f"  Unique terms found: {len(result.term_counts)}")
        print(f"  Fields scanned: {', '.join(result.fields_scanned)}")

        # Show top terms
        top = result.get_top_terms(limit=30)
        print(f"\n  Top {len(top)} terms by frequency:")
        print(f"  {'─' * 50}")

        for idx, (term, count) in enumerate(top, 1):
            acc_count = len(result.matched_accessions.get(term, []))
            print(f"    {idx:3d}. {term:<35s} ({count} occurrences, {acc_count} accessions)")

        # Let user select terms to approve
        print(f"\n  Select terms to add to your vocabulary.")
        print(f"  Enter numbers (comma-separated), 'all' for all, or 'none' to skip.")

        selection = Menu.prompt_text("Selection:")

        if selection.lower() == "none" or not selection.strip():
            return []

        if selection.lower() == "all":
            return [term for term, _ in top]

        try:
            indices = [int(s.strip()) for s in selection.split(",")]
            approved = []
            for i in indices:
                if 1 <= i <= len(top):
                    approved.append(top[i - 1][0])
            return approved
        except ValueError:
            print("  Invalid selection.")
            return []

    def apply_results_to_vocabulary(
        self,
        approved_terms: list[str],
        target_column: str,
        merge_with_existing: bool = True,
    ) -> None:
        """
        Add approved terms to the structured vocabulary for a column.

        Args:
            approved_terms: Terms to add.
            target_column: Which column's vocabulary to update.
            merge_with_existing: If True, add to existing terms. If False, replace.
        """
        # Map column names to data.yml keys
        vocab_keys = {
            "Access Issues": "access_issues_vocabulary",
            "Conservation Issues": "conservation_issues_vocabulary",
            "Digital Issues": "digital_issues_vocabulary",
            "Documentation and Use Issues": "documentation_use_issues_options",
            "Other Processing Information": "other_processing_options",
            "Physical Space Management Issues": "physical_space_options",
        }

        key = vocab_keys.get(target_column)
        if not key:
            # Use a generic key
            key = f"vocabulary_{target_column.lower().replace(' ', '_')}"

        existing = self.config.get_data(key, default=[]) or []

        if merge_with_existing:
            # Add new terms that aren't already present
            combined = list(existing)
            for term in approved_terms:
                if term not in combined:
                    combined.append(term)
            updated = combined
        else:
            updated = approved_terms

        self.config.set_data(key, value=updated)
        self.config.save_data()

        new_count = len(updated) - len(existing) if merge_with_existing else len(updated)
        self.logger.summary(
            f"Vocabulary updated for '{target_column}': "
            f"{new_count} new term(s), {len(updated)} total."
        )
        print(f"  Vocabulary updated: {len(updated)} total terms for '{target_column}'.")

    # -------------------------------------------------------------------------
    # Interactive Menu Integration
    # -------------------------------------------------------------------------

    def scan_menu(self, scan_type: str = "general") -> None:
        """
        Launch the scanning menu for a specific scan type.

        Args:
            scan_type: "formats", "subjects", "issues", or "general"
        """
        print(f"\n  {'=' * 50}")
        print(f"  Scanning Framework")
        print(f"  {'=' * 50}")

        # Check for saved configurations
        saved = self.get_saved_configurations()

        options = ["Run a new guided scan"]
        if saved:
            options.append(f"Run a saved scan ({len(saved)} available)")
            options.append("View/delete saved scans")
        options.append("Cancel")

        choice = Menu.prompt_choice("What would you like to do?", options)

        if choice == 0:
            # New guided scan
            target_column = self._select_target_column(scan_type)
            result = self.run_guided_scan(target_column=target_column)
            if result and result.term_counts:
                approved = self.present_results(result)
                if approved and target_column:
                    merge = Menu.prompt_yes_no(
                        "Merge with existing vocabulary? (No = replace entirely)"
                    )
                    self.apply_results_to_vocabulary(approved, target_column, merge)

        elif choice == 1 and saved:
            # Run saved scan
            scan_name_idx = Menu.prompt_choice("Select a configuration:", saved)
            result = self.run_saved_scan(saved[scan_name_idx])
            if result and result.term_counts:
                approved = self.present_results(result)
                config = self._load_configuration(saved[scan_name_idx])
                if approved and config and config.target_column:
                    merge = Menu.prompt_yes_no("Merge with existing vocabulary?")
                    self.apply_results_to_vocabulary(
                        approved, config.target_column, merge
                    )

        elif (choice == 2 and saved) or (choice == 1 and not saved):
            if saved:
                self._manage_saved_scans()

    def _select_target_column(self, scan_type: str) -> str:
        """Let the user select which column the scan results are for."""
        if scan_type == "formats":
            # List format columns
            formats = self.config.get("format_keywords", default={})
            format_names = list(formats.keys())
            if format_names:
                choice = Menu.prompt_choice("Which format column?", format_names + ["New format column"])
                if choice < len(format_names):
                    return format_names[choice]
                else:
                    name = Menu.prompt_text("New format column name:")
                    return name
            return ""

        elif scan_type == "issues":
            columns = [
                "Access Issues",
                "Conservation Issues",
                "Digital Issues",
                "Documentation and Use Issues",
                "Other Processing Information",
                "Physical Space Management Issues",
            ]
            choice = Menu.prompt_choice("Which issue column?", columns)
            return columns[choice]

        elif scan_type == "subjects":
            return "Subject Descriptors"

        else:
            # General: let user choose any column
            all_columns = [
                "Access Issues",
                "Conservation Issues",
                "Digital Issues",
                "Documentation and Use Issues",
                "Other Processing Information",
                "Physical Space Management Issues",
                "Format Keywords",
                "Subject Descriptors",
                "Other (custom)",
            ]
            choice = Menu.prompt_choice("Target column:", all_columns)
            if choice == len(all_columns) - 1:
                return Menu.prompt_text("Custom target name:")
            return all_columns[choice]

    def _manage_saved_scans(self) -> None:
        """View and manage saved scan configurations."""
        saved = self.get_saved_configurations()
        if not saved:
            print("  No saved configurations.")
            return

        print("\n  Saved Scan Configurations:")
        for idx, name in enumerate(saved, 1):
            config = self._load_configuration(name)
            if config:
                print(f"    {idx}. {name}")
                print(f"       Fields: {', '.join(config.fields[:3])}{'...' if len(config.fields) > 3 else ''}")
                print(f"       Approach: {config.approach}")
                if config.target_column:
                    print(f"       Target: {config.target_column}")
                if config.target_terms:
                    print(f"       Terms: {len(config.target_terms)} defined")

        choice = Menu.prompt_choice(
            "Action:",
            ["Run a configuration", "Delete a configuration", "Cancel"],
        )

        if choice == 0:
            scan_idx = Menu.prompt_choice("Which configuration?", saved)
            result = self.run_saved_scan(saved[scan_idx])
            if result:
                self.present_results(result)

        elif choice == 1:
            del_idx = Menu.prompt_choice("Which configuration to delete?", saved)
            if Menu.prompt_yes_no(f"Delete '{saved[del_idx]}'?"):
                self.delete_configuration(saved[del_idx])
                print(f"  Configuration '{saved[del_idx]}' deleted.")
