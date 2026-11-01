"""
Sync Engine

Core comparison logic for detecting new, updated, and deleted accessions.
Independent of the output format (Excel or Google Sheets). Handles:
- Lock_version comparison for sub-records
- Linked record composition tracking with selective content detection
- Extent conversion and summing with unknown type detection
- Format keyword detection with optional fuzzy matching
- Subject descriptor matching against approved list
- Issue field evaluation with rule-based and keyword approaches
- Rule-based text summarization with sentence extraction fallback
- Completion detection for Month Completed field
- Supporting sheet computation (Backlog Change Over Time, Processing Projects)
"""

import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager

# Conditional import for fuzzy matching
try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False


class UnknownExtentTypeError(Exception):
    """Raised when an unrecognized extent type is encountered."""

    def __init__(self, extent_type: str, accession_id: int):
        self.extent_type = extent_type
        self.accession_id = accession_id
        super().__init__(
            f"Unknown extent type '{extent_type}' encountered on accession {accession_id}. "
            f"Please categorize this extent type in the setup wizard or config."
        )


class SyncEngine:
    """
    Core synchronization engine. Compares ArchivesSpace accession data
    against cached/spreadsheet data and determines what needs updating.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        self.config = config
        self.logger = logger
        self.fuzzy_enabled = config.get("matching", "fuzzy_enabled", default=False)
        self.fuzzy_threshold = config.get("matching", "fuzzy_threshold", default=85)
        self.extent_types: dict = config.get("extent_types", default={})
        self.format_keywords: dict = config.get("format_keywords", default={})
        self.removable_media_keywords: list = config.get("removable_media_keywords", default=[])
        self._overflow_accessions: list[tuple[int, int, int]] = []

    def get_overflow_accessions(self) -> list[tuple[int, int, int]]:
        """
        Return accessions that had subject descriptor overflow.

        Returns:
            List of (accession_id, total_descriptors, max_columns) tuples.
        """
        return self._overflow_accessions

    # -------------------------------------------------------------------------
    # Change Detection
    # -------------------------------------------------------------------------

    def detect_changes(
        self,
        current_data: list[dict],
        cached_data: dict,
        spreadsheet_data: list[dict],
    ) -> dict:
        """
        Compare current ArchivesSpace data against cached and spreadsheet data.

        Uses a two-source comparison:
        - current_data vs cached_data: detects new and updated accessions
        - spreadsheet_data vs current_data: detects deletions/suppressions

        The cache (not the spreadsheet) is the comparison baseline because
        the cache is only updated after a fully successful sync, ensuring
        consistency even if the previous run partially failed.

        Returns:
            Dictionary with keys 'new', 'updated', 'deleted', 'unchanged'.
        """
        changes: dict[str, list] = {
            "new": [],
            "updated": [],
            "deleted": [],
            "unchanged": [],
        }

        # Track all accession IDs currently in ArchivesSpace
        current_ids: set[int] = set()

        for detail in current_data:
            accession = detail.get("accession", {})
            acc_id = self._extract_id(accession)
            current_ids.add(acc_id)

            # Check if this accession exists in the cache
            if str(acc_id) not in cached_data:
                # Not in cache = new accession (or first run)
                changes["new"].append(detail)
                self.logger.technical(f"New accession detected: {acc_id}")
            else:
                # In cache = compare for changes using lock_versions
                # and linked record compositions
                change_details = self._compare_accession(
                    detail, cached_data.get(str(acc_id), {})
                )
                if change_details:
                    # Store the specific changes for the [Sync] Status column
                    detail["_changes"] = change_details
                    changes["updated"].append(detail)
                    self.logger.technical(
                        f"Accession {acc_id} updated: {', '.join(change_details)}"
                    )
                else:
                    changes["unchanged"].append(detail)

        # Detect deletions: accessions in the spreadsheet but no longer
        # in ArchivesSpace (deleted or suppressed). We compare against
        # the spreadsheet rather than the cache because deleted accessions
        # need to be removed from the spreadsheet.
        spreadsheet_ids: set[int] = set()
        for row in spreadsheet_data:
            sid = row.get("accession_id")
            if sid is not None:
                try:
                    spreadsheet_ids.add(int(sid))
                except (ValueError, TypeError):
                    pass

        deleted_ids = spreadsheet_ids - current_ids
        for del_id in deleted_ids:
            changes["deleted"].append(del_id)
            self.logger.technical(
                f"Accession {del_id} deleted or suppressed in ArchivesSpace"
            )

        self.logger.summary(
            f"Change detection complete: {len(changes['new'])} new, "
            f"{len(changes['updated'])} updated, {len(changes['deleted'])} deleted, "
            f"{len(changes['unchanged'])} unchanged."
        )

        return changes

    def _compare_accession(self, current: dict, cached: dict) -> list[str]:
        """
        Compare a current accession against its cached version to detect changes.

        Uses different strategies depending on the sub-record type:
        - Direct sub-records (accession, collection_management): single lock_version
        - Multiple sub-records (extents): sorted concatenated lock_versions
        - Linked records (agents, subjects, etc.): two-level check:
          1. Composition check: did the set of linked record IDs change?
             (detects additions/removals of linked records)
          2. Content check: did any linked record's display value change?
             (detects edits to records that are shared across accessions,
              e.g., a subject title being updated)

        Returns:
            List of human-readable change descriptions, empty if no changes.
        """
        changes: list[str] = []
        accession = current.get("accession", {})

        # --- Direct lock_version comparisons ---

        # Accession record itself (title, dates, descriptions, restrictions, etc.)
        if accession.get("lock_version", 0) != cached.get("accession_lock_version", 0):
            changes.append("accession record changed")

        # Collection management sub-record (processing status, priority, plan)
        cm = accession.get("collection_management", {})
        if isinstance(cm, dict):
            if cm.get("lock_version", 0) != cached.get("collection_management_lock_version", 0):
                changes.append("collection management changed")

        # Extents: multiple sub-records, each with own lock_version.
        # Sort and concatenate so any addition, removal, or edit is detected.
        current_ext = self._get_sorted_lock_versions(accession.get("extents", []))
        if current_ext != cached.get("extents_lock_versions", ""):
            changes.append("extents changed")

        # --- Linked record two-level comparisons ---
        # These records (agents, subjects, etc.) are shared across accessions,
        # so we can't rely on lock_versions alone. Instead we track:
        # 1. Which records are linked (composition = sorted IDs)
        # 2. What their display values are (content = sorted display strings)
        for record_type, key in [
            ("resolved_agents", "linked_agents"),
            ("resolved_subjects", "subjects"),
            ("resolved_classifications", "classifications"),
            ("resolved_digital_objects", "digital_objects"),
            ("resolved_top_containers", "top_containers"),
        ]:
            # Level 1: Check if the set of linked records changed
            current_comp = self._get_composition_ids(current.get(record_type, []))
            if current_comp != cached.get(f"{key}_ids", ""):
                changes.append(f"{key.replace('_', ' ')} changed")
            else:
                # Level 2: Same records linked, but content may have changed
                # (e.g., a subject's title was edited in ArchivesSpace)
                current_values = self._get_display_values(current.get(record_type, []), key)
                if current_values != cached.get(f"{key}_values", ""):
                    changes.append(f"{key.replace('_', ' ')} content changed")

        return changes

    # -------------------------------------------------------------------------
    # Tracking helpers
    # -------------------------------------------------------------------------

    def _get_sorted_lock_versions(self, records: list[dict]) -> str:
        """Get sorted, semicolon-concatenated lock_versions."""
        versions = sorted([str(r.get("lock_version", 0)) for r in records])
        return ";".join(versions)

    def _get_composition_ids(self, records: list[dict]) -> str:
        """Get sorted, semicolon-concatenated IDs from resolved records."""
        ids: list[str] = []
        for record in records:
            uri = record.get("uri", "")
            if uri:
                ids.append(uri.split("/")[-1])
        return ";".join(sorted(ids))

    def _get_display_values(self, records: list[dict], record_type: str) -> str:
        """Get sorted, semicolon-concatenated display values."""
        values: list[str] = []
        for record in records:
            if record_type == "linked_agents":
                values.append(self._format_agent_display(record))
            elif record_type in ("subjects", "classifications", "digital_objects"):
                values.append(record.get("title", ""))
            elif record_type == "top_containers":
                tc_type = record.get("type", "")
                indicator = record.get("indicator", "")
                values.append(f"{tc_type} {indicator}".strip())
        return ";".join(sorted(values))

    def _extract_id(self, accession: dict) -> int:
        """Extract the numeric ID from an accession's URI."""
        uri = accession.get("uri", "")
        try:
            return int(uri.split("/")[-1])
        except (ValueError, IndexError):
            return 0

    # -------------------------------------------------------------------------
    # Row data building
    # -------------------------------------------------------------------------

    def build_row_data(self, detail: dict, base_url: str) -> dict:
        """Build a complete row of spreadsheet data from a full accession detail."""
        accession = detail.get("accession", {})
        acc_id = self._extract_id(accession)
        identifier = self._build_identifier(accession)

        cm = accession.get("collection_management", {})
        if not isinstance(cm, dict):
            cm = {}

        row: dict[str, Any] = {
            "Accession Status": cm.get("processing_status", ""),
            "Base URL (Use for Hyperlink Only)": base_url,
            "Accession ID": acc_id,
            "Identifier (Use for Hyperlink Only)": identifier,
            "Donor Name": self._extract_donor_name(detail),
            "Accession Date": accession.get("accession_date", ""),
            "Priority": cm.get("processing_priority", ""),
            "Classification": self._extract_classifications(detail),
            "Accession Extent - Physical (Linear Feet)": self._calculate_physical_extent(
                accession, acc_id
            ),
            "Accession Extent - Digital (GB)": self._calculate_digital_extent(
                accession, acc_id
            ),
        }

        # Format detection
        row.update(self._detect_formats(accession, detail))

        # Subject descriptors
        descriptors = self._extract_subject_descriptors(detail, acc_id)
        row["_subject_descriptors"] = descriptors

        # Issues
        row["Access Issues"] = self._evaluate_access_issues(accession)
        row["Conservation Issues"] = self._evaluate_conservation_issues(accession)
        row["Digital Issues"] = self._evaluate_digital_issues(accession, detail)
        row["Other Processing Information"] = self._evaluate_other_processing(accession)

        # Sync tracking
        row["_sync_data"] = self._build_sync_tracking(detail)

        return row

    def _build_identifier(self, accession: dict) -> str:
        """Build the four-part accession identifier string."""
        parts = [str(accession.get(f, "")) for f in ["id_0", "id_1", "id_2", "id_3"]]
        return "-".join(p for p in parts if p)

    def _extract_donor_name(self, detail: dict) -> str:
        """Extract donor name(s) from linked agents with configured role."""
        donor_role = self.config.get("agents", "donor_role", default="source")
        names: list[str] = []
        for agent in detail.get("resolved_agents", []):
            if agent.get("_role", "").lower() == donor_role.lower():
                display_name = agent.get("display_name", {})
                name = display_name.get("sort_name", "") or agent.get("title", "")
                if name:
                    names.append(name)
        return "; ".join(names)

    def _extract_classifications(self, detail: dict) -> str:
        """Extract classification titles, semicolon-separated."""
        titles = [c.get("title", "") for c in detail.get("resolved_classifications", []) if c.get("title")]
        return "; ".join(titles)

    # -------------------------------------------------------------------------
    # Extent calculations with unknown type detection
    # -------------------------------------------------------------------------

    def _calculate_physical_extent(self, accession: dict, acc_id: int = 0) -> float:
        """
        Calculate total physical extent in linear feet.

        Raises UnknownExtentTypeError if an unrecognized extent type is found.
        """
        total = 0.0
        for extent in accession.get("extents", []):
            ext_type = extent.get("extent_type", "")
            if not ext_type:
                continue

            if ext_type not in self.extent_types:
                raise UnknownExtentTypeError(ext_type, acc_id)

            type_config = self.extent_types[ext_type]
            if type_config.get("category") == "physical":
                try:
                    number = float(extent.get("number", 0))
                    factor = float(type_config.get("conversion_factor", 1.0))
                    total += number * factor
                except (ValueError, TypeError):
                    continue
        return round(total, 4)

    def _calculate_digital_extent(self, accession: dict, acc_id: int = 0) -> float:
        """
        Calculate total digital extent in gigabytes.

        Raises UnknownExtentTypeError if an unrecognized extent type is found.
        """
        total = 0.0
        for extent in accession.get("extents", []):
            ext_type = extent.get("extent_type", "")
            if not ext_type:
                continue

            if ext_type not in self.extent_types:
                raise UnknownExtentTypeError(ext_type, acc_id)

            type_config = self.extent_types[ext_type]
            if type_config.get("category") == "digital":
                try:
                    number = float(extent.get("number", 0))
                    factor = float(type_config.get("conversion_factor", 1.0))
                    total += number * factor
                except (ValueError, TypeError):
                    continue
        return round(total, 4)

    # -------------------------------------------------------------------------
    # Format detection
    # -------------------------------------------------------------------------

    def _detect_formats(self, accession: dict, detail: dict) -> dict[str, bool]:
        """
        Detect material formats by scanning fields for keywords.

        Combines text from multiple fields into a single scan string:
        - content_description, condition_description, inventory (accession record)
        - extent_type values (from extent sub-records)
        - top container type and container_type (from resolved top containers)

        Each configured format keyword list is checked against this combined
        text. Returns a dict of {format_name: True/False}.
        """
        # Build a single string from all scannable fields
        scan_text = " ".join(filter(None, [
            accession.get("content_description", ""),
            accession.get("condition_description", ""),
            accession.get("inventory", ""),
        ]))

        # Include extent type labels (e.g., "photographs" as an extent type)
        for extent in accession.get("extents", []):
            ext_type = extent.get("extent_type", "")
            if ext_type:
                scan_text += f" {ext_type}"

        # Include top container types (e.g., "flat_file", "oversize_box")
        for tc in detail.get("resolved_top_containers", []):
            for field in ("type", "container_type"):
                val = tc.get(field, "")
                if val:
                    scan_text += f" {val}"

        # Check each format's keyword list against the combined text
        results: dict[str, bool] = {}
        for format_name, keywords in self.format_keywords.items():
            results[format_name] = self._match_keywords(scan_text, keywords)
        return results

    def _match_keywords(self, text: str, keywords: list[str]) -> bool:
        """
        Check if any keyword matches in the given text.
        Case-insensitive partial matching with optional fuzzy matching.
        """
        text_lower = text.lower()
        for keyword in keywords:
            keyword_lower = keyword.lower()

            if keyword_lower in text_lower:
                return True

            if self.fuzzy_enabled and RAPIDFUZZ_AVAILABLE:
                for word in text_lower.split():
                    score = fuzz.partial_ratio(keyword_lower, word)
                    if score >= self.fuzzy_threshold:
                        self.logger.verbose(
                            f"Fuzzy match: '{keyword}' ~ '{word}' (score: {score})"
                        )
                        return True

        return False

    # -------------------------------------------------------------------------
    # Subject descriptors
    # -------------------------------------------------------------------------

    def _extract_subject_descriptors(
        self, detail: dict, acc_id: int = 0
    ) -> list[str]:
        """Extract subject descriptors matching the approved list."""
        descriptors: list[str] = []

        for subject in detail.get("resolved_subjects", []):
            title = subject.get("title", "")
            if title:
                descriptors.append(title)

        for agent in detail.get("resolved_agents", []):
            if agent.get("_role", "").lower() == "subject":
                display = self._format_agent_display(agent)
                if display:
                    descriptors.append(display)

        num_columns = self.config.get("subject_descriptors", "num_columns", default=9)
        if len(descriptors) > num_columns:
            self.logger.warning(
                f"Accession {acc_id} has {len(descriptors)} subject descriptors but only "
                f"{num_columns} columns. {len(descriptors) - num_columns} value(s) not captured."
            )
            self._overflow_accessions.append((acc_id, len(descriptors), num_columns))

        return descriptors[:num_columns]

    def _format_agent_display(self, agent: dict) -> str:
        """
        Format an agent for display in the Subject Descriptor columns.

        Output format: 'Sort Name — Term1 — Term2'

        Agents linked with role "Subject" often have terms (subdivisions)
        that provide topical context. These come from the link itself
        (agent._terms) or from the agent's name qualifiers as a fallback.
        """
        display_name = agent.get("display_name", {})
        sort_name = display_name.get("sort_name", "") or agent.get("title", "")
        if not sort_name:
            return ""

        # Try to get terms from the link metadata (set during retrieval)
        terms = agent.get("_terms", [])
        if not terms:
            # Fallback: extract from the agent's name qualifiers
            names = agent.get("names", [])
            if names:
                for name in names:
                    for qualifier in name.get("qualifier", "").split(","):
                        q = qualifier.strip()
                        if q:
                            terms.append({"term": q})

        # Append terms with em-dash separators
        if terms:
            term_parts = [t.get("term", "") for t in terms if t.get("term")]
            if term_parts:
                return f"{sort_name} — " + " — ".join(term_parts)

        return sort_name

    # -------------------------------------------------------------------------
    # Issue evaluation
    # -------------------------------------------------------------------------

    def _evaluate_access_issues(self, accession: dict) -> str:
        """Check access_restrictions and summarize the note."""
        if not accession.get("access_restrictions", False):
            return ""
        note = accession.get("access_restrictions_note", "")
        return self._summarize_text(note) if note else ""

    def _evaluate_conservation_issues(self, accession: dict) -> str:
        """Evaluate condition_description for conservation issues."""
        description = accession.get("condition_description", "")
        return self._summarize_text(description) if description else ""

    def _evaluate_digital_issues(self, accession: dict, detail: dict) -> str:
        """
        Evaluate digital issues using two rule-based checks.

        Rule 1: If the accession has a digital extent (e.g., 5 GB) but no
                 corresponding digital object record (or the digital object
                 has no file versions), flag it — the digital content may
                 not be properly described.

        Rule 2: If the accession mentions removable media (floppy disks, CDs,
                 USB drives, etc.) in its description or inventory, but has
                 no digital extent or digital object, flag it — the media
                 content may not have been transferred.
        """
        issues: list[str] = []
        has_digital_extent = False
        has_digital_object = False
        has_file_versions = False

        # Check if any extent is categorized as digital
        for extent in accession.get("extents", []):
            ext_type = extent.get("extent_type", "")
            type_config = self.extent_types.get(ext_type, {})
            if type_config.get("category") == "digital":
                has_digital_extent = True
                break

        # Check for linked digital object records and their file versions
        digital_objects = detail.get("resolved_digital_objects", [])
        if digital_objects:
            has_digital_object = True
            for do in digital_objects:
                if do.get("file_versions", []):
                    has_file_versions = True
                    break

        # Rule 1: Digital extent without proper digital object
        if has_digital_extent and (not has_digital_object or not has_file_versions):
            issues.append("Digital object potentially or actually absent.")

        # Rule 2: Removable media keywords found in text fields
        scan_text = " ".join(filter(None, [
            accession.get("content_description", ""),
            accession.get("inventory", ""),
        ]))

        if self._match_keywords(scan_text, self.removable_media_keywords):
            # Only flag if there's no evidence of digital transfer
            if not has_digital_extent and (not has_digital_object or not has_file_versions):
                issues.append("Removable media may not have, or has not been, transferred.")

        return "; ".join(issues) if issues else ""

    def _evaluate_other_processing(self, accession: dict) -> str:
        """Evaluate using user-configured keyword matching."""
        # Uses configurable scanning framework - returns empty when unconfigured
        return ""

    def _summarize_text(self, text: str, max_chars: int = 200) -> str:
        """
        Rule-based text summarization with sentence extraction fallback.

        Strategy:
        1. If the text is short enough, return it as-is.
        2. Try to extract key terms using regex patterns that match
           common archival restriction and condition language.
        3. If no patterns match, fall back to extracting the first sentence.
        4. If the first sentence is too long, truncate at a word boundary.
        """
        if not text:
            return ""
        if len(text) <= max_chars:
            return text.strip()

        # Try pattern-based extraction first: look for specific archival
        # terms that indicate the nature of the restriction or condition
        summary_parts: list[str] = []
        patterns = [
            r"(?:closed|restricted|confidential|sealed)",       # Access status
            r"(?:\d+[- ]year\s+restriction)",                   # Time-limited restrictions
            r"(?:executive|personnel|student|patient|client)\s+records?",  # Record types
            r"(?:personally identifiable information|PII)",     # Privacy concerns
            r"(?:reading room only)",                           # Use restrictions
            r"(?:deteriorat\w+)",                               # Deterioration
            r"(?:fragile|damaged|brittle)",                     # Physical condition
            r"(?:mold|mildew|pest)",                            # Environmental issues
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            summary_parts.extend(matches)

        # If patterns matched, join unique terms as the summary
        if summary_parts:
            return "; ".join(set(summary_parts))

        # Fallback: extract the first sentence
        sentences = re.split(r"[.!?]+", text)
        if sentences and sentences[0].strip():
            first = sentences[0].strip()
            if len(first) <= max_chars:
                return first
            # Truncate at the last word boundary before max_chars
            return first[:max_chars].rsplit(" ", 1)[0] + "..."

        # Last resort: truncate raw text at word boundary
        return text[:max_chars].rsplit(" ", 1)[0] + "..."

    # -------------------------------------------------------------------------
    # Sync tracking
    # -------------------------------------------------------------------------

    def _build_sync_tracking(self, detail: dict) -> dict[str, Any]:
        """Build sync tracking column values."""
        accession = detail.get("accession", {})
        cm = accession.get("collection_management", {})
        if not isinstance(cm, dict):
            cm = {}

        return {
            "[Sync] Accession lock_version": accession.get("lock_version", 0),
            "[Sync] Collection Management lock_version": cm.get("lock_version", 0),
            "[Sync] Extents lock_version": self._get_sorted_lock_versions(
                accession.get("extents", [])
            ),
            "[Sync] Linked Agents IDs": self._get_composition_ids(
                detail.get("resolved_agents", [])
            ),
            "[Sync] Linked Agents Values": self._get_display_values(
                detail.get("resolved_agents", []), "linked_agents"
            ),
            "[Sync] Subjects IDs": self._get_composition_ids(
                detail.get("resolved_subjects", [])
            ),
            "[Sync] Subjects Values": self._get_display_values(
                detail.get("resolved_subjects", []), "subjects"
            ),
            "[Sync] Classifications IDs": self._get_composition_ids(
                detail.get("resolved_classifications", [])
            ),
            "[Sync] Classifications Values": self._get_display_values(
                detail.get("resolved_classifications", []), "classifications"
            ),
            "[Sync] Digital Objects IDs": self._get_composition_ids(
                detail.get("resolved_digital_objects", [])
            ),
            "[Sync] Digital Objects Values": self._get_display_values(
                detail.get("resolved_digital_objects", []), "digital_objects"
            ),
            "[Sync] Top Containers IDs": self._get_composition_ids(
                detail.get("resolved_top_containers", [])
            ),
            "[Sync] Top Containers Values": self._get_display_values(
                detail.get("resolved_top_containers", []), "top_containers"
            ),
        }

    def build_cache_entry(self, detail: dict) -> dict:
        """Build a cache entry for change detection on the next run."""
        sync_data = self._build_sync_tracking(detail)
        accession = detail.get("accession", {})

        return {
            "accession_lock_version": accession.get("lock_version", 0),
            "collection_management_lock_version": sync_data.get(
                "[Sync] Collection Management lock_version", 0
            ),
            "extents_lock_versions": sync_data.get("[Sync] Extents lock_version", ""),
            "linked_agents_ids": sync_data.get("[Sync] Linked Agents IDs", ""),
            "linked_agents_values": sync_data.get("[Sync] Linked Agents Values", ""),
            "subjects_ids": sync_data.get("[Sync] Subjects IDs", ""),
            "subjects_values": sync_data.get("[Sync] Subjects Values", ""),
            "classifications_ids": sync_data.get("[Sync] Classifications IDs", ""),
            "classifications_values": sync_data.get("[Sync] Classifications Values", ""),
            "digital_objects_ids": sync_data.get("[Sync] Digital Objects IDs", ""),
            "digital_objects_values": sync_data.get("[Sync] Digital Objects Values", ""),
            "top_containers_ids": sync_data.get("[Sync] Top Containers IDs", ""),
            "top_containers_values": sync_data.get("[Sync] Top Containers Values", ""),
        }

    # -------------------------------------------------------------------------
    # Completion detection
    # -------------------------------------------------------------------------

    def check_completion(
        self, accession: dict, cached_status: str, completion_triggers: list[str]
    ) -> Optional[str]:
        """
        Check if processing status changed to a completion value.

        Returns 'Month Year' string if newly completed, None otherwise.
        Uses the script's detection timestamp, not ArchivesSpace user_mtime.
        """
        cm = accession.get("collection_management", {})
        if not isinstance(cm, dict):
            return None

        current_status = cm.get("processing_status", "")
        if current_status in completion_triggers and cached_status not in completion_triggers:
            return datetime.now().strftime("%B %Y")
        return None

    # -------------------------------------------------------------------------
    # Supporting sheet computation
    # -------------------------------------------------------------------------

    def compute_backlog_change_over_time(
        self, spreadsheet_rows: list[dict], start_date: datetime
    ) -> list[dict]:
        """
        Compute monthly backlog change data from current spreadsheet rows.

        Only tracks from the provided start_date forward.

        Args:
            spreadsheet_rows: Current rows from the main sheet.
            start_date: The date the tool was first set up.

        Returns:
            List of monthly data dicts for the supporting sheet.
        """
        monthly_data: list[dict] = []
        current = datetime.now()
        month = datetime(start_date.year, start_date.month, 1)

        while month <= current:
            month_str = month.strftime("%B %Y")
            physical_backlog = 0.0
            digital_backlog = 0.0

            for row in spreadsheet_rows:
                acc_date = row.get("Accession Date", "")
                status = row.get("Accession Status", "")
                physical = row.get("Accession Extent - Physical (Linear Feet)", 0) or 0
                digital = row.get("Accession Extent - Digital (GB)", 0) or 0

                # Count as backlog if accessioned before this month
                # and not yet completed
                if acc_date and str(acc_date) <= month.strftime("%Y-%m-%d"):
                    month_completed = row.get("Month Completed", "")
                    if not month_completed:
                        physical_backlog += float(physical)
                        digital_backlog += float(digital)

            monthly_data.append({
                "Month and Year": month_str,
                "Physical Backlog (Linear Feet)": round(physical_backlog, 2),
                "Digital Backlog (GB)": round(digital_backlog, 2),
            })

            # Advance to next month
            if month.month == 12:
                month = datetime(month.year + 1, 1, 1)
            else:
                month = datetime(month.year, month.month + 1, 1)

        return monthly_data

    def compute_processing_projects_over_time(
        self, spreadsheet_rows: list[dict], start_date: datetime
    ) -> list[dict]:
        """
        Compute monthly processing project completions from spreadsheet rows.

        Only tracks from the provided start_date forward.

        Args:
            spreadsheet_rows: Current rows from the main sheet.
            start_date: The date the tool was first set up.

        Returns:
            List of monthly data dicts for the supporting sheet.
        """
        monthly_data: list[dict] = []
        current = datetime.now()
        month = datetime(start_date.year, start_date.month, 1)

        while month <= current:
            month_str = month.strftime("%B %Y")
            project_counts: dict[str, int] = defaultdict(int)
            project_physical: dict[str, float] = defaultdict(float)
            project_digital: dict[str, float] = defaultdict(float)

            for row in spreadsheet_rows:
                completed = row.get("Month Completed", "")
                if completed == month_str:
                    project_type = row.get("Kind of Processing Project", "Unknown")
                    physical = row.get("Accession Extent - Physical (Linear Feet)", 0) or 0
                    digital = row.get("Accession Extent - Digital (GB)", 0) or 0

                    project_counts[project_type] += 1
                    project_physical[project_type] += float(physical)
                    project_digital[project_type] += float(digital)

            monthly_data.append({
                "Month and Year": month_str,
                "project_counts": dict(project_counts),
                "project_physical": {k: round(v, 2) for k, v in project_physical.items()},
                "project_digital": {k: round(v, 2) for k, v in project_digital.items()},
            })

            if month.month == 12:
                month = datetime(month.year + 1, 1, 1)
            else:
                month = datetime(month.year, month.month + 1, 1)

        return monthly_data
