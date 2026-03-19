"""
Sync Engine

Core comparison logic for detecting new, updated, and deleted accessions.
Independent of the output format (Excel or Google Sheets). Handles:
- Lock_version comparison for sub-records
- Linked record composition tracking
- Selective content change detection
- Extent conversion and summing
- Format keyword detection
- Subject descriptor matching
- Issue field evaluation
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

try:
    from rapidfuzz import fuzz

    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager


class SyncEngine:
    """
    Core synchronization engine. Compares ArchivesSpace accession data
    against cached/spreadsheet data and determines what needs updating.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        """
        Initialize the SyncEngine.

        Args:
            config: ConfigManager instance.
            logger: LoggingManager instance.
        """
        self.config = config
        self.logger = logger
        self.fuzzy_enabled = config.get("matching", "fuzzy_enabled", default=False)
        self.fuzzy_threshold = config.get("matching", "fuzzy_threshold", default=85)
        self.extent_types = config.get("extent_types", default={})
        self.format_keywords = config.get("format_keywords", default={})
        self.removable_media_keywords = config.get("removable_media_keywords", default=[])

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
        Compare current ArchivesSpace data against cached and spreadsheet data
        to determine what has changed.

        Args:
            current_data: List of full accession detail records from ArchivesSpace.
            cached_data: Cached data from the previous successful sync.
            spreadsheet_data: Current spreadsheet row data keyed by accession ID.

        Returns:
            Dictionary with keys 'new', 'updated', 'deleted', 'unchanged',
            each containing lists of accession records or IDs with change details.
        """
        changes = {
            "new": [],
            "updated": [],
            "deleted": [],
            "unchanged": [],
        }

        current_ids = set()

        for detail in current_data:
            accession = detail.get("accession", {})
            acc_id = self._extract_id(accession)
            current_ids.add(acc_id)

            if acc_id not in cached_data:
                changes["new"].append(detail)
                self.logger.technical(f"New accession detected: {acc_id}")
            else:
                change_details = self._compare_accession(detail, cached_data.get(acc_id, {}))
                if change_details:
                    detail["_changes"] = change_details
                    changes["updated"].append(detail)
                    change_str = ", ".join(change_details)
                    self.logger.technical(
                        f"Accession {acc_id} updated: {change_str}"
                    )
                else:
                    changes["unchanged"].append(detail)

        # Detect deletions
        cached_ids = set(cached_data.keys()) if isinstance(cached_data, dict) else set()
        spreadsheet_ids = set()
        for row in spreadsheet_data:
            sid = row.get("accession_id")
            if sid:
                spreadsheet_ids.add(int(sid))

        deleted_ids = spreadsheet_ids - current_ids
        for del_id in deleted_ids:
            changes["deleted"].append(del_id)
            self.logger.technical(f"Accession {del_id} deleted or suppressed in ArchivesSpace")

        self.logger.summary(
            f"Change detection complete: {len(changes['new'])} new, "
            f"{len(changes['updated'])} updated, {len(changes['deleted'])} deleted, "
            f"{len(changes['unchanged'])} unchanged."
        )

        return changes

    def _compare_accession(self, current: dict, cached: dict) -> list[str]:
        """
        Compare a current accession record against its cached version.

        Args:
            current: Current full detail record from ArchivesSpace.
            cached: Cached data from the previous sync.

        Returns:
            List of change descriptions, empty if no changes.
        """
        changes = []
        accession = current.get("accession", {})

        # Check accession lock_version
        current_lv = accession.get("lock_version", 0)
        cached_lv = cached.get("accession_lock_version", 0)
        if current_lv != cached_lv:
            changes.append("accession record changed")

        # Check collection_management lock_version
        cm = accession.get("collection_management", {})
        if isinstance(cm, dict):
            current_cm_lv = cm.get("lock_version", 0)
            cached_cm_lv = cached.get("collection_management_lock_version", 0)
            if current_cm_lv != cached_cm_lv:
                changes.append("collection management changed")

        # Check extents lock_versions
        current_ext_lvs = self._get_sorted_lock_versions(accession.get("extents", []))
        cached_ext_lvs = cached.get("extents_lock_versions", "")
        if current_ext_lvs != cached_ext_lvs:
            changes.append("extents changed")

        # Check linked record compositions
        for record_type, key in [
            ("resolved_agents", "linked_agents"),
            ("resolved_subjects", "subjects"),
            ("resolved_classifications", "classifications"),
            ("resolved_digital_objects", "digital_objects"),
            ("resolved_top_containers", "top_containers"),
        ]:
            current_comp = self._get_composition_ids(current.get(record_type, []))
            cached_comp = cached.get(f"{key}_ids", "")
            if current_comp != cached_comp:
                changes.append(f"{key.replace('_', ' ')} changed")
            else:
                # Check display values even if composition hasn't changed
                current_values = self._get_display_values(current.get(record_type, []), key)
                cached_values = cached.get(f"{key}_values", "")
                if current_values != cached_values:
                    changes.append(f"{key.replace('_', ' ')} content changed")

        return changes

    # -------------------------------------------------------------------------
    # Lock Version and Composition Tracking
    # -------------------------------------------------------------------------

    def _get_sorted_lock_versions(self, records: list[dict]) -> str:
        """Get sorted, semicolon-concatenated lock_versions from a list of sub-records."""
        versions = sorted([str(r.get("lock_version", 0)) for r in records])
        return ";".join(versions)

    def _get_composition_ids(self, records: list[dict]) -> str:
        """Get sorted, semicolon-concatenated IDs from a list of resolved records."""
        ids = []
        for record in records:
            uri = record.get("uri", "")
            if uri:
                record_id = uri.split("/")[-1]
                ids.append(record_id)
        return ";".join(sorted(ids))

    def _get_display_values(self, records: list[dict], record_type: str) -> str:
        """Get sorted, semicolon-concatenated display values from resolved records."""
        values = []
        for record in records:
            if record_type == "linked_agents":
                values.append(self._format_agent_display(record))
            elif record_type == "subjects":
                values.append(record.get("title", ""))
            elif record_type == "classifications":
                values.append(record.get("title", ""))
            elif record_type == "digital_objects":
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
    # Data Extraction for Spreadsheet Columns
    # -------------------------------------------------------------------------

    def build_row_data(self, detail: dict, base_url: str) -> dict:
        """
        Build a complete row of spreadsheet data from a full accession detail record.

        Args:
            detail: Full detail record from ArchivesSpace (accession + resolved records).
            base_url: The ArchivesSpace base URL from config.

        Returns:
            Dictionary mapping column names to values.
        """
        accession = detail.get("accession", {})
        acc_id = self._extract_id(accession)
        identifier = self._build_identifier(accession)

        # Collection management fields
        cm = accession.get("collection_management", {})
        if not isinstance(cm, dict):
            cm = {}

        row = {
            "Accession Status": cm.get("processing_status", ""),
            "Base URL (Use for Hyperlink Only)": base_url,
            "Accession ID": acc_id,
            # Col D and F are formulas, set by the spreadsheet module
            "Identifier (Use for Hyperlink Only)": identifier,
            "Donor Name": self._extract_donor_name(detail),
            "Accession Date": accession.get("accession_date", ""),
            "Priority": cm.get("processing_priority", ""),
            "Classification": self._extract_classifications(detail),
            "Accession Extent - Physical (Linear Feet)": self._calculate_physical_extent(accession),
            "Accession Extent - Digital (GB)": self._calculate_digital_extent(accession),
        }

        # Format detection columns
        format_results = self._detect_formats(accession, detail)
        row.update(format_results)

        # Total Number of Formats (calculated by formula in spreadsheet)

        # Subject descriptors
        subject_descriptors = self._extract_subject_descriptors(detail)
        row["_subject_descriptors"] = subject_descriptors

        # Issue columns
        row["Access Issues"] = self._evaluate_access_issues(accession)
        row["Conservation Issues"] = self._evaluate_conservation_issues(accession)
        row["Digital Issues"] = self._evaluate_digital_issues(accession, detail)
        row["Other Processing Information"] = self._evaluate_other_processing(accession)

        # Sync tracking data
        row["_sync_data"] = self._build_sync_tracking(detail)

        return row

    def _build_identifier(self, accession: dict) -> str:
        """Build the four-part accession identifier string."""
        parts = []
        for field in ["id_0", "id_1", "id_2", "id_3"]:
            val = accession.get(field, "")
            if val:
                parts.append(str(val))
        return "-".join(parts)

    def _extract_donor_name(self, detail: dict) -> str:
        """Extract donor name(s) from linked agents with configured role."""
        donor_role = self.config.get("agents", "donor_role", default="source")
        agents = detail.get("resolved_agents", [])
        names = []

        for agent in agents:
            if agent.get("_role", "").lower() == donor_role.lower():
                display_name = agent.get("display_name", {})
                name = display_name.get("sort_name", "") or agent.get("title", "")
                if name:
                    names.append(name)

        return "; ".join(names)

    def _extract_classifications(self, detail: dict) -> str:
        """Extract classification titles, semicolon-separated."""
        classifications = detail.get("resolved_classifications", [])
        titles = [c.get("title", "") for c in classifications if c.get("title")]
        return "; ".join(titles)

    def _calculate_physical_extent(self, accession: dict) -> float:
        """
        Calculate total physical extent in linear feet by converting
        and summing all physical extent sub-records.
        """
        total = 0.0
        for extent in accession.get("extents", []):
            ext_type = extent.get("extent_type", "")
            type_config = self.extent_types.get(ext_type, {})

            if type_config.get("category") == "physical":
                try:
                    number = float(extent.get("number", 0))
                    factor = float(type_config.get("conversion_factor", 1.0))
                    total += number * factor
                except (ValueError, TypeError):
                    continue

        return round(total, 4)

    def _calculate_digital_extent(self, accession: dict) -> float:
        """
        Calculate total digital extent in gigabytes by converting
        and summing all digital extent sub-records.
        """
        total = 0.0
        for extent in accession.get("extents", []):
            ext_type = extent.get("extent_type", "")
            type_config = self.extent_types.get(ext_type, {})

            if type_config.get("category") == "digital":
                try:
                    number = float(extent.get("number", 0))
                    factor = float(type_config.get("conversion_factor", 1.0))
                    total += number * factor
                except (ValueError, TypeError):
                    continue

        return round(total, 4)

    # -------------------------------------------------------------------------
    # Format Detection
    # -------------------------------------------------------------------------

    def _detect_formats(self, accession: dict, detail: dict) -> dict:
        """
        Detect material formats by scanning configured fields for keywords.

        Returns a dictionary mapping format column names to boolean values.
        """
        # Build the text to scan
        scan_text = " ".join(
            filter(
                None,
                [
                    accession.get("content_description", ""),
                    accession.get("condition_description", ""),
                    accession.get("inventory", ""),
                ],
            )
        ).lower()

        # Add extent types to scan text
        for extent in accession.get("extents", []):
            ext_type = extent.get("extent_type", "")
            if ext_type:
                scan_text += f" {ext_type.lower()}"

        # Add top container types
        for tc in detail.get("resolved_top_containers", []):
            tc_type = tc.get("type", "")
            container_type = tc.get("container_type", "")
            if tc_type:
                scan_text += f" {tc_type.lower()}"
            if container_type:
                scan_text += f" {container_type.lower()}"

        results = {}
        for format_name, keywords in self.format_keywords.items():
            detected = self._match_keywords(scan_text, keywords)
            results[format_name] = detected

        return results

    def _match_keywords(self, text: str, keywords: list[str]) -> bool:
        """
        Check if any keyword matches in the given text.

        Uses case-insensitive partial matching, with optional fuzzy matching.

        Args:
            text: The text to search in.
            keywords: List of keywords to search for.

        Returns:
            True if any keyword matches.
        """
        text_lower = text.lower()
        for keyword in keywords:
            keyword_lower = keyword.lower()

            # Standard case-insensitive partial match
            if keyword_lower in text_lower:
                return True

            # Optional fuzzy matching
            if self.fuzzy_enabled and RAPIDFUZZ_AVAILABLE:
                # Check each word in the text against the keyword
                words = text_lower.split()
                for word in words:
                    score = fuzz.partial_ratio(keyword_lower, word)
                    if score >= self.fuzzy_threshold:
                        self.logger.verbose(
                            f"Fuzzy match: '{keyword}' ~ '{word}' (score: {score})"
                        )
                        return True

        return False

    # -------------------------------------------------------------------------
    # Subject Descriptor Extraction
    # -------------------------------------------------------------------------

    def _extract_subject_descriptors(self, detail: dict) -> list[str]:
        """
        Extract subject descriptors by matching linked subjects and agents
        (with role "Subject") against the approved list.

        Returns a list of display values for matched subjects/agents.
        """
        # TODO: Load approved list from spreadsheet's hidden sheet
        # For now, return all subjects and agents with role "Subject"
        descriptors = []

        # Subjects
        for subject in detail.get("resolved_subjects", []):
            title = subject.get("title", "")
            if title:
                descriptors.append(title)

        # Agents with role "Subject"
        for agent in detail.get("resolved_agents", []):
            if agent.get("_role", "").lower() == "subject":
                display = self._format_agent_display(agent)
                if display:
                    descriptors.append(display)

        num_columns = self.config.get("subject_descriptors", "num_columns", default=9)
        if len(descriptors) > num_columns:
            self.logger.warning(
                f"Accession has {len(descriptors)} subject descriptors but only "
                f"{num_columns} columns. {len(descriptors) - num_columns} values will not "
                f"be captured."
            )

        return descriptors[:num_columns]

    def _format_agent_display(self, agent: dict) -> str:
        """
        Format an agent for display in the spreadsheet.

        Format: "Sort Name — Term1 — Term2"
        """
        display_name = agent.get("display_name", {})
        sort_name = display_name.get("sort_name", "") or agent.get("title", "")

        if not sort_name:
            return ""

        terms = agent.get("_terms", [])
        if not terms:
            # Check the agent's own terms and subdivisions
            names = agent.get("names", [])
            if names:
                for name in names:
                    for qualifier in name.get("qualifier", "").split(","):
                        qualifier = qualifier.strip()
                        if qualifier:
                            terms.append({"term": qualifier})

        if terms:
            term_parts = [t.get("term", "") for t in terms if t.get("term")]
            if term_parts:
                return f"{sort_name} — " + " — ".join(term_parts)

        return sort_name

    # -------------------------------------------------------------------------
    # Issue Evaluation
    # -------------------------------------------------------------------------

    def _evaluate_access_issues(self, accession: dict) -> str:
        """
        Evaluate access issues from the accession record.

        Checks access_restrictions boolean and processes access_restrictions_note.
        """
        if not accession.get("access_restrictions", False):
            return ""

        note = accession.get("access_restrictions_note", "")
        if not note:
            return ""

        return self._summarize_text(note)

    def _evaluate_conservation_issues(self, accession: dict) -> str:
        """
        Evaluate conservation issues from the condition_description field.
        """
        description = accession.get("condition_description", "")
        if not description:
            return ""

        return self._summarize_text(description)

    def _evaluate_digital_issues(self, accession: dict, detail: dict) -> list[str]:
        """
        Evaluate digital issues using rule-based checks.

        Default rules:
        1. "Digital object potentially or actually absent" - digital extent
           exists but no digital object (or no file_versions)
        2. "Removable media may not have, or has not been, transferred" -
           removable media keywords found but no digital extent or digital object
        """
        issues = []
        has_digital_extent = False
        has_digital_object = False
        has_file_versions = False

        # Check for digital extents
        for extent in accession.get("extents", []):
            ext_type = extent.get("extent_type", "")
            type_config = self.extent_types.get(ext_type, {})
            if type_config.get("category") == "digital":
                has_digital_extent = True
                break

        # Check for digital objects with file versions
        digital_objects = detail.get("resolved_digital_objects", [])
        if digital_objects:
            has_digital_object = True
            for do in digital_objects:
                if do.get("file_versions", []):
                    has_file_versions = True
                    break

        # Rule 1: Digital extent but no digital object (or no file versions)
        if has_digital_extent and (not has_digital_object or not has_file_versions):
            issues.append("Digital object potentially or actually absent.")

        # Rule 2: Removable media keywords but no digital extent/object
        scan_text = " ".join(
            filter(
                None,
                [
                    accession.get("content_description", ""),
                    accession.get("inventory", ""),
                ],
            )
        ).lower()

        has_removable_media = self._match_keywords(scan_text, self.removable_media_keywords)
        if has_removable_media and not has_digital_extent and (
            not has_digital_object or not has_file_versions
        ):
            issues.append("Removable media may not have, or has not been, transferred.")

        return "; ".join(issues) if issues else ""

    def _evaluate_other_processing(self, accession: dict) -> str:
        """
        Evaluate other processing information via user-configured keyword matching.
        """
        # This will use the configurable scanning framework
        # For now, return empty - populated by user scan configurations
        scan_configs = self.config.get("issue_scan_configs", default={})
        if not scan_configs:
            return ""

        # TODO: Implement configurable scanning framework evaluation
        return ""

    def _summarize_text(self, text: str, max_chars: int = 200) -> str:
        """
        Summarize text using rule-based extraction with sentence fallback.

        Rule-based approach extracts key elements (restriction type, timeframe).
        Falls back to first-sentence extraction if no rules match.

        Args:
            text: The text to summarize.
            max_chars: Maximum character length for sentence fallback.

        Returns:
            Summarized text string.
        """
        if not text:
            return ""

        # If text is already short, use as-is
        if len(text) <= max_chars:
            return text.strip()

        # Rule-based: try to extract key phrases
        summary_parts = []

        # Look for restriction patterns
        restriction_patterns = [
            r"(?:closed|restricted|confidential|sealed)",
            r"(?:\d+[- ]year\s+restriction)",
            r"(?:executive|personnel|student|patient|client)\s+records?",
            r"(?:personally identifiable information|PII)",
            r"(?:reading room only)",
        ]

        for pattern in restriction_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            summary_parts.extend(matches)

        if summary_parts:
            return "; ".join(set(summary_parts))

        # Sentence extraction fallback: take the first sentence
        sentences = re.split(r"[.!?]+", text)
        if sentences and sentences[0].strip():
            first_sentence = sentences[0].strip()
            if len(first_sentence) <= max_chars:
                return first_sentence
            return first_sentence[:max_chars].rsplit(" ", 1)[0] + "..."

        return text[:max_chars].rsplit(" ", 1)[0] + "..."

    # -------------------------------------------------------------------------
    # Sync Tracking Data
    # -------------------------------------------------------------------------

    def _build_sync_tracking(self, detail: dict) -> dict:
        """
        Build the sync tracking data for a row (lock_versions, compositions, values).

        Returns a dictionary of sync column values.
        """
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
        """
        Build a cache entry for a single accession.

        This stores the data needed for change detection on the next run.
        """
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
    # Completion Detection
    # -------------------------------------------------------------------------

    def check_completion(
        self, accession: dict, cached_status: str, completion_triggers: list[str]
    ) -> Optional[str]:
        """
        Check if an accession's processing status has changed to a completion value.

        Args:
            accession: Current accession record.
            cached_status: The processing status from the cache/spreadsheet.
            completion_triggers: List of status values indicating completion.

        Returns:
            Formatted "Month Year" string if newly completed, None otherwise.
        """
        cm = accession.get("collection_management", {})
        if not isinstance(cm, dict):
            return None

        current_status = cm.get("processing_status", "")

        if (
            current_status in completion_triggers
            and cached_status not in completion_triggers
        ):
            now = datetime.now()
            return now.strftime("%B %Y")

        return None
