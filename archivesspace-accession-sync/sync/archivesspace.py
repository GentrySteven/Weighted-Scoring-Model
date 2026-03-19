"""
ArchivesSpace API Module

Handles all interaction with the ArchivesSpace API via ArchivesSnake,
including authentication, accession retrieval, lock_version tracking,
and linked record resolution.
"""

import time
from typing import Any, Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager


class ArchivesSpaceError(Exception):
    """Raised when an ArchivesSpace API operation fails."""

    pass


class ArchivesSpaceClient:
    """
    Client for interacting with the ArchivesSpace API.

    Uses ArchivesSnake for authentication and session management.
    Implements request throttling, retry logic, and automatic
    re-authentication on token expiration.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        """
        Initialize the ArchivesSpace client.

        Args:
            config: ConfigManager instance with connection settings.
            logger: LoggingManager instance for logging API operations.
        """
        self.config = config
        self.logger = logger
        self.base_url = config.get_base_url()
        self.repo_id = config.get_repository_id()
        self.repo_uri = config.get_repository_uri()
        self.throttle_delay = config.get("throttling", "archivesspace", default=0.5)
        self.max_retries = config.get("retry", "max_retries", default=5)

        self._client = None
        self._reauth_attempts = 0
        self._max_reauth = 3

    def connect(self) -> bool:
        """
        Establish a connection to the ArchivesSpace API.

        Returns:
            True if connection was successful, False otherwise.
        """
        try:
            from asnake.client import ASnakeClient

            username = self.config.get_credential("archivesspace", "username")
            password = self.config.get_credential("archivesspace", "password")

            if not username or not password:
                raise ArchivesSpaceError(
                    "ArchivesSpace credentials not found in credentials.yml."
                )

            self._client = ASnakeClient(
                baseurl=self.base_url,
                username=username,
                password=password,
            )
            self._client.authorize()
            self.logger.summary("Connected to ArchivesSpace successfully.")
            self.logger.technical(f"Connected to {self.base_url}")
            return True

        except ImportError:
            raise ArchivesSpaceError(
                "ArchivesSnake is not installed. Run: pip install archivessnake"
            )
        except Exception as e:
            self.logger.error(f"Failed to connect to ArchivesSpace: {e}")
            return False

    def _request(self, method: str, uri: str, **kwargs) -> Any:
        """
        Make a throttled, retried request to the ArchivesSpace API.

        Args:
            method: HTTP method ('get', 'post', etc.)
            uri: The API endpoint URI.
            **kwargs: Additional arguments passed to the request.

        Returns:
            The JSON response from the API.

        Raises:
            ArchivesSpaceError: If the request fails after all retries.
        """
        if not self._client:
            raise ArchivesSpaceError("Not connected to ArchivesSpace. Call connect() first.")

        last_error = None

        for attempt in range(self.max_retries + 1):
            try:
                # Apply throttling
                if attempt > 0 or self.throttle_delay > 0:
                    delay = self.throttle_delay if attempt == 0 else (2**attempt)
                    time.sleep(delay)

                response = getattr(self._client, method)(uri, **kwargs)

                # Check for authentication errors (HTTP 412 in ArchivesSpace)
                if hasattr(response, "status_code") and response.status_code == 412:
                    if self._reauth_attempts < self._max_reauth:
                        self._reauth_attempts += 1
                        self.logger.technical(
                            f"Session expired, re-authenticating "
                            f"(attempt {self._reauth_attempts}/{self._max_reauth})"
                        )
                        self._client.authorize()
                        continue
                    else:
                        raise ArchivesSpaceError(
                            "Failed to re-authenticate after "
                            f"{self._max_reauth} attempts."
                        )

                # Check for rate limiting (HTTP 429)
                if hasattr(response, "status_code") and response.status_code == 429:
                    retry_after = int(
                        response.headers.get("Retry-After", 2 ** (attempt + 1))
                    )
                    self.logger.technical(
                        f"Rate limited. Waiting {retry_after} seconds."
                    )
                    time.sleep(retry_after)
                    continue

                # Check for other HTTP errors
                if hasattr(response, "status_code") and response.status_code >= 400:
                    raise ArchivesSpaceError(
                        f"API error {response.status_code}: {response.text}"
                    )

                # Reset re-auth counter on success
                self._reauth_attempts = 0

                if hasattr(response, "json"):
                    return response.json()
                return response

            except ArchivesSpaceError:
                raise
            except Exception as e:
                last_error = e
                if attempt < self.max_retries:
                    self.logger.technical(
                        f"Request failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}"
                    )
                continue

        raise ArchivesSpaceError(
            f"Request to {uri} failed after {self.max_retries + 1} attempts: {last_error}"
        )

    def get_all_accession_ids(self) -> list[int]:
        """
        Retrieve all accession IDs in the configured repository.

        Returns:
            List of accession IDs (integers).
        """
        self.logger.technical(f"Retrieving all accession IDs from {self.repo_uri}")
        uri = f"{self.repo_uri}/accessions?all_ids=true"
        ids = self._request("get", uri)
        self.logger.summary(f"Found {len(ids)} accessions in repository.")
        return ids

    def get_accessions_page(self, page: int = 1, page_size: int = 50) -> dict:
        """
        Retrieve a paginated list of accessions with basic fields.

        Args:
            page: Page number (1-indexed).
            page_size: Number of records per page.

        Returns:
            Dictionary containing 'results' and pagination metadata.
        """
        uri = f"{self.repo_uri}/accessions?page={page}&page_size={page_size}"
        return self._request("get", uri)

    def get_accession(self, accession_id: int) -> dict:
        """
        Retrieve a single accession record with full detail.

        Args:
            accession_id: The internal ArchivesSpace accession ID.

        Returns:
            Full accession JSON record.
        """
        uri = f"{self.repo_uri}/accessions/{accession_id}"
        self.logger.verbose(f"Retrieving accession {accession_id}")
        return self._request("get", uri)

    def get_sample_accession(self) -> Optional[dict]:
        """
        Retrieve a single accession for connection testing.

        Returns:
            An accession record, or None if no accessions exist.
        """
        try:
            page = self.get_accessions_page(page=1, page_size=1)
            results = page.get("results", [])
            if results:
                return results[0]
            return None
        except ArchivesSpaceError:
            return None

    def get_all_accessions(self) -> list[dict]:
        """
        Retrieve all accession records with full detail using pagination.

        Uses a two-step approach:
        1. Get paginated listing with basic data
        2. Retrieve full detail for each accession

        Returns:
            List of full accession records.
        """
        accessions = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            self.logger.technical(f"Fetching accessions page {page}")
            result = self.get_accessions_page(page=page, page_size=50)

            total_pages = result.get("last_page", 1)
            page_results = result.get("results", [])
            accessions.extend(page_results)
            page += 1

        self.logger.summary(f"Retrieved {len(accessions)} accession records.")
        return accessions

    def get_accession_full_detail(self, accession_id: int) -> dict:
        """
        Retrieve a complete accession record with all sub-records resolved.

        This includes resolving linked agents, subjects, classifications,
        digital objects, and top containers.

        Args:
            accession_id: The internal ArchivesSpace accession ID.

        Returns:
            Dictionary with the full accession data and resolved linked records.
        """
        accession = self.get_accession(accession_id)

        # Resolve linked agents
        resolved_agents = []
        for agent_link in accession.get("linked_agents", []):
            agent_uri = agent_link.get("ref", "")
            if agent_uri:
                try:
                    agent = self._request("get", agent_uri)
                    agent["_role"] = agent_link.get("role", "")
                    agent["_terms"] = agent_link.get("terms", [])
                    agent["_relator"] = agent_link.get("relator", "")
                    resolved_agents.append(agent)
                except ArchivesSpaceError:
                    self.logger.warning(f"Could not resolve agent: {agent_uri}")

        # Resolve subjects
        resolved_subjects = []
        for subject_link in accession.get("subjects", []):
            subject_uri = subject_link.get("ref", "")
            if subject_uri:
                try:
                    subject = self._request("get", subject_uri)
                    resolved_subjects.append(subject)
                except ArchivesSpaceError:
                    self.logger.warning(f"Could not resolve subject: {subject_uri}")

        # Resolve classifications
        resolved_classifications = []
        for class_link in accession.get("classifications", []):
            class_uri = class_link.get("ref", "")
            if class_uri:
                try:
                    classification = self._request("get", class_uri)
                    resolved_classifications.append(classification)
                except ArchivesSpaceError:
                    self.logger.warning(f"Could not resolve classification: {class_uri}")

        # Resolve instances and top containers
        resolved_top_containers = []
        for instance in accession.get("instances", []):
            sub_container = instance.get("sub_container", {})
            tc_ref = sub_container.get("top_container", {}).get("ref", "")
            if tc_ref:
                try:
                    top_container = self._request("get", tc_ref)
                    resolved_top_containers.append(top_container)
                except ArchivesSpaceError:
                    self.logger.warning(f"Could not resolve top container: {tc_ref}")

        # Resolve digital objects
        resolved_digital_objects = []
        for instance in accession.get("instances", []):
            do_ref = instance.get("digital_object", {}).get("ref", "")
            if do_ref:
                try:
                    digital_object = self._request("get", do_ref)
                    resolved_digital_objects.append(digital_object)
                except ArchivesSpaceError:
                    self.logger.warning(f"Could not resolve digital object: {do_ref}")

        return {
            "accession": accession,
            "resolved_agents": resolved_agents,
            "resolved_subjects": resolved_subjects,
            "resolved_classifications": resolved_classifications,
            "resolved_top_containers": resolved_top_containers,
            "resolved_digital_objects": resolved_digital_objects,
        }

    def extract_accession_id(self, accession: dict) -> int:
        """
        Extract the internal database ID from an accession's URI.

        Args:
            accession: An accession record dictionary.

        Returns:
            The integer database ID.
        """
        uri = accession.get("uri", "")
        return int(uri.split("/")[-1]) if uri else 0

    def extract_identifier(self, accession: dict) -> str:
        """
        Extract the four-part accession identifier and concatenate it.

        Args:
            accession: An accession record dictionary.

        Returns:
            The concatenated identifier string (e.g., "2023-001").
        """
        parts = []
        for field in ["id_0", "id_1", "id_2", "id_3"]:
            val = accession.get(field, "")
            if val:
                parts.append(str(val))
        return "-".join(parts) if parts else ""

    def extract_lock_version(self, record: dict) -> int:
        """
        Extract the lock_version from any ArchivesSpace record.

        Args:
            record: An ArchivesSpace record dictionary.

        Returns:
            The lock_version integer.
        """
        return record.get("lock_version", 0)

    def get_extent_types(self) -> list[str]:
        """
        Retrieve all unique extent types used across accessions in the repository.

        Returns:
            Sorted list of unique extent type strings.
        """
        self.logger.technical("Scanning repository for extent types...")
        extent_types = set()
        accessions = self.get_all_accessions()

        for acc in accessions:
            for extent in acc.get("extents", []):
                ext_type = extent.get("extent_type", "")
                if ext_type:
                    extent_types.add(ext_type)

        result = sorted(extent_types)
        self.logger.summary(f"Found {len(result)} unique extent types.")
        return result

    def get_processing_statuses(self) -> list[str]:
        """
        Retrieve available processing status values from ArchivesSpace.

        Returns:
            List of processing status enum values.
        """
        try:
            uri = "/config/enumerations"
            enums = self._request("get", uri)

            for enum in enums:
                if isinstance(enum, dict) and enum.get("name") == "collection_management_processing_status":
                    return [v.get("value", "") for v in enum.get("enumeration_values", []) if not v.get("suppressed")]

            # Try alternative approach via specific enumeration
            uri = "/config/enumerations/names/collection_management_processing_status"
            try:
                enum = self._request("get", uri)
                return [v.get("value", "") for v in enum.get("enumeration_values", []) if not v.get("suppressed")]
            except ArchivesSpaceError:
                pass

        except ArchivesSpaceError as e:
            self.logger.warning(f"Could not retrieve processing statuses: {e}")

        return []

    def scan_fields_for_terms(
        self,
        fields: list[str],
        accessions: Optional[list[dict]] = None,
    ) -> dict[str, int]:
        """
        Scan specified fields across all accessions and return term frequencies.

        Used by the scanning framework to help users build keyword lists
        and structured vocabularies.

        Args:
            fields: List of field names to scan (e.g., ['content_description',
                     'condition_description', 'inventory']).
            accessions: Pre-fetched accession list, or None to fetch fresh.

        Returns:
            Dictionary mapping terms to their frequency count.
        """
        if accessions is None:
            accessions = self.get_all_accessions()

        term_counts: dict[str, int] = {}

        for acc in accessions:
            for field in fields:
                value = acc.get(field, "")
                if value and isinstance(value, str):
                    # Split into words and count
                    words = value.lower().split()
                    for word in words:
                        word = word.strip(".,;:!?()[]{}\"'")
                        if len(word) > 2:
                            term_counts[word] = term_counts.get(word, 0) + 1

        return dict(sorted(term_counts.items(), key=lambda x: x[1], reverse=True))

    def scan_subjects_and_agents(self) -> dict[str, list[dict]]:
        """
        Scan all accessions to find linked subjects and agents with role "Subject".

        Returns only subjects and agents that are actually linked to accession
        records, not the entire repository's subject/agent databases.

        Returns:
            Dictionary with 'subjects' and 'agents' lists, each containing
            records with their titles and URIs.
        """
        self.logger.summary("Scanning accessions for linked subjects and agents...")
        accessions = self.get_all_accessions()

        subject_uris = set()
        agent_uris = set()

        for acc in accessions:
            for subj in acc.get("subjects", []):
                ref = subj.get("ref", "")
                if ref:
                    subject_uris.add(ref)

            for agent in acc.get("linked_agents", []):
                if agent.get("role", "").lower() == "subject":
                    ref = agent.get("ref", "")
                    if ref:
                        agent_uris.add(ref)

        # Resolve subjects
        subjects = []
        for uri in subject_uris:
            try:
                subject = self._request("get", uri)
                subjects.append({
                    "uri": uri,
                    "title": subject.get("title", ""),
                    "lock_version": subject.get("lock_version", 0),
                })
            except ArchivesSpaceError:
                self.logger.warning(f"Could not resolve subject: {uri}")

        # Resolve agents
        agents = []
        for uri in agent_uris:
            try:
                agent = self._request("get", uri)
                sort_name = agent.get("display_name", {}).get("sort_name", "")
                if not sort_name:
                    sort_name = agent.get("title", "")
                agents.append({
                    "uri": uri,
                    "sort_name": sort_name,
                    "lock_version": agent.get("lock_version", 0),
                })
            except ArchivesSpaceError:
                self.logger.warning(f"Could not resolve agent: {uri}")

        self.logger.summary(
            f"Found {len(subjects)} unique subjects and "
            f"{len(agents)} unique agents with role 'Subject'."
        )

        return {"subjects": subjects, "agents": agents}
