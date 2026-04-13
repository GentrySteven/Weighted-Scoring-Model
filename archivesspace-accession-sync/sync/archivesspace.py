"""
ArchivesSpace API Module

Handles all interaction with the ArchivesSpace API via ArchivesSnake,
including authentication, accession retrieval, lock_version tracking,
and linked record resolution.

ArchivesSnake is an optional dependency. If not installed, this module
provides a helpful error message directing the user to install it.
"""

import time
from typing import Any, Optional

from sync.config_manager import ConfigManager
from sync.logging_manager import LoggingManager

# Conditional import for graceful degradation
try:
    from asnake.client import ASnakeClient
    ASNAKE_AVAILABLE = True
except ImportError:
    ASNAKE_AVAILABLE = False


class ArchivesSpaceError(Exception):
    """Raised when an ArchivesSpace API operation fails."""
    pass


class ArchivesSpaceClient:
    """
    Client for interacting with the ArchivesSpace API.

    Uses ArchivesSnake for authentication and session management.
    Implements request throttling, retry logic with exponential backoff,
    and automatic re-authentication on token expiration.
    """

    def __init__(self, config: ConfigManager, logger: LoggingManager):
        """
        Initialize the ArchivesSpace client.

        Args:
            config: ConfigManager instance with connection settings.
            logger: LoggingManager instance for logging API operations.

        Raises:
            ArchivesSpaceError: If ArchivesSnake is not installed.
        """
        if not ASNAKE_AVAILABLE:
            raise ArchivesSpaceError(
                "ArchivesSnake is not installed. Install it with:\n"
                "  pip install ArchivesSnake\n"
                "Or install all ArchivesSpace dependencies with:\n"
                "  pip install archivesspace-accession-sync[aspace]"
            )

        self.config = config
        self.logger = logger
        self.base_url = config.get_base_url()
        self.repo_id = config.get_repository_id()
        self.repo_uri = config.get_repository_uri()
        self.throttle_delay = config.get("throttling", "archivesspace", default=0.5)
        self.max_retries = config.get("retry", "max_retries", default=5)

        self._client: Optional[ASnakeClient] = None
        self._reauth_attempts = 0
        self._max_reauth = 3

    def connect(self) -> bool:
        """
        Establish a connection to the ArchivesSpace API.

        Returns:
            True if connection was successful, False otherwise.
        """
        try:
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

        except ArchivesSpaceError:
            raise
        except Exception as e:
            self.logger.error(f"Failed to connect to ArchivesSpace: {e}")
            return False

    def _request(self, method: str, uri: str, **kwargs: Any) -> Any:
        """
        Make a throttled, retried request to the ArchivesSpace API.

        Implements exponential backoff on failure and automatic
        re-authentication on session expiration (HTTP 412).

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
            raise ArchivesSpaceError("Not connected. Call connect() first.")

        last_error = None

        for attempt in range(self.max_retries + 1):
            try:
                # Apply throttling between requests
                if self.throttle_delay > 0:
                    time.sleep(self.throttle_delay)

                # Apply exponential backoff on retries
                if attempt > 0:
                    backoff = 2 ** attempt
                    self.logger.technical(f"Retry {attempt}/{self.max_retries}, waiting {backoff}s")
                    time.sleep(backoff)

                response = getattr(self._client, method)(uri, **kwargs)

                # Handle authentication errors (HTTP 412)
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
                            f"Failed to re-authenticate after {self._max_reauth} attempts."
                        )

                # Handle rate limiting (HTTP 429)
                if hasattr(response, "status_code") and response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 2 ** (attempt + 1)))
                    self.logger.technical(f"Rate limited. Waiting {retry_after} seconds.")
                    time.sleep(retry_after)
                    continue

                # Handle other HTTP errors
                if hasattr(response, "status_code") and response.status_code >= 400:
                    raise ArchivesSpaceError(
                        f"API error {response.status_code}: {response.text}"
                    )

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

    # -------------------------------------------------------------------------
    # Accession retrieval
    # -------------------------------------------------------------------------

    def get_all_accession_ids(self) -> list[int]:
        """Retrieve all accession IDs in the configured repository."""
        self.logger.technical(f"Retrieving all accession IDs from {self.repo_uri}")
        uri = f"{self.repo_uri}/accessions?all_ids=true"
        ids = self._request("get", uri)
        self.logger.summary(f"Found {len(ids)} accessions in repository.")
        return ids

    def get_accessions_page(self, page: int = 1, page_size: int = 50) -> dict:
        """Retrieve a paginated list of accessions with basic fields."""
        uri = f"{self.repo_uri}/accessions?page={page}&page_size={page_size}"
        return self._request("get", uri)

    def get_accession(self, accession_id: int) -> dict:
        """Retrieve a single accession record with full detail."""
        uri = f"{self.repo_uri}/accessions/{accession_id}"
        self.logger.verbose(f"Retrieving accession {accession_id}")
        return self._request("get", uri)

    def get_sample_accession(self) -> Optional[dict]:
        """Retrieve a single accession for connection testing."""
        try:
            page = self.get_accessions_page(page=1, page_size=1)
            results = page.get("results", [])
            return results[0] if results else None
        except ArchivesSpaceError:
            return None

    def get_all_accessions(self) -> list[dict]:
        """Retrieve all accession records using pagination."""
        from sync.progress import ProgressTracker

        accessions: list[dict] = []
        page = 1

        # First request to determine total pages
        self.logger.technical("Fetching accessions page 1")
        result = self.get_accessions_page(page=1, page_size=50)
        total_pages = result.get("last_page", 1)
        accessions.extend(result.get("results", []))

        if total_pages > 1:
            tracker = ProgressTracker(
                total=total_pages, desc="Retrieving accession pages", unit="pages"
            )
            tracker.update(1)  # First page already fetched

            for page in range(2, total_pages + 1):
                self.logger.technical(f"Fetching accessions page {page}")
                result = self.get_accessions_page(page=page, page_size=50)
                accessions.extend(result.get("results", []))
                tracker.update(1)

            tracker.close()

        self.logger.summary(f"Retrieved {len(accessions)} accession records.")
        return accessions

    def get_accession_full_detail(self, accession_id: int) -> dict:
        """
        Retrieve a complete accession record with all sub-records resolved.

        This includes resolving linked agents, subjects, classifications,
        digital objects, and top containers.
        """
        accession = self.get_accession(accession_id)

        resolved_agents = self._resolve_linked_records(
            accession.get("linked_agents", []), "ref", "agent"
        )
        # Preserve role and terms metadata from the link
        for i, agent_link in enumerate(accession.get("linked_agents", [])):
            if i < len(resolved_agents):
                resolved_agents[i]["_role"] = agent_link.get("role", "")
                resolved_agents[i]["_terms"] = agent_link.get("terms", [])
                resolved_agents[i]["_relator"] = agent_link.get("relator", "")

        resolved_subjects = self._resolve_linked_records(
            accession.get("subjects", []), "ref", "subject"
        )

        resolved_classifications = self._resolve_linked_records(
            accession.get("classifications", []), "ref", "classification"
        )

        resolved_top_containers: list[dict] = []
        resolved_digital_objects: list[dict] = []
        for instance in accession.get("instances", []):
            # Top containers
            sub_container = instance.get("sub_container", {})
            tc_ref = sub_container.get("top_container", {}).get("ref", "")
            if tc_ref:
                try:
                    tc = self._request("get", tc_ref)
                    resolved_top_containers.append(tc)
                except ArchivesSpaceError:
                    self.logger.warning(f"Could not resolve top container: {tc_ref}")

            # Digital objects
            do_ref = instance.get("digital_object", {}).get("ref", "")
            if do_ref:
                try:
                    do = self._request("get", do_ref)
                    resolved_digital_objects.append(do)
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

    def _resolve_linked_records(
        self, links: list[dict], ref_key: str, record_type: str
    ) -> list[dict]:
        """Resolve a list of linked record references to full records."""
        resolved: list[dict] = []
        for link in links:
            ref = link.get(ref_key, "")
            if ref:
                try:
                    record = self._request("get", ref)
                    resolved.append(record)
                except ArchivesSpaceError:
                    self.logger.warning(f"Could not resolve {record_type}: {ref}")
        return resolved

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def extract_accession_id(self, accession: dict) -> int:
        """Extract the internal database ID from an accession's URI."""
        uri = accession.get("uri", "")
        try:
            return int(uri.split("/")[-1])
        except (ValueError, IndexError):
            return 0

    def extract_identifier(self, accession: dict) -> str:
        """Extract and concatenate the four-part accession identifier."""
        parts = [str(accession.get(f, "")) for f in ["id_0", "id_1", "id_2", "id_3"]]
        return "-".join(p for p in parts if p)

    def extract_lock_version(self, record: dict) -> int:
        """Extract the lock_version from any ArchivesSpace record."""
        return record.get("lock_version", 0)

    # -------------------------------------------------------------------------
    # Scanning and enumeration
    # -------------------------------------------------------------------------

    def get_extent_types(self) -> list[str]:
        """Retrieve all unique extent types used across accessions."""
        self.logger.technical("Scanning repository for extent types...")
        accessions = self.get_all_accessions()
        extent_types: set[str] = set()
        for acc in accessions:
            for extent in acc.get("extents", []):
                ext_type = extent.get("extent_type", "")
                if ext_type:
                    extent_types.add(ext_type)
        result = sorted(extent_types)
        self.logger.summary(f"Found {len(result)} unique extent types.")
        return result

    def get_processing_statuses(self) -> list[str]:
        """Retrieve available processing status enum values."""
        try:
            uri = "/config/enumerations/names/collection_management_processing_status"
            enum = self._request("get", uri)
            return [
                v.get("value", "")
                for v in enum.get("enumeration_values", [])
                if not v.get("suppressed")
            ]
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

        Used by the scanning framework to help users build keyword lists.
        """
        if accessions is None:
            accessions = self.get_all_accessions()

        term_counts: dict[str, int] = {}
        for acc in accessions:
            for field in fields:
                value = acc.get(field, "")
                if value and isinstance(value, str):
                    words = value.lower().split()
                    for word in words:
                        word = word.strip(".,;:!?()[]{}\"'")
                        if len(word) > 2:
                            term_counts[word] = term_counts.get(word, 0) + 1

        return dict(sorted(term_counts.items(), key=lambda x: x[1], reverse=True))

    def scan_subjects_and_agents(self) -> dict[str, list[dict]]:
        """
        Scan all accessions to find linked subjects and agents with role "Subject".

        Returns only subjects and agents actually linked to accession records.
        """
        from sync.progress import progress_bar

        self.logger.summary("Scanning accessions for linked subjects and agents...")
        accessions = self.get_all_accessions()

        # First pass: collect unique URIs from all accession links
        subject_uris: set[str] = set()
        agent_uris: set[str] = set()

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

        # Second pass: resolve each unique subject URI to its full record
        subjects: list[dict] = []
        if subject_uris:
            for uri in progress_bar(sorted(subject_uris), desc="Resolving subjects", unit="subjects"):
                try:
                    subject = self._request("get", uri)
                    subjects.append({
                        "uri": uri,
                        "title": subject.get("title", ""),
                        "lock_version": subject.get("lock_version", 0),
                    })
                except ArchivesSpaceError:
                    self.logger.warning(f"Could not resolve subject: {uri}")

        # Third pass: resolve each unique agent URI
        agents: list[dict] = []
        if agent_uris:
            for uri in progress_bar(sorted(agent_uris), desc="Resolving agents", unit="agents"):
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
