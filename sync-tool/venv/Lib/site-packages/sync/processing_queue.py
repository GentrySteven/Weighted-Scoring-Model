"""
Processing Queue Module

Computes prioritized processing queues from accession data. This is the
program's replacement for the original "Processing Scoring Model" Google
Sheet, which used pivot tables and IMPORTRANGE formulas to filter and
group accessions.

Instead of relying on spreadsheet formulas, this module computes the
queues in Python during sync, then writes the results as static rows
to one or more "Processing Queue" sheets in the workbook.

A processing queue answers the question: "Which projects should be
processed next?" Accessions are filtered by status, grouped into
"projects" by a configurable field (default: Donor Name), aggregated
to compute project-level metrics, and sorted by average final score
(highest first).

Two view modes are supported:
- "indented": One row per project with sub-rows for each accession
- "flat": One row per accession with the project name in a column
"""

from collections import defaultdict
from typing import Any, Optional


class ProcessingQueueBuilder:
    """
    Computes processing queues from spreadsheet rows.

    Each queue is configured with a name, status filter, grouping field,
    and view mode. The builder produces a list of (row, col, value)
    tuples ready to be written to a worksheet.
    """

    def __init__(self, spreadsheet_rows: list[dict], queue_config: dict):
        """
        Args:
            spreadsheet_rows: List of row dicts from the main accession sheet.
            queue_config: A single queue config dict from data.yml with keys
                'name', 'status_values', 'grouping_field', 'view_mode'.
        """
        self.rows = spreadsheet_rows
        self.config = queue_config
        self.name = queue_config.get("name", "Processing Queue")
        self.status_values = queue_config.get("status_values", [])
        self.grouping_field = queue_config.get("grouping_field", "Donor Name")
        self.view_mode = queue_config.get("view_mode", "indented")

    def filter_rows(self) -> list[dict]:
        """
        Filter rows to those matching the queue's status values.

        Rows with empty/missing status are excluded. Status comparison
        is case-insensitive to handle inconsistencies in user data.
        """
        if not self.status_values:
            return list(self.rows)

        normalized = {s.lower().strip() for s in self.status_values}
        result = []
        for row in self.rows:
            status = row.get("Accession Status", "")
            if status and str(status).lower().strip() in normalized:
                result.append(row)
        return result

    def group_by_field(self, rows: list[dict]) -> dict[str, list[dict]]:
        """
        Group filtered rows by the configured grouping field.

        Rows with empty/missing values for the grouping field are placed
        in an "(Unassigned)" group rather than discarded, so the user can
        see them and assign them appropriately.
        """
        groups: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            value = row.get(self.grouping_field, "")
            if not value:
                value = "(Unassigned)"
            groups[str(value)].append(row)
        return dict(groups)

    def compute_project_metrics(self, project_rows: list[dict]) -> dict:
        """
        Compute aggregate metrics for a single project (group of accessions).

        Returns a dict with totals for physical extent, digital extent,
        format count, subject descriptor count, issue count, and the
        average final accession score.
        """
        physical = 0.0
        digital = 0.0
        formats = 0
        subjects = 0
        issues = 0
        scores: list[float] = []

        for row in project_rows:
            try:
                physical += float(row.get("Accession Extent - Physical (Linear Feet)", 0) or 0)
            except (ValueError, TypeError):
                pass
            try:
                digital += float(row.get("Accession Extent - Digital (GB)", 0) or 0)
            except (ValueError, TypeError):
                pass
            try:
                formats += int(row.get("Total Number of Formats", 0) or 0)
            except (ValueError, TypeError):
                pass
            try:
                subjects += int(row.get("Total Number of Subject Descriptors", 0) or 0)
            except (ValueError, TypeError):
                pass
            try:
                issues += int(row.get("Total Number of Issues", 0) or 0)
            except (ValueError, TypeError):
                pass
            try:
                score = float(row.get("Final Accession Score", 0) or 0)
                if score > 0:
                    scores.append(score)
            except (ValueError, TypeError):
                pass

        avg_score = sum(scores) / len(scores) if scores else 0.0

        return {
            "accession_count": len(project_rows),
            "physical_extent": round(physical, 4),
            "digital_extent": round(digital, 4),
            "total_formats": formats,
            "total_subjects": subjects,
            "total_issues": issues,
            "average_score": round(avg_score, 4),
        }

    def build_sorted_projects(self) -> list[dict]:
        """
        Build the complete list of projects, sorted by average score descending.

        Returns:
            List of project dicts, each containing:
              - name: The grouping value (e.g., donor name)
              - rows: The accession rows in this project
              - metrics: Computed aggregate metrics
        """
        filtered = self.filter_rows()
        groups = self.group_by_field(filtered)

        projects = []
        for name, rows in groups.items():
            metrics = self.compute_project_metrics(rows)
            projects.append({
                "name": name,
                "rows": rows,
                "metrics": metrics,
            })

        # Sort by average score descending (highest priority first).
        # Projects with the same average score are sub-sorted alphabetically
        # by name for deterministic output.
        projects.sort(
            key=lambda p: (-p["metrics"]["average_score"], p["name"])
        )
        return projects

    def build_cells(self) -> list[tuple[int, int, Any]]:
        """
        Build cell content for the processing queue sheet.

        Layout depends on view_mode:
        - "indented": Project header rows followed by indented accession rows
        - "flat": One row per accession with project name in the first column

        Returns:
            List of (row, col, value) tuples (1-indexed).
        """
        projects = self.build_sorted_projects()

        if self.view_mode == "flat":
            return self._build_flat_cells(projects)
        return self._build_indented_cells(projects)

    def _build_indented_cells(self, projects: list[dict]) -> list[tuple[int, int, Any]]:
        """
        Build cells for the indented view (project rows with sub-rows).

        Layout:
            Row 1:  Headers
            Row 2:  Project A header (totals, average score)
            Row 3:    └ Accession 1
            Row 4:    └ Accession 2
            Row 5:  Project B header
            Row 6:    └ Accession 1
            ...
        """
        cells: list[tuple[int, int, Any]] = []

        # Header row
        headers = [
            self.grouping_field,
            "Accession Number",
            "Donor Number",
            "Physical Extent (Linear Feet)",
            "Digital Extent (GB)",
            "Special Formats (Total)",
            "Subject Descriptors (Total)",
            "Issues (Total)",
            "Final Accession Score",
        ]
        for col_idx, header in enumerate(headers, 1):
            cells.append((1, col_idx, header))

        current_row = 2
        for project in projects:
            metrics = project["metrics"]
            # Project header row: name in col 1, totals in cols 4-9
            cells.append((current_row, 1, f"{project['name']} (Total)"))
            cells.append((current_row, 4, metrics["physical_extent"]))
            cells.append((current_row, 5, metrics["digital_extent"]))
            cells.append((current_row, 6, metrics["total_formats"]))
            cells.append((current_row, 7, metrics["total_subjects"]))
            cells.append((current_row, 8, metrics["total_issues"]))
            cells.append((current_row, 9, metrics["average_score"]))
            current_row += 1

            # Indented accession sub-rows
            for row in project["rows"]:
                # Indent visually with leading spaces in col 2
                accession_num = row.get("Accession Number") or row.get(
                    "Identifier (Use for Hyperlink Only)", ""
                )
                cells.append((current_row, 2, f"  {accession_num}"))
                cells.append((current_row, 3, row.get("Donor Number", "")))
                cells.append((current_row, 4, self._safe_float(
                    row.get("Accession Extent - Physical (Linear Feet)")
                )))
                cells.append((current_row, 5, self._safe_float(
                    row.get("Accession Extent - Digital (GB)")
                )))
                cells.append((current_row, 6, self._safe_int(
                    row.get("Total Number of Formats")
                )))
                cells.append((current_row, 7, self._safe_int(
                    row.get("Total Number of Subject Descriptors")
                )))
                cells.append((current_row, 8, self._safe_int(
                    row.get("Total Number of Issues")
                )))
                cells.append((current_row, 9, self._safe_float(
                    row.get("Final Accession Score")
                )))
                current_row += 1

        return cells

    def _build_flat_cells(self, projects: list[dict]) -> list[tuple[int, int, Any]]:
        """
        Build cells for the flat view (one row per accession).

        Layout:
            Row 1:  Headers
            Row 2:  Accession 1 (project name in col 1)
            Row 3:  Accession 2 (project name in col 1)
            ...

        Within each project, accessions are sorted by individual final score
        descending. Projects appear in order of average score descending.
        """
        cells: list[tuple[int, int, Any]] = []

        headers = [
            self.grouping_field,
            "Project Avg Score",
            "Accession Number",
            "Donor Number",
            "Physical Extent (Linear Feet)",
            "Digital Extent (GB)",
            "Special Formats (Total)",
            "Subject Descriptors (Total)",
            "Issues (Total)",
            "Final Accession Score",
        ]
        for col_idx, header in enumerate(headers, 1):
            cells.append((1, col_idx, header))

        current_row = 2
        for project in projects:
            avg = project["metrics"]["average_score"]
            # Sort accessions within the project by score descending
            sorted_rows = sorted(
                project["rows"],
                key=lambda r: -self._safe_float(r.get("Final Accession Score", 0)),
            )

            for row in sorted_rows:
                cells.append((current_row, 1, project["name"]))
                cells.append((current_row, 2, avg))
                accession_num = row.get("Accession Number") or row.get(
                    "Identifier (Use for Hyperlink Only)", ""
                )
                cells.append((current_row, 3, accession_num))
                cells.append((current_row, 4, row.get("Donor Number", "")))
                cells.append((current_row, 5, self._safe_float(
                    row.get("Accession Extent - Physical (Linear Feet)")
                )))
                cells.append((current_row, 6, self._safe_float(
                    row.get("Accession Extent - Digital (GB)")
                )))
                cells.append((current_row, 7, self._safe_int(
                    row.get("Total Number of Formats")
                )))
                cells.append((current_row, 8, self._safe_int(
                    row.get("Total Number of Subject Descriptors")
                )))
                cells.append((current_row, 9, self._safe_int(
                    row.get("Total Number of Issues")
                )))
                cells.append((current_row, 10, self._safe_float(
                    row.get("Final Accession Score")
                )))
                current_row += 1

        return cells

    def _safe_float(self, value: Any) -> float:
        """Convert to float, returning 0.0 on failure."""
        try:
            return float(value or 0)
        except (ValueError, TypeError):
            return 0.0

    def _safe_int(self, value: Any) -> int:
        """Convert to int, returning 0 on failure."""
        try:
            return int(value or 0)
        except (ValueError, TypeError):
            return 0


class BacklogAtAGlanceBuilder:
    """
    Computes the "Backlog At a Glance" summary dashboard.

    Produces a snapshot table showing accession counts and extent totals
    for each configured status group, plus a TOTAL row.
    """

    SHEET_NAME = "Backlog At a Glance"

    def __init__(
        self,
        spreadsheet_rows: list[dict],
        status_groups: list[dict],
        project_count_func: Optional[callable] = None,
    ):
        """
        Args:
            spreadsheet_rows: All rows from the main accession sheet.
            status_groups: List of status group configs from data.yml.
            project_count_func: Optional callable that takes a list of rows
                and returns the project count (used for the General Backlog
                "Processing Projects Remaining" metric).
        """
        self.rows = spreadsheet_rows
        self.status_groups = status_groups
        self.project_count_func = project_count_func

    def build_cells(self) -> list[tuple[int, int, Any]]:
        """
        Build the summary dashboard cell content.

        Layout (matches the original "Backlog At a Glance" sheet):
            Row 1: Header row with column labels
            Row 2: First status group (label, count, physical, digital, projects)
            Row 3: Second status group
            ...
            Row N: TOTAL row (sum across groups)
        """
        cells: list[tuple[int, int, Any]] = []

        # Header row
        cells.append((1, 1, ""))
        cells.append((1, 2, "Total Number of Accessions"))
        cells.append((1, 3, "Physical Extent (Linear Feet)"))
        cells.append((1, 4, "Digital Extent (GB)"))
        cells.append((1, 5, "Processing Projects Remaining"))

        # Compute and write rows for each status group
        total_accessions = 0
        total_physical = 0.0
        total_digital = 0.0

        current_row = 2
        for group in self.status_groups:
            label = group.get("label", "Unknown")
            status_values = group.get("status_values", [])
            show_projects = group.get("show_project_count", False)

            metrics = self._compute_group_metrics(status_values)

            cells.append((current_row, 1, label))
            cells.append((current_row, 2, metrics["count"]))
            cells.append((current_row, 3, metrics["physical"]))
            cells.append((current_row, 4, metrics["digital"]))

            if show_projects and self.project_count_func:
                project_rows = self._filter_by_status(status_values)
                cells.append(
                    (current_row, 5, self.project_count_func(project_rows))
                )
            else:
                cells.append((current_row, 5, "N/A"))

            total_accessions += metrics["count"]
            total_physical += metrics["physical"]
            total_digital += metrics["digital"]
            current_row += 1

        # TOTAL row
        cells.append((current_row, 1, "TOTAL"))
        cells.append((current_row, 2, total_accessions))
        cells.append((current_row, 3, round(total_physical, 4)))
        cells.append((current_row, 4, round(total_digital, 4)))

        return cells

    def _compute_group_metrics(self, status_values: list[str]) -> dict:
        """Compute count and extents for accessions matching given statuses."""
        rows = self._filter_by_status(status_values)
        physical = 0.0
        digital = 0.0
        for row in rows:
            try:
                physical += float(
                    row.get("Accession Extent - Physical (Linear Feet)", 0) or 0
                )
            except (ValueError, TypeError):
                pass
            try:
                digital += float(row.get("Accession Extent - Digital (GB)", 0) or 0)
            except (ValueError, TypeError):
                pass
        return {
            "count": len(rows),
            "physical": round(physical, 4),
            "digital": round(digital, 4),
        }

    def _filter_by_status(self, status_values: list[str]) -> list[dict]:
        """Filter spreadsheet rows by accession status values."""
        if not status_values:
            return []
        normalized = {s.lower().strip() for s in status_values}
        return [
            row for row in self.rows
            if str(row.get("Accession Status", "")).lower().strip() in normalized
        ]


def queue_sheet_name(queue_name: str) -> str:
    """
    Convert a queue name into a sheet name.

    Sheet names have constraints (Excel forbids \\ / ? * [ ] :, max 31 chars),
    so we sanitize and prefix to ensure uniqueness across multiple queues.
    The "Queue - " prefix distinguishes generated queue sheets from other
    sheets in the workbook.
    """
    # Strip characters Excel doesn't allow in sheet titles
    forbidden = set('\\/?*[]:')
    safe = "".join(c for c in queue_name if c not in forbidden)
    safe = safe.strip()[:22]  # Leave room for "Queue - " prefix (8 chars), max 31
    return f"Queue - {safe}"
