"""
Visualizations Module

Generates chart visualizations from accession data. This is the program's
replacement for the original standalone "Visualizations" Google Sheet,
which used IMPORTRANGE formulas to pull data from the main spreadsheet
into hidden tables that feed native chart objects.

Architecture:
- Three builder classes compute derived data tables in Python:
    - MonthlyChangeTableBuilder: backlog totals and month-over-month
      deltas, plus monthly accessioning vs. processing counts/extents
    - CompletionByStatusTableBuilder: monthly completion counts and
      extents broken out by the configured Backlog At a Glance status
      groups (shared with the existing summary dashboard)
    - SubjectDescriptorCountsBuilder: overall count of how many
      accessions reference each subject descriptor, sorted descending
- Data tables are written to hidden sheets prefixed "Viz - " so users
  who open the workbook don't see raw aggregation tables by default.
- Chart objects are created on a visible "Visualizations" sheet once
  when the workbook is first built. The data ranges use whole-column
  references so the charts automatically pick up new rows as the
  tables grow each month.

Chart types produced (openpyxl and Google Sheets compatible):
  1. Pie chart: Top 10 Subject Descriptors in Backlog
  2. Column chart (grouped): Physical and Digital Backlog Over Time
  3. Column chart (grouped): Growth or Reduction in Backlog Over Time
  4. Column chart (grouped): Accessioning vs. Processing (by count)
  5. Column chart (grouped): Accessioning vs. Processing (physical extent)
  6. Column chart (grouped): Physical Processing Completed per Status Group
  7. Column chart (grouped): Digital Processing Completed per Status Group
  8. Column chart (grouped): Accessions Completed per Status Group
"""

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Optional


# Sheet names used throughout this module. The "Viz - " prefix marks
# data tables that are regenerated every sync and hidden from the user
# by default — editing them directly is pointless since the program
# overwrites them on the next run.
VIZ_SHEET = "Visualizations"
VIZ_MONTHLY_TABLE = "Viz - Monthly Change"
VIZ_COMPLETION_TABLE = "Viz - Completion by Status"
VIZ_SUBJECTS_TABLE = "Viz - Subject Counts"

TOP_N_SUBJECTS = 10


# -----------------------------------------------------------------------------
# Data table builders
# -----------------------------------------------------------------------------

class MonthlyChangeTableBuilder:
    """
    Builds the Monthly Change table that feeds charts 2, 3, 4, 5.

    Column layout (hidden sheet):
      A: Month and Year
      B: Physical Backlog (Linear Feet) — end-of-month total
      C: Digital Backlog (GB) — end-of-month total
      D: Accessions Acquired This Month (count)
      E: Physical Extent Acquired This Month (LF)
      F: Accessions Completed This Month (count)
      G: Physical Extent Completed This Month (LF)
      H: (reserved, keeps layout stable if extra series added)
      I: (reserved)
      J: Physical Backlog Delta (month-over-month, LF)
      K: Digital Backlog Delta (month-over-month, GB)

    The original visualization sheet stored these in columns A-K. We
    preserve the same column letters so chart definitions translate
    1:1 to openpyxl Reference objects.
    """

    HEADERS = [
        "Month and Year",                              # A
        "Physical Backlog (Linear Feet)",              # B
        "Digital Backlog (GB)",                        # C
        "Accessions Acquired",                         # D
        "Physical Extent Acquired (LF)",               # E
        "Accessions Completed",                        # F
        "Physical Extent Completed (LF)",              # G
        "",                                            # H (reserved)
        "",                                            # I (reserved)
        "Physical Backlog Delta (LF)",                 # J
        "Digital Backlog Delta (GB)",                  # K
    ]

    def __init__(self, rows: list[dict], start_date: Optional[datetime] = None):
        """
        Args:
            rows: Accession rows from the main spreadsheet.
            start_date: First month to include. If None, uses the earliest
                accession date found across all rows.
        """
        self.rows = rows
        self.start_date = start_date

    def build_cells(self) -> list[tuple[int, int, Any]]:
        """Build (row, col, value) tuples for the monthly change table."""
        monthly = self._compute_monthly_aggregates()
        if not monthly:
            return self._header_only()

        cells: list[tuple[int, int, Any]] = []
        # Header row
        for col_idx, header in enumerate(self.HEADERS, 1):
            cells.append((1, col_idx, header))

        # One row per month, sorted chronologically
        months_sorted = sorted(monthly.keys())
        prev_physical = 0.0
        prev_digital = 0.0
        for row_idx, month_key in enumerate(months_sorted, 2):
            m = monthly[month_key]
            physical = m["physical_total"]
            digital = m["digital_total"]
            delta_p = physical - prev_physical
            delta_d = digital - prev_digital

            cells.append((row_idx, 1, self._format_month(month_key)))
            cells.append((row_idx, 2, round(physical, 4)))
            cells.append((row_idx, 3, round(digital, 4)))
            cells.append((row_idx, 4, m["acquired_count"]))
            cells.append((row_idx, 5, round(m["acquired_physical"], 4)))
            cells.append((row_idx, 6, m["completed_count"]))
            cells.append((row_idx, 7, round(m["completed_physical"], 4)))
            cells.append((row_idx, 10, round(delta_p, 4)))
            cells.append((row_idx, 11, round(delta_d, 4)))

            prev_physical = physical
            prev_digital = digital

        return cells

    def _header_only(self) -> list[tuple[int, int, Any]]:
        """Return just the header row when there's no data to aggregate."""
        return [(1, c, h) for c, h in enumerate(self.HEADERS, 1)]

    def _compute_monthly_aggregates(self) -> dict[tuple, dict]:
        """
        Build a month-keyed dict of aggregate metrics.

        Each month's dict contains:
          - physical_total: cumulative physical extent in backlog as of end of month
          - digital_total: cumulative digital extent in backlog as of end of month
          - acquired_count / acquired_physical: new acquisitions that month
          - completed_count / completed_physical: completions that month
        """
        # Determine month range. Walk once to find min/max date.
        earliest: Optional[datetime] = self.start_date
        latest: Optional[datetime] = None
        acquired: dict[tuple, list[dict]] = defaultdict(list)
        completed: dict[tuple, list[dict]] = defaultdict(list)

        for row in self.rows:
            acc_date = self._parse_date(row.get("Accession Date"))
            mon_done = self._parse_date(row.get("Month Completed"))

            if acc_date:
                key = (acc_date.year, acc_date.month)
                acquired[key].append(row)
                if earliest is None or acc_date < earliest:
                    earliest = acc_date
                if latest is None or acc_date > latest:
                    latest = acc_date
            if mon_done:
                key = (mon_done.year, mon_done.month)
                completed[key].append(row)
                if latest is None or mon_done > latest:
                    latest = mon_done

        if not earliest or not latest:
            return {}

        # Enumerate every month from earliest to latest (inclusive)
        months: list[tuple] = []
        y, m = earliest.year, earliest.month
        end_y, end_m = latest.year, latest.month
        while (y, m) <= (end_y, end_m):
            months.append((y, m))
            m += 1
            if m > 12:
                m = 1
                y += 1

        # Running totals for cumulative backlog
        cumulative_physical = 0.0
        cumulative_digital = 0.0
        result: dict[tuple, dict] = {}

        for month_key in months:
            acq_rows = acquired.get(month_key, [])
            comp_rows = completed.get(month_key, [])

            acquired_physical = sum(self._num(r.get("Accession Extent - Physical (Linear Feet)")) for r in acq_rows)
            acquired_digital = sum(self._num(r.get("Accession Extent - Digital (GB)")) for r in acq_rows)
            completed_physical = sum(self._num(r.get("Accession Extent - Physical (Linear Feet)")) for r in comp_rows)
            completed_digital = sum(self._num(r.get("Accession Extent - Digital (GB)")) for r in comp_rows)

            cumulative_physical += acquired_physical - completed_physical
            cumulative_digital += acquired_digital - completed_digital

            result[month_key] = {
                "physical_total": cumulative_physical,
                "digital_total": cumulative_digital,
                "acquired_count": len(acq_rows),
                "acquired_physical": acquired_physical,
                "completed_count": len(comp_rows),
                "completed_physical": completed_physical,
            }

        return result

    @staticmethod
    def _parse_date(val: Any) -> Optional[datetime]:
        """Parse a date cell value into a datetime. Accepts several formats."""
        if not val:
            return None
        if isinstance(val, datetime):
            return val
        s = str(val).strip()
        if not s:
            return None
        # Try common formats. ArchivesSpace emits YYYY-MM-DD; the program
        # uses MM/YYYY for Month Completed (first-of-month convention).
        for fmt in ("%Y-%m-%d", "%m/%Y", "%Y-%m", "%Y/%m/%d", "%B %d, %Y"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _num(val: Any) -> float:
        """Coerce to float, returning 0.0 on any error."""
        try:
            return float(val or 0)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _format_month(month_key: tuple) -> str:
        """Convert (year, month) to 'Month D, YYYY' matching the original sheet."""
        y, m = month_key
        return datetime(y, m, 1).strftime("%B 1, %Y")


class CompletionByStatusTableBuilder:
    """
    Builds the Completion by Status table that feeds charts 6, 7, 8.

    Uses the configured `backlog_at_a_glance.status_groups` from data.yml
    as the column groups so the visualization breakdown matches the
    summary dashboard's bucketing.

    For N status groups, the table has 1 + 3*N columns:
      A: Month and Year
      For each group (in config order):
        col i:   Count completed (accessions)
        col i+1: Physical extent completed (LF)
        col i+2: Digital extent completed (GB)
    """

    def __init__(
        self, rows: list[dict], status_groups: list[dict],
        start_date: Optional[datetime] = None,
    ):
        self.rows = rows
        self.status_groups = status_groups
        self.start_date = start_date

    def build_cells(self) -> list[tuple[int, int, Any]]:
        """Build (row, col, value) tuples for the completion-by-status table."""
        cells: list[tuple[int, int, Any]] = []

        # Header row: Month + 3 headers per group
        cells.append((1, 1, "Month and Year"))
        col = 2
        for group in self.status_groups:
            label = group.get("label", "?")
            cells.append((1, col, f"{label} — Accessions"))
            cells.append((1, col + 1, f"{label} — Physical (LF)"))
            cells.append((1, col + 2, f"{label} — Digital (GB)"))
            col += 3

        # Aggregate per (month, status-group)
        monthly = self._compute_monthly_completion()
        if not monthly:
            return cells

        months_sorted = sorted(monthly.keys())
        for row_idx, month_key in enumerate(months_sorted, 2):
            cells.append((row_idx, 1, MonthlyChangeTableBuilder._format_month(month_key)))
            col = 2
            for group in self.status_groups:
                label = group.get("label", "?")
                g = monthly[month_key].get(label, {"count": 0, "physical": 0.0, "digital": 0.0})
                cells.append((row_idx, col, g["count"]))
                cells.append((row_idx, col + 1, round(g["physical"], 4)))
                cells.append((row_idx, col + 2, round(g["digital"], 4)))
                col += 3

        return cells

    def _compute_monthly_completion(self) -> dict[tuple, dict[str, dict]]:
        """
        For each (year, month), return a dict keyed by status-group label
        with count, physical, and digital completion totals. Only rows
        with a Month Completed value are included.
        """
        # Build lookup: status value (lowercased) -> group label
        status_to_group: dict[str, str] = {}
        for group in self.status_groups:
            label = group.get("label", "?")
            for s in group.get("status_values", []):
                status_to_group[s.lower().strip()] = label

        result: dict[tuple, dict[str, dict]] = defaultdict(
            lambda: defaultdict(lambda: {"count": 0, "physical": 0.0, "digital": 0.0})
        )
        for row in self.rows:
            mon = MonthlyChangeTableBuilder._parse_date(row.get("Month Completed"))
            if not mon:
                continue
            status = str(row.get("Accession Status", "")).lower().strip()
            group_label = status_to_group.get(status)
            if not group_label:
                continue  # status not mapped to any configured group
            key = (mon.year, mon.month)
            cell = result[key][group_label]
            cell["count"] += 1
            cell["physical"] += MonthlyChangeTableBuilder._num(
                row.get("Accession Extent - Physical (Linear Feet)")
            )
            cell["digital"] += MonthlyChangeTableBuilder._num(
                row.get("Accession Extent - Digital (GB)")
            )
        return dict(result)


class SubjectDescriptorCountsBuilder:
    """
    Builds the Subject Descriptor Counts table that feeds chart 1.

    Column layout:
      A: Subject Descriptor
      B: Number of Accessions

    Counts are across all accessions in the provided rows (the caller
    filters to backlog-only accessions before passing rows in). An
    accession that lists the same descriptor in two slots is counted
    once for that descriptor.
    """

    def __init__(self, rows: list[dict], top_n: int = TOP_N_SUBJECTS):
        self.rows = rows
        self.top_n = top_n

    def build_cells(self) -> list[tuple[int, int, Any]]:
        """Build (row, col, value) tuples. Two columns: descriptor, count."""
        counts = self._count_descriptors()
        cells: list[tuple[int, int, Any]] = [
            (1, 1, "Subject Descriptor"),
            (1, 2, "Number of Accessions"),
        ]
        # Sort descending by count; tiebreak alphabetically for determinism
        sorted_items = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        for row_idx, (name, count) in enumerate(sorted_items[:self.top_n], 2):
            cells.append((row_idx, 1, name))
            cells.append((row_idx, 2, count))
        return cells

    def _count_descriptors(self) -> dict[str, int]:
        """
        Count how many accessions reference each descriptor. An accession
        contributes at most 1 to each descriptor it lists (dedupe per row).
        """
        counter: Counter = Counter()
        for row in self.rows:
            seen_in_row: set[str] = set()
            for col_name, val in row.items():
                if not col_name.startswith("Subject Descriptor ("):
                    continue
                if val and str(val).strip():
                    s = str(val).strip()
                    if s not in seen_in_row:
                        counter[s] += 1
                        seen_in_row.add(s)
        return dict(counter)


# -----------------------------------------------------------------------------
# Chart specifications
# -----------------------------------------------------------------------------

def chart_specs(num_status_groups: int) -> list[dict]:
    """
    Return the declarative chart specifications for all 8 visualizations.

    Each spec is backend-agnostic — both the openpyxl writer and the
    Google Sheets writer consume the same structure. The number of
    status groups affects which column ranges in the completion table
    each of charts 6, 7, 8 should pull from.

    A spec dict contains:
      id:        stable identifier
      title:     chart title
      kind:      'pie' | 'column_grouped' | 'column_stacked'
      table:     source sheet name
      x_axis:    x-axis label (column chart only)
      y_axis:    y-axis label (column chart only)
      anchor:    cell where the chart's top-left corner goes on the
                 Visualizations sheet (e.g., "A1", "J1")
      categories: (first_col, first_row, last_row) — 1-indexed range
                  for category axis data on the source sheet
      series:    list of {name, col} — each series reads from one
                 column on the source table; name is displayed in legend
    """
    # Completion table column layout: col 1 = Month, then 3 cols per group.
    # chart 8 = count:     col 2, col 5, col 8, ... (first of each triplet)
    # chart 6 = physical:  col 3, col 6, col 9, ...
    # chart 7 = digital:   col 4, col 7, col 10, ...

    # Series for each of the three "completion by status" charts
    def status_group_series(offset: int) -> list[dict]:
        """offset=0 is count, 1 is physical, 2 is digital."""
        return [
            {
                "name_header_row": 1,
                "col": 2 + offset + (i * 3),
            }
            for i in range(num_status_groups)
        ]

    return [
        {
            "id": "top_subjects",
            "title": f"Top {TOP_N_SUBJECTS} Subject Descriptors in Backlog",
            "kind": "pie",
            "table": VIZ_SUBJECTS_TABLE,
            "anchor": "A1",
            "categories": {"col": 1, "first_row": 2, "last_row": TOP_N_SUBJECTS + 1},
            "series": [{"name_header_row": 1, "col": 2}],
        },
        {
            "id": "backlog_over_time",
            "title": "Physical and Digital Backlog Over Time",
            "kind": "column_grouped",
            "table": VIZ_MONTHLY_TABLE,
            "x_axis": "Month and Year",
            "y_axis": "Backlog Size",
            "anchor": "J1",
            "categories": {"col": 1, "first_row": 2, "last_row": None},  # None = use last row of data
            "series": [
                {"name_header_row": 1, "col": 2},  # Physical LF
                {"name_header_row": 1, "col": 3},  # Digital GB
            ],
        },
        {
            "id": "backlog_delta",
            "title": "Growth or Reduction in Backlog Over Time",
            "kind": "column_grouped",
            "table": VIZ_MONTHLY_TABLE,
            "x_axis": "Month and Year",
            "y_axis": "Month-over-Month Change",
            "anchor": "A20",
            "categories": {"col": 1, "first_row": 2, "last_row": None},
            "series": [
                {"name_header_row": 1, "col": 10},  # Physical delta
                {"name_header_row": 1, "col": 11},  # Digital delta
            ],
        },
        {
            "id": "acc_vs_proc_count",
            "title": "Accessioning vs. Processing (Accessions per Month)",
            "kind": "column_grouped",
            "table": VIZ_MONTHLY_TABLE,
            "x_axis": "Month and Year",
            "y_axis": "Number of Accessions",
            "anchor": "J20",
            "categories": {"col": 1, "first_row": 2, "last_row": None},
            "series": [
                {"name_header_row": 1, "col": 4},   # Acquired count
                {"name_header_row": 1, "col": 6},   # Completed count
            ],
        },
        {
            "id": "acc_vs_proc_extent",
            "title": "Accessioning vs. Processing (Physical Extent per Month)",
            "kind": "column_grouped",
            "table": VIZ_MONTHLY_TABLE,
            "x_axis": "Month and Year",
            "y_axis": "Linear Feet",
            "anchor": "A39",
            "categories": {"col": 1, "first_row": 2, "last_row": None},
            "series": [
                {"name_header_row": 1, "col": 5},   # Acquired physical
                {"name_header_row": 1, "col": 7},   # Completed physical
            ],
        },
        {
            "id": "completion_count",
            "title": "Accessions Completed per Month (by Status Group)",
            "kind": "column_grouped",
            "table": VIZ_COMPLETION_TABLE,
            "x_axis": "Month and Year",
            "y_axis": "Number of Accessions",
            "anchor": "J39",
            "categories": {"col": 1, "first_row": 2, "last_row": None},
            "series": status_group_series(0),
        },
        {
            "id": "completion_physical",
            "title": "Physical Extent Completed per Month (by Status Group)",
            "kind": "column_grouped",
            "table": VIZ_COMPLETION_TABLE,
            "x_axis": "Month and Year",
            "y_axis": "Linear Feet",
            "anchor": "A58",
            "categories": {"col": 1, "first_row": 2, "last_row": None},
            "series": status_group_series(1),
        },
        {
            "id": "completion_digital",
            "title": "Digital Extent Completed per Month (by Status Group)",
            "kind": "column_grouped",
            "table": VIZ_COMPLETION_TABLE,
            "x_axis": "Month and Year",
            "y_axis": "Gigabytes",
            "anchor": "J58",
            "categories": {"col": 1, "first_row": 2, "last_row": None},
            "series": status_group_series(2),
        },
    ]


# -----------------------------------------------------------------------------
# Helpers for writing cells into a worksheet
# -----------------------------------------------------------------------------

def cells_to_values_array(cells: list[tuple[int, int, Any]]) -> list[list]:
    """
    Convert (row, col, value) tuples to a 2D values array for Google
    Sheets batch update. Any empty positions are filled with "" so the
    array is rectangular.
    """
    if not cells:
        return []
    max_row = max(c[0] for c in cells)
    max_col = max(c[1] for c in cells)
    values: list[list] = [["" for _ in range(max_col)] for _ in range(max_row)]
    for row, col, value in cells:
        values[row - 1][col - 1] = value
    return values


def backlog_rows_for_subjects(
    rows: list[dict], backlog_status_values: Optional[list[str]] = None,
) -> list[dict]:
    """
    Filter rows to those in the configured backlog (for chart 1 input).

    If `backlog_status_values` is provided (e.g., ["Backlog - General"]),
    only rows whose Accession Status matches are returned. If None or
    empty, all rows are returned (the caller can decide what "backlog"
    means in their institution).
    """
    if not backlog_status_values:
        return list(rows)
    normalized = {s.lower().strip() for s in backlog_status_values}
    return [
        r for r in rows
        if str(r.get("Accession Status", "")).lower().strip() in normalized
    ]
