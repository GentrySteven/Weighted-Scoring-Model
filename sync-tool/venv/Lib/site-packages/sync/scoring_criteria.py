"""
Scoring Criteria Sheet Generator

Builds the "Scoring Criteria" sheet content from the configured scoring
dimensions in data.yml. This module is the bridge between the abstract
scoring model in data.yml and the concrete spreadsheet representation.

The scoring criteria sheet is the source that the UWS formulas in the
main "Accession Data and Scores" sheet reference. It is regenerated
from data.yml whenever the user changes the scoring configuration.

For Excel embedded mode, the sheet is created within the same workbook
as the main accession data. For Excel linked-workbook mode, it lives
in a separate file. For Google Sheets, it lives in a separate Google
Sheet referenced via IMPORTRANGE (managed externally).

Sheet Layout (matches the original Scoring Criteria spreadsheet):
    Row 1: Section headers (e.g., "Quantitative Factor(s)", "Strategic Alignment Factor(s)")
    Row 3+: One block per dimension with label, score table, and (for date ranges)
            dynamic TODAY() formulas for the date boundaries
    Bottom: Weights table
"""

from typing import Any


SHEET_NAME = "Scoring Criteria - DO NOT MOVE"


class ScoringCriteriaBuilder:
    """
    Builds the rows for the scoring criteria sheet from data.yml.

    Output is a list of (row, col, value) tuples that can be applied
    to either an openpyxl worksheet (Excel) or a Google Sheets values
    array. This keeps the builder format-agnostic.
    """

    def __init__(self, dimensions: dict):
        """
        Args:
            dimensions: The "dimensions" dict from data.yml's scoring_criteria.
        """
        self.dimensions = dimensions
        self._cells: list[tuple[int, int, Any]] = []

    def build(self) -> list[tuple[int, int, Any]]:
        """
        Build all cell values for the scoring criteria sheet.

        Returns:
            List of (row, col, value) tuples (1-indexed positions).
        """
        self._cells = []

        # --- Section headers (row 1) ---
        # Group dimensions by category for the section layout
        quantitative = [
            (k, d) for k, d in self.dimensions.items()
            if d.get("category") == "quantitative"
        ]
        strategic = [
            (k, d) for k, d in self.dimensions.items()
            if d.get("category") == "strategic"
        ]

        if quantitative:
            self._cells.append((1, 1, "Quantitative Factor(s)"))
        if strategic:
            self._cells.append((1, 6, "Strategic Alignment Factor(s)"))

        # --- Dimension blocks ---
        # Quantitative dimensions go in columns A-D
        next_quant_row = 3
        for key, dim in quantitative:
            next_quant_row = self._add_dimension_block(
                dim, start_row=next_quant_row, start_col=1
            )
            next_quant_row += 2  # Spacing between blocks

        # Strategic dimensions go in columns F-G (right side)
        next_strat_row = 3
        for key, dim in strategic:
            next_strat_row = self._add_dimension_block(
                dim, start_row=next_strat_row, start_col=6
            )
            next_strat_row += 2

        # --- Weights table ---
        # Place at the bottom under the columns
        weights_row = max(next_quant_row, next_strat_row) + 2
        self._add_weights_table(weights_row)

        return self._cells

    def _add_dimension_block(self, dim: dict, start_row: int, start_col: int) -> int:
        """
        Add a single dimension's score table to the sheet.

        Returns:
            The row number after the block (for placing the next block).
        """
        label = dim.get("label", "Unknown")
        scoring_type = dim.get("scoring_type", "")
        max_score = self._get_max_score(dim)

        # Block title with scale (e.g., "Time in Backlog (1-4)")
        title = f"{label} (1-{max_score})"
        self._cells.append((start_row, start_col, title))

        # Column headers
        header_row = start_row + 1
        if scoring_type == "date_range":
            # Time in Backlog has 4 columns: label, score, beginning date, ending date
            self._cells.append((header_row, start_col, "Length of Time"))
            self._cells.append((header_row, start_col + 1, "Numerical Scores"))
            self._cells.append((header_row, start_col + 2, '"Beginning" Time'))
            self._cells.append((header_row, start_col + 3, '"Ending" Time'))

            thresholds = dim.get("thresholds", [])
            for i, t in enumerate(thresholds):
                row = header_row + 1 + i
                self._cells.append((row, start_col, t.get("label", "")))
                self._cells.append((row, start_col + 1, float(t.get("score", 0))))

                # Dynamic TODAY() formulas for date boundaries
                min_years = t.get("min_years", 0)
                max_years = t.get("max_years")
                # Beginning (older boundary)
                if max_years is None:
                    self._cells.append((row, start_col + 2, "N/A"))
                else:
                    self._cells.append(
                        (row, start_col + 2, f"=TODAY() - ((365 * {max_years}) - 1)")
                    )
                # Ending (newer boundary)
                if min_years == 0:
                    self._cells.append((row, start_col + 3, "=TODAY()"))
                else:
                    self._cells.append(
                        (row, start_col + 3, f"=TODAY() - (365 * {min_years})")
                    )

            return header_row + 1 + len(thresholds)

        elif scoring_type == "value_map":
            # Priority has 2 columns: ranking, score
            self._cells.append((header_row, start_col, "Ranking"))
            self._cells.append((header_row, start_col + 1, "Numerical Scores"))

            mappings = dim.get("mappings", [])
            for i, m in enumerate(mappings):
                row = header_row + 1 + i
                self._cells.append((row, start_col, m.get("value", "")))
                self._cells.append((row, start_col + 1, float(m.get("score", 0))))

            return header_row + 1 + len(mappings)

        elif scoring_type == "count_range":
            # Subject Descriptors has 2 columns: count, score
            self._cells.append((header_row, start_col, "Number of Descriptors"))
            self._cells.append((header_row, start_col + 1, "Numerical Scores"))

            thresholds = dim.get("thresholds", [])
            for i, t in enumerate(thresholds):
                row = header_row + 1 + i
                self._cells.append((row, start_col, t.get("label", "")))
                self._cells.append((row, start_col + 1, float(t.get("score", 0))))

            return header_row + 1 + len(thresholds)

        return start_row + 1

    def _add_weights_table(self, start_row: int) -> None:
        """Add the weights table at the bottom of the sheet."""
        self._cells.append((start_row, 3, "Weights"))

        # Header row with each dimension's weight column
        header_row = start_row + 1
        for i, (key, dim) in enumerate(self.dimensions.items()):
            self._cells.append((header_row, 3 + i, f"{dim['label']} (Weight)"))

        # Values row
        value_row = start_row + 2
        for i, (key, dim) in enumerate(self.dimensions.items()):
            self._cells.append((value_row, 3 + i, float(dim.get("weight", 0))))

    def _get_max_score(self, dim: dict) -> int:
        """Get the highest score in a dimension's thresholds or mappings."""
        scoring_type = dim.get("scoring_type", "")
        if scoring_type == "value_map":
            return max(
                (int(m.get("score", 0)) for m in dim.get("mappings", [])),
                default=4,
            )
        else:
            return max(
                (int(t.get("score", 0)) for t in dim.get("thresholds", [])),
                default=4,
            )

    def get_cell_references(self) -> dict:
        """
        Build a map of dimension key -> cell range references.

        This is used by the main accession sheet's UWS formulas to know
        where to look up the score values from the criteria sheet.

        Returns:
            Dict like:
            {
                "time_in_backlog": {
                    "type": "date_range",
                    "score_range": "B5:B8",
                    "begin_range": "C5:C8",
                    "end_range": "D5:D8",
                    "weight_cell": "C21",
                },
                ...
            }
        """
        refs: dict[str, dict] = {}
        quantitative = [
            (k, d) for k, d in self.dimensions.items()
            if d.get("category") == "quantitative"
        ]
        strategic = [
            (k, d) for k, d in self.dimensions.items()
            if d.get("category") == "strategic"
        ]

        # Quantitative blocks (columns A-D)
        next_row = 3
        for key, dim in quantitative:
            n_levels = self._count_levels(dim)
            score_start = next_row + 2  # title row + header row + 1
            score_end = score_start + n_levels - 1
            refs[key] = {
                "type": dim.get("scoring_type"),
                "score_range": f"B{score_start}:B{score_end}",
            }
            if dim.get("scoring_type") == "date_range":
                refs[key]["begin_range"] = f"C{score_start}:C{score_end}"
                refs[key]["end_range"] = f"D{score_start}:D{score_end}"
            elif dim.get("scoring_type") == "value_map":
                refs[key]["value_range"] = f"A{score_start}:A{score_end}"
            next_row = score_end + 2

        # Strategic blocks (columns F-G)
        next_strat_row = 3
        for key, dim in strategic:
            n_levels = self._count_levels(dim)
            score_start = next_strat_row + 2
            score_end = score_start + n_levels - 1
            refs[key] = {
                "type": dim.get("scoring_type"),
                "score_range": f"G{score_start}:G{score_end}",
            }
            if dim.get("scoring_type") == "value_map":
                refs[key]["value_range"] = f"F{score_start}:F{score_end}"
            else:
                refs[key]["count_range"] = f"F{score_start}:F{score_end}"
            next_strat_row = score_end + 2

        # Weight cells (in the weights table at the bottom)
        weight_row = max(next_row, next_strat_row) + 4  # +2 spacing +2 to value row
        for i, (key, dim) in enumerate(self.dimensions.items()):
            col_letter = chr(ord("C") + i)
            if key in refs:
                refs[key]["weight_cell"] = f"{col_letter}{weight_row}"

        return refs

    def _count_levels(self, dim: dict) -> int:
        """Count the number of score levels in a dimension."""
        if dim.get("scoring_type") == "value_map":
            return len(dim.get("mappings", []))
        return len(dim.get("thresholds", []))


def write_to_openpyxl_sheet(ws, dimensions: dict) -> dict:
    """
    Write the scoring criteria to an openpyxl worksheet.

    Args:
        ws: An openpyxl worksheet object.
        dimensions: The dimensions dict from data.yml.

    Returns:
        The cell references map for use in formula generation.
    """
    builder = ScoringCriteriaBuilder(dimensions)
    cells = builder.build()

    for row, col, value in cells:
        ws.cell(row=row, column=col, value=value)

    return builder.get_cell_references()


def build_values_array(dimensions: dict) -> tuple[list[list[Any]], dict]:
    """
    Build a 2D values array suitable for Google Sheets batch update.

    Args:
        dimensions: The dimensions dict from data.yml.

    Returns:
        Tuple of (values_array, cell_references).
        values_array is a list of rows, each row a list of cell values.
    """
    builder = ScoringCriteriaBuilder(dimensions)
    cells = builder.build()

    if not cells:
        return [], {}

    # Determine the size of the array
    max_row = max(c[0] for c in cells)
    max_col = max(c[1] for c in cells)

    # Initialize empty 2D array
    values: list[list[Any]] = [
        ["" for _ in range(max_col)] for _ in range(max_row)
    ]

    # Populate cells
    for row, col, value in cells:
        values[row - 1][col - 1] = value

    return values, builder.get_cell_references()
