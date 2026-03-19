# Scoring Formulas Guide

This guide explains how the scoring formulas work in the Accession Data and Scores spreadsheet and how to set them up for both Google Sheets and Excel.

## Overview

The scoring system uses a weighted model to prioritize accessions for processing. Each accession receives scores across three dimensions, which are multiplied by configurable weights and summed to produce a final score.

## Scoring Dimensions

1. **Time in Backlog** — how long the accession has been waiting for processing
2. **Priority** — the processing priority assigned in ArchivesSpace
3. **Subject Descriptors** — the number of subject descriptors associated with the accession

## Formula Structure

For each dimension, there are three columns:

- **Unweighted Score (UWS)**: A nested IF/AND formula that assigns a score based on how the accession's values align with scoring criteria
- **Weight**: A value imported from a separate scoring criteria source
- **Weighted Score (WS)**: UWS × Weight

The **Final Accession Score** is the SUM of all weighted scores.

## Google Sheets Setup

In Google Sheets, the scoring criteria are stored in a separate spreadsheet and imported using the IMPORTRANGE function.

The UWS formulas use IF and AND to evaluate accession data against the imported criteria. The Weight columns use IMPORTRANGE to pull weight values directly.

## Excel Setup

Since IMPORTRANGE is not available in Excel, two alternatives are offered during setup:

### Option A: Linked Workbook

The scoring criteria are stored in a separate Excel file. Formulas reference this file using standard Excel external references:

```
='[Scoring Criteria.xlsx]Sheet1'!A1
```

**Pros**: Maintains the multi-spreadsheet architecture, consistent with Google Sheets approach.
**Cons**: Links break if the source file is moved or renamed.

### Option B: Embedded Sheet

The scoring criteria are stored in a dedicated sheet within the same workbook. Formulas reference the internal sheet:

```
='Scoring Criteria'!A1
```

**Pros**: Self-contained, no broken links.
**Cons**: Criteria changes must be made in the workbook rather than a central source.

## Choosing an Option

During setup, the wizard will ask which option you prefer. You can change this later by reconfiguring settings from the interactive menu.

## Important Notes

- All scoring columns are **protected during sync** — the tool never overwrites these formulas
- The scoring formulas already exist in the uploaded spreadsheet template and do not need to be recreated
- The IF and AND functions used in the formulas work identically in both Google Sheets and Excel
