# Keyword Detection Guide

This guide explains how to configure format keyword detection and the scanning framework for the archivesspace-accession-sync tool.

## Overview

The tool uses keyword detection to automatically identify material formats and processing issues in accession records. It scans specified fields for configured keywords and sets the corresponding columns to TRUE or populates them with structured responses.

## Fields Scanned

For format detection, the following fields are scanned:

- `content_description` — describes what the accession contains
- `condition_description` — describes the physical condition
- `inventory` — lists materials in the accession
- `extent_type` — from the extents sub-record
- Top container `type` and `container_type` — from linked top containers

## Matching Modes

### Standard Matching (Default)

Case-insensitive partial matching. The keyword "photograph" would match "Photographs", "photographic prints", or "PHOTOGRAPH."

### Fuzzy Matching (Optional)

Uses the RapidFuzz library to match approximate strings. Enable in `config.yml`:

```yaml
matching:
  fuzzy_enabled: true
  fuzzy_threshold: 85
```

The threshold (0-100) controls how similar a word must be to count as a match. Higher values are stricter. When fuzzy matching triggers, the match is logged so you can verify it was appropriate.

## Configuring Format Keywords

Format keywords are stored in `config.yml` under `format_keywords`:

```yaml
format_keywords:
  "Photographic Material(s)":
    - "photograph"
    - "photo"
    - "daguerreotype"
  "Oversize Material?":
    - "oversize"
    - "oversized"
    - "flat file"
```

## Repository Scanning

The tool can scan your repository's accession records to suggest additional keywords based on your institution's actual descriptive vocabulary.

From the interactive menu, select "Scan repository for format keywords." The tool will analyze the content of the scanned fields across all accessions and present frequently occurring terms for your review.

You can keep the default keyword lists, merge scan results with defaults, or replace defaults entirely with scan results.

## Configurable Scanning Framework

For issue columns, a more flexible scanning framework is available. Through the guided wizard or the interactive menu, you can define:

1. **Which fields to scan** — choose from any accession or sub-record field
2. **What to look for** — open scan (all terms), targeted scan (specific terms), or both
3. **Matching approach** — exact, partial, or fuzzy
4. **Named configurations** — save and reuse scan configurations

## Adding or Removing Format Columns

Format columns can be added or removed in `config.yml`. Add a new entry under `format_keywords` to create a new column, or remove an entry to disable one. The tool will adjust formulas and column structures accordingly on the next run.
