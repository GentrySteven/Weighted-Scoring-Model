# Extent Conversion Guide

This guide explains how to configure extent type conversion factors for the archivesspace-accession-sync tool.

## Overview

ArchivesSpace repositories use a variety of extent types to describe the physical and digital size of accessions. This tool converts all physical extent types to **linear feet** and all digital extent types to **gigabytes**, allowing consistent comparison across accessions.

## How It Works

During the first-run setup, the tool scans your repository for all unique extent types and asks you to categorize each one as either **physical** or **digital**, and to provide a conversion factor.

The conversion factor expresses: **1 unit of this extent type = X linear feet (or gigabytes).**

## Common Conversion Factors

### Physical Extent Types

| Extent Type | Category | Conversion Factor | Notes |
|------------|----------|------------------|-------|
| linear_feet | physical | 1.0 | Base unit |
| linear_foot | physical | 1.0 | Alternate spelling |
| linear_inches | physical | 0.0833 | 1 inch = 1/12 foot |
| cubic_feet | physical | 1.0 | Approximate equivalence |
| items | physical | 0.01 | Rough estimate, adjust per institution |

### Digital Extent Types

| Extent Type | Category | Conversion Factor | Notes |
|------------|----------|------------------|-------|
| gigabytes | digital | 1.0 | Base unit |
| megabytes | digital | 0.001 | 1 MB = 0.001 GB |
| terabytes | digital | 1000.0 | 1 TB = 1000 GB |
| kilobytes | digital | 0.000001 | 1 KB = 0.000001 GB |

## Configuring in config.yml

Extent type mappings are stored in the `extent_types` section of `config.yml`:

```yaml
extent_types:
  linear_feet:
    category: physical
    conversion_factor: 1.0
  cubic_feet:
    category: physical
    conversion_factor: 1.0
  gigabytes:
    category: digital
    conversion_factor: 1.0
  megabytes:
    category: digital
    conversion_factor: 0.001
```

## Handling New Extent Types

If the tool encounters an extent type that isn't in the configuration during a sync, it will halt and prompt you to categorize the new type. This ensures no extent data is silently excluded.

## Re-running the Setup

You can reconfigure extent types at any time by selecting "Reconfigure settings" from the interactive menu.
