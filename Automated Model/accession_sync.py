#!/usr/bin/env python3
"""
archivesspace-accession-sync

Main entry point. Run this script to launch the tool.

Usage:
    python accession_sync.py                         # Interactive menu
    python accession_sync.py --target excel          # Sync to Excel
    python accession_sync.py --target google_sheets  # Sync to Google Sheets
    python accession_sync.py --dry-run               # Preview changes
    python accession_sync.py --auto --target excel   # Automatic mode (for scheduled runs)
"""

from sync.cli import main

if __name__ == "__main__":
    main()
