"""
Diagnostic script: run this to find out which copy of the code Python is loading.

Usage:
    python diagnose.py
"""
import importlib
import sys
import os

print("=" * 60)
print("  Code Location Diagnostic")
print("=" * 60)
print()
print(f"  Python executable: {sys.executable}")
print(f"  Working directory: {os.getcwd()}")
print()

# Check where each module is loaded from
modules_to_check = [
    "sync.wizard",
    "sync.google_sheets",
    "sync.menu",
    "sync.notifications",
    "sync.config_manager",
]

print("  Module locations:")
print("  " + "-" * 50)
for mod_name in modules_to_check:
    try:
        mod = importlib.import_module(mod_name)
        location = getattr(mod, "__file__", "(unknown)")
        print(f"  {mod_name}")
        print(f"    -> {location}")
    except Exception as e:
        print(f"  {mod_name}")
        print(f"    -> IMPORT ERROR: {e}")
print()

# Check specific markers that prove the updated code is loaded
print("  Version checks (markers from recent updates):")
print("  " + "-" * 50)

# Check 1: menu.py should have "default is" format
try:
    import inspect
    from sync.menu import Menu
    source = inspect.getsource(Menu.prompt_text)
    if "default is" in source:
        print("  menu.py prompt_text:      ✓ UPDATED (has 'default is' format)")
    else:
        print("  menu.py prompt_text:      ✗ OLD VERSION (missing 'default is')")
except Exception as e:
    print(f"  menu.py prompt_text:      ERROR: {e}")

# Check 2: google_sheets.py should use Drive API, not Sheets API create
try:
    from sync.google_sheets import GoogleSheetsManager
    source = inspect.getsource(GoogleSheetsManager.create_spreadsheet)
    if "files().create" in source or "files()" in source:
        print("  google_sheets.py create:  ✓ UPDATED (uses Drive API)")
    elif "spreadsheets().create" in source:
        print("  google_sheets.py create:  ✗ OLD VERSION (uses Sheets API create)")
    else:
        print("  google_sheets.py create:  ? UNKNOWN (could not detect method)")
except Exception as e:
    print(f"  google_sheets.py create:  ERROR: {e}")

# Check 3: notifications.py send_test_email should return tuple
try:
    from sync.notifications import NotificationManager
    source = inspect.getsource(NotificationManager.send_test_email)
    if "tuple" in source or 'False, ""' in source or "False, str(e)" in source:
        print("  notifications.py test:    ✓ UPDATED (returns tuple)")
    else:
        print("  notifications.py test:    ✗ OLD VERSION (returns bool)")
except Exception as e:
    print(f"  notifications.py test:    ERROR: {e}")

# Check 4: wizard.py should have _clean_file_path
try:
    from sync.wizard import SetupWizard
    if hasattr(SetupWizard, "_clean_file_path"):
        print("  wizard.py path cleaning:  ✓ UPDATED (has _clean_file_path)")
    else:
        print("  wizard.py path cleaning:  ✗ OLD VERSION (missing _clean_file_path)")
except Exception as e:
    print(f"  wizard.py path cleaning:  ERROR: {e}")

# Check 5: wizard.py should have _setup_existing_sheet (dual-path Phase 16)
try:
    from sync.wizard import SetupWizard
    if hasattr(SetupWizard, "_setup_existing_sheet"):
        print("  wizard.py dual-path:     ✓ UPDATED (has _setup_existing_sheet)")
    else:
        print("  wizard.py dual-path:     ✗ OLD VERSION (missing _setup_existing_sheet)")
except Exception as e:
    print(f"  wizard.py dual-path:     ERROR: {e}")

# Check 6: google_sheets.py should have setup_existing_spreadsheet
try:
    from sync.google_sheets import GoogleSheetsManager
    if hasattr(GoogleSheetsManager, "setup_existing_spreadsheet"):
        print("  google_sheets.py setup:  ✓ UPDATED (has setup_existing_spreadsheet)")
    else:
        print("  google_sheets.py setup:  ✗ OLD VERSION (missing setup_existing_spreadsheet)")
except Exception as e:
    print(f"  google_sheets.py setup:  ERROR: {e}")

print()
print("  If any module shows '✗ OLD VERSION', Python is loading")
print("  a stale copy. The 'Module locations' section above shows")
print("  which folder each file is coming from.")
print()
print("  If the paths point to a 'site-packages' folder, run:")
print("    pip install -e .[google]")
print("  from inside your sync-tool project folder.")
print()
print("  If the paths point to your project folder but still show")
print("  OLD VERSION, the .py files in that folder need replacing.")
print("=" * 60)
