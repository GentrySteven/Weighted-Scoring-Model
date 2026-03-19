# Platform Installation Guide

This guide covers installing Python 3.10+ on Windows, macOS, and Linux.

## Windows

### Option A: Python.org Installer (Recommended)

1. Visit [python.org/downloads](https://www.python.org/downloads/).
2. Download the latest Python 3 installer for Windows.
3. Run the installer.
4. **Important:** Check the box that says "Add Python to PATH" at the bottom of the installer window.
5. Click "Install Now."
6. Verify the installation by opening **Command Prompt** and typing:
   ```cmd
   python --version
   ```
   You should see something like `Python 3.12.x`.

### Option B: Microsoft Store

1. Open the Microsoft Store app.
2. Search for "Python 3."
3. Install the latest version (3.10 or higher).
4. Verify in Command Prompt:
   ```cmd
   python --version
   ```

### Which Terminal to Use on Windows

- **Command Prompt**: Press `Win + R`, type `cmd`, press Enter.
- **PowerShell**: Press `Win + R`, type `powershell`, press Enter.
- Either works. The tool's documentation shows examples for both.

## macOS

### Option A: Homebrew (Recommended)

If you have [Homebrew](https://brew.sh/) installed:

```bash
brew install python@3.12
```

Verify:
```bash
python3 --version
```

### Option B: Python.org Installer

1. Visit [python.org/downloads](https://www.python.org/downloads/).
2. Download the latest Python 3 installer for macOS.
3. Run the `.pkg` installer and follow the prompts.
4. Verify:
   ```bash
   python3 --version
   ```

### Which Terminal to Use on macOS

Open **Terminal** from Applications > Utilities > Terminal.

## Linux

Most Linux distributions include Python 3. Check your version:

```bash
python3 --version
```

If you need to install or upgrade:

### Ubuntu / Debian

```bash
sudo apt update
sudo apt install python3.12 python3.12-venv python3-pip
```

### Fedora

```bash
sudo dnf install python3.12
```

### Arch Linux

```bash
sudo pacman -S python
```

### Which Terminal to Use on Linux

Use your distribution's default terminal emulator (e.g., GNOME Terminal, Konsole, xterm).

## Verifying pip

After installing Python, verify that `pip` (the Python package installer) is available:

```bash
pip --version
# or
pip3 --version
```

If pip is not found, install it:

```bash
python3 -m ensurepip --upgrade
```

## Next Steps

Once Python is installed, return to the main [README](../README.md) and follow the Installation instructions.
