# Contributing to archivesspace-accession-sync

Thank you for your interest in contributing to this project! Contributions of all kinds are welcome, including bug reports, bug fixes, new features, and documentation improvements.

## Ways to Contribute

### Using GitHub (Recommended)

1. **Report a bug**: Open an issue using the bug report template.
2. **Request a feature**: Open an issue using the feature request template.
3. **Submit code changes**: Fork the repository, make your changes, and submit a pull request.

### Without GitHub

If you are not comfortable using GitHub, you are welcome to contact the maintainer directly. You can find contact information on the maintainer's institutional profile page at the University of Iowa Libraries. The maintainer can create issues and pull requests on your behalf.

## Development Workflow

### Setting Up Your Development Environment

1. Fork the repository on GitHub.
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/archivesspace-accession-sync.git
   cd archivesspace-accession-sync
   ```
3. Create a virtual environment and install development dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # macOS/Linux
   # or: venv\Scripts\activate  # Windows
   pip install .[excel,google,dev]
   ```
4. Create a branch from `develop` (not `main`):
   ```bash
   git checkout develop
   git checkout -b your-feature-branch
   ```

### Branch Strategy

- **`main`**: Stable releases only. Do not base work on this branch.
- **`develop`**: Active development. Base your branches here.

### Making Changes

1. Write your code on your feature branch.
2. Add or update tests if applicable.
3. Update documentation (README, docstrings, or docs/ files) if your change affects user-facing behavior.
4. Run the test suite to verify your changes:
   ```bash
   pytest
   ```

### Submitting a Pull Request

1. Push your branch to your fork.
2. Open a pull request against the `develop` branch of the main repository.
3. Fill out the pull request template.
4. The maintainer will review your PR and may request changes.

## Coding Recommendations

The following are recommended but not required:

- **Code formatting**: Use [black](https://github.com/psf/black) with the project's configuration (line length 100).
- **Docstrings**: Add docstrings to functions and classes explaining what they do.
- **Type hints**: Use type hints where practical.
- **Meaningful variable names**: Prioritize readability over brevity.
- **Comments**: Explain *why*, not *what* — the code should speak for itself on the *what*.

## Code of Conduct

Please review and follow our [Code of Conduct](CODE_OF_CONDUCT.md). We are committed to maintaining a welcoming and inclusive community.

## Questions?

If you have questions about contributing, feel free to open an issue with the "question" label or contact the maintainer directly.
