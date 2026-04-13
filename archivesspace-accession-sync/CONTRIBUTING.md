# Contributing to archivesspace-accession-sync

Contributions of all kinds are welcome — bug reports, bug fixes, new features, and documentation improvements.

## Ways to Contribute

### Using GitHub
1. **Report a bug**: Open an issue using the bug report template.
2. **Request a feature**: Open an issue using the feature request template.
3. **Submit code**: Fork, branch from `develop`, and submit a pull request.

### Without GitHub
Contact the maintainer (Steven Gentry) directly.

## Development Workflow

1. Fork and clone the repository
2. Create a virtual environment: `python -m venv venv && source venv/bin/activate`
3. Install dev dependencies: `pip install .[excel,matching,dev]`
4. Create a branch from `develop`: `git checkout develop && git checkout -b your-branch`
5. Make changes, add tests if applicable
6. Run tests: `pytest`
7. Submit a pull request against `develop`

## Coding Recommendations (not required)
- Use `black` for formatting (line length 100)
- Add docstrings to functions and classes
- Use type hints where practical

## Code of Conduct
See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
