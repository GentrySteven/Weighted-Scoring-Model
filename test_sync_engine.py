# =============================================================================
# archivesspace-accession-sync .gitignore
# =============================================================================

# Credentials (NEVER commit these)
credentials.yml
*.json
!examples/*.json

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
*.egg-info/
dist/
build/
*.egg

# Virtual environments
venv/
env/
.env/

# IDE files
.vscode/
.idea/
*.swp
*.swo
*~

# OS files
.DS_Store
Thumbs.db

# Logs
logs/
*.log

# Cache
cache/

# Staging files
staging_sync_*.json

# Preview files
preview/
[Preview]*

# Backups
[Backup]*
[Backups]*

# Excel temp files
~$*.xlsx

# Google OAuth tokens
token.json
token.pickle
