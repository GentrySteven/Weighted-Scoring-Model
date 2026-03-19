# Example configuration for a small archive using Excel
# Copy this file to config.yml in the project root and customize.

archivesspace:
  base_url: "http://localhost:8089"
  repository_id: 2

output:
  format: "excel"
  spreadsheet_name: "Accession Data and Scores"

excel:
  target_directory: "C:\\Users\\Archivist\\Documents\\accession-sync"

agents:
  donor_role: "source"

throttling:
  archivesspace: 0.25
  batch_mode: true

retry:
  max_retries: 3

logging:
  level: "standard"
  directory: "C:\\Users\\Archivist\\Documents\\accession-sync\\logs"
  consolidation_frequency: "monthly"

cache:
  directory: "C:\\Users\\Archivist\\Documents\\accession-sync\\cache"

preview:
  directory: "C:\\Users\\Archivist\\Documents\\accession-sync\\preview"

scheduling:
  frequency: "weekly"
  time: "18:00"

notifications:
  recipient_email: ""
