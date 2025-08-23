## Overview
This repo contains analytics transformation scripts for TocaBoca, done through dbt. It also at the moment contains Databricks ingestion scripts for the raw data, which should live in another repo, but for ease of use for this case they were merged.

### Repo structure
```text
tocaboca/
├── ingestion/                     # Databricks ingestion scripts
│   ├── includes/                  # Shared configuration & infra setup
│   │   ├── configuration.py       # Global configs (paths, schemas, constants)
│   │   └── infra/                 # One-off mounting scripts
│   │       └── mount_files_to_abfs_setup.py
│   ├── ingest_events.py
│   ├── ingest_products.py
│   └── ingest_exchange_rates.py
├── dbt/                           # dbt project for transformations
│   ├── models/                   
│   │   ├── bronze/
│   │   │   └── _source.yml
│   │   ├── silver/
│   │   └── gold/
│   ├── seeds/
├── README.md
