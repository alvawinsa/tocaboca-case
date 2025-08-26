## Overview
This repo contains analytics transformation scripts for TocaBoca, done through dbt. It also at the moment contains Databricks ingestion scripts for the raw data, which should live in another repo, but for ease of use for this case they were merged.

⚠️ Note: I sometimes mistakenly refer to this analytics project/tables as TocaBoca instead of TobaBoba 🙂

### Repo structure
```text
tocaboca/
├── ingestion/                     # Databricks ingestion scripts
│   └── includes/                  # Shared configuration & infra setup
├── dbt/                           # dbt project for transformations
│   ├── models/                   
│   │   ├── bronze/                # All bronze, raw source
│   │   ├── silver/                # All cleaned, models
│   │   └── gold/                  # All transformed, aggregated models in star schema
│   ├── macros/                    # No macros created but this is where they'd live
│   └── snapshots/                 # No snapshots right now but this is where they'd be
├── analytics_queries/             # Ad-hoc / analysis SQL queries for for a Databricks dashboard
├── README.md
```

### Ingestion
These are the scripts that first mount, then save the tables from Azure Data Storage to Delta format in Databricks. As mentioned above, wouldn't normally live in the same repo, but added it here for this case since that'd be easier. They are extremely bare bones and not scalable, but a basic script to set things up. For example, in a prod setting I'd make them incremental, setup error handling and control these jobs with IaC instead.

### dbt
This project follows the **Medallion Architecture**:  

- **Bronze** → Raw, untransformed data (landing zone)  
- **Silver** → Cleaned, standardized data. Naming conventions, data quality tests. No business logic  
- **Gold** → Transformed, aggregated models structured as a **star schema**  

In a production environment, I'd also add a schema for presentation/mart layer where there'd be more wide tables for BI tools to consume or for ad hoc analytics, but for lack of time I added the queries behind my Databricks dashboard into `analytics_queries/`.
