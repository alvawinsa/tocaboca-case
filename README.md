## Overview
This repo contains analytics transformation scripts for TocaBoca, done through dbt. It also at the moment contains Databricks ingestion scripts for the raw data, which should live in another repo, but for ease of use for this case they were merged. I sometimes accidentally refer to this analytics project or the tables as "TocaBoca" instead of "TobaBoba" :) apologies.

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

### Ingestion
These are the scripts that first mount, then save the tables from Azure Data Storage to Delta format in Databricks. As mentioned above, wouldn't normally live in the same repo, but added it here for this case since that'd be easier. They are extremely bare bones and not scalable, but a basic script to set things up

### dbt
I follow a "medallion architecture" here in the modelling structure. Essentially what that is is simply that bronze contains all raw, not transformed. It's a landing zone. In silver we clean the data, do basic standardisation, naming conventions and test the data for certain expectations. No business logic happens here. And finally in gold, all models are transformed, aggregated and in my case conformed to a star schema. Normally, in a production setting, I'd also add a presentation/mart folder with more wide tables that can be consumed by a BI tool or used for ad hoc analytics or querying. But I didn't have time for that here. Instead in analytics_queries, you can find the queries behind the bare bones Databricks Dashboard I created for the KPIs. 
