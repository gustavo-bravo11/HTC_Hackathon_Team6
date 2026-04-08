# Gas Processing Dashboard

This repository contains Team 6's hackathon work for building an employer-facing analytics project around Alberta gas processing performance and environmental releases. The goal was to answer a practical industry question: can we build a site-level view for Keyera that shows how efficiently each site extracts gas and NGL products, how C3+ performance changes across facilities, and how environmental releases can be mapped using government data?

The project combines:

- facility operations and production data,
- greenhouse gas emissions data,
- spatial matching from latitude/longitude to Dominion Land Survey (DLS) locations,
- cleaned output tables for analysis,
- an exported dashboard bundle in `assetbundle-AssetExportHTC.qs`.

The result is a small end-to-end data project that combines data engineering, spatial enrichment, and BI dashboarding into a single analysis workflow.

## Team

- Gustavo, Data Engineer
- Mehrnaz, Data Architect
- Dylan, Data Strategist
- Adan, BI Engineer

## Project Framing

This README is written for employers reviewing the project as an example of applied analytics and data engineering work. The repository shows how the team:

- took messy public and operational-style datasets,
- normalized them into analysis-ready tables,
- solved a location-key mismatch with geospatial processing,
- merged production and emissions data into a usable analytical model,
- delivered the result as a dashboardable asset focused on operational efficiency and environmental visibility.

## Repository Overview

```text
.
|-- data_clean/
|   |-- clean_and_split_facility.py
|   |-- merge_emissions_data_dump.py
|   |-- data_cleaned/
|   `-- notebooks/
|-- lat_long_to_lsd/
|   |-- ab_emissions.csv
|   |-- ab_emissions_with_lsd.csv
|   `-- map.ipynb
|-- pySpark/
|   `-- find_dls_with_spatial.py
|-- assetbundle-AssetExportHTC.qs
`-- requirements.txt
```

## What This Project Produces

The repository builds and stores analysis-ready datasets for facility performance and emissions analysis, including:

- facility master data,
- facility capacity data,
- total production data,
- monthly production data,
- yearly CO2e totals matched back to facility IDs,
- dashboard assets for the final presentation layer.

The exported dashboard metadata shows the final app focused on:

- operator and facility filtering,
- raw gas and sales gas utilization,
- product yield trends,
- facility-level CO2 emissions,
- Alberta geospatial facility views.

At a business level, the dashboard is intended to help answer:

- Which facilities are using their capacity most effectively?
- How do raw gas, sales gas, and NGL-related outputs vary over time by site?
- How does the C3+ picture differ across facilities?
- Where are the highest reported environmental releases located geographically?

## Data Pipeline

The end-to-end flow in this repo is:

1. Clean and normalize facility data in `data_clean/clean_and_split_facility.py`.
2. Convert facility coordinates and standardize operator/facility fields.
3. Split the facility source into separate tables for facilities, capacity, total production, and monthly production.
4. Enrich emissions records with DLS location values using the spatial matching workflow in `pySpark/find_dls_with_spatial.py`.
5. Merge enriched emissions data back onto the cleaned facility table using the DLS location key in `data_clean/merge_emissions_data_dump.py`.
6. Export final CSVs for analysis and dashboarding.

## How The PySpark Step Was Done

The `pySpark` folder contains the spatial matching job that links emissions records to Alberta land system identifiers.

### Goal

The emissions file contains `Latitude` and `Longitude`, while the facility data uses DLS-style location identifiers such as `15-36-036-16W4M`. To join these datasets reliably, the project first converts each emissions point into a DLS location.

### Implementation

The script `pySpark/find_dls_with_spatial.py` is written as an AWS Glue PySpark job and works as follows:

1. Start a Spark and Glue context.
2. Read `ab_emissions.csv` from S3.
3. Load Alberta township and LSD shapefiles with GeoPandas from the same S3 workspace.
4. Reproject both shapefiles to `EPSG:4326` so they match the latitude/longitude coordinate system in the emissions file.
5. Broadcast both polygon datasets to Spark executors so each worker can run spatial lookups without repeatedly reloading shapefiles.
6. Use a Spark UDF to:
   - build a Shapely point from each row's longitude and latitude,
   - find the containing township polygon,
   - narrow the LSD search to polygons intersecting that township,
   - find the LSD polygon that contains the point,
   - format the result as a DLS string such as `11-17-056-21W4M`.
7. Write the enriched emissions dataset back to S3 as `ab_emissions_with_lsd.csv`.

### Why This Matters

This step is what makes the emissions-to-facility join possible. Without the spatial conversion, the emissions data and facility data use different location systems and cannot be merged cleanly.

### Inputs And Outputs

- Input: `lat_long_to_lsd/ab_emissions.csv`
- Intermediate cloud input: township and LSD shapefiles stored in S3
- Output: `lat_long_to_lsd/ab_emissions_with_lsd.csv`

## Local Data Cleaning

### `data_clean/clean_and_split_facility.py`

This script:

- loads the raw facility extract,
- normalizes column names,
- removes incomplete rows,
- converts coordinate fields to decimal degrees,
- standardizes operator names,
- splits the raw extract into multiple focused tables,
- reshapes monthly production data,
- writes cleaned CSV outputs under `data_clean/data_cleaned/`.

Expected outputs include:

- `data_clean/data_cleaned/facilities/facilities.csv`
- `data_clean/data_cleaned/facility_capacity/facility_capacity.csv`
- `data_clean/data_cleaned/facility_total_production/facility_total_production.csv`
- `data_clean/data_cleaned/facility_monthly_production/facility_monthly_production.csv`

### `data_clean/merge_emissions_data_dump.py`

This script:

- loads the emissions dataset already enriched with DLS codes,
- cleans and standardizes the emissions column names,
- selects yearly emissions fields,
- joins emissions data to facility records using DLS,
- aggregates yearly CO2e totals by facility ID,
- writes the merged output to `data_clean/data_cleaned/yearly_total_emissions_co2/yearly_total_emissions_co2.csv`.

## Notebooks

The notebooks in `data_clean/notebooks/` and `lat_long_to_lsd/map.ipynb` appear to support exploration, validation, and intermediate analysis during the hackathon. They are useful for understanding assumptions and sanity-checking the cleaned outputs, but the repeatable pipeline lives in the Python scripts.

## Setup

### Prerequisites

- Python 3.10+
- `pip`
- access to the raw facility source file referenced by the scripts
- for the PySpark step: AWS Glue-compatible environment, S3 access, GeoPandas, and shapefiles

Install local Python dependencies:

```bash
pip install -r requirements.txt
```

### Running The Local Scripts

From the repository root:

```bash
cd data_clean
python clean_and_split_facility.py
python merge_emissions_data_dump.py
```

Note: `clean_and_split_facility.py` expects a raw source file at `../data_sets/Facility_Data Pull.csv`, which is not committed in this repository.

### Running The PySpark Job

The spatial matching script is designed for AWS Glue rather than direct local execution. At a high level, the run requires:

- an S3 bucket containing `ab_emissions.csv`,
- township and LSD shapefiles stored under the configured path,
- a Glue job with the required geospatial Python libraries available,
- the script `pySpark/find_dls_with_spatial.py` uploaded as the job source.

The script currently uses:

- bucket: `htc-hackathon-team-6`
- path prefix: `spatial_matching`

So the job reads from and writes to paths like:

- `s3://htc-hackathon-team-6/spatial_matching/ab_emissions.csv`
- `s3://htc-hackathon-team-6/spatial_matching/ab_emissions_with_lsd.csv`

## Dashboard Assets

The file `assetbundle-AssetExportHTC.qs` is an exported dashboard bundle. The embedded metadata indicates a final dashboard named `Keyera Dashboard` built around the cleaned facility dataset and derived metrics such as:

- raw gas utilization,
- sales gas utilization,
- C2/C3/C4/C5 product yield views,
- yearly CO2 emissions by facility,
- interactive facility/operator/date filters.

For an employer reviewing the project, this bundle is the presentation layer of the work: the upstream scripts create a dataset that can support operational efficiency analysis, product-yield comparisons, and mapped environmental reporting at the site level.

## Current Limitations

- The original raw facility source file is not included in the repo.
- The PySpark geospatial step depends on AWS Glue, S3, and shapefiles that are not stored directly in this repository.
- There is no single orchestration script that runs the full pipeline end to end.
- Some exploratory work lives in notebooks rather than fully parameterized scripts.

## Suggested README Additions

If you want this README to be even stronger for employer review, the next useful additions are:

- a short challenge statement,
- the business value or insight produced,
- team member names and roles,
- screenshots of the final dashboard,
- a short "results" section with the main findings.
