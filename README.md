# Hotel Data Warehouse ETL Pipeline

## Project Overview

This project aims to create a basic analytical Data Warehouse (DWH) for a hotel, designed following Kimball's principles with support for data historization (SCD). It covers key stages of DWH development, including data modeling and database creation (documented separately), as well as the implementation of data generation, an automated ETL pipeline, and visualization. The pipeline generates synthetic hotel data (bookings and stays), extracts and transforms it into a structured DWH, automates the process via orchestration, and provides insights through visualized analytics.

## Project Structure

- `/dockercompose/` — Root directory for the project
  - `docker-compose.yaml` — Docker Compose configuration for running services
  - `dags/` — Directory containing Airflow DAG files
    - `generate_data_in_source_dag.py` — DAG for generating data in the source
    - `load_to_stage_dag.py` — DAG for loading data into staging
    - `load_to_dwh_dag.py` — DAG for loading data into DWH
    - `update_data_marts_dag.py` — DAG for updating data marts
    - `scripts/` — Directory with scripts inside dags
      - `generate_data_in_source.py` — Script for generating source data
      - `load_to_stage.py` — Script for loading data into staging
      - `load_to_dwh.py` — Script for loading data into DWH
      - `update_data_marts.py` — Script for updating data marts

### Component Descriptions

#### DAG Files (in `dags/`)
- **`generate_data_in_source_dag.py`**  
  Orchestrates the generation of synthetic data in the source. Executes `generate_data_in_source.py`.

- **`load_to_stage_dag.py`**  
  Manages the extraction of data from the source and loading it into the staging area. Calls `load_to_stage.py`.

- **`load_to_dwh_dag.py`**  
  Coordinates the transformation of data from staging and loading into the DWH with SCD2 support. Uses `load_to_dwh.py`.

- **`update_data_marts_dag.py`**  
  Updates data marts based on the loaded DWH data. Runs `update_data_marts.py`.

#### Scripts (in `dags/scripts/`)
- **`generate_data_in_source.py`**  
  Generates synthetic data and writes it to the source (PostgreSQL).

- **`load_to_stage.py`**  
  Extracts data from the source and loads it into staging tables in PostgreSQL staging-schema.

- **`load_to_dwh.py`**  
  Transforms data from staging (filtering, enrichment, SCD2 and SCD1 for dimensions) and loads it into DWH tables (facts and dimensions).

- **`update_data_marts.py`**  
  Aggregates data from the DWH and updates data marts for analytics (e.g., revenue by room type).

## Technologies

- **Data Storage**: PostgreSQL — used for storing source, staging, DWH, and data mart data.
- **Orchestration**: Apache Airflow — manages ETL process execution via DAG files.
- **Data Processing**: Python — primary language for scripts, with `psycopg2` for PostgreSQL interactions.
- **Visualization**: Apache Superset — provides data visualization and analytics dashboards.
- **Containerization**: Docker — runs services (Airflow, PostgreSQL, Superset) via `docker-compose.yaml`.