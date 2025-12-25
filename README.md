Personal Listening History — ETL → SQL Database → Dashboard

This project converts exported Spotify streaming history JSON files into a clean, combined CSV, loads that data into a SQL database (PostgreSQL), and visualizes it with a Power BI dashboard.

**Overview**
- **Source**: JSON files in [Source Data/](Source%20Data/)
- **ETL**: Combine and normalize into [Transformed Data/combined_streaming_history.csv](Transformed%20Data/combined_streaming_history.csv) via [ETL/pipeline.py](ETL/pipeline.py)
- **Database**: Load CSV into staging and final tables using [Queries/Database Upload.sql](Queries/Database%20Upload.sql) (PostgreSQL)
- **Dashboard**: Open and refresh [Dashboard/Dashboard.pbix](Dashboard/Dashboard.pbix) in Power BI Desktop

**Prerequisites**
- **Windows** with PowerShell
- **Python 3.10+** (standard library only; no extra packages required)
- **PostgreSQL 13+** (with `psql`) and access to a database where you can create schema/tables
- **Power BI Desktop** (to open `.pbix`)

**Folder Layout**
- [ETL/](ETL/) — ETL scripts: [pipeline.py](ETL/pipeline.py), [validate_csv_headers.py](ETL/validate_csv_headers.py)
- [Queries/](Queries/) — SQL scripts to stage and cast data into final table
- [Source Data/](Source%20Data/) — Raw Spotify JSON exports
- [Transformed Data/](Transformed%20Data/) — ETL outputs (combined CSV)
- [Dashboard/](Dashboard/) — Power BI report ([Dashboard.pbix](Dashboard/Dashboard.pbix))

**ETL: JSON → Combined CSV**
- What it does:
	- Finds files by pattern (default `Streaming_History*.json`) in the source folder
	- Reads array JSON and line-delimited JSON
	- Normalizes booleans (`shuffle`, `skipped`, `offline`, `incognito_mode`, `shuffled`) to `True`/`False` strings
	- Fixes a known album key (`master_metadata_album_album_name` → `master_metadata_album_name`)
	- Preserves first-seen field order across files
- Output: [Transformed Data/combined_streaming_history.csv](Transformed%20Data/combined_streaming_history.csv)

Run from the repo root (PowerShell):

```powershell
# Use defaults (looks for ../Source Data, writes ../Transformed Data)
python ETL\pipeline.py

# Explicit paths within this repo
python ETL\pipeline.py --source ".\Source Data" --out ".\Transformed Data" --verbose

# If your files differ from the default pattern
python ETL\pipeline.py --pattern "Streaming_History_Audio_*.json"
```

Optional: verify the CSV headers match expectations:

```powershell
python ETL\validate_csv_headers.py
```

This checks [Transformed Data/combined_streaming_history.csv](Transformed%20Data/combined_streaming_history.csv) and reports missing/extra columns.

**SQL Database Upload (PostgreSQL)**

The script in [Queries/Database Upload.sql](Queries/Database%20Upload.sql) performs:
- Create a staging table (`personal_spotify.listening_history`) with text columns
- `COPY` the CSV into staging
- Create a final table (`personal_spotify.complete_history`) with typed columns
- Insert from staging into final with proper casts and boolean normalization

Before running:
- Ensure the schema exists (once per database):

```sql
CREATE SCHEMA IF NOT EXISTS personal_spotify;
```

- Update the `COPY ... FROM` path in the SQL to point to the actual CSV on your machine, e.g.:

```sql
COPY personal_spotify.listening_history(...) 
FROM 'C:/Users/tinas/Downloads/temp/Transformed Data/combined_streaming_history.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');
```

Run the SQL with `psql` (replace host, user, db):

```powershell
psql -h localhost -U postgres -d your_database -f "Queries/Database Upload.sql"
```

If `psql` prompts for a password, supply your PostgreSQL user’s password. If you see permissions issues on `COPY`, you may need to use a path PostgreSQL can access or run `psql` locally on the server.

**Dashboard (Power BI Desktop)**
- Open [Dashboard/Dashboard.pbix](Dashboard/Dashboard.pbix) in Power BI Desktop
- If the report uses a database connection:
	- Set the connection to your PostgreSQL instance and database
	- Map to `personal_spotify.complete_history`
	- Apply credentials and refresh
- If the report uses the CSV:
	- Update the data source to [Transformed Data/combined_streaming_history.csv](Transformed%20Data/combined_streaming_history.csv)
	- Refresh to load new data

Power BI may prompt to install/update the PostgreSQL connector (Npgsql). Follow the prompt if required.

**Quickstart**
- Generate combined CSV:
	- `python ETL\pipeline.py --source ".\Source Data" --out ".\Transformed Data"`
- Validate CSV headers:
	- `python ETL\validate_csv_headers.py`
- Load into PostgreSQL:
	- Edit the `COPY` path in [Queries/Database Upload.sql](Queries/Database%20Upload.sql)
	- `psql -h localhost -U postgres -d your_database -f "Queries/Database Upload.sql"`
- Refresh the dashboard:
	- Open [Dashboard/Dashboard.pbix](Dashboard/Dashboard.pbix) and refresh

**Troubleshooting**
- No files found: Check the `--source` path and `--pattern`
- COPY failed (permissions/path): Use an absolute Windows path accessible to PostgreSQL; consider running `psql` on the same machine as the CSV
- Schema or table missing: Run `CREATE SCHEMA IF NOT EXISTS personal_spotify;` and re-run the SQL
- Boolean values look wrong: The ETL writes `True`/`False` strings; the final insert casts them via `CASE WHEN trim(...) = 'True' THEN True ELSE False END`
- Power BI connector prompt: Install/enable the PostgreSQL (Npgsql) connector when prompted

**Notes**
- ETL uses only the Python standard library; no external dependencies
- The final SQL normalizes types (timestamps, inet, booleans) suitable for analytics
- Further normalization (e.g., Snowflake schema) can be added in new SQL scripts
