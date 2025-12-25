Personal Listening History — JSON to CSV pipeline

This repository contains a small pipeline to convert Spotify streaming-history JSON files (in `Source Data/`) into CSV files (in `Transformed Data/`).

What I added
- `ETL/pipeline.py` — main script. Uses only Python stdlib.

How to run (PowerShell)

# Run with defaults (looks for ../Source Data and writes ../Transformed Data)
python ETL\pipeline.py

# Run with explicit absolute paths and create combined CSV
python ETL\pipeline.py --source "C:\Users\tinas\OneDrive\Desktop\TM\Projects\Python\Personal Listening History\Source Data" --out "C:\Users\tinas\OneDrive\Desktop\TM\Projects\Python\Personal Listening History\Transformed Data" --combine

# Want more logging
python ETL\pipeline.py --verbose

Outputs
- One CSV per JSON file, named like `Streaming_History_Audio_2024_6.csv`, written to `Transformed Data/`.
- If `--combine` is passed, a combined file `streaming_history_all.csv` is created in `Transformed Data/`.

Notes & next steps
- The script attempts to preserve column order based on first-seen keys.
- Non-primitive JSON values (lists, dicts) are JSON-encoded into the CSV cells.
- If you want to add filtering, column selection, or use pandas for richer parsing, I can add that next.
