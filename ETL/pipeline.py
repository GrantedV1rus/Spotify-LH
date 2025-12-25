"""Convert Spotify streaming-history JSON files to CSVs.

Usage examples:
	python ETL/pipeline.py --source "../Source Data" --out "../Transformed Data" --combine

This script:
 - Finds JSON files matching a pattern in the source directory.
 - Loads each file (supports both array JSON and line-delimited JSON objects).
 - Writes a single combined CSV with all records to the output directory.

This implementation uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List


def read_json_file(path: Path) -> List[Dict]:
	"""Read JSON file and return a list of records.

	Handles two common formats:
	  - a JSON array of objects
	  - newline-delimited JSON (one JSON object per line)
	"""
	text = path.read_text(encoding="utf-8")
	try:
		data = json.loads(text)
		if isinstance(data, list):
			return data
		# If it's a single object, return list
		if isinstance(data, dict):
			return [data]
	except json.JSONDecodeError:
		# Try line-delimited JSON
		records = []
		for line in text.splitlines():
			line = line.strip()
			if not line:
				continue
			try:
				obj = json.loads(line)
				records.append(obj)
			except json.JSONDecodeError:
				logging.warning("Skipping invalid JSON line in %s", path)
		return records
	# Fallback: if parsing produced something else, coerce to list
	return list(data)


def ensure_out_dir(path: Path) -> None:
	path.mkdir(parents=True, exist_ok=True)


def ordered_fieldnames(records: Iterable[Dict]) -> List[str]:
	"""Return fieldnames preserving first-seen order.

	This ensures columns are stable and readable.
	"""
	seen = OrderedDict()
	for rec in records:
		for k in rec.keys():
			if k not in seen:
				seen[k] = None
	return list(seen.keys())


def write_csv(path: Path, records: List[Dict]) -> None:
	if not records:
		logging.info("No records to write for %s", path)
		return
	# Determine columns in first-seen order
	fieldnames = ordered_fieldnames(records)
	# Fix known misnamed keys from source exports
	rename_map = {"master_metadata_album_album_name": "master_metadata_album_name"}
	for bad, good in rename_map.items():
		if bad in fieldnames:
			fieldnames = [good if fn == bad else fn for fn in fieldnames]
		if good not in fieldnames:
			fieldnames.append(good)
	# Ensure boolean fields are present so they always appear in CSV output
	boolean_keys = ["skipped", "shuffled", "incognito_mode", "offline"]
	for bk in boolean_keys:
		if bk not in fieldnames:
			fieldnames.append(bk)
	# Ensure parent directory exists
	path.parent.mkdir(parents=True, exist_ok=True)
	with path.open("w", encoding="utf-8", newline="") as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
		writer.writeheader()
		for rec in records:
			# Build row ensuring boolean conversion for certain keys
			row: Dict[str, object] = {}
			for k in fieldnames:
				# Support renamed keys
				source_key = k
				for bad, good in rename_map.items():
					if k == good and bad in rec:
						source_key = bad
						break
				v = rec.get(source_key)
				if k in boolean_keys:
					# If the source value is the string "true" (case-insensitive) -> 'True', else 'False'
					if isinstance(v, str):
						row_val = 'True' if v.lower() == 'true' else 'False'
					elif isinstance(v, bool):
						row_val = 'True' if v else 'False'
					elif v is None:
						row_val = 'False'
					else:
						# Fallback compare of string form
						row_val = 'True' if str(v).lower() == 'true' else 'False'
					row[k] = row_val
				else:
					# Convert non-primitive values to JSON strings for safety
					if isinstance(v, (dict, list)):
						row[k] = json.dumps(v, ensure_ascii=False)
					else:
						row[k] = v
			writer.writerow(row)


def convert_all(source_dir: Path, out_dir: Path, pattern: str = "Streaming_History*.json") -> Path:
	source_dir = source_dir.resolve()
	out_dir = out_dir.resolve()
	logging.info("Source: %s", source_dir)
	logging.info("Output: %s", out_dir)
	ensure_out_dir(out_dir)

	all_records: List[Dict] = []
	files = sorted(source_dir.glob(pattern))
	if not files:
		logging.warning("No files matching pattern %r in %s", pattern, source_dir)

	for f in files:
		logging.info("Processing %s", f.name)
		try:
			records = read_json_file(f)
		except Exception as e:
			logging.exception("Failed to read %s: %s", f, e)
			continue
		all_records.extend(records)

	combined_path = out_dir / "combined_streaming_history.csv"
	logging.info("Writing combined CSV (%d rows) to %s", len(all_records), combined_path.name)
	write_csv(combined_path, all_records)

	return combined_path


def parse_args() -> argparse.Namespace:
	p = argparse.ArgumentParser(description="Convert Spotify streaming-history JSON files into one combined CSV")
	p.add_argument("--source", "-s", type=Path, default=Path("../Source Data"), help="Source directory containing JSON files")
	p.add_argument("--out", "-o", type=Path, default=Path("../Transformed Data"), help="Output directory for CSV files")
	p.add_argument("--pattern", "-p", default="Streaming_History*.json", help="Glob pattern to match JSON files")
	p.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
	return p.parse_args()


def main() -> None:
	args = parse_args()
	logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s: %(message)s")
	combined = convert_all(args.source, args.out, pattern=args.pattern)
	logging.info("Done. Combined CSV path: %s", combined)


if __name__ == "__main__":
	main()
