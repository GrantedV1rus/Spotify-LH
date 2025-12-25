import csv
from pathlib import Path

EXPECTED_COLUMNS = [
    "ts","platform","ms_played","conn_country","ip_addr",
    "master_metadata_track_name","master_metadata_album_artist_name",
    "master_metadata_album_name","spotify_track_uri",
    "episode_name","episode_show_name","spotify_episode_uri",
    "audiobook_title","audiobook_uri","audiobook_chapter_uri",
    "audiobook_chapter_title","reason_start","reason_end",
    "shuffle","skipped","offline","offline_timestamp",
    "incognito_mode","shuffled"
]

def check_headers(csv_path: Path) -> None:
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
    missing = [c for c in EXPECTED_COLUMNS if c not in headers]
    extra = [h for h in headers if h not in EXPECTED_COLUMNS]
    print(f"Headers found ({len(headers)}): {headers}")
    print(f"Missing ({len(missing)}): {missing}")
    print(f"Extra ({len(extra)}): {extra}")

if __name__ == "__main__":
    # Default path relative to repo structure
    csv_path = Path(__file__).parent.parent / "Transformed Data" / "combined_streaming_history.csv"
    check_headers(csv_path)
