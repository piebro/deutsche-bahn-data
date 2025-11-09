import json
import sys
from pathlib import Path

import pandas as pd


def get_eva_to_station_mapping(df: pd.DataFrame) -> dict[str, str]:
    station_data_df = df[df["api_name"] == "station-data/v2/stations"]
    station_data_df = station_data_df.drop_duplicates(subset=["query_params"])

    eva_to_station = {}
    for _, row in station_data_df.iterrows():
        data = json.loads(row["response_data"])
        for station in data.get("result", []):
            station_name = station.get("name")
            for eva_dict in station.get("evaNumbers", []):
                eva_number = f"0{eva_dict.get('number')}"  # Add leading 0
                eva_to_station[eva_number] = station_name
    return eva_to_station


def main(year: str, month: str):
    output_dir = Path("config")
    output_dir.mkdir(exist_ok=True)

    parquet_files = list(Path(f"raw_data/year={year}/month={month}/").rglob("*.parquet"))
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    df = df[df["status_code"] == "200"]

    eva_to_station = get_eva_to_station_mapping(df)

    output_path = output_dir / "eva_to_station_name.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(eva_to_station, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(eva_to_station)} EVA to station mappings to {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: uv run scripts/update_eva_to_station_name_json.py <year> <month>")
        sys.exit(1)

    year = sys.argv[1]
    month = sys.argv[2]
    main(year, month)
