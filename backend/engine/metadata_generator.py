import json
import pandas as pd
from pathlib import Path


def generate_metadata(df: pd.DataFrame, output_path: str):
    """
    Generates metadata.json from dataframe:
    - actual column names
    - normalized aliases
    - sample values per column
    """

    metadata = {
        "columns": {}
    }

    for col in df.columns:
        series = df[col].dropna()

        values = (
            series.astype(str)
            .str.strip()
            .unique()
            .tolist()
        )

        metadata["columns"][col] = {
            "normalized": normalize(col),
            "dtype": str(df[col].dtype),
            "sample_values": values[:100]  # cap for size
        }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    return metadata


def normalize(text: str) -> str:
    return "".join(c.lower() for c in text if c.isalnum())


df = pd.read_parquet("data/finance.parquet")
generate_metadata(df, "schema/metadata.json")

# Change this at the bottom of metadata_generator.py:
if __name__ == "__main__":
    df = pd.read_parquet("data/finance.parquet")
    generate_metadata(df, "schema/metadata.json")
