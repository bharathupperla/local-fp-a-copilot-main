import json
import os
import pandas as pd
from engine.canonical_resolver import resolve_to_canonical

def generate_schema(df: pd.DataFrame, output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    canonical_columns = {}
    measures = []
    dimensions = []

    for col in df.columns:
        canonical = resolve_to_canonical(col)

        # Skip columns that we don't understand
        if not canonical:
            continue

        canonical_columns[canonical] = col  # canonical → real column

        # Classify type
        if pd.api.types.is_numeric_dtype(df[col]):
            measures.append(canonical)
        else:
            dimensions.append(canonical)

    schema = {
        "measures": sorted(set(measures)),
        "dimensions": sorted(set(dimensions)),
        "all_columns": sorted(set(list(canonical_columns.keys()))),
        "column_map": canonical_columns  # 🔥 VERY IMPORTANT
    }

    with open(output_path, "w") as f:
        json.dump(schema, f, indent=2)

    return schema
