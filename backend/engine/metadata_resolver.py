import json
import pandas as pd
from typing import Any


def load_metadata(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def resolve_filter_value(
    df: pd.DataFrame,
    column: str,
    value: Any,
    metadata: dict
):
    """
    Uses metadata + real data to resolve filters safely.
    """

    series = df[column].astype(str).str.strip()
    raw = str(value).strip()

    candidates = set()
    candidates.add(raw)
    candidates.add(raw.lower())
    candidates.add(raw.upper())

    # numeric normalization
    try:
        num = int(float(raw))
        candidates.add(str(num))
        candidates.add(str(num).zfill(2))
    except Exception:
        pass

    # metadata-driven candidates
    col_meta = metadata.get("columns", {}).get(column)
    if col_meta:
        for v in col_meta.get("sample_values", []):
            candidates.add(str(v))

    for c in candidates:
        mask = series.str.lower() == str(c).lower()
        if mask.any():
            return mask

    return None
