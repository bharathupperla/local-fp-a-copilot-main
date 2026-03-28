import pandas as pd
from typing import Any, Dict


def match_filter(df: pd.DataFrame, column: str, value: Any, metadata: Dict) -> pd.Series | None:
    """
    Matches filter value using metadata sample values.
    """

    value_str = str(value).strip().lower()

    # Pull known values from metadata
    known_values = metadata["columns"][column]["sample_values"]

    # Build normalized candidates
    candidates = set()

    for v in known_values:
        v_str = str(v).strip()
        candidates.add(v_str)
        candidates.add(v_str.lower())

        # numeric normalization
        try:
            candidates.add(str(int(float(v_str))))
            candidates.add(str(int(float(v_str))).zfill(2))
        except Exception:
            pass

    # Find best match
    matched = None
    for c in candidates:
        if c.lower() == value_str:
            matched = c
            break

    if matched is None:
        return None

    return df[column].astype(str).str.strip().str.lower() == matched.lower()
