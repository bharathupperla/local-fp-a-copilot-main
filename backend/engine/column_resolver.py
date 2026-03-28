from typing import Dict
import re


def normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.lower())


def build_column_map(df_columns) -> Dict[str, str]:
    """
    Maps normalized column names to actual dataframe columns
    """
    col_map = {}

    for col in df_columns:
        key = normalize(col)
        col_map[key] = col

    return col_map


def resolve_dataframe_column(canonical: str, column_map: Dict[str, str]) -> str | None:
    """
    Resolves canonical name to actual dataframe column name
    """
    norm = normalize(canonical)

    # Direct hit
    if norm in column_map:
        return column_map[norm]

    # Fuzzy containment
    for k, v in column_map.items():
        if norm in k or k in norm:
            return v

    return None
