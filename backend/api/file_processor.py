"""
file_processor.py
Drop in project root (same folder as main.py).

Chains your exact existing pipeline:
  1. Excel bytes → data/finance.parquet
  2. schema/schema.json  (your generate_schema())
  3. schema/metadata.json (your generate_metadata())
"""

import io
import os
import pandas as pd
from pathlib import Path

from engine.schema_loader import generate_schema
from engine.metadata_generator import generate_metadata

DATA_PATH     = "data/finance.parquet"
SCHEMA_PATH   = "schema/schema.json"
METADATA_PATH = "schema/metadata.json"


def _smart_cast(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each object column:
      - Try to convert to numeric first (handles pay rates, bill rates, etc.)
      - Try to convert to datetime if column name suggests a date
      - Only cast to string if neither works
    This prevents numeric columns from being stored as strings in parquet.
    """
    date_keywords = ["date", "dt", "start", "end", "join", "expiry", "created"]

    for col in df.columns:
        if df[col].dtype != object:
            continue  # already typed correctly, skip

        series = df[col]

        # 1. Try numeric conversion
        converted = pd.to_numeric(series, errors="coerce")
        # If more than 80% of non-null values converted successfully → it's numeric
        non_null = series.notna().sum()
        converted_ok = converted.notna().sum()
        if non_null > 0 and (converted_ok / non_null) >= 0.8:
            df[col] = converted
            continue

        # 2. Try date conversion if column name looks like a date
        if any(kw in col.lower() for kw in date_keywords):
            try:
                df[col] = pd.to_datetime(series, errors="coerce")
                continue
            except Exception:
                pass

        # 3. Truly a string column — cast cleanly
        df[col] = series.astype(str)

    return df


def process_uploaded_file(excel_bytes: bytes, filename: str) -> dict:
    """
    Full pipeline from Excel bytes → parquet + schema + metadata.
    """
    ext = Path(filename).suffix.lower()

    # ── Step 1: Read Excel ──────────────────────────────────────────────
    try:
        # calamine is 3-5x faster than openpyxl (pip install python-calamine)
        df = pd.read_excel(io.BytesIO(excel_bytes), engine="calamine")
    except Exception:
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        df = pd.read_excel(io.BytesIO(excel_bytes), engine=engine)

    # Strip whitespace from column names
    df.columns = [str(c).strip() for c in df.columns]

    # Drop fully empty rows
    df = df.dropna(how="all").reset_index(drop=True)

    print(f"[UPLOAD] Excel read: {len(df)} rows × {len(df.columns)} cols")

    # ── Step 2: Smart type casting (NOT blind string cast) ──────────────
    df = _smart_cast(df)

    # ── Step 3: Save Parquet ────────────────────────────────────────────
    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
    df.to_parquet(DATA_PATH, engine="pyarrow", compression="snappy", index=False)
    print(f"[UPLOAD] Parquet saved → {DATA_PATH}")

    # ── Step 4: Schema ──────────────────────────────────────────────────
    schema = generate_schema(df, SCHEMA_PATH)
    print(f"[UPLOAD] Schema written → {SCHEMA_PATH}")

    # ── Step 5: Metadata ────────────────────────────────────────────────
    generate_metadata(df, METADATA_PATH)
    print(f"[UPLOAD] Metadata written → {METADATA_PATH}")

    return {
        "rows":       len(df),
        "columns":    len(df.columns),
        "measures":   schema.get("measures", []),
        "dimensions": schema.get("dimensions", []),
    }