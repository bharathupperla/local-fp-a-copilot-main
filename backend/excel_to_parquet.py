import pandas as pd
from pathlib import Path

# =========================
# 🔽 CHANGE ONLY THIS LINE
# =========================
EXCEL_FILE_PATH = r"C:\Users\SAHITHI\Downloads\REV_MAR.xlsx"
# =========================

def convert_excel_to_parquet(excel_path: str):
    excel_path = Path(excel_path)

    if not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_path}")

    # Read Excel
    df = pd.read_excel(excel_path)

    # 🔒 IMPORTANT FIX:
    # Force all object columns to string
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str)

    parquet_path = excel_path.with_suffix(".parquet")

    df.to_parquet(
        parquet_path,
        engine="pyarrow",
        index=False
    )

    print("✅ Conversion successful")
    print(f"Excel   : {excel_path}")
    print(f"Parquet : {parquet_path}")


if __name__ == "__main__":
    convert_excel_to_parquet(EXCEL_FILE_PATH)
