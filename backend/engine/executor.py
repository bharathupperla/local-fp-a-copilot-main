import pandas as pd
import numpy as np
import re
from typing import Dict, Any, List
import hashlib
import time
from engine.column_resolver import build_column_map, resolve_dataframe_column
from engine.metadata_resolver import load_metadata


# -------------------------------------------------
# CACHE CONFIGURATION
# -------------------------------------------------
CACHE_TTL_SECONDS = 300
_company_profile_cache: Dict[str, Dict[str, Any]] = {}
_cache_timestamps: Dict[str, float] = {}


def _get_cache_key(customer_name: str) -> str:
    return hashlib.md5(customer_name.lower().strip().encode()).hexdigest()


def _is_cache_valid(cache_key: str) -> bool:
    if cache_key not in _cache_timestamps:
        return False
    return (time.time() - _cache_timestamps[cache_key]) < CACHE_TTL_SECONDS


def _get_cached_profile(customer_name: str) -> Dict[str, Any] | None:
    cache_key = _get_cache_key(customer_name)
    if cache_key in _company_profile_cache and _is_cache_valid(cache_key):
        print(f"[CACHE HIT] Company profile for: {customer_name}")
        return _company_profile_cache[cache_key]
    return None


def _set_cached_profile(customer_name: str, profile: Dict[str, Any]):
    cache_key = _get_cache_key(customer_name)
    _company_profile_cache[cache_key] = profile
    _cache_timestamps[cache_key] = time.time()
    print(f"[CACHE SET] Company profile for: {customer_name}")


def clear_cache():
    global _company_profile_cache, _cache_timestamps
    _company_profile_cache = {}
    _cache_timestamps = {}
    print("[CACHE CLEARED]")


def clear_customer_cache(customer_name: str):
    cache_key = _get_cache_key(customer_name)
    if cache_key in _company_profile_cache:
        del _company_profile_cache[cache_key]
    if cache_key in _cache_timestamps:
        del _cache_timestamps[cache_key]
    print(f"[CACHE CLEARED] Customer: {customer_name}")


# -------------------------------------------------
# SMART FILTER MATCHING
# Handles: "Baxter (PR)", "BeiGene-PR", "Goldman Sachs, Inc"
# Also handles cycle, quarter, week, worker name
# -------------------------------------------------
def normalize_filter_value(column: str, value: Any, df: pd.DataFrame, metadata: dict):
    if value is None:
        return None

    series = df[column].astype(str).str.strip()
    series_lower = series.str.lower()
    raw_value = str(value).strip()
    raw_lower = raw_value.lower()

    # -------------------------------------------------
    # 1. EXACT MATCH (case-insensitive)
    # -------------------------------------------------
    exact_mask = series_lower == raw_lower
    if exact_mask.any():
        return exact_mask

    # -------------------------------------------------
    # 1b. STARTSWITH MATCH — handles "Q2 2024", "Q2-FY25", "Cycle 02 Jan" etc.
    # Use word boundary to prevent "Week 27" matching "Week 270", "Week 271" etc.
    # -------------------------------------------------
    starts_mask = series_lower.str.match(r"^" + re.escape(raw_lower) + r"(?:[^0-9]|$)")
    if starts_mask.any():
        return starts_mask

    # -------------------------------------------------
    # 2. CONTAINS MATCH — handles partial company names
    # e.g. "baxter pr" matches "Baxter (PR)"
    # Only return if all matched rows belong to same unique value
    # -------------------------------------------------
    contains_mask = series_lower.str.contains(re.escape(raw_lower), regex=True, na=False)
    if contains_mask.any():
        matched_unique = series_lower[contains_mask].unique()
        if len(matched_unique) == 1:
            return contains_mask
        # Multiple matches — try to find exact company by checking if raw is contained in value
        for val in matched_unique:
            if raw_lower in val:
                single_mask = series_lower == val
                if single_mask.any():
                    return single_mask

    # -------------------------------------------------
    # 3. REVERSE CONTAINS — value contains the series entry
    # e.g. "BeiGene-PR" searched as "beigene" matches "BeiGene-PR"
    # -------------------------------------------------
    for idx, val in series_lower.items():
        if val and (val in raw_lower or raw_lower in val):
            reverse_mask = series_lower == val
            if reverse_mask.any():
                return reverse_mask
            break

    # -------------------------------------------------
    # 4. FUZZY — strip special chars and compare
    # e.g. "BeiGene PR" matches "BeiGene-PR"
    # -------------------------------------------------
    def _strip_special(s):
        return re.sub(r"[^a-z0-9\s]", " ", s.lower()).strip()

    raw_stripped = _strip_special(raw_lower)
    series_stripped = series_lower.apply(_strip_special)

    fuzzy_mask = series_stripped == raw_stripped
    if fuzzy_mask.any():
        return fuzzy_mask

    # Partial fuzzy — but only return if it matches a SINGLE unique value
    # to avoid accidentally aggregating across multiple companies
    fuzzy_contains = series_stripped.str.contains(re.escape(raw_stripped), regex=True, na=False)
    if fuzzy_contains.any():
        # Check how many unique original values matched
        matched_unique = series_lower[fuzzy_contains].unique()
        if len(matched_unique) == 1:
            # All matched rows belong to the same entity — safe to return
            return fuzzy_contains
        else:
            # Multiple entities matched — too ambiguous, try stricter approach
            # Pick the one whose stripped name most closely matches
            for orig in matched_unique:
                if _strip_special(orig) == raw_stripped:
                    return series_lower == orig

    # -------------------------------------------------
    # 5. Cycle / Week normalization
    # -------------------------------------------------
    try:
        num = int(raw_lower.replace("cycle", "").replace("week", "").strip())
        candidates = [
            f"cycle {num:02d}",
            f"cycle {num}",
            f"week {num:02d}",
            f"week {num}",
            f"q{num}",
        ]
        for c in candidates:
            mask = series_lower == c
            if mask.any():
                return mask
        # Startswith match — handles "Q2 2024", "Q2-FY25", "Cycle 02 - Jan" etc.
        for c in candidates:
            mask = series_lower.str.startswith(c)
            if mask.any():
                return mask
    except Exception:
        pass

    # Quarter startswith — "q2" matches "Q2 2024", "Q2-FY25" etc.
    # Use word boundary to prevent "q2" matching "q20", "q21" etc.
    if len(raw_lower) <= 3 and raw_lower.startswith("q"):
        # Exact match first
        mask = series_lower == raw_lower
        if mask.any():
            return mask
        # Then startswith with non-digit after — "q2 " or "q2-" but NOT "q20"
        mask = series_lower.str.match(r"^" + raw_lower + r"(?:[^0-9]|$)")
        if mask.any():
            return mask

    # Week exact boundary match — "week 27" should NOT match "week 270"
    if "week" in raw_lower:
        mask = series_lower == raw_lower
        if mask.any():
            return mask
        mask = series_lower.str.match(r"^" + re.escape(raw_lower) + r"(?:[^0-9]|$)")
        if mask.any():
            return mask

    # -------------------------------------------------
    # 6. Metadata-based matching
    # -------------------------------------------------
    col_meta = metadata.get("columns", {}).get(column)
    if col_meta:
        for sample in col_meta.get("sample_values", []):
            sample_str = str(sample).lower().strip()
            if raw_lower in sample_str or sample_str in raw_lower:
                mask = series_lower == sample_str
                if mask.any():
                    return mask

    return None


import re


# -------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------
def safe_sum(df: pd.DataFrame, column: str) -> float:
    if column in df.columns:
        return round(float(df[column].sum()), 2)
    return 0.0


def safe_mean(df: pd.DataFrame, column: str) -> float:
    if column in df.columns:
        val = df[column].mean()
        if pd.isna(val):
            return 0.0
        return round(float(val), 2)
    return 0.0


def safe_min(df: pd.DataFrame, column: str) -> float:
    if column in df.columns:
        val = df[column].min()
        if pd.isna(val):
            return 0.0
        return round(float(val), 2)
    return 0.0


def safe_max(df: pd.DataFrame, column: str) -> float:
    if column in df.columns:
        val = df[column].max()
        if pd.isna(val):
            return 0.0
        return round(float(val), 2)
    return 0.0


def safe_median(df: pd.DataFrame, column: str) -> float:
    if column in df.columns:
        val = df[column].median()
        if pd.isna(val):
            return 0.0
        return round(float(val), 2)
    return 0.0


def safe_std(df: pd.DataFrame, column: str) -> float:
    if column in df.columns:
        val = df[column].std()
        if pd.isna(val):
            return 0.0
        return round(float(val), 2)
    return 0.0


def safe_nunique(df: pd.DataFrame, column: str) -> int:
    if column in df.columns:
        return int(df[column].nunique())
    return 0


def safe_unique_list(df: pd.DataFrame, column: str, limit: int = 20) -> List[str]:
    if column in df.columns:
        return sorted([str(x) for x in df[column].dropna().unique().tolist()])[:limit]
    return []


def safe_value_counts(df: pd.DataFrame, column: str, limit: int = 10) -> List[Dict]:
    if column in df.columns:
        vc = df[column].value_counts().head(limit)
        return [{"value": str(k), "count": int(v)} for k, v in vc.items()]
    return []


# -------------------------------------------------
# COMPANY FULL PROFILE BUILDER
# -------------------------------------------------
def build_company_profile(df: pd.DataFrame, customer_name: str) -> Dict[str, Any]:
    cached = _get_cached_profile(customer_name)
    if cached:
        return cached

    # Smart match — try exact first, then fuzzy
    mask = df["Customer Name"].str.lower().str.strip() == customer_name.lower().strip()
    if not mask.any():
        # Try contains
        mask = df["Customer Name"].str.lower().str.strip().str.contains(
            re.escape(customer_name.lower().strip()), regex=True
        )
    if not mask.any():
        # Try stripping special chars
        def _s(s): return re.sub(r"[^a-z0-9\s]", " ", str(s).lower()).strip()
        norm_target = _s(customer_name)
        mask = df["Customer Name"].apply(_s) == norm_target

    df2 = df[mask].copy()

    if df2.empty:
        raise ValueError(f"No data found for customer: {customer_name}")

    # Use the actual name from data
    actual_name = df2["Customer Name"].iloc[0]

    result = {
        "company": actual_name,
        "total_records": int(len(df2)),
        "data_quality": {
            "total_rows": int(len(df2)),
            "columns_available": len(df2.columns),
            "null_percentage": round(float(df2.isnull().sum().sum() / (len(df2) * len(df2.columns)) * 100), 2)
        }
    }

    result["financial_summary"] = {
        "revenue": {
            "total": safe_sum(df2, "Revenue"),
            "average": safe_mean(df2, "Revenue"),
            "median": safe_median(df2, "Revenue"),
            "min": safe_min(df2, "Revenue"),
            "max": safe_max(df2, "Revenue"),
            "std_dev": safe_std(df2, "Revenue")
        },
        "gross_margin_dollars": {
            "total": safe_sum(df2, "GM$"),
            "average": safe_mean(df2, "GM$"),
            "median": safe_median(df2, "GM$"),
            "min": safe_min(df2, "GM$"),
            "max": safe_max(df2, "GM$")
        },
        "gross_margin_percentage": {
            # Correct formula: Total GM$ / Total Revenue * 100 (weighted average)
            "average": round(safe_sum(df2, "GM$") / max(safe_sum(df2, "Revenue"), 1) * 100, 2),
            "median": round(safe_median(df2, "GM%") * 100, 2),
            "min": round(safe_min(df2, "GM%") * 100, 2),
            "max": round(safe_max(df2, "GM%") * 100, 2),
            "std_dev": round(safe_std(df2, "GM%") * 100, 2)
        },
        "costs": {
            "total_base_cost": safe_sum(df2, "Base Cost"),
            "total_loaded_cost": safe_sum(df2, "Loaded Cost"),
            "total_load_factor": safe_sum(df2, "Load Factor"),
            "avg_load_factor_pct": round(safe_mean(df2, "Load Factor %") * 100, 2)
        },
        "net_revenue": {
            "total": safe_sum(df2, "Net Revenue"),
            "average": safe_mean(df2, "Net Revenue")
        },
        "vms": {
            "total_vms_fees": safe_sum(df2, "VMS Fees"),
            "avg_vms_pct": round(safe_mean(df2, "VMS%") * 100, 2)
        },
        "markup": {
            "avg_markup_pct": round(safe_mean(df2, "Markup %") * 100, 2),
            "min_markup_pct": round(safe_min(df2, "Markup %") * 100, 2),
            "max_markup_pct": round(safe_max(df2, "Markup %") * 100, 2)
        }
    }

    result["hours_analysis"] = {
        "total_hours": {
            "total": safe_sum(df2, "Total Hours"),
            "average_per_record": safe_mean(df2, "Total Hours"),
            "median": safe_median(df2, "Total Hours"),
            "min": safe_min(df2, "Total Hours"),
            "max": safe_max(df2, "Total Hours")
        },
        "regular_hours": {
            "total": safe_sum(df2, "Reg Hours"),
            "average": safe_mean(df2, "Reg Hours"),
            "percentage_of_total": round(safe_sum(df2, "Reg Hours") / max(safe_sum(df2, "Total Hours"), 1) * 100, 2)
        },
        "overtime_hours": {
            "total": safe_sum(df2, "OT Hours"),
            "average": safe_mean(df2, "OT Hours"),
            "percentage_of_total": round(safe_sum(df2, "OT Hours") / max(safe_sum(df2, "Total Hours"), 1) * 100, 2)
        },
        "double_time_hours": {
            "total": safe_sum(df2, "DT Hours"),
            "average": safe_mean(df2, "DT Hours"),
            "percentage_of_total": round(safe_sum(df2, "DT Hours") / max(safe_sum(df2, "Total Hours"), 1) * 100, 2)
        },
        "gm_per_hour": {
            "average": safe_mean(df2, "GM($) / HR"),
            "median": safe_median(df2, "GM($) / HR"),
            "min": safe_min(df2, "GM($) / HR"),
            "max": safe_max(df2, "GM($) / HR")
        }
    }

    result["rate_analysis"] = {
        "bill_rates": {
            "regular": {
                "average": safe_mean(df2, "Bill Rate Reg"),
                "median": safe_median(df2, "Bill Rate Reg"),
                "min": safe_min(df2, "Bill Rate Reg"),
                "max": safe_max(df2, "Bill Rate Reg")
            },
            "overtime": {
                "average": safe_mean(df2, "OT Bill Rate"),
                "min": safe_min(df2, "OT Bill Rate"),
                "max": safe_max(df2, "OT Bill Rate")
            },
            "double_time": {
                "average": safe_mean(df2, "DT Bill Rate"),
                "min": safe_min(df2, "DT Bill Rate"),
                "max": safe_max(df2, "DT Bill Rate")
            }
        },
        "pay_rates": {
            "regular": {
                "average": safe_mean(df2, "Pay Rate Reg"),
                "median": safe_median(df2, "Pay Rate Reg"),
                "min": safe_min(df2, "Pay Rate Reg"),
                "max": safe_max(df2, "Pay Rate Reg")
            },
            "overtime": {
                "average": safe_mean(df2, "Pay Rate OT"),
                "min": safe_min(df2, "Pay Rate OT"),
                "max": safe_max(df2, "Pay Rate OT")
            },
            "double_time": {
                "average": safe_mean(df2, "Pay Rate DT"),
                "min": safe_min(df2, "Pay Rate DT"),
                "max": safe_max(df2, "Pay Rate DT")
            }
        },
        "spread": {
            "average": round(safe_mean(df2, "Bill Rate Reg") - safe_mean(df2, "Pay Rate Reg"), 2),
            "min": round(safe_min(df2, "Bill Rate Reg") - safe_max(df2, "Pay Rate Reg"), 2),
            "max": round(safe_max(df2, "Bill Rate Reg") - safe_min(df2, "Pay Rate Reg"), 2)
        }
    }

    try:
        if "Cycle " in df2.columns:
            cycle_agg = df2.groupby("Cycle ").agg({
                "Revenue": ["sum", "mean", "count"],
                "GM$": ["sum", "mean"],
                "GM%": "mean",
                "Total Hours": "sum",
                "Worker Name": "nunique"
            }).round(2)
            cycle_agg.columns = [
                "revenue_total", "revenue_avg", "record_count",
                "gm_dollars_total", "gm_dollars_avg",
                "gm_pct_avg", "total_hours", "worker_count"
            ]
            cycle_agg = cycle_agg.reset_index()
            cycle_agg["gm_pct_avg"] = (cycle_agg["gm_pct_avg"] * 100).round(2)
            cycle_agg = cycle_agg.rename(columns={"Cycle ": "cycle"})
            cycle_agg = cycle_agg.sort_values("cycle")
            result["cycle_breakdown"] = cycle_agg.to_dict(orient="records")

            if len(cycle_agg) > 1:
                result["cycle_trends"] = {
                    "best_revenue_cycle": cycle_agg.loc[cycle_agg["revenue_total"].idxmax(), "cycle"],
                    "best_revenue_amount": float(cycle_agg["revenue_total"].max()),
                    "worst_revenue_cycle": cycle_agg.loc[cycle_agg["revenue_total"].idxmin(), "cycle"],
                    "worst_revenue_amount": float(cycle_agg["revenue_total"].min()),
                    "best_margin_cycle": cycle_agg.loc[cycle_agg["gm_pct_avg"].idxmax(), "cycle"],
                    "best_margin_pct": float(cycle_agg["gm_pct_avg"].max()),
                    "worst_margin_cycle": cycle_agg.loc[cycle_agg["gm_pct_avg"].idxmin(), "cycle"],
                    "worst_margin_pct": float(cycle_agg["gm_pct_avg"].min()),
                    "revenue_growth": round(
                        (cycle_agg["revenue_total"].iloc[-1] - cycle_agg["revenue_total"].iloc[0])
                        / max(cycle_agg["revenue_total"].iloc[0], 1) * 100, 2
                    )
                }
    except Exception as e:
        print(f"[WARN] Cycle breakdown failed: {e}")
        result["cycle_breakdown"] = []

    try:
        if "Quarter" in df2.columns:
            quarter_agg = df2.groupby("Quarter").agg({
                "Revenue": ["sum", "mean"],
                "GM$": "sum",
                "GM%": "mean",
                "Total Hours": "sum",
                "Worker Name": "nunique"
            }).round(2)
            quarter_agg.columns = [
                "revenue_total", "revenue_avg", "gm_dollars_total",
                "gm_pct_avg", "total_hours", "worker_count"
            ]
            quarter_agg = quarter_agg.reset_index()
            quarter_agg["gm_pct_avg"] = (quarter_agg["gm_pct_avg"] * 100).round(2)
            quarter_agg = quarter_agg.rename(columns={"Quarter": "quarter"})
            result["quarter_breakdown"] = quarter_agg.to_dict(orient="records")
    except Exception as e:
        print(f"[WARN] Quarter breakdown failed: {e}")
        result["quarter_breakdown"] = []

    try:
        if "Location Code" in df2.columns:
            loc_agg = df2.groupby("Location Code").agg({
                "Revenue": "sum",
                "GM$": "sum",
                "GM%": "mean",
                "Total Hours": "sum",
                "Worker Name": "nunique",
                "File #": "nunique"
            }).round(2)
            loc_agg.columns = ["revenue", "gm_dollars", "gm_pct_avg", "total_hours", "worker_count", "file_count"]
            loc_agg = loc_agg.reset_index()
            loc_agg["gm_pct_avg"] = (loc_agg["gm_pct_avg"] * 100).round(2)
            loc_agg = loc_agg.rename(columns={"Location Code": "location"})
            loc_agg = loc_agg.sort_values("revenue", ascending=False)
            result["location_breakdown"] = loc_agg.to_dict(orient="records")
            result["location_summary"] = {
                "total_locations": len(loc_agg),
                "top_location_by_revenue": loc_agg.iloc[0]["location"] if len(loc_agg) > 0 else None,
                "top_location_revenue": float(loc_agg.iloc[0]["revenue"]) if len(loc_agg) > 0 else 0
            }
    except Exception as e:
        print(f"[WARN] Location breakdown failed: {e}")
        result["location_breakdown"] = []

    try:
        if "Worker Name" in df2.columns:
            worker_agg = df2.groupby("Worker Name").agg({
                "Revenue": "sum",
                "GM$": "sum",
                "GM%": "mean",
                "Total Hours": "sum"
            }).round(2)
            worker_agg.columns = ["revenue", "gm_dollars", "gm_pct_avg", "total_hours"]
            worker_agg = worker_agg.reset_index()
            worker_agg["gm_pct_avg"] = (worker_agg["gm_pct_avg"] * 100).round(2)
            worker_agg = worker_agg.rename(columns={"Worker Name": "worker_name"})
            worker_agg = worker_agg.sort_values("revenue", ascending=False)
            result["worker_analysis"] = {
                "total_workers": len(worker_agg),
                "top_workers_by_revenue": worker_agg.head(10).to_dict(orient="records"),
                "bottom_workers_by_revenue": worker_agg.tail(5).to_dict(orient="records"),
                "top_workers_by_margin": worker_agg.nlargest(10, "gm_pct_avg").to_dict(orient="records"),
                "avg_revenue_per_worker": round(float(worker_agg["revenue"].mean()), 2),
                "avg_hours_per_worker": round(float(worker_agg["total_hours"].mean()), 2)
            }
    except Exception as e:
        print(f"[WARN] Worker analysis failed: {e}")
        result["worker_analysis"] = {"total_workers": 0}

    try:
        if "Job Category" in df2.columns:
            job_agg = df2.groupby("Job Category").agg({
                "Revenue": "sum", "GM$": "sum", "GM%": "mean",
                "Total Hours": "sum", "Worker Name": "nunique"
            }).round(2)
            job_agg.columns = ["revenue", "gm_dollars", "gm_pct_avg", "total_hours", "worker_count"]
            job_agg = job_agg.reset_index()
            job_agg["gm_pct_avg"] = (job_agg["gm_pct_avg"] * 100).round(2)
            job_agg = job_agg.rename(columns={"Job Category": "category"})
            job_agg = job_agg.sort_values("revenue", ascending=False)
            result["job_category_breakdown"] = job_agg.to_dict(orient="records")
    except Exception as e:
        print(f"[WARN] Job category breakdown failed: {e}")
        result["job_category_breakdown"] = []

    try:
        if "Work Type" in df2.columns:
            wt_agg = df2.groupby("Work Type").agg({
                "Revenue": "sum", "GM%": "mean", "Worker Name": "nunique"
            }).round(2)
            wt_agg.columns = ["revenue", "gm_pct_avg", "worker_count"]
            wt_agg = wt_agg.reset_index()
            wt_agg["gm_pct_avg"] = (wt_agg["gm_pct_avg"] * 100).round(2)
            wt_agg = wt_agg.rename(columns={"Work Type": "work_type"})
            wt_agg = wt_agg.sort_values("revenue", ascending=False)
            result["work_type_breakdown"] = wt_agg.to_dict(orient="records")
    except Exception as e:
        print(f"[WARN] Work type breakdown failed: {e}")
        result["work_type_breakdown"] = []

    try:
        if "Recruiter" in df2.columns:
            rec_agg = df2.groupby("Recruiter").agg({
                "Revenue": "sum", "GM$": "sum", "Worker Name": "nunique"
            }).round(2)
            rec_agg.columns = ["revenue", "gm_dollars", "worker_count"]
            rec_agg = rec_agg.reset_index()
            rec_agg = rec_agg.rename(columns={"Recruiter": "recruiter"})
            rec_agg = rec_agg.sort_values("revenue", ascending=False)
            result["recruiter_analysis"] = {
                "total_recruiters": len(rec_agg),
                "top_recruiters": rec_agg.head(15).to_dict(orient="records")
            }
    except Exception as e:
        print(f"[WARN] Recruiter analysis failed: {e}")
        result["recruiter_analysis"] = {"total_recruiters": 0}

    try:
        result["date_analysis"] = {
            "join_dates": {
                "earliest": str(df2["Join Date"].min()) if "Join Date" in df2.columns and not pd.isna(df2["Join Date"].min()) else None,
                "latest": str(df2["Join Date"].max()) if "Join Date" in df2.columns and not pd.isna(df2["Join Date"].max()) else None
            },
            "end_dates": {
                "earliest": str(df2["End Date"].min()) if "End Date" in df2.columns and not pd.isna(df2["End Date"].min()) else None,
                "latest": str(df2["End Date"].max()) if "End Date" in df2.columns and not pd.isna(df2["End Date"].max()) else None
            },
            "client_start_date": str(df2["Client Start Date"].min()) if "Client Start Date" in df2.columns and not pd.isna(df2["Client Start Date"].min()) else None
        }
    except Exception as e:
        print(f"[WARN] Date analysis failed: {e}")
        result["date_analysis"] = {}

    # Group column — try "Group" first, fall back to "Vertical Client"
    group_col = "Group" if "Group" in df2.columns else "Vertical Client"

    result["categorical_data"] = {
        "industries": safe_unique_list(df2, "Industry"),
        "countries": safe_unique_list(df2, "Country"),
        "msp_list": safe_unique_list(df2, "M.S.P"),
        "vertical_teams": safe_unique_list(df2, "Vertical Team"),
        "vertical_clients": safe_unique_list(df2, group_col),   # uses Group column
        "requirement_categories": safe_unique_list(df2, "Requirement category"),
        "cs_cd_teams": safe_unique_list(df2, "CS/CD  Team"),
        "msp_ht_vop": safe_unique_list(df2, "MSP/HT/VOP/Payroll")
    }

    result["key_personnel"] = {
        "cdl_list": safe_unique_list(df2, "CDL", 30),
        "cdm_list": safe_unique_list(df2, "CDM", 20),
        "sourcing_csa": safe_unique_list(df2, "Sourcing CSA", 20),
        "sourcing_csm": safe_unique_list(df2, "Sourcing CSM", 20),
        "client_executives": safe_unique_list(df2, "Client Executive", 10),
        "client_managers": safe_unique_list(df2, "Client Manager", 10)
    }

    try:
        result["hires_exits"] = {
            "hires_by_cycle": safe_value_counts(df2, "Hires", 10),
            "exits_by_cycle": safe_value_counts(df2, "Exits", 10)
        }
    except Exception as e:
        result["hires_exits"] = {}

    total_revenue = safe_sum(df2, "Revenue")
    total_cost    = safe_sum(df2, "Base Cost")
    total_hours   = safe_sum(df2, "Total Hours")

    result["performance_indicators"] = {
        "revenue_per_hour": round(total_revenue / max(total_hours, 1), 2),
        "cost_per_hour":    round(total_cost / max(total_hours, 1), 2),
        "profit_per_hour":  round((total_revenue - total_cost) / max(total_hours, 1), 2),
        "efficiency_ratio": round(total_revenue / max(total_cost, 1), 4),
        "avg_workers_per_cycle": round(safe_nunique(df2, "Worker Name") / max(safe_nunique(df2, "Cycle "), 1), 1)
    }

    _set_cached_profile(customer_name, result)
    return result


# -------------------------------------------------
# MAIN EXECUTOR
# -------------------------------------------------
def execute(intent: Dict[str, Any], df: pd.DataFrame, schema: dict):
    if not isinstance(intent, dict):
        raise ValueError("Intent must be a dictionary")

    # Company full profile
    if intent.get("type") == "company_full_profile":
        cust = intent.get("filters", {}).get("customer_name")
        if not cust:
            raise ValueError("No customer name provided")
        return build_company_profile(df, cust)

    measures    = intent.get("measures") or []
    filters     = intent.get("filters") or {}
    aggregation = intent.get("aggregation", "sum")

    if not measures:
        raise ValueError("No measures to execute")

    # Load metadata and column map
    metadata   = load_metadata("schema/metadata.json")
    column_map = build_column_map(df.columns)
    working_df = df.copy()

    # Apply filters — allow empty filters (aggregate across all data)
    skip_values = {"all", "all cycles", "overall", "together", ""}
    for canonical_col, raw_value in filters.items():
        if str(raw_value).lower().strip() in skip_values:
            continue

        real_col = resolve_dataframe_column(canonical_col, column_map)
        if not real_col:
            print(f"[WARN] Filter column not found: {canonical_col} — skipping")
            continue

        mask = normalize_filter_value(
            column=real_col,
            value=raw_value,
            df=working_df,
            metadata=metadata
        )

        if mask is None or not mask.any():
            raise ValueError(f"No rows matched filter: {canonical_col} = {raw_value}")

        working_df = working_df[mask]

    if working_df.empty:
        raise ValueError("Filters resulted in empty dataset")

    # Apply measure
    measure = measures[0]

    # Headcount = unique workers — handle before column resolution
    if measure == "headcount":
        if "Worker Name" in working_df.columns:
            # Show what week/cycle values are in the filtered data for debugging
            for col in ["Week #", "Week", "Cycle ", "Quarter"]:
                if col in working_df.columns:
                    print(f"  [{col} values in filtered set]: {sorted(working_df[col].dropna().unique().tolist())[:5]}")
            unique_workers = working_df["Worker Name"].dropna().str.strip().str.lower().nunique()
            count = int(unique_workers)
            print(f"Using measure: headcount (unique Worker Name)")
            print(f"Row count: {len(working_df)}, Unique workers: {count}")
            return count
        raise ValueError("Worker Name column not found for headcount")

    # Direct override map — bypasses fuzzy resolver which confuses GM$ and GM%
    MEASURE_COLUMN_OVERRIDE = {
        "gm_dollars":    ["GM$", "GM Dollars", "Gross Margin $", "GrossMargin$"],
        "gm_pct":        ["GM%", "GM Pct", "Gross Margin %", "GrossMargin%"],
        "revenue":       ["Revenue", "Total Revenue"],
        "bill_rate_reg": ["Bill Rate Reg", "Bill Rate"],
        "pay_rate_reg":  ["Pay Rate Reg", "Pay Rate"],
        "total_hours":   ["Total Hours"],
        "reg_hours":     ["Reg Hours", "Regular Hours"],
        "ot_hours":      ["OT Hours", "Overtime Hours"],
        "base_cost":     ["Base Cost"],
    }

    real_measure = None
    # Try direct override first
    if measure in MEASURE_COLUMN_OVERRIDE:
        for candidate in MEASURE_COLUMN_OVERRIDE[measure]:
            if candidate in df.columns:
                real_measure = candidate
                break
            # Case-insensitive check
            for col in df.columns:
                if col.lower().strip() == candidate.lower().strip():
                    real_measure = col
                    break
            if real_measure:
                break

    # Fall back to resolver if override didn't find it
    if not real_measure:
        real_measure = resolve_dataframe_column(measure, column_map)

    if not real_measure:
        raise ValueError(f"Measure column not found: {measure}")

    print(f"Using measure column: {real_measure}")
    print(f"Row count: {len(working_df)}")

    # Special case: GM% must be calculated as Total GM$ / Total Revenue * 100
    # NOT as an average of per-row GM% values (that gives wrong weighted result)
    if measure == "gm_pct":
        gm_col  = None
        rev_col = None
        for candidate in ["GM$", "GM Dollars", "Gross Margin $"]:
            for col in working_df.columns:
                if col.strip() == candidate or col.lower().strip() == candidate.lower():
                    gm_col = col
                    break
            if gm_col:
                break
        for candidate in ["Revenue", "Total Revenue"]:
            for col in working_df.columns:
                if col.strip() == candidate or col.lower().strip() == candidate.lower():
                    rev_col = col
                    break
            if rev_col:
                break

        if gm_col and rev_col:
            total_gm  = pd.to_numeric(working_df[gm_col],  errors="coerce").sum()
            total_rev = pd.to_numeric(working_df[rev_col], errors="coerce").sum()
            if total_rev != 0:
                return float((total_gm / total_rev) * 100)

    series = pd.to_numeric(working_df[real_measure], errors="coerce").dropna()

    if series.empty:
        raise ValueError("No numeric data available for aggregation")

    # Headcount = unique worker names
    if measure == "headcount":
        if "Worker Name" in working_df.columns:
            return int(working_df["Worker Name"].dropna().nunique())
        raise ValueError("Worker Name column not found for headcount")

    if aggregation == "sum":   return float(series.sum())
    if aggregation == "avg":   return float(series.mean())
    if aggregation == "min":   return float(series.min())
    if aggregation == "max":   return float(series.max())
    if aggregation == "count": return int(series.count())

    raise ValueError(f"Unsupported aggregation: {aggregation}")