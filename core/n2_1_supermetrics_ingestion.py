# # notebook 2- 1-Supermetrics ingestion

from __future__ import annotations

from typing import Union
from pathlib import Path
import pandas as pd

# ==================================================
# Supermetrics ingestion (raw, no logic)
# ==================================================
def load_supermetrics_export(
        path: Union[str, Path],
) -> pd.DataFrame:
    """
    Load Supermetrics export from CSV / Excel into a raw DataFrame.

    Rules:
    - No aggregation
    - No joins
    - No semantic logic
    - No derived metrics

    Returns:
        supermetrics_df with canonical column names
    """

    # ================================
    # Load file
    # ================================
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    elif path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        raise ValueError("Unsupported file type. Use CSV or Excel.")
    
    # Trim headers defensively
    df.columns = df.columns.str.strip()
    
    # ==================================
    # Column mapping (Supermetrics -> canonical)
    # ==================================
    COLUMN_MAP = {
    #     "Date": "date",
    #     # dekat sini boleh tambah date_start, date_stop,  beza dgn campaign start stop.
    #     "Ad ID": "ad_id",
    #     "Campaign ID": "campaign_id",
    #     "Campaign Name": "campaign_name",
    #     "Ad set ID": "adset_id",
    #     "Ad name": "ad_name",
    #     "Impressions": "impressions",
    #     "Link clicks": "clicks_link",
    #     "Cost": "spend",
    #     "Actions": "actions",
    # }
        # Core grain
        "Date": "date",

        # Campaign-level
        "Campaign ID": "campaign_id",
        "Campaign name": "campaign_name",
        "Campaign start date": "campaign_start_date",
        "Campaign end date": "campaign_end_date",
        "Campaign status": "campaign_status",
        "Campaign objective": "campaign_objective",

        # ======================
        # Ad set–level
        # ======================
        "Ad set ID": "adset_id",
        "Ad set name": "adset_name",
        "Ad set status": "adset_status",
        "Ad set start time": "adset_start_time",
        "Ad set end time": "adset_end_time",

        # Ad-level
        "Ad ID": "ad_id",
        "Ad name": "ad_name",
        "Creative name": "creative_name",
        "Ad status": "ad_status",

        # Metrics (authoritative)
        "Impressions": "impressions",
        "Cost": "spend",

        "Link clicks": "clicks",
        "Clicks (all)": "clicks_all",

        "Actions": "actions",
        "Cost per action (CPA)": "cpa",

        # Optional diagnostics (kept raw, not used downstream yet)
        "CPM (cost per 1000 impressions)": "cpm",
        "Cost per 1000 people reached": "cost_per_1000_reach",
        "CTR (link click-through rate)": "ctr_link_reported",
        "CTR (all)": "ctr_all_reported",
        "CPC (cost per link click)": "cpc_link",
        "CPC (all)": "cpc_all",

    }
    
    # ===============================
    # Validate presence (FAIL FAST)
    # ===============================
    missing = [c for c in COLUMN_MAP if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required Supermetrics columns: {missing}")
    
    # ===============================
    # Select + rename
    # ===============================
    df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)

    # ------------------------
    # Minimal type coercion (safe)
    # ------------------------
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    for col in ["campaign_start_date", "campaign_end_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    
    for col in ["adset_start_time", "adset_end_time"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    
    # Fix numeric IDs sometimes exported as floats
    for col in ["campaign_id", "adset_id","ad_id"]:
    #for col in ["ad_id", "campaign_id", "adset_id"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
        )

    # Numeric metrics (NO derivation)
    numeric_cols = [
        "impressions",
        "clicks",
        "clicks_all",
        "spend",
        "actions",
        "cpa",
        "cpm",
        "cost_per_1000_reach",
        "ctr_link_reported",
        "ctr_all_reported",
        "cpc_link",
        "cpc_all",
    ]

    for col in numeric_cols:
    #for col in ["impressions", "clicks_link", "clicks_all", "spend", "actions", "cpa"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


