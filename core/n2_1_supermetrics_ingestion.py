# # notebook 2- 1-Supermetrics ingestion

from __future__ import annotations

from typing import Union
import pandas as pd

# ==================================================
# Supermetrics ingestion (raw, no logic)
# ==================================================
def load_supermetrics_export(
        path: Union[str, bytes],
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
    if str(path).lower().endswith(".csv"):
        df = pd.read_csv(path)
    elif str(path).lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(path)
    else:
        raise ValueError("Unsupported file type. Use CSV or Excel.")
    
    # ==================================
    # Column mapping (Supermetrics -> canonical)
    # ==================================
    COLUMN_MAP = {
        "Date": "date",
        # dekat sini boleh tambah date_start, date_stop,  beza dgn campaign start stop.
        "Ad ID": "ad_id",
        "Campaign ID": "campaign_id",
        "Campaign Name": "campaign_name",
        "Ad set ID": "adset_id",
        "Ad name": "ad_name",
        "Impressions": "impressions",
        "Link clicks": "clicks_link",
        "Cost": "spend",
        "Actions": "actions",
    }
    
    missing = [c for c in COLUMN_MAP if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required Supermetrics columns: {missing}")
    
    df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)

    # ------------------------
    # Minimal type coercion (safe)
    # ------------------------
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    
    # Fix numeric IDs sometimes exported as floats
    for col in ["ad_id", "campaign_id", "adset_id"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
        )

    # Numeric metrics (NO derivation)
    for col in ["impressions", "clicks_link", "spend", "actions"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


