# notebook 3- 2- features

# ========================================================
# MARK: STEP 8 — Feature Engineering (FINAL, Clean Version)
# ========================================================

from __future__ import annotations

import pandas as pd
import numpy as np

BASE_METRICS = [
    "impressions",
    "clicks",
    "clicks_all",
    "spend",
    "actions",
    "cpa",
    "cpm",
    "cost_per_1000_reach",
    "ctr_link",
    "ctr_all",
    "cpc_link",
    "cpc_all",
]

def build_metric_features(     # function name is build_ctr_features just because i wanted to streamline with other files. in reality, it should be named like build_metric_features
        df: pd.DataFrame,
        min_history_days: int = 7,
) -> pd.DataFrame:
    """
    Build metric-agnositc time-series features from DAILY x CAMPAIGN grain.

    Includes:
    - lag features
    - rolling window features
    - percentage change (momentum)
    - retargetting pool proxy
    - time-based features

    Assumes df contains at least:
    - date
    - campaign_id
    - ctr_link
    - impressions
    - clicks
    - spend
    """

    if df.empty:
        return df.copy()
    
    df = df.copy()

    # ------------------------------------
    # 1. Sort correctly (Critical)
    # ------------------------------------
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["campaign_id", "date"])

    # ------------------------------------
    # 2. Determine usable metrics
    # ------------------------------------
    metrics = [m for m in BASE_METRICS if m in df.columns]

    # ------------------------------------
    # 3. LAG FEATURES
    # ------------------------------------
    # LAG_FEATURES = {
    #     "ctr_link": [1, 7],
    #     # "results_count": [1, 7],
    #     # "results_cost": [1, 7],
    #     #"result_value": [1, 7],
    #     "spend": [1, 7],
    #     "impressions": [1, 7],
    #     "clicks": [1, 7],
    # }

    #for col, lags in LAG_FEATURES.items():
    for col in metrics:
        for lag in (1, 7):
            df[f"{col}_lag_{lag}"] = (
                df.groupby("campaign_id")[col].shift(lag)
            )

    # ------------------------------------
    # 4. ROLLING WINDOW FEATURES
    # ------------------------------------
    # ROLLING_FEATURES = {
    #     "ctr_link": [7, 14, 28],
    #     # "results_count": [7, 14],
    #     # "results_cost": [7, 14],
    #     #"result_value" : [7, 14],
    #     "spend": [7, 14],
    #     "impressions": [7, 14],
    #     "clicks": [7, 14],
    # }
    
    # for col, windows in ROLLING_FEATURES.items():
    for col in metrics:
        for w in (7, 14, 28):

            min_p = max(3, w // 2)

            df[f"{col}_roll_{w}"] = (
                df.groupby("campaign_id")[col]
                .shift(1)
                .rolling(w, min_periods=min_p)
                .mean()
            )
    
    # ---------------------------------------
    # 5. Momentum / Percentage Change
    # ---------------------------------------
    for col in metrics:
        lag_col = f"{col}_lag_1"
        if lag_col in df.columns:
            df[f"{col}_pct_change"] = (
                (df[col] - df[lag_col]) / 
                df[lag_col].replace({0: np.nan})
            )
    
    # if {"ctr_link", "ctr_link_lag_1"}.issubset(df.columns):
    # #if "ctr_link" in df.columns and "ctr_link_lag_1" in df.columns:
    #     df["ctr_link_pct_change"] = (
    #         (df["ctr_link"] - df["ctr_link_lag_1"]) /
    #         df["ctr_link_lag_1"].replace({0: np.nan})
    #     )
    
    # if {"spend", "spend_lag_1"}.issubset(df.columns):
    # #if "spend" in df.columns and "spend_lag_1" in df.columns:
    #     df["spend_pct_change"] = (
    #         (df["spend"] - df["spend_lag_1"]) /
    #         df["spend_lag_1"].replace({0: np.nan})
    #     )

    # ------------------------------------------
    # 6. RETARGETING POOL PROXY
    # ------------------------------------------
    """ When using both meta ingestion and supermetrics"""
    # if "results_value" in df.columns:
    #     df["retargeting_pool"] = (
    #         df.groupby("campaign_id")["results_value"].cumsum()
    #     )
    # else:
    #     df["retargeting_pool"] = np.nan

    if "actions" in df.columns:
        df["retargeting_pool"] = (
            df.groupby("campaign_id")["actions"]
                #.apply(lambda x: x.fillna(0).cumsum())
                .transform(lambda x: x.fillna(0).cumsum())
        )
    else:
        df["retargeting_pool"] = np.nan
    
    # -------------------------------------------
    # 7. Time-Based Features
    # -------------------------------------------
    df["day_of_week"] = df["date"].dt.dayofweek
    df["week_number"] = df["date"].dt.isocalendar().week.astype(int)

    # ------------------------------------------------------------
    # 8. Remove Rows that do not have enough history for lag/rolling features
    # ------------------------------------------------------------
    history_cols = [
        c for c in df.columns
        if c.endswith("_lag_1") or c.endswith("_roll_7")
        # if "_lag_" in c or "_roll_" in c
    ]
    
    if history_cols:
        df = df.dropna(subset=history_cols)

    # Optional: strict history window
    if min_history_days > 1:
        counts = df.groupby("campaign_id")["date"].transform("count")
        df = df[counts >= min_history_days]

    # ---------------------------------------------
    # 8. Final cleanup
    # ---------------------------------------------
    df = df.replace([np.inf, -np.inf], np.nan)

    return df