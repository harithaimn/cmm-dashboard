# notebook 3- 2- features

# ========================================================
# MARK: STEP 8 — Feature Engineering (FINAL, Clean Version)
# ========================================================

from __future__ import annotations

import pandas as pd
import numpy as np

def build_ctr_features(
        df: pd.DataFrame,
        min_history_days: int = 7,
) -> pd.DataFrame:
    """
    Build CTR prediction features from DAILY x CAMPAIGN data.

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
    # 2. LAG FEATURES
    # ------------------------------------
    lag_features = {
        "ctr_link": [1, 7],
        # "results_count": [1, 7],
        # "results_cost": [1, 7],
        "result_value": [1, 7],
        "spend": [1, 7],
        "impressions": [1, 7],
    }

    for col, lags in lag_features.items():
        if col in df.columns:
            for lag in lags:
                df[f"{col}_lag_{lag}"] = (
                    df.groupby("campaign_id")[col].shift(lag)
                )

    # ------------------------------------
    # 3. ROLLING WINDOW FEATURES
    # ------------------------------------
    rolling_features = {
        "ctr_link": [7, 14, 28],
        # "results_count": [7, 14],
        # "results_cost": [7, 14],
        "result_value" : [7, 14],
        "spend": [7, 14],
        "impressions": [7, 14],
    }
    
    for col, windows in rolling_features.items():
        if col in df.columns:
            for w in windows:
                df[f"{col}_roll_{w}"] = (
                    df.groupby("campaign_id")[col]
                    .shift(1)
                    .rolling(w, min_periods=3)
                    .mean()
                )
    
    # ---------------------------------------
    # 4. Momentum / Percentage Change
    # ---------------------------------------
    # if "ctr_link" in df.columns and "ctr_link_lag_1" in df.columns:
    #     df["ctr_link_pct_change"] = (
    #         (df["ctr_link"] - df["ctr_link_lag_1"]) /
    #         df["ctr_link_lag_1"].replace({0: np.nan})
    #     )

    # if "spend" in df.columns and "spend_lag_1" in df.columns:
    #     df["spend_pct_change"] = (
    #         (df["spend"] - df["spend_lag_1"]) /
    #         df["spend_lag_1"].replace({0: np.nan})
    #     )

    # ------------------------------------------
    # 5. RETARGETING POOL PROXY
    # ------------------------------------------
    if "results_value" in df.columns:
        df["retargeting_pool"] = (
            df.groupby("campaign_id")["results_value"].cumsum()
        )
    
    # -------------------------------------------
    # 6. Time-Based Features
    # -------------------------------------------
    df["day_of_week"] = df["date"].dt.dayofweek
    df["week_number"] = df["date"].dt.isocalendar().week.astype(int)

    # ------------------------------------------------------------
    # 7. Remove Rows that do not have enough history for lag/rolling features
    # ------------------------------------------------------------
    history_cols = [
        c for c in df.columns
        if c.endswith("_lag_1") or c.endswith("_roll_7")
        # if "_lag_" in c or "_roll_" in c
    ]
    
    if history_cols:
        df = df.dropna(subset=history_cols)

    # ---------------------------------------------
    # 8. Final cleanup
    # ---------------------------------------------
    df = df.replace([np.inf, -np.inf], np.nan)

    return df
                    