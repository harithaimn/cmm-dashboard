# notebook 3- 1-aggregation

# ========================================================
# MARK: STEP 7 — AGGREGATE INTO DAILY × CAMPAIGN GRAIN
# ========================================================

from __future__ import annotations

import pandas as pd
import numpy as np

def aggregate_daily_campaign(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate row-level Meta Ads data into DAILY × CAMPAIGN grain.

    Operations:
    - group by date + campaign_id (+ campaign_name if present)
    - sum raw volume metrics
    - mean rate metrics
    - recompute CTR correctly (clicks / impressions)
    - clean infinities

    Assumes df already contains:
    - date
    - campaign_id
    - impressions
    - clicks
    - spend
    """

    if df.empty:
        return df.copy()
    
    df = df.copy()

    # ----------------------------------
    # 1. Ensure date is date (not datetime)
    # ----------------------------------
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # ----------------------------------
    # 2. Define grouping keys
    # ----------------------------------
    group_cols = ["date", "campaign_id"]

    if "campaign_name" in df.columns:
        group_cols.append("campaign_name")
    
    # -----------------------------------
    # 3. Define aggregation rules
    # -----------------------------------
    agg_dict = {
        "impressions": "sum",
        "clicks": "sum",
        "spend": "sum",
        #"results_count",
        #"post_engagements",
        # "reach",
        # "post_reactions",
        # "website_purchases",
        # "actions",
    }

    # mean_cols = [
    #     "cpa",
    #     "cpm",
    #     "cost_per_1000_reach",
    #     "results_cost",
    # ]

    # agg_dict: dict[str, str] = {}

    # for col in sum_cols:
    #     if col in df.columns:
    #         agg_dict[col] = "sum"

    # for col in mean_cols:
    #     if col in df.columns:
    #         agg_dict[col] = "mean"

    # Preserve a representative results type if present
    if "results_type" in df.columns:
        agg_dict["results_type"] = "sum"

    # ----------------------------
    # 4. Perform aggregation
    # ----------------------------
    df_agg = (
        df
        .groupby(group_cols, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )

    # ---------------------------------
    # 5. Recompute TRUE CTR (correct way)
    # ----------------------------------
    df_agg["ctr_link"] = (
        df_agg["clicks"] /
        df_agg["impressions"].replace({0: np.nan})
    )

    # -----------------------------------------------
    # 6. Recompute additional derived metrics (safe)
    # -----------------------------------------------
    # if {"spend", "clicks"}.issubset(df_agg.columns):
    #     df_agg["cpc"] = (
    #         df_agg["spend"] /
    #         df_agg["clicks"].replace({0: np.nan})
    #     )

    # if {"spend", "impressions"}.issubset(df_agg.columns):
    #     df_agg["cpc"] = (
    #         df_agg["spend"] / 
    #         df_agg["clicks"].replace({0: np.nan})
    #     )
    
    # if {"spend", "impressions"}.issubset(df_agg.columns):
    #     df_agg["cpm_recalc"] = (
    #         df_agg["spend"] /
    #         df_agg["impressions"].replace({0: np.nan})
    #     ) * 1000

    # --------------------------------
    # 7. Clean infinities
    # --------------------------------
    df_agg = df_agg.replace([np.inf, -np.inf], np.nan)

    return df_agg