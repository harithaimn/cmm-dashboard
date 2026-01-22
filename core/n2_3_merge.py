# # notebook 2- 3-merge

from __future__ import annotations

import pandas as pd


def build_canonical_daily_df(
        supermetrics_df: pd.DataFrame,
        meta_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the single source of truth by mergin Supermetrics + Meta.

    Rules:
    - Supermetrics
    - Meta
    - Join on (campaign_id, date)
    - one_to_one validation enforced?
    """

    # ----------------------------
    # 1. Defensive copies
    # ----------------------------
    sm = supermetrics_df.copy()
    meta = meta_df.copy()

    # ----------------------------
    # 2. Normalize join keys
    # ----------------------------
    sm["campaign_id"] = sm["campaign_id"].astype(str)
    meta["campaign_id"] = meta["campaign_id"].astype(str)

    sm["date"] = pd.to_datetime(sm["date"]).dt.date
    meta["date"] = pd.to_datetime(meta["date"]).dt.date

    # ----------------------------
    # 3. Assert uniqueness (FAIL FAST)
    # ----------------------------
    # sm_dupes = sm.duplicated(subset=["campaign_id", "date"])
    # if sm_dupes.any():
    #     dup = sm.loc[sm_dupes, ["campaign_id", "date"]].head()
    #     raise ValueError(
    #         f"❌ Supermetrics is not unique on (campaign_id, date).\n{dup}"
    #     )
    
    # meta_dupes = meta.duplicated(subset=["campaign_id", "date"])
    # if meta_dupes.any():
    #     dup = meta.loc[meta_dupes, ["campaign_id", "date"]].head()
    #     raise ValueError(
    #         f"❌ Meta is not unique on (campaign_id, date).\n{dup}"
    #     )
    if sm.duplicated(subset=["campaign_id", "date"]).any():
        dup = sm.loc[
            sm.duplicated(subset=["campaign_id", "date"]),
            ["campaign_id", "date"]
        ].head()
        raise ValueError(
            f"❌ Supermetrics is not unique on (campaign_id, date).\n{dup}"
        )

    if meta.duplicated(subset=["campaign_id", "date"]).any():
        dup = meta.loc[
            meta.duplicated(subset=["campaign_id", "date"]),
            ["campaign_id", "date"]
        ].head()
        raise ValueError(
            f"❌ Meta is not unique on (campaign_id, date).\n{dup}"
        )
    
    # ------------------------------
    # 4. Merge (this Will crash if invalid)
    # ------------------------------
    canonical_df = sm.merge(
        meta,
        how="left",
        on=["campaign_id", "date"],
        validate="one_to_one",
        suffixes=("", "_meta"),
    )

    # ------------------------------
    # 5. Post-merge sanity checks
    # ------------------------------
    #missing_meta = canonical_df["campaign_result_type"].isna().sum()

    missing_ratio = canonical_df["campaign_result_type"].isna().mean()

    if missing_ratio > 0.2:
        raise ValueError(
            f"❌ Too many rows missing Meta enrichment: {missing_ratio:.1%}"
        )
    
    return canonical_df