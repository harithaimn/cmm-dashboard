# notebook 3- 4- rules

from __future__ import annotations

import pandas as pd
import numpy as np

# ========================================================
# MARK: STEP 11 — Predictions & Risk Flags (FINAL)
# ========================================================

# ---------------------------------
# Thresholds (explicit)
# ---------------------------------
CTR_DROP_THRESHOLD = 0.85  # 85% of baseline
SPEND_SPIKE_THRESHOLD = 1.25  # 125% of previous spend
MIN_MEANINGFUL_SPEND = 50  # Minimum spend to consider spike
RETARGETTING_POOL_THRESHOLD = 2500  # Minimum pool size to scale

# ========================================================
# MARK: 11.5 — Output DataFrame for Step 12 (recommendations)
# ========================================================

def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate rule-based recommendations from model signals.

    Expected input columns (some optional, but recommend):
    - pred_ctr_link
    - ctr_link
    - ctr_link_roll_7 or ctr_link_lag_1
    - spend
    - spend_lag_1
    - retargetting_pool
    - campaign_name (optional, for messages)

    Output columns:
    - ctr_drop_flag
    - spend_spike_flag
    - retargetting_pool_large
    - reason
    - action
    - summary
    - alert_msg
    """

    if df.empty:
        return df.copy()
    
    df = df.copy()

    # ----------------------------------------------
    # 1. Safety checks
    # ----------------------------------------------
    required = {"pred_ctr_link", "ctr_link"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for rules: {missing}")
    
    # ----------------------------------------------
    # 2. Flags
    # ----------------------------------------------

    # 2.1 CTR Drop Flag
    # Rule:
    # predicted CTR < 85% of recent baseline
    if "ctr_link_roll_7" in df.columns:
        baseline_ctr = df["ctr_link_roll_7"]
    elif "ctr_link_lag_1" in df.columns:
        baseline_ctr = df["ctr_link_lag_1"]
    else:
        baseline_ctr = np.nan

    df["ctr_drop_ratio"] = (
        df["pred_ctr_link"] /
        baseline_ctr.replace({0: np.nan})
    )
    
    df["ctr_drop_flag"] = (
        df["pred_ctr_link"] < CTR_DROP_THRESHOLD
    ).astype(int)

    # -----------------------------------------
    # 3. Spend Spike Signal
    # -----------------------------------------

    # 2.2 Spend Spike Flag
    # Rule:
    # spend > 125% of yesterday AND meaningful spend
    if {"spend", "spend_lag_1"}.issubset(df.columns):
        df["spend_spike_ratio"] = (
            df["spend"] /
            df["spend_lag_1"].replace({0: np.nan})
        )

        df["spend_spike_flag"] = (
            (df["spend_spike_ratio"] > SPEND_SPIKE_THRESHOLD) &
            (df["spend"] > MIN_MEANINGFUL_SPEND)
        ).astype(int)
    else:
        df["spend_spike_ratio"] = np.nan
        df["spend_spike_flag"] = 0

    # 2.3 Retargetting Pool Flag Large
    # Rule:
    # enough accumulated results to scale
    if "retargetting_pool" in df.columns:
        df["retargetting_pool_large"] = (
            df["retargetting_pool"] >= RETARGETTING_POOL_THRESHOLD
        ).astype(int)
    else:
        df["retargetting_pool_large"] = 0

# # ========================================================
# # MARK: STEP 12— Recommendation Engine Goals
# # ========================================================

#     # ---------------------------------------------------
#     # 3. REASON (WHY)
#     # ---------------------------------------------------
#     def _reason(row) -> str:
#         if row["ctr_drop_flag"] == 1:
#             return "CTR is predicted to drop significantly from recent performance"
#         if row["spend_spike_flag"] == 1:
#             return "Spend increased unusually compared to previous days."
#         if row["retargetting_pool_large"] == 1:
#             return "Retargetting pool has grown large enough to scale."
#         return "No significant anomaly detected."
    
#     df["reason"] = df.apply(_reason, axis=1)

#     # ---------------------------------------------------
#     # 4. ACTION (WHAT TO DO)
#     # ---------------------------------------------------
#     def _action(row) -> str:
#         if row["ctr_drop_flag"] == 1:
#             return (
#                 "Refresh creatives, test new hooks, reduce frequency, " \
#                 "and review audience overlap."
#             )
        
#         if row["spend_spike_flag"] == 1:
#             return (
#                 "Check budget pacing, control spend allocation, " \
#                 "and review delivery settings."
#             )
#         if row["retargeting_pool_large"] == 1:
#             return (
#                 "Consider launching or scaling retargeting campaigns "
#                 "to convert warm audiences."
#             )
#         return "No action needed."
    
#     df["action"] = df.apply(_action, axis=1)

#     # ---------------------------------------------------
#     # 5. SUMMARY (DASHBOARD ONE-LINER)
#     # ---------------------------------------------------
#     def _summary(row) -> str:
#         if row["ctr_drop_flag"] == 1:
#             return "⚠ CTR predicted to drop."
#         if row["spend_spike_flag"] == 1:
#             return "⚠ Spend spike detected."
#         if row["retargeting_pool_large"] == 1:
#             return "ℹ Retargeting pool ready to scale."
#         return "Stable performance."
    
#     df["summary"] = df.apply(_summary, axis=1)

#     # ----------------------------------------------------
#     # 6. ALERT MESSAGE (Whatsapp/Slack ready)
#     # ----------------------------------------------------
#     def _alert(row) -> str:
#         name = row.get("campaign_name", "This campaign")

#         if row["ctr_drop_flag"] == 1:
#             return (
#                 f"⚠ ALERT: CTR drop expected for {name}.\n"
#                 f"Predicted CTR: {row['pred_ctr_link']:.4f}\n"
#                 f"Baseline CTR: {baseline.loc[row.name]:.4f}\n"
#                 f"Recommended Action: Refresh creatives or audiences."
#             )
        
#         if row["spend_spike_flag"] == 1:
#             return (
#                 f"⚠ Spend Spike Detected for {name}.\n"
#                 f"Today's Spend: {row['spend']:.2f}\n"
#                 f"Please review budget pacing."
#             )
        
#         return ""
    
#     df["alert_msg"] = df.apply(_alert, axis=1)

#     return df
