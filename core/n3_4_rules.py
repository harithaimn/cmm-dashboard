# notebook 3- 4- rules

from __future__ import annotations

import pandas as pd
import numpy as np

# ========================================================
# MARK: STEP 11 — Predictions & Risk Flags (FINAL)
# ========================================================


# ========================================================
# METRIC SIGNAL CONFIG
# ========================================================

METRIC_RULES = {
    # Lower is bad
    "ctr_link": {
        "direction": "down",
        "threshold": 0.85,
        "baseline": "roll_7",
    },
    "ctr_all": {
        "direction": "down",
        "threshold": 0.85,
        "baseline": "roll_7",
    },
    
    # Higher is bad
    "cpc_link": {
        "direction": "up",
        "threshold": 1.20,
        "baseline": "roll_7",
    },
    "cpc_all": {
        "direction": "up",
        "threshold": 1.20,
        "baseline": "roll_7",
    },
    "cpa": {
        "direction": "up",
        "threshold": 1.25,
        "baseline": "roll_7",
    },
    "cpm": {
        "direction": "up",
        "threshold": 1.15,
        "baseline": "roll_7",
    },
    "cost_per_1000_reach": {
        "direction": "up",
        "threshold": 1.15,
        "baseline": "roll_7",
    },
}
# ---------------------------------
# Thresholds (explicit)
# ---------------------------------
"""
CTR_DROP_THRESHOLD = 0.85  # 85% of baseline
SPEND_SPIKE_THRESHOLD = 1.25  # 125% of previous spend
MIN_MEANINGFUL_SPEND = 50  # Minimum spend to consider spike
RETARGETTING_POOL_THRESHOLD = 2500  # Minimum pool size to scale
"""

# ========================================================
# SEVERITY BUCKETS (for prioritisation, not language)
# ========================================================

def severity_from_ratio(ratio: float, direction: str) -> str:
    if pd.isna(ratio):
        return "unknown"

    if direction == "down":
        if ratio < 0.70:
            return "critical"
        if ratio < 0.85:
            return "warning"
        return "normal"
    
    if direction == "up":
        if ratio > 1.50:
            return "critical"
        if ratio > 1.20:
            return "warning"
        return "normal"

    return "normal"

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

    """
    # ----------------------------------------------
    # 1. Safety checks
    # ----------------------------------------------
    required = {"pred_ctr_link", "ctr_link"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for rules: {missing}")
    """
    # ----------------------------------------------
    # 2. Flags
    # ----------------------------------------------

    signal_flags = []

    for metric, cfg in METRIC_RULES.items():
        pred_col = f"pred_{metric}"
        baseline_col = f"{metric}_{cfg['baseline']}"

        if pred_col not in df.columns or baseline_col not in df.columns:
            continue

        ratio_col = f"{metric}_ratio"
        severity_col = f"{metric}_severity"
        flag_col = f"{metric}_flag"

        # -------------------------
        # Ratio
        # -------------------------
        df[ratio_col] = (
            df[pred_col] /
            df[baseline_col].replace({0: np.nan})
        )

        # --------------------------
        # Severity
        # --------------------------
        df[severity_col] = df[ratio_col].apply(
            lambda r: severity_from_ratio(r, cfg["direction"])
        )

        # ----------------------------
        # Binary alert flag (for gating)
        # ----------------------------
        if cfg["direction"] == "down":
            df[flag_col] = (df[ratio_col] < cfg["threshold"]).astype(int)
        else:
            df[flag_col] = (df[ratio_col] > cfg["threshold"]).astype(int)
        
        signal_flags.append(flag_col)
    
    # ===============================================
    # GLOBAL PRIORITY SIGNALS
    # ===============================================

    if signal_flags:
        df["signal_count"] = df[signal_flags].sum(axis=1)
    else:
        df["signal_count"] = 0
    
    severity_rank = {"critical": 3, "warning": 2, "normal": 1, "unknown": 0}

    severity_cols = [c for c in df.columns if c.endswith("_severity")]

    if severity_cols:
        df["max_severity"] = (
            df[severity_cols]
            .apply(lambda row: max(row.map(severity_rank)), axis=1)
            .map({v: k for k, v in severity_rank.items()})
        )
    else:
        df["max_severity"] = "normal"

    return df

    # # 2.1 CTR Drop Flag
    # # Rule:
    # # predicted CTR < 85% of recent baseline
    # if "ctr_link_roll_7" in df.columns:
    #     baseline_ctr = df["ctr_link_roll_7"]
    # elif "ctr_link_lag_1" in df.columns:
    #     baseline_ctr = df["ctr_link_lag_1"]
    # else:
    #     baseline_ctr = np.nan

    # df["ctr_drop_ratio"] = (
    #     df["pred_ctr_link"] /
    #     baseline_ctr.replace({0: np.nan})
    # )
    
    # df["ctr_drop_flag"] = (
    #     df["pred_ctr_link"] < CTR_DROP_THRESHOLD
    # ).astype(int)

    # -----------------------------------------
    # 3. Spend Spike Signal
    # -----------------------------------------
    """
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
    """

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
