from __future__ import annotations

import pandas as pd
import streamlit as st
from pathlib import Path

# =========================================
# CONFIG
# =========================================
DATA_PATH = Path("data/predictions/predictions.parquet")

st.set_page_config(
    page_title="Home Page",
    layout="wide",
)

st.title(" Home Page")
st.caption("Welcome to the CMM Predictions and Monitoring Dashboard.")
st.caption("This dashboard provides insights into campaign performance predictions and monitoring alerts.")
st.caption("Account-level health across all clients")

# ========================================
# LOAD DATA
# ========================================
if not DATA_PATH.exists():
    st.error("No prediction data found. Run Daily Refresh first.")
    st.stop()

df = pd.read_parquet(DATA_PATH)

# ========================================
# REQUIRED COLUMNS CHECK
# ========================================
required_cols = {
    "campaign_id",
    "campaign_name",
    "ctr_drop_flag",
    "spend_spike_flag",
    "retargeting_pool",
    "ctr_link",
    "pred_ctr_link",
}

missing = required_cols - set(df.columns)
if missing:
    st.error(f"Missing required columns: {missing}")
    st.stop()

# ========================================
# CLIENT-LEVEL AGGREGATION
# ========================================
client_df = (
    df
    .groupby("client")
    .agg(
        campaigns=("campaign_id", "nunique"),
        ctr_drops=("ctr_drop_flag", "sum"),
        spend_spikes=("spend_spike_flag", "sum"),
        avg_ctr_link=("ctr_link", "mean"),
        avg_pred_ctr_link=("pred_ctr_link", "mean"),
        retargeting_campaigns=("retargeting_pool", "max"),
    )
    .reset_index()
)

# ==========================
# DERIVED SIGNALS
# ==========================
def classify_retargeting(pool):
    if pool >= 5000:
        return "High"
    if pool >= 1500:
        return "Medium"
    return "Low"

def classify_fatigue(row):
    ratio = row["ctr_drops"] / max(row["campaigns"], 1)
    if ratio >= 0.3:
        return "High"
    if ratio > 0:
        return "Medium"
    return "Low"

def classify_status(row):
    if row["ctr_drops"] / max(row["campaigns"], 1) >= 0.3:
        return "Critical"
    if row["ctr_drops"] > 0 or row["spend_spikes"] > 0:
        return "At Risk"
    return "Healthy"

client_df["Retargeting Depth"] = client_df["retargeting_pool"].apply(classify_retargeting)
client_df["Creative Fatigue"] = client_df.apply(classify_fatigue, axis=1)
client_df["Status"] = client_df.appl(classify_status, axis=1)

# CTR Trend Proxy
client_df["CTR Trend"] = (
    client_df["avg_ctr"] - client_df["avg_pred_ctr"]
).apply(lambda x: "↗" if x > 0.001 else "↘" if x < -0.001 else "→")

# CPA Trend placeholder (future)
client_df["CPA Trend"] = "→"

# ========================================
# DISPLAY TABLE
# ========================================
display_df = client_df[
    [
        "Campaign",
        "CTR Trend",
        "CPA Trend",
        "Retargeting Depth",
        "Creative Fatigue",
        "Status",
    ]
].rename(columns={"client": "Client"})

def highlight_status(val):
    if val == "Critical":
        return "background-color:#ffcccc;font-weight:bold"
    if val == "At Risk":
        return "background-color:#fff2cc"
    if val == "Healthy":
        return "background-color:#e6ffea"
    return ""

st.subheader("📋 Account Health Summary")

st.dataframe(
    display_df
    .style
    .applymap(highlight_status, subset=["Status"]),
    use_container_width=True,
)

# ========================================================
# KPI STRIP
# ========================================================
col1, col2, col3 = st.columns(3)

col1.metric(
    "Healthy",
    (display_df["Status"] == "Healthy").sum(),
)

col2.metric(
    "At Risk",
    (display_df["Status"] == "At Risk").sum(),
)

col3.metric(
    "Critical",
    (display_df["Status"] == "Critical").sum(),
)

st.caption("Health is derived from CTR drops, spend anomalies, and retargeting depth.")