from __future__ import annotations

import pandas as pd
import streamlit as st
from pathlib import Path

# =========================================
# CONFIG
# =========================================
DATA_PATH = Path("data/predictions/predictions.parquet")
RETARGETING_THRESHOLD = 2500

st.set_page_config(
    page_title="Creative Effectiveness",
    layout="wide",
)

st.title("🎨 Creative Effectiveness")
st.caption("Retargeting readiness & audience depth")

# ========================================
# LOAD DATA
# ========================================
if not DATA_PATH.exists():
    st.error("Feature data not found. Run training or daily refresh first.")
    st.stop()

df = pd.read_parquet(DATA_PATH)

# ========================================
# VALIDATION
# ========================================
required_cols = {
    "date",
    "client",
    "campaign_id",
    "campaign_name",
    "objective",
    "retargeting_pool",
}

missing = required_cols - set(df.columns)
if missing:
    st.error(f"Missing required columns: {missing}")
    st.stop()

df["date"] = pd.to_datetime(df["date"])

# ========================================
# FILTERS
# ========================================
st.sidebar.header("Filters")

clients = sorted(df["client"].dropna().unique())
client_sel = st.sidebar.selectbox("Client", clients)

df = df[df["client"] == client_sel]

objectives = sorted(df["objective"].dropna().unique())
objective_sel = st.sidebar.multiselect(
    "Objective",
    objectives,
    default=objectives,
)

df = df[df["objective"].isin(objective_sel)]

campaigns = sorted(df["campaign_name"].unique())
campaign_sel = st.sidebar.selectbox("Campaign", campaigns)

df_campaign = df[df["campaign_name"] == campaign_sel]

# ========================================
# KPI STRIP
# ========================================
latest = df_campaign.sort_values("date").iloc[-1]

days_active = df_campaign["date"].nunique()
daily_growth = (
    df_campaign["retargeting_pool"].diff().mean()
    if days_active > 1 else 0
)

c1, c2, c3 = st.columns(3)

c1.metric(
    "Current Retargeting Pool",
    f"{int(latest['retargeting_pool']):,}",
)

c2.metric(
    "Avg Daily Growth",
    f"{daily_growth:,.1f}"
)

c3.metric(
    "Days Active",
    f"{days_active}"
)

st.divider()

# =======================================
# RETARGETING POOL GROWTH
# ======================================
st.subheader("📈 Retargeting Pool Growth")

plot_df = df_campaign.sort_values("date").set_index("date")

st.line_chart(plot_df["retargeting_pool"])

st.markdown(
    f"**Reference:** Retargeting typically becomes viable around "
    f"**{RETARGETING_THRESHOLD:,} users**."
)

# =======================================
# DISTRIBUTION ACROSS CAMPAIGNS
# ======================================
st.subheader("📊 Retargeting Pool by Campaign (Latest)")

latest_df = (
    df.sort_values("date")
    .groupby(["campaign_id", "campaign_name"], as_index=False)
    .last()
)

latest_df = latest_df.sort_values("retargeting_pool", ascending=False)

st.bar_chart(
    latest_df.set_index("campaign_name")["retargeting_pool"]
)

st.caption(
    "This page show audience capacity only. "
    "No predictions or recommendation are made here."
)