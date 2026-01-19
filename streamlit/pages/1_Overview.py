from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

# =========================================
# CONFIG
# =========================================
PREDICTIONS_PATH = Path("data/predictions/predictions.parquet")

DEFAULT_REFRESH_DAYS = 7
REFRESH_COOLDOWN_MINUTES = 10

# =========================================
# PIPELINE IMPORT (SAFE)
# =========================================
from pipelines.file2_n4_daily_refresh import run_daily_refresh

# =========================================
# PAGE SETUP
# =========================================
st.set_page_config(
    page_title="Overview - Prediction Dashboard",
    layout="wide",
)

st.title("📊 Prediction Dashboard Overview")
st.caption("Overall performance trends across campaigns")

# ======================================================
# CLIENT-SAFE REFRESH BUTTON
# ======================================================
col_left, col_right = st.columns([5, 1])

with col_right:
    refresh_clicked = st.button("🔄 Refresh Data")

if "last_refresh_ts" not in st.session_state:
    st.session_state["last_refresh_ts"] = None

if refresh_clicked:
    now = datetime.utcnow()

    if (
        st.session_state["last_refresh_ts"]
        and (now - st.session_state["last_refresh_ts"]).seconds
        < REFRESH_COOLDOWN_MINUTES * 60
    ):
        st.warning("⏳ Please wait before refreshing again.")
    else:
        with st.spinner("Refreshing latest data (last 30 days)..."):
            run_daily_refresh(
                access_token=os.environ["META_ACCESS_TOKEN"],
                ad_account_id=os.environ["META_AD_ACCOUNT_ID"],
                date_since=(now - timedelta(days=DEFAULT_REFRESH_DAYS)).strftime("%Y-%m-%d"),
                date_until=now.strftime("%Y-%m-%d"),
                model_path="artifacts/models/ctr_link",
            )

        st.session_state["last_refresh_ts"] = now
        st.success("✅ Data refreshed successfully")

# ==================================================
# LOAD DATA
# ==================================================
if not PREDICTIONS_PATH.exists():
    st.warning("No prediction data found. Please run the pipeline to generate predictions.")
    st.stop()

df = pd.read_parquet(PREDICTIONS_PATH)

# ==================================================
# BASIC SANITY
# ==================================================
df["date"] = pd.to_datetime(df["date"])


# ==================================================
# FILTERS
# ==================================================
with st.expander("🔍 Filters", expanded=True):
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        date_range = st.date_input(
            "Date range",
            [df["date"].min().date(), df["date"].max().date()],
        )

    with col2:
        campaigns = st.multiselect(
            "Campaign",
            sorted(df["campaign_name"].dropna().unique()),
        )

    with col3:
        objectives = st.multiselect(
            "Objective",
            sorted(df["objective"].dropna().unique())
            if "objective" in df.columns
            else [],
        )

    with col4:
        status = st.multiselect(
            "Status",
            ["ACTIVE", "PASSIVE"],
            default=["ACTIVE"],
        )

# Apply filters
mask = (df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])

if campaigns:
    mask &= df["campaign_name"].isin(campaigns)

if objectives and "objective" in df.columns:
    mask &= df["objective"].isin(objectives)

if status and "campaign_activity_status" in df.columns:
    mask &= df["campaign_activity_status"].isin(status)

df_filt = df.loc[mask]

# ==================================================
# KPI CARDS
# ==================================================
st.subheader("📊Key Metrics")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Spend", f"{df_filt['spend'].sum():,.2f}")
col2.metric("Impressions", f"{int(df_filt['impressions'].sum()):,}")
col3.metric("Clicks", f"{int(df_filt['clicks'].sum()):,}")
col4.metric("CTR (Link)", f"{df_filt['ctr_link'].mean():.2%}")

# ==================================================
# CTR vs PREDICTED CTR - LINE
# ==================================================
st.subheader("📈 CTR (Link) vs Predicted CTR Over Time")

ctr_ts = (
    df_filt
    .groupby("date")[["ctr_link", "pred_ctr_link"]]
    .mean()
    .reset_index()
)

st.line_chart(
    ctr_ts.set_index("date"),
)

# ==================================================
# SPEND vs CTR -- SCATTER
# ==================================================
st.subheader("💸 Spend vs CTR")

st.scatter_chart(
    df_filt[["spend", "ctr_link"]],
)

# ===================================================
# FOOTER
# ===================================================
st.caption(
    f"Last updated: {df['date'].max().strftime('%Y-%m-%d')} · "
    "Data refresh is limited to last 7 days"
)