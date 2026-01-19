from __future__ import annotations

import pandas as pd
import streamlit as st
from pathlib import Path

# =========================================
# CONFIG
# =========================================
DATA_PATH = Path("data/predictions/predictions.parquet")

st.set_page_config(
    page_title="Offer & Conversion Insights",
    layout="wide",
)

st.title("🎨🛒 Offer & Conversion Insights")
st.caption("Conversion behavior & cost diagnostics (no predictions)")

# =========================================
# LOAD DATA
# ========================================
if not DATA_PATH.exists():
    st.error("Canonical dataset not found. Run canonical build first.")
    st.stop()

df = pd.read_parquet(DATA_PATH)

# =========================================
# VALIDATION
# =========================================
required_cols = {
    "date",
    "campaign_id",
    "campaign_name",
    "objective",
    "spend",
    "result_value",
    "cost_per_result",
    "result_type",
    "result_category",
}

missing = required_cols - set(df.columns)
if missing:
    st.error(f"Missing required columns: {missing}")
    st.stop()

df["date"] = pd.to_datetime(df["date"])

# =========================================
# SIDEBAR FILTERS
# =========================================
st.sidebar.header("Filters")

# clients = sorted(df["client"].dropna().unique())
# client_sel = st.sidebar.selectbox("Client", clients)
# df = df[df["client"] == client_sel]

objectives = sorted(df["objective"].dropna().unique())
objective_sel = st.sidebar.multiselect(
    "Objective",
    objectives,
    default=objectives,
)

df = df[df["objective"].isin(objective_sel)]

campaigns = sorted(df["campaign_name"].dropna().unique())
campaign_sel = st.sidebar.selectbox("Campaign", ["All"] + campaigns)
if campaign_sel != "All":
    df = df[df["campaign_name"] == campaign_sel]

categories = sorted(df["result_category"].dropna().unique())
category_sel = st.sidebar.multiselect(
    "Result Category",
    categories,
    default=categories,
)

df = df[df["result_category"].isin(category_sel)]

date_min, date_max = df["date"].min(), df["date"].max()
date_range = st.sidebar.date_input(
    "Date range",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)

df = df[
    (df["date"] >= pd.to_datetime(date_range[0])) &
    (df["date"] <= pd.to_datetime(date_range[1]))
]

# =======================================
# KPI STRIP
# =======================================
total_conversions = df["result_value"].sum()
avg_cpa = df["cost_per_result"].mean()
spend_per_conv = (
    df["spend"].sum() / total_conversions
    if total_conversions > 0 else 0
)

c1, c2, c3 = st.columns(3)

c1.metric("Total Conversions", f"{int(total_conversions):,}")
c2.metric("Avg CPA", f"{avg_cpa:,.2f}")
c3.metric("Spend per Conversion", f"{spend_per_conv:.2f}")
          
st.divider()

# =======================================
# CPA TREND
# =======================================
st.subheader("📉 Cost per Result Trend")

trend_df = (
    df.groupby("date", as_index=False)
    .agg(cost_per_result=("cost_per_result", "mean"))
    .sort_values("date")
    .set_index("date")
)

st.line_chart(trend_df)

# =======================================
# SPEND vs CONVERSIONS
# =======================================
st.subheader("💸 Spend vs Conversions")

sv_df = (
    df.groupby("date", as_index=False)
    .agg(
        spend=("spend", "sum"),
        conversions=("result_value", "sum"),
    )
    .sort_values("date")
    .set_index("date")
)

st.line_chart(sv_df)

# =======================================
# CONVERSIONS MIX
# =======================================
st.subheader("📈 Conversions Mix by Category")

mix_df = (
    df.groupby("result_category", as_index=False)
    .agg(result_value=("result_value", "sum"))
    .sort_values("result_value", ascending=False)
)

st.bar_chart(
    mix_df.set_index("result_category")["result_value"]
)

st.caption(
    "This page is diagnostics only. "
    "No predictions or recommendation are made here."
)