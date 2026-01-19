from __future__ import annotations

import pandas as pd
import streamlit as st
from pathlib import Path

# =========================================
# CONFIG
# =========================================
DATA_PATH = Path("data/predictions/predictions.parquet")

st.set_page_config(
    page_title="Creative Effectiveness",
    layout="wide",
)

st.title("🎨 Creative Effectiveness")
st.caption("Campaign-level creative performance & fatigue signals.")

# ========================================
# LOAD DATA
# ========================================
if not DATA_PATH.exists():
    st.error("Prediction data not found. Run Daily Refresh first.")
    st.stop()

df = pd.read_parquet(DATA_PATH)

# ========================================
# VALIDATION
# ========================================
required_cols = {
    "date",
    #client,
    "campaign_id",
    "campaign_name",
    "ctr_link",
    "ctr_link_roll_7",
    "pred_ctr_link",
    "ctr_drop_flag",
    "impressions",
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

campaigns = sorted(df["campaign_name"].unique())
campaign_sel = st.sidebar.selectbox("Campaign", ["All"] + campaigns)

if campaign_sel != "All":
    df = df[df["campaign_name"] == campaign_sel]

# ========================================
# KPI Strip
# ========================================
fatigue_rate = (df["ctr_drop_flag"] == 1).mean() * 100

c1, c2, c3 = st.columns(3)

c1.metric("Avg CTR", f"{df['ctr_link'].mean():.2%}")
c2.metric("Creative Fatigue Risk", f"{fatigue_rate:.1f}%")
c3.metric("Avg Impressions", f"{df['impresssions'].mean():,.0f}")

st.divider()

# ========================================================
# CTR Trend (Per Campaign)
# =======================================================
st.subheader("📈 CTR Trend vs Prediction")

if campaign_sel == "All":
    st.info("Select a campaign to view CTR trend.")
else:
    plot_df = df.sort_values("date")

    st.line_chart(
        plot_df.set_index("date")[
            ["ctr_link", "pred_ctr_link", "ctr_link_roll_7"]
        ]
    )

# ========================================================
# CREATIVE FATIGUE TABLE
# ========================================================
st.subheader(" Creative Fatigue Risk")

table_df = (
    df
    .groupby(["campaign_id", "campaign_name"])
    .agg(
        ctr=("ctr_link", "mean"),
        ctr_7d=("ctr_link_roll_7", "mean"),
        pred_ctr=("pred_ctr_link", "mean"),
        impressions=("impressions", "sum"),
        fatigue_flag=("ctr_drop_flag", "max"),
    )
    .reset_index()
)

table_df["ctr_delta"] = table_df["ctr"] - table_df["pred_ctr"]

def highlight_fatigue(val):
    if val == 1:
        return "background-color: #ffcccc;font-weight:bold"
    return ""

st.dataframe(
    table_df[
        [
            "campaign_name",
            "ctr",
            "ctr_7d",
            "pred_ctr",
            "ctr_delta",
            "impressions",
            "fatigue_flag",
        ]
    ]
    .rename(columns={
        "campaign_name": "Campaign",
        "ctr": "CTR",
        "ctr_7d": "7-Day Avg CTR",
        "pred_ctr": "Predicted CTR",
        "ctr_delta": "CTR Delta",
        "impressions": "Impressions",
        "fatigue_flag": "Creative Fatigue Risk",
    })
    .style
    .applymap(highlight_fatigue, subset=["Fatigue Risk"]),
    use_container_width=True,
)

st.caption(
    "Fatigue Risk is derived from rule-based signals." \
    "This page does not compute flags."
)