from __future__ import annotations

import pandas as pd
import streamlit as st
from pathlib import Path

# =========================================
# CONFIG
# =========================================
DATA_PATH = Path("data/predictions/predictions.parquet")

st.set_page_config(
    page_title="Action Recommendations",
    layout="wide",
)

st.title("🚦 Action Recommendations")
st.caption("What needs attention today")

# =========================================
# LOAD DATA
# ========================================
if not DATA_PATH.exists():
    st.error("No prediction data found. Run daily refresh first.")
    st.stop()

df = pd.read_parquet(DATA_PATH)

# =========================================
# VALIDATION
# =========================================
required_cols = {
    "campaign_name",
    #"client",
    "ctr_drop_flag",
    "spend_spike_flag",
    "retargeting_pool_large",
    "reason",
    "action",
    "summary",
    "alert_msg",
}

missing = required_cols - set(df.columns)
if missing:
    st.error(f"Missing required columns: {missing}")
    st.stop()

# Use latest date only
df["date"] = pd.to_datetime(df["date"])
latest_date = df["date"].max()
df = df[df["date"] == latest_date]

# ========================================
# FILTERS
# ========================================
st.sidebar.header("Filters")

clients = sorted(df["client"].dropna().unique())
client_sel = st.sidebar.selectbox("Client", clients)

df = df[df["client"] == client_sel]

issues_only = st.sidebar.checkbox(
    "Show only campaigns with issues",
    value=True,
)

if issues_only:
    df = df[
        (df["ctr_drop_flag"] == 1)
        | (df["spend_spike_flag"] == 1)
        | (df["retargeting_pool_large"] == 1)
    ]

# ========================================
# PRIORITY SORTING (DISPLAY ONLY)
# ========================================
def priority_score(row) -> int:
    if row["ctr_drop_flag"] == 1:
        return 1
    if row["spend_spike_flag"] == 1:
        return 2
    if row["retargeting_pool_large"] == 1:
        return 3
    return 4

df["priority"] = df.apply(priority_score, axis=1)
df = df.sort_values("priority")

# ========================================
# STATUS RENDERING
# ========================================
def render_status(row) -> tuple[str: str]:
    if row["ctr_drop_flag"] == 1 or row["spend_spike_flag"] == 1:
        return "⚠", "At Risk"
    if row["retargeting_pool_large"] == 1:
        return "ℹ", "Opportunity"
    return "✅", "Healthy"

# ========================================
# ALERT CARDS
# ========================================
if df.empty:
    st.success("✅ All campaigns are healthy today.")
    st.stop()

for _, row in df.iterrows():
    icon, status = render_status(row)

    with st.container(border=True):
        c1, c2 = st.columns([1, 6])

        with c1:
            st.markdown(
                f"<h1 style='text-align:center'>{icon}</h1>",
                unsafe_allow_html=True,
            )
        
        with c2:
            st.markdown(f"### {row['campaign_name']}")
            st.markdown(f"**Status:** {status}")
            st.markdown(f"**Summary**: {row['summary']}")
            st.markdown(f"**Reason**: {row['reason']}")
            st.markdown(f"**Recommend Action:** {row['action']}")

            if row["alert_msg"]:
                with st.expander("📣 Alert message (WhatsApp-ready)"):
                    st.code(row["alert_msg"])

# ========================================
# FOOTER
# ========================================
st.caption(
    f"Recommendations generated on {latest_date.strftime('%Y-%m-%d')} ."
    "This page displays decisions, it does not create them.?"
)