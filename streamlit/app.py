from __future__ import annotations

import os
import streamlit as st

"""
I will redo this to cover it all.
"""

# ===================================================
# CONFIG
# ===================================================
# DATA_DIR = Path("data/predictions")
# PREDICTIONS_FILE = DATA_DIR / "daily_predictions_latest.csv"
# ALERTS_FILE = DATA_DIR / "daily_alerts_latest.csv"

# ===================================================
# APP CONFIG
# ===================================================
st.set_page_config(
    page_title="Client Marketing Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ===================================================
# ROLE CONFIG
# ===================================================
# Simple role toggle (upgrade later if needed)
IS_ADMIN = os.getenv("IS_ADMIN", "false").lower() == "true"

# st.title("📊 CMM Predictions and Monitoring Dashboard")
# st.caption("Predictions, alerts, and recommendations (read-only)")

# ===============================================
# HEADER
# ================================================
st.markdown(
    """
    <style>
        .app-title {
            font-size: 26px;
            font-weight: 600;
            margin-bottom: 0;
        }
        .app-subtitle {
            font-size: 14px;
            color: #666;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown("<div class='app-title'> 📊 CMM Predictions and Monitoring Dashboard</div>", unsafe_allow_html=True)
st.markdown("<div class='app-subtitle'> Predictions, alerts, and recommendations</div>", unsafe_allow_html=True)


# ===================================================
# SIDEBAR
# ===================================================
st.sidebar.title("Navigation")

st.sidebar.markdown("---")

st.sidebar.caption("Client Views")

st.sidebar.page_link(
    "pages/0_Home.py",
    label="🏠 Portfolio Overview",
)

st.sidebar.page_link(
    "pages/1_Overview.py",
    label="📈 Performance Overview",
)

st.sidebar.page_link(
    "pages/2_Creative_Effectiveness.py",
    label="🎨 Creative Effectiveness",
)

st.sidebar.page_link(
    "pages/3_Audience.py",
    label="🧠 Audience Structure",
)

st.sidebar.page_link(
    "pages/4_Offer_Conversion_insights.py",
    label="💰 Offer & Conversion Insights",
)

st.sidebar.page_link(
    "pages/5_Recommendations.py",
    label="🚦 Action Recommendations",
)


# ===================================================
# ADMIN SECTION
# ===================================================
if IS_ADMIN:
    st.sidebar.markdown("---")
    st.sidebar.caption("Admin")

    st.sidebar.page_link(
        "pages/0_Admin.py",
        label="🛠 Admin Control Panel",
    )

# ===================================================
# FOOTER
# ===================================================
st.sidebar.markdown("---")
st.sidebar.caption("© INVOKE Analytics 2026")

# # ===================================================
# # DATA LOADING (NO COMPUTE)
# # ===================================================
# @st.cache_data(ttl=300)
# def load_data():
#     if not PREDICTIONS_FILE.exists():
#         st.error("Prediction file not found. Run daily_refresh pipeline first.")
#         st.stop()

#     df_pred = pd.read_csv(PREDICTIONS_FILE)

#     if ALERTS_FILE.exists():
#         df_alerts = pd.read_csv(ALERTS_FILE)
#     else:
#         df_alerts = pd.DataFrame()

#     return df_pred, df_alerts

# df, df_alerts = load_data()


# """
# This will be inside each tab, and maybe down below.
# """
# # =====================================
# # SIDEBAR FILTERS
# # =====================================
# st.sidebar.header("Filters")

# """
# This, I can set the date. Filter >3 months.
# Filter by Campaign active.
# Etc

# """
# # Date filter
# df["date"] = pd.to_datetime(df["date"])

# min_date = df["date"].min()
# max_date = df["date"].max()

# date_range = st.sidebar.date_input(
#     "Date range",
#     value=(min_date, max_date),
#     min_value=min_date,
#     max_value=max_date,
# )

# # Campaign filter
# campaigns = ["All"] + sorted(df["campaign_name"].dropna().unique().tolist())
# selected_campaign = st.sidebar.selectbox("Campaign", campaigns)

# # Alerts-only toggle
# alert_only = st.sidebar.checkbox("Show only alerts", value=False)


# # ==============================================
# # APPLY FILTERS
# # ==============================================
# df_view = df.copy()

# if date_range:
#     start, end = date_range
#     df_view = df_view[
#         (df_view["date"] >= pd.to_datetime(start)) &
#         (df_view["date"] <= pd.to_datetime(end))
#     ]

# if selected_campaign != "All":
#     df_view = df_view[df_view["campaign_name"] == selected_campaign]

# if alert_only:
#     df_view = df_view[df_view["alert_msg"].notna() & (df_view["alert_msg"] != "")]


# # =====================================
# # KPI Summary
# # =====================================
# st.subheader("Key Metrics")

# col1, col2, col3, col4 = st.columns(4)

# col1.metric("Rows", len(df_view))
# col2.metric("Campaigns", df_view["campaign_id"].nunique())
# col3.metric("Alerts", int((df_view["alert_msg"] != "").sum()))
# col4.metric(
#     "Avg CTR",
#     f"{df_view['ctr_link'].mean():.4f}" if "ctr_link" in df_view else "-"
# )

# # ======================================
# # ALERTS SECTION
# # ======================================
# st.subheader("🚨 Active Alerts")

# if df_alerts.empty:
#     st.info("No alerts detected.")
# else:
#     st.dataframe(
#         df_alerts[
#             [
#                 "date",
#                 "campaign_name",
#                 "summary",
#                 "reason",
#                 "action",
#                 "alert_msg",
#             ]
#         ],
#         use_container_width=True,
#     )

# # =======================================
# # CTR Trend Chart
# # =======================================
# st.subheader("CTR Trend")

# if {"date", "ctr_link", "pred_ctr_link"}.issubset(df_view.columns):
#     chart_df = (
#         df_view
#         .sort_values("date")
#         .set_index("date")[["ctr_link", "pred_ctr_link"]]
#     )

#     st.line_chart(chart_df)
# else:
#     st.warning("CTR columns missing from dataset.")


# # =========================================
# # DETAILED TABLE
# # =========================================
# st.subheader("Detailed Campaign Table")

# display_cols = [
#     "date",
#     "campaign_name",
#     "ctr_link",
#     "pred_ctr_link",
#     "ctr_diff",
#     "ctr_pct_change",
#     "ctr_drop_flag,"
#     "spend_spike_flag",
#     "retargeting_pool_large",
#     "summary",
#     "action",
# ]

# display_cols = [c for c in display_cols if c in df_view.columns]

# st.dataframe(
#     df_view[display_cols]
#     .sort_values("date", ascending=False),
#     use_container_width=True,
# )

# # ====================================
# # FOOTER
# # ===================================
# st.caption(
#     "⚠ This dashbooard is read-only."
#     "All predictions are generated by the daily_refresh pipeline."
# )