
from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd


# ===================================================
# Pipeline imports
# ===================================================
from pipelines.file2_n4_daily_refresh import run_daily_refresh

# ===================================================
# CONFIG
# ===================================================
DATA_DIR = Path("data/predictions")
CANONICAL_PATH = DATA_DIR / "canonical_daily.parquet"
PREDICTIONS_PATH = DATA_DIR / "predictions.parquet"
ALERTS_PATH = DATA_DIR / "alerts.parquet"

SUPERMETRICS_PATH = Path("data/raw/supermetrics_export.csv")
MODEL_PATH = Path("artifacts/models/ctr_link")

DEFAULT_LOOKBACK_DAYS = 120

# ===================================================
# Helpers
# ===================================================
def file_info(path: Path) -> dict:
    if not path.exists():
        return {"exists": False}
    
    return {
        "exists": True,
        "last_modified": datetime.fromtimestamp(path.stat().st_mtime),
        "size_mb": round(path.stat().st_size / (1024 * 1024), 2),
    }

def count_rows(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        return len(pd.read_parquet(path))
    except Exception:
        return None
    
# ===================================================
# PAGE
# ===================================================
st.set_page_config(
    page_title="Admin - Date Pipeline Control",
    layout="centered",
)

st.title("🛠 Admin Control Panel")
st.caption("Operator-only. Triggers pipelines. No analytics")

st.divider()

# ==================================================
# ENV CHECK
# ==================================================
st.subheader("🔐 Environment Status")

missing_env = []
for key in ["META_ACCESS_TOKEN", "META_AD_ACCOUNT_ID"]:
    if key not in os.environ or not os.environ[key]:
        missing_env.append(key)

if missing_env:
    st.error(f"Missing environment variables: {', '.join(missing_env)}")
    st.stop()
else:
    st.success("All required environment variables are set.")

# ===================================================
# DATA STATUS
# ===================================================
st.subheader("📊 Data Artifacts Status")

cols = st.columns(5)

with cols[0]:
    st.markdown("**Canonical Dataset**")
    info = file_info(CANONICAL_PATH)
    if info["exists"]:
        st.write(f"🕒 Last Modified: {info['last_modified']}")
        st.write(f"📏 Size: {info['size_mb']} MB")
        st.write(f"📊 Rows: {count_rows(CANONICAL_PATH)} ")
    else:
        st.warning("Not found")

with cols[1]:
    st.markdown("**Predictions**")
    info = file_info(PREDICTIONS_PATH)
    if info["exists"]:
        st.write(f"🕒 Last Modified: {info['last_modified']}")
        st.write(f"📏 Size: {info['size_mb']} MB")
        st.write(f"📊 Rows: {count_rows(PREDICTIONS_PATH)} ")
    else:
        st.warning("Not found")

with cols[2]:
    st.markdown("**Alerts**")
    info = file_info(ALERTS_PATH)
    if info["exists"]:
        st.write(f"🕒 Last Modified: {info['last_modified']}")
        st.write(f"📏 Size: {info['size_mb']} MB")
        st.write(f"📊 Rows: {count_rows(ALERTS_PATH)} ")
    else:
        st.warning("Not found")

st.divider()

# ==================================================
# SUPERMETRICS INSTRUCTIONS
# =================================================
st.subheader("📥 Supermetrics Export Instructions")

st.info(
    """
    **Step 1**
    Refresh Supermetrics inside Google Sheets.

    **Step 2**
    Export the updated sheet as CSV.

    **Step 3**
    Overwrite the file below:
    """
)

st.code(str(SUPERMETRICS_PATH.resolve()))

if SUPERMETRICS_PATH.exists():
    st.success("File found.")
else:
    st.warning("File not found. Please follow the instructions above.")

st.divider()

# ==================================================
# PIPELINE BUTTONS
# ==================================================
st.subheader("② Pipeline Execution")

col_a, col_b = st.columns(2)

# =================================================
# Run Meta Ingestion (standard daily refresh)
# =================================================
with col_a:
    if st.button("▶ Run Meta Ingestion (Daily Refresh)", use_container_width=True):
        with st.spinner("Running Meta Ingestion Pipeline..."):
            try:
                run_daily_refresh(
                    supermetrics_path=SUPERMETRICS_PATH,
                    access_token=os.environ["META_ACCESS_TOKEN"],
                    ad_account_id=os.environ["META_AD_ACCOUNT_ID"],
                    date_since=(datetime.now() - pd.Timedelta(days=30)).strftime("%Y-%m-%d"),
                    date_until=datetime.now().strftime("%Y-%m-%d"),
                    model_path=MODEL_PATH,
                    min_history_days=7,
                )
                st.success("✅ Meta Ingestion Pipeline completed successfully.")
            except Exception as e:
                st.error(f"❌ Pipeline failed with error: {e}")

# =================================================
# Rebuild Canonical Dataset (wider range)
# ================================================
with col_b:
    if st.button("♻ Rebuild Canonical Dataset (Full)", use_container_width=True):
        with st.spinner("Rebuilding Canonical Dataset..."):
            try:
                run_daily_refresh(
                    supermetrics_path=SUPERMETRICS_PATH,
                    access_token=os.environ["META_ACCESS_TOKEN"],
                    ad_account_id=os.environ["META_AD_ACCOUNT_ID"],
                    date_since=(datetime.now() - pd.Timedelta(days=DEFAULT_LOOKBACK_DAYS)).strftime("%Y-%m-%d"),
                    date_until=datetime.now().strftime("%Y-%m-%d"),
                    model_path=MODEL_PATH,
                    min_history_days=7,
                )
                st.success("✅ Canonical Dataset rebuilt successfully.")
            except Exception as e:
                st.error(f"❌ Pipeline failed with error: {e}")

st.divider()

# ==================================================
# FOOTER
# ==================================================
st.caption(
    "⚠ This page is for operators only." \
    "© 2026 INVOKE Analytics. All rights reserved."
)

