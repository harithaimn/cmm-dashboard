# file2a - Meta ingestion checkpoint

from __future__ import annotations

import argparse
import os
from pathlib import Path
from datetime import datetime

import pandas as pd

from core.n2_2_meta_ingestion import fetch_meta_daily_fact_table

# =============================
# CONFIG
# =============================
OUTPUT_DIR = Path("data/raw")
OUTPUT_META = OUTPUT_DIR / "meta.parquet"
OUTPUT_META_INFO = OUTPUT_DIR / "meta_info.parquet"

# =============================
# ENV HELPERS
# =============================
def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

# =============================
# META INGESTION (CHECKPOINTED)
# =============================
def run_meta_ingestion(
    *,
    ad_account_id: str,
    access_token: str,
    date_since: str,
    date_until: str,
) -> None:
    print("\n===============================")
    print("⬇️ META INGESTION START")
    print("===============================")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df_meta = fetch_meta_daily_fact_table(
        access_token=access_token,
        ad_account_id=ad_account_id,
        date_since=date_since,
        date_until=date_until
    )

    if df_meta.empty:
        raise RuntimeError("Meta ingestion returned empty DataFrame.")
    
    df_meta.to_parquet(OUTPUT_META, index=False)

    info = {
        "rows": len(df_meta),
        "date_since": date_since,
        "date_until": date_until,
        "generated_at": datetime.utcnow().isoformat(),
    }

    pd.Series(info).to_json(OUTPUT_META_INFO)

    print("✅ META INGESTION COMPLETE.")
    print(f"Saved -> {OUTPUT_META.resolve()}")
    print(f"Rows -> {len(df_meta)}")

# =============================
# CLI
# =============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser("Meta ingestion checkpoint")

    parser.add_argument("--date-since", required=True)
    parser.add_argument("--date-until", required=True)

    args = parser.parse_args()

    run_meta_ingestion(
        access_token=require_env("META_ACCESS_TOKEN"),
        ad_account_id=require_env("META_AD_ACCOUNT_ID"),
        date_since=args.date_since,
        date_until=args.date_until,
    )