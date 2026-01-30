# notebook 3- 5- train

from __future__ import annotations

#import argparse
import os
from pathlib import Path
from datetime import datetime

import pandas as pd

# -------------------------------
# Core pipeline modules
# -------------------------------
from core.n1_1_cleaning import (
    clean_campaign_name,
    extract_objective_dynamic,
    normalize_objective,
)

from core.n2_1_supermetrics_ingestion import load_supermetrics_export
# from core.n2_2_meta_ingestion import fetch_meta_daily_fact_table
# from core.n2_3_merge import build_canonical_daily_df

from core.n3_1_aggregation import aggregate_daily_campaign
from core.n3_2_features import build_metric_features
from core.n3_3_model import train_metric_model, save_model

from dotenv import load_dotenv

load_dotenv()

# ========================================================
# MARK: STEP 11 — Predictions & Risk Flags (FINAL)
# ========================================================

# =======================================================
# CONFIG
# =======================================================
#DEFAULT_MODEL_PATH = "artifacts/models/ctr_link"
SUPER_METRICS_PATH = Path(os.getenv("SUPER_METRICS_PATH"))
DATE_SINCE = os.getenv("DATE_SINCE")
#DATE_UNTIL = os.getenv("DATE_UNTIL")
DATE_UNTIL = datetime.today().strftime("%Y-%m-%d")

#DEFAULT_MIN_HISTORY_DAYS = 7
MIN_HISTORY_DAYS = int(os.getenv("MIN_HISTORY_DAYS", "7"))
DEFAULT_TEST_DAYS = 14
#DEFAULT_TARGET = "ctr_link"
RANDOM_SEED = 42

# Metrics we train (ONE model per metric)
TARGETS = [
    "ctr_link",
    "ctr_all",
    "cpc_link",
    "cpc_all",
    "cpa",
    "cpm",
    "cost_per_1000_reach",
]

# Outputs
OUTPUT_DIR = Path("data/training")
OUTPUT_CANONICAL = OUTPUT_DIR / "canonical_daily.parquet"
OUTPUT_FEATURES = OUTPUT_DIR / "features.parquet"
MODEL_DIR = Path("artifacts/models")


# ========================================================
# ENV HELPERS
# ========================================================
def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

if not SUPER_METRICS_PATH or not DATE_SINCE:
    raise RuntimeError("Missing required env vars: SUPER_METRICS_PATH or DATE_SINCE")
# ========================================================
# MAIN TRAINING PIPELINE
# ========================================================
def run_training(
        *,
        supermetrics_path: Path,
        #access_token: str,
        #ad_account_id: str,
        date_since: str,
        date_until: str,
        #model_path: Path,
        min_history_days: int,
) -> None:
    """
    End-to-end metric model training pipeline.

    Flow:
    # (ingest) Meta API
    Supermetrics
    #-> cleaning
    -> aggregation daily x campaign
    -> feature engineering
    -> model training (train one model per metric)
    -> save artifacts

    This pipeline:
    ✔ Trains a new model
    ✔ Overwrites existing artifacts
    ✖ does not run daily
    ✖ does not call LLM
    ✖ does not generate alerts
    """

    print("\n==============================")
    print("🚀 METRIC TRAINING PIPELINE START")
    print("==============================")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    #model_path.parent.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------
    # 0. INGEST — Supermetrics (authoritative metrics)
    # -------------------------------------------------
    print("[0/6] Loading Supermetrics export...")
    df_super = load_supermetrics_export(supermetrics_path)

    if df_super.empty:
        raise RuntimeError("Supermetrics export is empty.")

    """ from here i commented out Meta ingest and Merge. later i'll fix ingest and merging. """
    # # -------------------------------------------------
    # # 1. INGEST (Meta Graph API)
    # # -------------------------------------------------
    # print("[1/6] Fetching Meta data...")
    # df_raw = fetch_meta_daily_fact_table(
    #     access_token=access_token,
    #     ad_account_id=ad_account_id,
    #     date_since=date_since,
    #     date_until=date_until,
    # )

    # if df_raw.empty:
    #     raise RuntimeError("No data returned from Meta API.")
    

    # # -----------------------------------------------------
    # # 1.5 MERGE - Single Source of Truth
    # # -----------------------------------------------------
    # print("[1.5/6] Merging Supermetrics + Meta...")
    # df = build_canonical_daily_df(
    #     supermetrics_df=df_super,
    #     meta_df=df_raw,
    # )

    # if df.empty:
    #     raise RuntimeError("Merged canonical DataFrame is empty.")
    
    # # --------------------------------------------------
    # # 2. CLEANING / ONTOLOGY
    # # --------------------------------------------------
    # print(["[2/6] Cleaning campaign semantics..."])

    # if "campaign_name" in df.columns:
    #     df["campaign_name_clean"] = df["campaign_name"].apply(clean_campaign_name)
    #     df["objective_raw"] = df["campaign_name_clean"].apply(
    #         extract_objective_dynamic
    #     )
    #     df["objective"] = df["objective_raw"].apply(normalize_objective)

    # # --------------------------------------------------
    # 3. AGGREGATION (DAILY x CAMPAIGN)
    # --------------------------------------------------
    print("[3/6] Aggregating daily campaign data...")
    df_daily = aggregate_daily_campaign(df_super)
    #df_daily = aggregate_daily_campaign(df)

    if df_daily.empty:
        raise RuntimeError("Aggregation resulted in empty DataFrame.")

    df_daily.to_parquet(OUTPUT_CANONICAL, index=False)
    
    # --------------------------------------------------
    # 4. FEATURE ENGINEERING
    # --------------------------------------------------
    #print("[4/6] Building CTR features...")
    print("[4/6] Building features...")
    df_features = build_metric_features(
        df_daily,
        min_history_days=min_history_days,
    )

    if df_features.empty:
        raise RuntimeError("Feature engineering resulted in empty DataFrame.")
    
    df_features.to_parquet(OUTPUT_FEATURES, index=False)
    
    # ---------------------------------------------------
    # 5. MODEL TRAINING
    # ---------------------------------------------------
    print("[5/6] Training metric model...")

    for target in TARGETS:
        if target not in df_features.columns:
            print(f"⚠️ Skipping {target}: column not found")
            continue

        print(f"\n🧠 Training model for: {target}")

        model, metadata = train_metric_model(
            df_features,
            #target=DEFAULT_TARGET,
            target=target,
            test_days=DEFAULT_TEST_DAYS,
            random_seed=RANDOM_SEED,
            
        )
        # =============================================
        # 6. SAVE ARTIFACTS
        # ============================================
        save_model(
            model,
            metadata,
            MODEL_DIR / target,
        )
    
    print("\n==============================")
    print("✅ TRAINING COMPLETE")
    print("==============================")
    print(f"📦 Models saved to: {MODEL_DIR.resolve()}")
    print(f"🧠 Targets trained: {TARGETS}")    
        #model_path.parent.mkdir(parents=True, exist_ok=True)
    """
    # ---------------------------------------------------
    # 6. SAVE ARTIFACTS
    # ---------------------------------------------------
    print("[6/6] Saving model artifacts...")
    save_model(model, metadata, str(model_path))

    # print("\n✅ Training complete")
    # print(f"Model saved to: {model_path}")
    # print(f"Metrics: {metadata['metrics']}")

    print("\n✅ TRAINING COMPLETE")
    print(f"📦 Model path: {model_path.resolve()}")
    print(f"📊 Metrics: {metadata['metrics']}")
    print(f"🧠 Features used: {len(metadata['features'])}")
    """


# ====================================================
# CLI Entrypoint
# ====================================================

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Metric Model Training Pipeline")

    # parser.add_argument(
    #     "--supermetrics-path",
    #     type=Path,
    #     required=True,
    #     help="Path to Supermetrics export CSV",
    # )

    # # parser.add_argument("--access-token", required=True, help="Meta API access token")
    # # parser.add_argument("--ad-account-id", required=True, help="Meta Ad Account ID")
    # #parser.add_argument("--date-since", required=True, help="Start date (YYYY-MM-DD)")
    # parser.add_argument(
    #     "--date-since",
    #     default=os.getenv("DATE_SINCE"),
    #     help="Start date (YYYY-MM-DD). Can be set via env.",
    # )
    
    # parser.add_argument(
    #     "--date-until",
    #     default=os.getenv("DATE_UNTIL", datetime.today().strftime("%Y-%m-%d")),
    #     help="End date (YYYY-MM-DD). Defaults to today.",
    # )
    
    # # parser.add_argument(
    # #     "--model-path",
    # #     type=Path,
    # #     default=DEFAULT_MODEL_PATH,
    # #     help="Path prefix to save model artifacts",
    # # )
    # parser.add_argument(
    #     "--min-history-days",
    #     type=int,
    #     #default=DEFAULT_MIN_HISTORY_DAYS,
    #     default=MIN_HISTORY_DAYS,
    #     help="Minimum history days to use for feature engineering",
    # )

    # args = parser.parse_args()

    # if not args.date_since:
    #     raise RuntimeError("date-since must be provided via CLI or DATE_SINCE env var")


    # --------- secrets from environment ----------
    """ for now, unused since i am using supermetrics ingestion only """
    # access_token = require_env("META_ACCESS_TOKEN")
    # ad_account_id = require_env("META_AD_ACCOUNT_ID")

    #Path(args.model_path).parent.mkdir(parents=True, exist_ok=True)

    run_training(
        #supermetrics_path=args.supermetrics_path,
        supermetrics_path=SUPER_METRICS_PATH,
        date_since=DATE_SINCE,
        date_until=DATE_UNTIL,
        min_history_days=MIN_HISTORY_DAYS,
        # access_token=args.access_token,
        # ad_account_id=args.ad_account_id,
        # access_token=access_token,
        # ad_account_id=ad_account_id,
        #date_since=args.date_since,
        #date_until=args.date_until,
        #model_path=args.model_path,
        #min_history_days=args.min_history_days,
    )
