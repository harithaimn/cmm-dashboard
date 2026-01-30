# notebook 4- 1- daily refresh

from __future__ import annotations

import argparse
import os
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

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
from core.n2_2_meta_ingestion import fetch_meta_daily_fact_table
from core.n2_3_merge import build_canonical_daily_df

from core.n3_1_aggregation import aggregate_daily_campaign
from core.n3_2_features import build_metric_features
from core.n3_3_model import load_model, predict_ctr
from core.n3_4_rules import generate_signals

from core.n3_5_llm import (
    get_openai_client,
    generate_llm_explanation,
)

# ===================================================
# CONFIG
# ===================================================
DEFAULT_MODEL_PATH = Path("artifacts/models/ctr_link")
DEFAULT_MIN_HISTORY_DAYS = 7

OUTPUT_DIR = Path("data/predictions")
OUTPUT_CANONICAL = OUTPUT_DIR / "canonical_daily.parquet"
OUTPUT_FEATURES = OUTPUT_DIR / "features.parquet"
OUTPUT_PREDICTIONS = OUTPUT_DIR / "predictions.parquet"
OUTPUT_ALERTS = OUTPUT_DIR / "alerts.parquet"

META_CHECKPOINT = Path("data/raw/meta.parquet")

# ===================================================
# LLM CONFIG (ENV-DRIVEN)
# ===================================================
ENABLE_LLM = os.getenv("ENABLE_LLM", "true").lower() == "true"
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4.1-mini")
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.3"))

# ===================================================
# ENV HELPERS
# ===================================================
def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

# ===================================================
# DAILY REFRESH PIPELINE
# ===================================================
def run_daily_refresh(
        *,
        supermetrics_path: Path,
        # access_token: str,
        # ad_account_id: str,
        date_since: str,
        date_until: str,
        model_path: str,
        min_history_days: int,
) -> None:
    """
    Daily Prediction + recommendation pipeline.

    Flow: 
    Supermetrics
    Meta API
    -> cleaning
    -> aggregation
    -> feature engineering
    -> load trained model
    -> predict CTR
    -> apply rules
    -> write outputs
    """

    print("\n==============================")
    print("🔄 DAILY REFRESH PIPELINE START")
    print("==============================")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_date = datetime.utcnow().strftime("%Y-%m-%d")

    client = None
    llm_enabled = ENABLE_LLM

    if llm_enabled:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("⚠️ No OpenAI API key found. Disabling LLM.")
            llm_enabled = False
        else:
            client = get_openai_client(api_key)

    #total_steps = 8

    steps = [
        "Load Supermetrics",
        "Aggregate daily campaign",
        "Build features",
        "Load model",
        "Predict",
        "Apply rules",
        "Write outputs",
    ]
    pbar = tqdm(total=len(steps), desc="Pipeline", unit="step")

    # -------------------------------------------------
    # 0. INGEST — Supermetrics (authoritative metrics)
    # -------------------------------------------------
    print("[0/8] Loading Supermetrics export...")
    df_super = load_supermetrics_export(supermetrics_path)

    if df_super.empty:
        raise RuntimeError("Supermetrics export is empty.")
    pbar.update(1)
    
    # -------------------------------------------------
    # 1. INGEST Latest Data (Meta Graph API)
    # -------------------------------------------------
    # print("[1/8] Fetching latest Meta data...")
    # df_raw = fetch_meta_daily_fact_table(
    #     access_token=access_token,
    #     ad_account_id=ad_account_id,
    #     date_since=date_since,
    #     date_until=date_until,
    # )

    # if df_raw.empty:
    #     raise RuntimeError("No data returned from Meta API.")
    # pbar.update(1)

    """ from here, i commented out since i tested using supermetrics only for now. i'll come back later to fix meta pipeline and merging."""
# """
#     print("[1/8] Loading Meta data...")

#     if META_CHECKPOINT.exists():
#         print("↪ Using cached Meta ingestion")
#         df_raw = pd.read_parquet(META_CHECKPOINT)
#     else:
#         print("⬇ Fetching Meta from API")
#         df_raw = fetch_meta_daily_fact_table(
#             access_token=access_token,
#             ad_account_id=ad_account_id,
#             date_since=date_since,
#             date_until=date_until,
#         )
#         df_raw.to_parquet(META_CHECKPOINT, index=False)
#         print(f"✅ Meta saved to {META_CHECKPOINT}")

#     if df_raw.empty:
#             raise RuntimeError("No data returned from Meta API.")
#     pbar.update(1)
# """
    # ----------------------------------------------
    # 1.5 Merge - Single Source of Truth
    # ----------------------------------------------
# """
#     print("[1.5/8] Merging Supermetrics + Meta...")
#     df = build_canonical_daily_df(
#         supermetrics_df=df_super,
#         meta_df=df_raw,
#     )

#     if df.empty:
#         raise RuntimeError("Merged canonical DataFrame is empty.")
    
#     df.to_parquet(OUTPUT_CANONICAL, index=False)
#     pbar.update(1)
# """
#     # -------------------------------------------------
#     # 2. CLEANING / ONTOLOGY
#     # -------------------------------------------------
# """
#     print("[2/8] Cleaning campaign semantics...")

#     if "campaign_name" in df.columns:
#         df["campaign_name_clean"] = df["campaign_name"].apply(
#             clean_campaign_name
#         )
#         df["objective_raw"] = df["campaign_name_clean"].apply(
#             extract_objective_dynamic
#         )
#         df["objective"] = df["objective_raw"].apply(normalize_objective)

#     pbar.update(1)
# """

    # -------------------------------------------------
    # 3. AGGREGATION
    # -------------------------------------------------
    print("[3/8] Aggregating daily campaign data...")
    #df_daily = aggregate_daily_campaign(df)
    df_daily = aggregate_daily_campaign(df_super)

    if df_daily.empty:
        raise RuntimeError("Aggregation resulted in empty DataFrame.")
    pbar.update(1)
    
    # -------------------------------------------------
    # 4. FEATURE ENGINEERING
    # -------------------------------------------------
    print("[4/8] Building Metric features...")  # not ctr centric, but all
    df_features = build_metric_features(df_daily, min_history_days=min_history_days)

    if df_features.empty:
        raise RuntimeError("Feature engineering resulted in empty DataFrame.")
    
    df_features.to_parquet(OUTPUT_FEATURES, index=False)
    pbar.update(1)
    
    # ----------------------------------------------------
    # 5. LOAD TRAINED MODEL
    # ----------------------------------------------------
    print("[5/8] Loading trained models...")

    # model, metadata = load_model(model_path)
    model_dir = Path(model_path)
    if not model_dir.exists():
        raise RuntimeError(f"Model directory not found: {model_dir}")

    models = {}

    for model_file in model_dir.glob("*.joblib"):
        metric = model_file.stem
        print(f"↪ Loading model for {metric}")
        model, metadata = load_model(model_dir / metric)
        models[metric] = (model, metadata)

    if not models:
        raise RuntimeError("No trained models found in model directory.")

    pbar.update(1)

    # -----------------------------------------------------
    # 6. PREDICT METRICS  (all , not just CTR)
    # -----------------------------------------------------
    print("[6/8] Predicting metrics...")

    for metric, (model, metadata) in models.items():
        df_features[f"pred_{metric}"] = predict_ctr(
            model=model,
            df=df_features,
            feature_cols=metadata["features"],
            output_name=f"pred_{metric}",
        )

    pbar.update(1)

    # -------------------------------------------------------
    # 7. APPLY RULES
    # -------------------------------------------------------
    print("[7/8] Generating recommendations...")
    df_out = generate_signals(df_features)

    pbar.update(1)

    # -------------------------------------------------------
    # 7.5 LLM EXPLANATIONS (Human-readable layer)
    # -------------------------------------------------------
    if llm_enabled:
        print("[7.5/8] Generating LLM explanations...")

        llm_client = get_openai_client(os.getenv("OPENAI_API_KEY"))

        # reasons = []
        explanations = []
        recommendations = []
        summaries = []

        # llm_texts = []

        for _, row in df_out.iterrows():
            # Only explain rows with actual signals
            if row.get("signal_count", 0) == 0:
                # reasons.append("")
                explanations.append("")
                recommendations.append("")
                summaries.append("Performance is stable.")
                #llm_texts.append("")
                continue
            
            payload = {
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name"),
                "date": str(row.get("date")),
                "overall_severity": row["max_severity"],
                "signals": {
                    metric: {
                        "severity": row[f"{metric}_severity"],
                        "ratio": round(row[f"{metric}_ratio"], 2),
                        "actual": round(row[metric], 4),
                        "predicted": round(row[f"pred_{metric}"], 4),
                    }
                    for metric in [
                        "ctr_link",
                        "ctr_all",
                        "cpc_link",
                        "cpc_all",
                        "cpa",
                        "cpm",
                        "cost_per_1000_reach",
                    ]
                    if f"{metric}_flag" in row and row[f"{metric}_flag"] == 1
                }
            }

            try:
                text = generate_llm_explanation(
                    client=llm_client,
                    payload=payload,
                )

                explanations.append(text["explanation"])
                recommendations.append(text["recommendation"])
                summaries.append(text["summary"])

            except Exception as e:
                print(f"LLM failed to generate explanation for {row['campaign_name']}.")
                print(e)
                explanations.append("")
                recommendations.append("")
                summaries.append("LLM error.")
            
        df_out["llm_explanation"] = explanations
        df_out["llm_recommendation"] = recommendations
        df_out["llm_summary"] = summaries

                # """
                # # Actual performance
                # "metrics_actual": {
                #     k: row.get(k)
                #     for k in (
                #         "ctr_link", "ctr_all",
                #         "cpc_link", "cpc_all",
                #         "cpa", "cpm",
                #         "cost_per_1000_reach",
                #     )
                #     if k in row
                # },
                # """

                # "ctr_link": row.get("ctr_link"),
                # "ctr_all": row.get("ctr_all"),
                # "cpc_link": row.get("cpc_link"),
                # "cpc_all": row.get("cpc_all"),
                # "cpa": row.get("cpa"),
                # "cpm": row.get("cpm"),
                # "cost_per_1000_reach": row.get("cost_per_1000_reach"),

                # """
                # # Model Predictions
                # "metrics_predicted": {
                #     k: row.get(f"pred_{k}")
                #     for k in (
                #         "ctr_link", "ctr_all",
                #         "cpc_link", "cpc_all",
                #         "cpa", "cpm",
                #         "cost_per_1000_reach",
                #     )
                #     if f"pred_{k}" in row
                # },
                # """

                # "pred_ctr_link": row.get("pred_ctr_link"),
                # "pred_ctr_all": row.get("pred_ctr_all"),
                # "pred_cpc_link": row.get("pred_cpc_link"),
                # "pred_cpc_all": row.get("pred_cpc_all"),
                # "pred_cpa": row.get("pred_cpa"),
                # "pred_cpm": row.get("pred_cpm"),
                # "pred_cost_per_1000_reach": row.get("pred_cost_per_1000_reach"),

                # Signals
                # "signals": {
                #     k: row[k]
                #     for k in row.index
                #     if k.endswith("_flag")
                # }
            #     """
            #     "signals": [
            #         k.replace("_flag", "")
            #         for k in row.index
            #         if k.endswith("_flag") and row[k] == 1
            #     ],
            # }
            # """

        #     """
        #     try:
        #         explanation = generate_llm_explanation(
        #             client=client,
        #             payload=payload,
        #             model=LLM_MODEL,
        #             temperature=LLM_TEMPERATURE,
        #         )

        #         # reasons.append(llm_out["reason"])
        #         # recommendations.append(llm_out["recommendation"])
        #         # summaries.append(llm_out["summary"])
        #     except Exception as e:
        #         print(f" ⚠️ LLM failed for campaign {row.get('campaign_id')} / {row.get('campaign_name')}: {e}")
        #         # reasons.append("")
        #         # recommendations.append("")
        #         # summaries.append("")
        #         explanation = ""
        #     """

        #     """ llm_texts.append(explanation) """
        
        # """ df_out["llm_explanation"] = llm_texts """
        # df_out["llm_reason"] = reasons
        # df_out["llm_recommendation"] = recommendations
        # df_out["llm_summary"] = summaries


    # -------------------------------------------------------
    # 8. WRITE OUTPUTS
    # -------------------------------------------------------
    print(["[8/8] Writing outputs..."])

    # Full predictions
    # full_path = OUTPUT_DIR / "daily_predictions_latest.csv"
    # df_out.to_csv(full_path, index=False)

    # # Alerts-only view
    # alerts = df_out[df_out["alert_msg"] != ""]
    # alerts_path = OUTPUT_DIR / "daily_alerts_latest.csv"
    # alerts.to_csv(alerts_path, index=False)

    df_out.to_parquet(OUTPUT_PREDICTIONS, index=False)

    # alerts = df_out[df_out["alert_msg"].notna() & (df_out["alert_msg"] != "")]
    # alerts.to_parquet(OUTPUT_ALERTS, index=False)

    alerts = df_out[df_out["signal_count"] > 0]

    alerts.to_parquet(OUTPUT_ALERTS, index=False)
    
    pbar.update(1)
    pbar.close()

    print("\n==============================")
    print("✅ DAILY REFRESH COMPLETE")
    print("==============================")
    print(f"Run date    -> {run_date}")
    print(f"Predictions -> {OUTPUT_PREDICTIONS.resolve()}")
    print(f"Alerts      -> {OUTPUT_ALERTS.resolve()}")
    print(f"Rows        -> {len(df_out)}")
    # print(f"Run date    -> {run_date}")

# =====================================================
# CLI Entrypoint
# =====================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily Metrics Prediction Pipeline")

    parser.add_argument(
        "--supermetrics-path",
        type=Path,
        required=True,
        help="Path to Supermetrics export CSV",
    )
    
    #parser.add_argument("--access-token", required=True, help="Meta API access token")
    #parser.add_argument("--ad-account-id", required=True, help="Meta Ad Account ID")
    #parser.add_argument("--date-since", required=True, help="Start date (YYYY-MM-DD)")
    #parser.add_argument("--date-until", required=True, help="End date (YYYY-MM-DD)")

    parser.add_argument(
        "--date-since",
        default=os.getenv("DATE_SINCE"),
        help="Start date (YYYY-MM-DD). CLI > ENV.",
    )

    parser.add_argument(
        "--date-until",
        default=os.getenv("DATE_UNTIL", datetime.today().strftime("%Y-%m-%d")),
        help="End date (YYYY-MM-DD). Defaults to today.",
    )
    
    parser.add_argument(
        "--model-path",
        default=DEFAULT_MODEL_PATH,
        type=Path,
        help="Path prefix of trained model artifacts",
    )

    parser.add_argument(
        "--min-history-days",
        type=int,
        default=DEFAULT_MIN_HISTORY_DAYS,
    )

    args = parser.parse_args()

    if not args.date_since:
        raise RuntimeError("date-since must be provided via CLI or DATE_SINCE env var")
    
    # Secrets
    access_token = require_env("META_ACCESS_TOKEN")
    ad_account_id = require_env("META_AD_ACCOUNT_ID")

    run_daily_refresh(
        supermetrics_path=args.supermetrics_path,
        #access_token=args.access_token,
        #ad_account_id=args.ad_account_id,
        # access_token=access_token,
        # ad_account_id=ad_account_id,
        date_since=args.date_since,
        date_until=args.date_until,
        model_path=args.model_path,
        min_history_days=args.min_history_days,
    )