# notebook 3- 3- models

# ========================================================
# MARK: STEP 9 — Time-Aware Train/Test Split (FINAL CODE)
# ========================================================

from __future__ import annotations

import json
from pathlib import Path
from typing import Tuple, Dict, Any, List

import numpy as np
import pandas as pd
import joblib
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# ========================================================
# 1. Train CTR model
# ========================================================
def train_ctr_model(
        df: pd.DataFrame,
        target: str = "ctr_link",
        test_days: int = 14,
        random_seed: int = 42,
) -> Tuple[xgb.XGBRegressor, Dict[str, Any]]:
    """
    Train XGBoost CTR model using time-aware split.

    Returns:
        model      : trained XGBRegressor
        metadata   : dict with metrics, features, cutoff_date
    """

    if df.empty:
        raise ValueError("Training DataFrame is empty.")
    
    df = df.copy()

    # -----------------------------------
    # 1. Sort chronologically
    # -----------------------------------
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date")

    if target not in df.columns:
        raise ValueError(f"Target column '{target}' not found.")
    
    # ---------------------------------------------
    # 2. Define features (exclusive non-predictive columns)
    # ---------------------------------------------
    non_feature_cols = {
        "date",
        "campaign_id",
        "campaign_name",
        target,
        #"results_type", # text column
    }

    candidate_features = [
        c for c in df.columns
        if c not in non_feature_cols

    ]

    # Keep numeric only
    feature_cols = [
        c for c in df.columns
        if c not in non_feature_cols
    ]

    if not feature_cols:
        raise ValueError("No numeric features available for training.")
    
    # ---------------------------------------------
    # 3. Time-aware train/test split
    # ---------------------------------------------
    max_date = df["date"].max()
    cutoff_date = max_date - pd.Timedelta(days=test_days)

    train_df = df[df["date"] <= cutoff_date]
    test_df = df[df["date"] > cutoff_date]

    if train_df.empty or test_df.empty:
        raise ValueError("Insufficient data for time-based split.")
    
    X_train = train_df[feature_cols]
    y_train = train_df[target]

    X_test = test_df[feature_cols]
    y_test = test_df[target]

# =======================================================
# MARK: 10.1 — Baseline Linear Regression (quick sanity check)
# ======================================================

    # ---------------------------------------------
    # 4. Clean NaN / inf (XGBoost requirement)
    # ---------------------------------------------

# ========================================================
# MARK: 10.3 — Train XGBoost
# ========================================================

    X_train = X_train.replace([np.inf, -np.inf], np.nan)
    X_test = X_test.replace([np.inf, -np.inf], np.nan)

    y_train = y_train.replace([np.inf, -np.inf], np.nan)
    y_test = y_test.replace([np.inf, -np.inf], np.nan)

    mask_train = y_train.notna()
    mask_test = y_test.notna()

    X_train = X_train.loc[mask_train]
    y_train = y_train.loc[mask_train]

    X_test = X_test.loc[mask_test]
    y_test = y_test.loc[mask_test]

# ===========================================================
# MARK: 10.2 — XGBoost Model Definition
# ===========================================================

    # ---------------------------------------------
    # 5. Define model (faithful to notebook)
    # ---------------------------------------------
    model = xgb.XGBRegressor(
        n_estimators=400,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        random_state=random_seed,
        objective="reg:squarederror",
        eval_metrics="rmse",
        tree_method="hist",
    )

    # -------------------------------------------
    # 6. Train
    # -------------------------------------------
    model.fit(X_train, y_train)

# ================================================
# MARK: 10.4 — Evaluate Model (MAE, RMSE, R²)
# ================================================

    # -------------------------------------------
    # 7. Evaluate
    # -------------------------------------------
    preds = model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    mse = mean_squared_error(y_test, preds)
    rmse = mse ** 0.5
    r2 = r2_score(y_test, preds)

    """
    Should print output here, so i know it's working.
    """
    print(f"XGBoost MAE : {mae:.6f}")
    print(f"XGBoost RMSE: {rmse:.6f}")
    print(f"XGBoost R²  : {r2:.6f}")

    print("\nStep 10.4 — Evaluation complete.")

# ====================================================
# MARK: 10.5 — Save Model & Feature List
# ====================================================

    # -------------------------------------------
    # 8. Metadata
    # -------------------------------------------
    metadata = {
        "target": target,
        "features": feature_cols,
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "cutoff_date": str(cutoff_date.date()),
        "metrics": {
            "mae": float(mae),
            "rmse": float(rmse),
            "r2": float(r2),
        },
    }

    return model, metadata

# ======================================================
# 2. Save model + metadata
# ======================================================
def save_model (
        model: xgb.XGBRegressor,
        metadata: Dict[str, Any],
        path: str,
) -> None:
    """
    Save trained model and metadata to disk.

    Files written:
    - <path>.joblib
    - <path>.json    
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, path.with_suffix(".joblib"))

    with open(path.with_suffix(".json"), "w") as f:
        json.dump(metadata, f, indent=2)

# ===================================================
# 3. Load model + metadata
# ===================================================

def load_model(path: str) -> Tuple[xgb.XGBRegressor, Dict[str, Any]]:
    """
    Load trained model and metadata.
    """
    path = Path(path)

    model = joblib.load(path.with_suffix(".joblib"))

    with open(path.with_suffix(".json"), "r") as f:
        metadata = json.load(f)

    return model, metadata

# =================================================
# 4. Predict CTR
# =================================================
def predict_ctr(
        model: xgb.XGBRegressor,
        df: pd.DataFrame,
        feature_cols: list[str] | None = None,
) -> pd.Series:
    """
    Predict CTR for all rows in df.
    """

    missing = set(feature_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing feature columns in input DataFrame: {missing}")
    
    #X = df.copy()

    X = df[feature_cols].replace([np.inf, -np.inf], np.nan)

    # if feature_cols is None:
    #     feature_cols = model.get_booster().feature_names

    # X = X[feature_cols]
    # X = X.replace([np.inf, -np.inf], np.nan)

    preds = model.predict(X)
    
    return pd.Series(preds, index=df.index, name="pred_ctr_link")