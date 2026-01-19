# Multi-Client Dashboard — Predictive Analytics & Recommendations

Production-ready data pipeline, ML modeling, and Streamlit dashboard answering one core business question:

> **“What is the prediction for campaigns/ads?”**

This repository is designed for **end-to-end execution**: raw data → cleaned features → predictions → alerts → action recommendations → interactive dashboard.

---

## 1. System Overview

**High-level flow**

```
Raw Data
  ↓
Data Pipelines (ETL / Refresh)
  ↓
Feature Engineering
  ↓
ML Models
  ↓
Predictions + Alerts (parquet)
  ↓
Recommendations Engine
  ↓
Streamlit Dashboard
```

**Core outputs**

* `predictions.parquet` — model outputs
* `alerts.parquet` — rule / threshold-based signals
* Actionable recommendations surfaced in UI

---

## 2. Repository Structure

```
.
├── app.py                      # Streamlit entry point
├── requirements.txt
├── README.md
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── outputs/
│       ├── predictions.parquet
│       └── alerts.parquet
│
├── pipelines/
│   ├── 1_ingestion.py
│   ├── 2_n4_daily_refresh.py    # run_daily_refresh()
│   └── utils.py
│
├── features/
│   ├── build_features.py
│   └── validators.py
│
├── models/
│   ├── train.py
│   ├── predict.py
│   └── registry.py
│
├── recommendations/
│   └── 5_Recommendations.py
│
├── tests/
│   ├── test_pipelines.py
│   ├── test_features.py
│   └── test_models.py
│
└── configs/
    ├── paths.yaml
    └── model.yaml
```

---

## 3. Environment Setup

### 3.1 Create virtual environment

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

### 3.2 Install dependencies

```bash
pip install -r requirements.txt
```

---

## 4. Running the System

### 4.1 Run full daily data refresh

This executes ingestion, cleaning, validation, and feature generation.

```bash
python -m pipelines.2_n4_daily_refresh
```

Expected outputs:

* `data/processed/*.parquet`
* Logs indicating row counts and schema checks

---

### 4.2 Train model (optional)

```bash
python -m models.train
```

Artifacts saved to:

* `models/artifacts/`

---

### 4.3 Generate predictions

```bash
python -m models.predict
```

Outputs:

* `data/outputs/predictions.parquet`

---

### 4.4 Generate alerts & recommendations

```bash
python -m recommendations.5_Recommendations
```

Outputs:

* `data/outputs/alerts.parquet`

---

### 4.5 Launch dashboard

```bash
streamlit run app.py
```

Dashboard pages:

* Data health & freshness
* Predictions
* Alerts
* **Action Recommendations (KEY PAGE)**

---

## 5. Testing & Validation

### 5.1 Run unit tests

```bash
pytest tests/
```

### 5.2 Manual pipeline sanity checks

Recommended checks after every run:

* Row count before vs after cleaning
* Null rate deltas
* Feature distribution drift
* Prediction range & monotonicity

---

## 6. Design Principles

* **Fail loud**: validations > silent failures
* **Deterministic outputs**: reproducible runs
* **Separation of concerns**: pipeline ≠ modeling ≠ UI
* **Parquet-first**: fast IO, schema clarity
* **Business-first ML**: outputs mapped to actions

---

## 7. Common Failure Modes

| Area      | Failure       | Mitigation            |
| --------- | ------------- | --------------------- |
| Pipeline  | Schema drift  | Explicit validators   |
| Features  | Leakage       | Time-aware splits     |
| Models    | Stale weights | Versioned artifacts   |
| Dashboard | Empty outputs | Guardrails + warnings |

---

## 8. Extending the System

* Add new clients via config-only changes
* Swap model via `models/registry.py`
* Add LLM-based explanations on top of alerts
* Integrate scheduler (cron / Airflow / Dagster)

---

## 9. Operating Mode

This repo is built for:

* Fast iteration
* Real stakeholders
* Messy data
* Production constraints

If it breaks, it should **break loudly and early**.

---

## 10. License

Internal / proprietary. Adapt as required.
