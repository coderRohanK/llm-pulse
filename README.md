# ⚡ LLM Pulse — LLM API Observability & Cost Intelligence Platform

> **End-to-end data engineering + ML project built on Databricks**  
> Ingests, processes, and analyzes LLM API usage data to give companies full visibility into AI costs, model performance, and output quality — with a 30-day spend forecast powered by machine learning.

---

## 📌 Table of Contents

- [Problem Statement](#-problem-statement)
- [Solution Overview](#-solution-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Data Sources](#-data-sources)
- [Medallion Architecture](#-medallion-architecture)
- [Machine Learning Models](#-machine-learning-models)
- [Data Consumption Layer](#-data-consumption-layer)
- [Key Results](#-key-results)
- [How to Run](#-how-to-run)
- [Screenshots](#-screenshots)
- [What I Learned](#-what-i-learned)

---

## 🎯 Problem Statement

Companies using LLM APIs (OpenAI, Anthropic, Google) are flying blind on costs and quality.

**The pain points:**
- A CTO gets a monthly AI bill of $80,000 with zero breakdown of where it went
- No visibility into which team, feature, or model is responsible for the spend
- No early warning when model output quality silently degrades
- No way to forecast next month's AI costs before the bill arrives
- Engineers use expensive GPT-4 for tasks that GPT-3.5 could handle at 10x lower cost

**LLM Pulse solves all of this** by building a production-grade observability platform on Databricks that tracks every API call, calculates its cost, monitors quality, detects anomalies, and forecasts future spend using machine learning.

---

## 💡 Solution Overview

LLM Pulse is built around three core capabilities:

| Capability | What it does |
|---|---|
| **Cost Intelligence** | Tracks spend per team, per model, per feature — updated daily with 7-day rolling averages |
| **Quality Monitoring** | Monitors thumbs-up/down feedback rates per model, flags quality drops before customers notice |
| **ML Forecasting** | Predicts next 30 days of spend per team using XGBoost time-series model + detects cost and quality anomalies using Isolation Forest |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                    │
│                                                                         │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────────┐   │
│  │ Apache Kafka │   │   AWS S3     │   │  AWS S3 (CSV files)      │   │
│  │ (simulated   │   │  (90 days    │   │  pricing.csv             │   │
│  │  live stream)│   │  JSON logs)  │   │  feedback.csv            │   │
│  └──────┬───────┘   └──────┬───────┘   └────────────┬─────────────┘   │
└─────────┼─────────────────┼────────────────────────┼─────────────────┘
          │                 │                          │
          ▼                 ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER (Databricks)                         │
│                                                                         │
│  LakeFlow Declarative    Autoloader               Autoloader            │
│  Pipeline (Streaming)    (Incremental JSON)       (Incremental CSV)     │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              MEDALLION ARCHITECTURE (Delta Lake / Unity Catalog)        │
│                                                                         │
│  BRONZE                  SILVER                    GOLD                 │
│  ────────                ──────                    ────                 │
│  api_calls_historical    api_calls                 daily_cost_by_team   │
│  api_calls_live     ──►  (+ cost_usd calc)    ──►  model_performance    │
│  model_pricing           quality_feedback           feature_cost         │
│  quality_feedback        model_pricing              quality_score_daily  │
│                                                    cost_predictions     │
│                                                    quality_alerts       │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    MACHINE LEARNING (MLflow + Unity Catalog)            │
│                                                                         │
│  Model 1: XGBoost Cost Forecasting                                      │
│  → 16 time-series features → predicts 30-day spend per team            │
│  → MAE: $0.036  |  RMSE: $0.048  |  MAPE: 14.15%                      │
│                                                                         │
│  Model 2: Isolation Forest Anomaly Detection                            │
│  → Detects cost spikes and quality drops automatically                  │
│  → Generated 71 alerts (54 cost spikes + 17 quality drops)             │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    CONSUMPTION LAYER                                    │
│                                                                         │
│  AIBI Dashboard     Genie Space          Streamlit App                  │
│  (6 charts)         (NL querying)        (4 views: Finance /            │
│                                           Engineering / Quality /       │
│                                           Alerts)                       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

### Data Engineering
| Tool | Purpose |
|---|---|
| **Databricks** (Free Trial) | Core platform — pipelines, ML, dashboards, apps |
| **Apache Kafka** (Docker) | Real-time event streaming from LLM API simulator |
| **AWS S3** | Landing zone for historical JSON files and CSV references |
| **Delta Lake** | ACID table format for all medallion layers |
| **LakeFlow Declarative Pipelines** | ETL framework — formerly Delta Live Tables |
| **Autoloader** | Incremental file ingestion with schema evolution |
| **Unity Catalog** | Data governance, lineage tracking, model registry |
| **LakeFlow Jobs** | Pipeline orchestration and scheduling |

### Machine Learning
| Tool | Purpose |
|---|---|
| **XGBoost** | Time-series cost forecasting model |
| **Prophet** | Alternative forecasting (compared in MLflow) |
| **Scikit-learn** | Isolation Forest anomaly detection |
| **MLflow** | Experiment tracking, model registry, serving |
| **Databricks Model Serving** | Real-time ML inference REST endpoint |

### Application & Visualization
| Tool | Purpose |
|---|---|
| **Streamlit** | Frontend portal with 4 analytical views |
| **Databricks Apps** | Hosting the Streamlit application |
| **Databricks AIBI Dashboards** | 6-chart analytics dashboard |
| **Databricks Genie** | Natural language querying on gold tables |
| **Plotly** | Interactive charts inside the Streamlit app |

### Infrastructure & Dev
| Tool | Purpose |
|---|---|
| **Docker + docker-compose** | Runs Kafka + Zookeeper locally |
| **Python** | Simulator, notebooks, pipeline code |
| **Pandas** | Data exploration and feature engineering |
| **Faker** | Realistic synthetic data generation |
| **Boto3** | AWS SDK for S3 uploads |
| **GitHub** | Version control and portfolio |

---

## 📁 Project Structure

```
llm-pulse/
│
├── 📂 local/                          # Runs on your laptop
│   ├── docker-compose.yml             # Starts Kafka + Zookeeper
│   ├── simulator.py                   # Streams live events to Kafka every 2s
│   ├── historical_backfill.py         # Generates 90 days of data → S3
│   ├── generate_feedback.py           # Generates quality feedback CSV
│   ├── upload_csvs.py                 # Uploads pricing + feedback to S3
│   ├── pricing.csv                    # LLM pricing reference (3 models)
│   ├── feedback.csv                   # Simulated thumbs up/down data
│   └── app.py                         # Streamlit app (local dev version)
│
├── 📂 databricks/                     # Runs inside Databricks
│   ├── 00_setup_and_verify.py         # Unity Catalog setup + verification
│   │
│   ├── 📂 ingestion/                  # LakeFlow Pipeline: llm_pulse_bronze_ingestion
│   │   ├── 01_bronze_s3_ingestion.py  # Autoloader: S3 → bronze tables
│   │   └── 02_bronze_kafka_ingestion.py # Kafka stream → bronze.api_calls_live
│   │
│   ├── 📂 transformations/            # LakeFlow Pipeline: llm_pulse_transformations
│   │   ├── 03_silver_transformations.py # Bronze → silver (clean + cost_usd)
│   │   └── 04_gold_aggregations.py    # Silver → gold (4 aggregated views)
│   │
│   ├── 06_cost_forecasting_model.py   # XGBoost model + MLflow + predictions
│   └── 07_quality_anomaly_detection.py # Isolation Forest + quality alerts
│
└── 📂 app/                            # Databricks App files
    ├── app.py                         # Full Streamlit application
    ├── app.yaml                       # Databricks App configuration
    └── requirements.txt               # Python dependencies
```

---

## 📊 Data Sources

All data in this project is **simulated** — no real company data is used. The simulator generates realistic patterns:

### LLM API Call Events (streamed via Kafka + batch via S3)
Each event represents one API call made to an LLM:

```json
{
  "call_id":          "uuid",
  "timestamp":        "2026-01-15T10:23:11Z",
  "team":             "product",
  "feature":          "chatbot",
  "model":            "gpt-4o",
  "environment":      "prod",
  "prompt_tokens":    1240,
  "completion_tokens": 380,
  "latency_ms":       1823,
  "status":           "success",
  "status_code":      200,
  "user_id":          "user_247"
}
```

**Simulation parameters:**
- 5 teams: product, data, marketing, ops, finance
- 3 models: gpt-4o, gpt-3.5-turbo, claude-3-sonnet
- 10 features across teams (chatbot, summarizer, invoice_parser, etc.)
- ~200 events/weekday, ~75 events/weekend
- Error rates vary by team (ops: 6%, marketing: 1%)
- 90 days of historical data + live streaming

### Model Pricing Reference
```
model              prompt_price/1k    completion_price/1k
gpt-4o             $0.005             $0.015
gpt-3.5-turbo      $0.0005            $0.0015
claude-3-sonnet    $0.003             $0.015
```

### Quality Feedback
~30 thumbs up/down feedback entries per day per model, with realistic quality rates:
- gpt-4o: 91% thumbs up
- claude-3-sonnet: 88% thumbs up
- gpt-3.5-turbo: 79% thumbs up

---

## 🥇 Medallion Architecture

### Bronze Layer — Raw ingestion

| Table | Source | Rows | Description |
|---|---|---|---|
| `api_calls_historical` | S3 JSON files via Autoloader | ~18,000 | 90 days of API call logs |
| `api_calls_live` | Kafka stream | live | Real-time events from simulator |
| `model_pricing` | S3 CSV via Autoloader | 3 | Pricing per model per 1k tokens |
| `quality_feedback` | S3 CSV via Autoloader | ~2,700 | Thumbs up/down per model per day |

### Silver Layer — Cleaned and enriched

**Key transformations applied:**
- String timestamps → proper `TimestampType`
- Calculated `cost_usd` per call by joining with pricing table:
  ```
  cost_usd = (prompt_tokens / 1000 × prompt_price)
           + (completion_tokens / 1000 × completion_price)
  ```
- Added `is_slow` flag (latency > 3000ms)
- Added `is_error` boolean
- Added `total_tokens` = prompt + completion
- Extracted `event_date` for partitioning
- Data quality checks with `@dlt.expect_or_drop`:
  - `call_id IS NOT NULL`
  - `prompt_tokens > 0`
  - `latency_ms >= 0`
  - `team IN ('product','data','marketing','ops','finance')`
  - `model IN ('gpt-4o','gpt-3.5-turbo','claude-3-sonnet')`

### Gold Layer — Aggregated and ready to consume

| Table | Key columns | Used by |
|---|---|---|
| `daily_cost_by_team` | event_date, team, total_cost_usd, rolling_7d_avg_cost | Forecasting ML + Dashboard |
| `model_performance_summary` | event_date, model, avg_latency_ms, error_rate_pct, slow_call_rate_pct | Engineering View |
| `feature_cost_breakdown` | event_date, team, feature, total_cost_usd | Finance View |
| `quality_score_daily` | feedback_date, model, quality_score_pct, rolling_7d_quality_score | Anomaly ML + Quality View |
| `cost_predictions` | prediction_date, team, predicted_cost_usd | Dashboard forecast chart |
| `quality_alerts` | alert_date, alert_type, alert_severity, actual_value | Alerts View |

---

## 🤖 Machine Learning Models

### Model 1 — XGBoost Cost Forecasting

**Goal:** Predict daily LLM spend per team for the next 30 days.

**Features engineered (16 total):**
```
Date features:     day_of_week, day_of_month, month, week_of_year, is_weekend
Lag features:      cost_lag_1d, cost_lag_3d, cost_lag_7d, cost_lag_14d, cost_lag_30d
Rolling stats:     rolling_mean_7d, rolling_mean_14d, rolling_std_7d
Volume features:   total_calls, rolling_7d_avg_cost
Categorical:       team_encoded
```

**Training approach:**
- Time-based split (not random) — last 14 days as test set
- Prevents data leakage in time-series prediction

**Results:**
```
MAE  (Mean Absolute Error):   $0.0365  — off by 3.6 cents per day per team
RMSE (Root Mean Sq Error):    $0.0483
MAPE (Mean Abs % Error):      14.15%   — within acceptable range for 90 days of data
```

**MLflow tracking:** Parameters, metrics, feature importance chart, and model artifact all logged to Unity Catalog.

---

### Model 2 — Isolation Forest Anomaly Detection

**Goal:** Automatically detect when costs spike or quality drops without manual threshold setting.

**Two detectors:**

**Cost anomaly detector** — trained on `daily_cost_by_team` gold table:
- Features: total_cost_usd, total_calls, rolling_7d_avg_cost, day_of_week
- Flags days where spend is abnormally high relative to team's historical pattern

**Quality anomaly detector** — trained on `quality_score_daily` gold table:
- Features: quality_score_pct, rolling_7d_quality_score, total_feedback
- Flags days where thumbs-up rate drops significantly for a model
- Also uses statistical control chart (rolling mean ± 2σ) as baseline

**Results generated:**
```
Total alerts:     71
Cost spikes:      54  (warning severity)
Quality drops:    10  (warning severity)
Quality drops:     7  (critical severity)
```

---

## 📱 Data Consumption Layer

### AIBI Dashboard — 6 charts + 3 KPI counters

| Visualization | Chart type | Data source |
|---|---|---|
| Total spend this month | Counter | gold.daily_cost_by_team |
| Total API calls | Counter | gold.daily_cost_by_team |
| Active critical alerts | Counter | gold.quality_alerts |
| Daily spend by team | Line chart | gold.daily_cost_by_team |
| Total spend by model | Bar chart | gold.model_performance_summary |
| Quality score trend | Line chart | gold.quality_score_daily |
| Most expensive features | Horizontal bar | gold.feature_cost_breakdown |
| Active alerts summary | Table | gold.quality_alerts |
| 30-day cost forecast | Line chart | gold.cost_predictions |

### Genie Space — Natural Language Analytics

Connected to all 6 gold tables. Sample questions answered:
- *"Which team spent the most on LLM APIs last month?"*
- *"Which model has the highest error rate?"*
- *"How many critical quality alerts do we have?"*
- *"What is the predicted total spend for all teams next week?"*

### Streamlit Application — 4 analytical views

**Finance View**
- 4 KPI metrics at top: total spend, total calls, avg cost/call, 30-day forecast
- Daily spend trend chart with 7-day rolling average
- Total spend by model bar chart
- ML-predicted cost forecast line chart
- Most expensive features table

**Engineering View**
- Per-model metrics: avg latency, error rate, slow call rate, total cost
- Latency trend over time per model
- Error rate trend over time per model

**Quality View**
- Latest quality score per model with 7-day average delta
- Quality score trend with warning (85%) and critical (75%) threshold lines
- Raw daily quality score chart

**Alerts View**
- Alert counters: total, critical, warning
- Filterable alerts table by severity and type
- Shows deviation from threshold for each alert

---

## 📈 Key Results

| Metric | Value |
|---|---|
| Total API calls ingested | 15,950 |
| Total cost tracked | $114.77 |
| Date range covered | 90 days |
| Teams monitored | 5 |
| Models tracked | 3 (gpt-4o, gpt-3.5-turbo, claude-3-sonnet) |
| Features analyzed | 10 product features |
| Forecast MAE | $0.036 per team per day |
| Forecast MAPE | 14.15% |
| Anomaly alerts generated | 71 total |
| gpt-4o vs gpt-3.5 cost ratio | ~14x more expensive |
| Most expensive feature | invoice_parser (finance team) at $11.85 |
| Average quality score (gpt-4o) | ~88-91% thumbs up |

---

## 🚀 How to Run

### Prerequisites
- Databricks 14-day free trial account
- AWS free tier account
- Docker Desktop installed
- Python 3.8+ on your laptop

### Step 1 — Clone this repository
```bash
git clone https://github.com/YOUR_USERNAME/llm-pulse.git
cd llm-pulse
```

### Step 2 — Start Kafka locally
```bash
docker-compose up -d

# Create the topic
docker exec kafka kafka-topics \
  --create --topic llm-api-logs \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### Step 3 — Install Python dependencies
```bash
pip install kafka-python faker pandas boto3 streamlit plotly requests
```

### Step 4 — Configure AWS credentials
```bash
aws configure
# Enter your Access Key ID, Secret Access Key, region: ap-south-2
```

### Step 5 — Generate historical data
```bash
# Generate quality feedback
python generate_feedback.py

# Upload pricing and feedback CSVs to S3
python upload_csvs.py

# Generate 90 days of historical API logs → upload to S3
python historical_backfill.py
```

### Step 6 — Start live simulator
```bash
python simulator.py
# Keep this running — sends events to Kafka every 2 seconds
```

### Step 7 — Set up Databricks
1. Create Unity Catalog: `llm_pulse_dev` with schemas: `landing`, `bronze`, `silver`, `gold`
2. Create external location pointing to your S3 bucket
3. Run `00_setup_and_verify.py` notebook to verify setup

### Step 8 — Run ingestion pipelines
1. Create LakeFlow Pipeline: `llm_pulse_bronze_ingestion`
2. Add files: `01_bronze_s3_ingestion.py` and `02_bronze_kafka_ingestion.py`
3. Run pipeline — creates 4 bronze tables

### Step 9 — Run transformation pipelines
1. Create LakeFlow Pipeline: `llm_pulse_transformations`
2. Add files: `03_silver_transformations.py` and `04_gold_aggregations.py`
3. Run pipeline — creates 3 silver tables and 4 gold tables

### Step 10 — Train ML models
```python
# Run in Databricks notebooks:
# 06_cost_forecasting_model.py  → trains XGBoost, logs to MLflow
# 07_quality_anomaly_detection.py → trains Isolation Forest, generates alerts
```

### Step 11 — Run the Streamlit app locally
```bash
streamlit run app.py
# Opens at http://localhost:8501
```

---

## 🔑 Environment Variables

For running the Streamlit app locally, set these in `app.py`:

```python
HOST      = "your-databricks-host.cloud.databricks.com"
TOKEN     = "your-databricks-token"
WAREHOUSE = "your-sql-warehouse-id"
```

---

## 📸 Screenshots

### AIBI Dashboard
![Dashboard showing cost trends, model spend, quality scores]

### Streamlit App — Finance View
![Finance view with KPIs, cost trend, model breakdown, forecast]

### Streamlit App — Alerts View
![Alerts table showing 71 anomaly alerts with severity filter]

### MLflow Experiment
![MLflow run showing MAE=0.036, RMSE=0.048, feature importance chart]

### Unity Catalog Lineage
![Lineage graph showing S3 → bronze → silver → gold flow]

---

## 🧠 What I Learned

**Data Engineering**
- Building a medallion architecture from scratch using LakeFlow Declarative Pipelines
- Setting up incremental ingestion with Autoloader — never re-reading the same file twice
- Connecting Databricks to AWS S3 securely using IAM roles and Unity Catalog external locations
- Writing data quality checks that automatically drop bad records with `@dlt.expect_or_drop`
- Understanding the difference between streaming tables and materialized views in Delta Lake

**Machine Learning**
- Time-series feature engineering — lag features, rolling statistics, date components
- Why you must use time-based splits for time-series data (not random splits)
- Tracking ML experiments with MLflow — logging parameters, metrics, and artifacts
- Registering models in Unity Catalog for governance and versioning
- Using Isolation Forest for unsupervised anomaly detection without labeled data

**Cloud & Infrastructure**
- Setting up Kafka with Docker for local streaming
- Configuring IAM trust relationships between Databricks and AWS
- Understanding PAT token scopes and OAuth authentication in Databricks
- Deploying Streamlit apps on Databricks Apps platform

**Software Engineering**
- Designing a clean project structure separating ingestion, transformation, and ML layers
- Using environment variables for credentials instead of hardcoding
- Building a multi-view Streamlit dashboard with sidebar navigation and filters

---

## 📋 Project Timeline

| Day | Focus | Output |
|---|---|---|
| 1 | Environment setup | Databricks workspace, AWS account, Kafka running |
| 2 | Data simulation | 90-day historical data + live simulator |
| 3 | Bronze ingestion | 4 bronze delta tables from S3 + Kafka |
| 4 | Silver transformation | 3 cleaned silver tables with cost_usd calculated |
| 5 | Gold aggregation | 4 gold materialized views with rolling averages |
| 6 | ML Model 1 | XGBoost forecasting model + MLflow tracking |
| 7 | ML Model 2 | Anomaly detection + 71 alerts generated |
| 8 | Dashboard | AIBI dashboard with 6 charts + model serving endpoint |
| 9 | App + Genie | Streamlit 4-view portal + Genie NL space |
| 10 | Documentation | GitHub README + resume write-up |

---

## 👤 Author

**Your Name**  
Data Engineering & ML Intern  
📧 your.email@example.com  
🔗 [LinkedIn](https://linkedin.com/in/yourprofile)  
🐙 [GitHub](https://github.com/yourusername)

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

*Built with ❤️ on Databricks Free Trial — $0 total cost*
