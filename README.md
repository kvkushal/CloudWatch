# CloudWatch — Cloud Cost Intelligence Platform

**Live Demo:** https://cloudwatchai.streamlit.app/

A real-time cloud cost monitoring and optimization platform built with DynamoDB, Redis, Flask, and Streamlit. Tracks spend across multiple AWS accounts, detects anomalies using statistical z-score analysis, and surfaces actionable cost optimization recommendations.

---

## What it does

Most cloud billing dashboards tell you what you spent. This one tells you what went wrong and what to do about it.

- Monitors daily spend per account across EC2, RDS, S3, Lambda, and CloudFront
- Flags cost anomalies the moment daily spend crosses 2 standard deviations from the historical mean
- Generates rightsizing, reserved instance, and cleanup recommendations per resource
- Shows budget utilization in real time with severity-graded alerts
- Breaks down costs by service and region with interactive charts

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Streamlit Dashboard                   │
│              (Cost Intelligence UI / demo_data)          │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP
┌────────────────────────▼────────────────────────────────┐
│                     Flask REST API                       │
│   /dashboard  /trend  /alerts  /recommendations  /costs  │
└──────────┬──────────────────────────────┬───────────────┘
           │                              │
┌──────────▼──────────┐      ┌────────────▼──────────────┐
│    DynamoDB Local   │      │          Redis             │
│                     │      │                            │
│  ResourceUsage      │      │  dashboard:{acct}  HASH    │
│  DailyCostSummary   │      │  anomaly:{acct}    HASH    │
│  Alerts             │      │  alerts:recent:{} LIST     │
│  Recommendations    │      │  cost_rank:{date}  ZSET    │
│                     │      │  trend:{acct}:7d   STRING  │
└──────────┬──────────┘      └────────────────────────────┘
           │
┌──────────▼──────────────────────────────────────────────┐
│              Background Workers                          │
│   data_generator → anomaly_detector → rec_engine        │
└─────────────────────────────────────────────────────────┘
```

---

## Database Design

### DynamoDB — four tables

**ResourceUsage**
- Partition key: `account_id`, Sort key: `resource_type#timestamp`
- GSI 1 `ResourceTypeIndex` — scatter-gather queries by service type
- GSI 2 `RegionIndex` — cost breakdown by region
- TTL on `expires_at` — records auto-deleted after 90 days

**DailyCostSummary**
- Partition key: `account_id`, Sort key: `date` (ISO 8601)
- ISO date strings sort lexicographically so `between()` range queries work without a numeric sort key
- `ConsistentRead=True` on anomaly detection reads to prevent false negatives from stale replicas

**OptimizationRecommendations**
- Partition key: `account_id`, Sort key: `rec_id#timestamp`
- Denormalized: `resource_id`, `rec_type`, and `savings` stored directly on each item to avoid double queries on list reads

**Alerts**
- Partition key: `account_id`, Sort key: `alert_timestamp`
- TTL on `expires_at` — alerts expire after 30 days automatically

### Redis — five data structures

| Key pattern | Structure | Purpose |
|---|---|---|
| `dashboard:{acct}` | HASH | Low-latency snapshot, 60s TTL |
| `anomaly:{acct}` | HASH | Rolling mean/std for z-score detection |
| `alerts:recent:{acct}` | LIST | Bounded FIFO queue, newest-first |
| `cost_rank:{date}` | ZSET | Cost rankings by account, sorted by score |
| `trend:{acct}:7d` | STRING (JSON) | 7-day trend cache, 10min TTL |

### CAP trade-off

DynamoDB is AP by default (eventual consistency). Strong consistency (`ConsistentRead=True`) is used only on anomaly detection and alert reads where stale data could produce false alerts. All analytics reads use eventual consistency to halve RCU cost.

---

## Anomaly Detection

Uses **Welford's online algorithm** for numerically stable incremental mean and variance. The naive approach (`E[X²] - E[X]²`) suffers from catastrophic cancellation at large values. Welford's method tracks the sum of squared deviations directly and updates in O(1) per new data point without storing the full history.

```
z = (daily_cost - mean) / std_dev

|z| > 2.0  →  warning
|z| > 3.0  →  critical
```

Stats are persisted in Redis and updated after each detection run. Detection runs before the update so the current day's cost is evaluated against past history, not included in its own baseline.

---

## Optimization Engine — 5 rules

| Rule | Trigger | Action |
|---|---|---|
| Idle resource | Avg usage < 20% of baseline over 7 days | `terminate_idle` |
| Rightsizing | Avg usage 20–40% of baseline | `rightsize` |
| Reserved instances | Resource active ≥ 25 of last 30 days | `switch_to_reserved` |
| S3 archiving | Avg access < 30% baseline over 20 days | `move_to_glacier` |
| Unused Lambda | Avg invocations < 10% baseline over 14 days | `delete_unused` |

Deduplication prevents the same recommendation firing twice for the same resource across runs.

---

## Stack

| Layer | Technology |
|---|---|
| Dashboard | Streamlit, Plotly |
| API | Flask |
| Primary database | DynamoDB Local (AWS SDK via boto3) |
| Cache layer | Redis 7 |
| Data generation | Python (seeded synthetic data, 30-day history) |
| Deployment | Streamlit Community Cloud (demo), Docker (local) |

---

## Running locally

**Prerequisites:** Docker Desktop, Python 3.12, WSL (Windows) or any Unix shell

```bash
# Clone
git clone https://github.com/kvkushal/CloudWatch.git
cd CloudWatch

# Create virtualenv
python3 -m venv venv
source venv/bin/activate
pip install flask boto3 redis streamlit plotly pandas requests

# Start DynamoDB Local and Redis
docker compose up -d

# Seed data
python3 data_generator.py
python3 recommendation_engine.py
python3 anomaly_detector.py

# Start API
python3 api.py &

# Start dashboard
DEMO_MODE=false streamlit run dashboard.py
```

Dashboard: http://localhost:8501  
API: http://localhost:5000/api/health

---

## API endpoints

```
GET /api/health
GET /api/accounts
GET /api/dashboard/<account_id>
GET /api/costs/<account_id>?start=&end=
GET /api/trend/<account_id>
GET /api/rankings?date=&top=
GET /api/usage/by-service/<type>
GET /api/usage/by-region/<region>
GET /api/alerts/<account_id>?source=redis|dynamodb
GET /api/anomaly-stats/<account_id>
GET /api/recommendations/<account_id>
GET /api/summary
```

---

## Project structure

```
CloudWatch/
├── dashboard.py            # Streamlit UI
├── demo_data.py            # Static data for hosted demo
├── api.py                  # Flask REST API
├── dynamo_manager.py       # DynamoDB access layer
├── redis_manager.py        # Redis caching layer
├── anomaly_detector.py     # Z-score anomaly detection
├── recommendation_engine.py# Cost optimization rules
├── data_generator.py       # Synthetic data seeder
├── requirements.txt
├── docker-compose.yml
└── .streamlit/
    └── config.toml
```
