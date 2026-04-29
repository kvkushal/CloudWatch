# CloudWatch — Cloud Cost Intelligence Platform

**Live Demo:** https://cloudwatchai.streamlit.app/

A real-time cloud cost monitoring and optimization platform built with **DynamoDB** (NoSQL), **Redis** (in-memory cache), **Flask** (REST API), and **Streamlit** (dashboard). Tracks spend across multiple AWS accounts, detects anomalies using statistical z-score analysis, and surfaces actionable cost optimization recommendations.

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
│              (Cache-Aside Pattern)                        │
└──────────┬──────────────────────────────┬───────────────┘
           │                              │
┌──────────▼──────────┐      ┌────────────▼──────────────┐
│    DynamoDB Local   │      │          Redis             │
│   (Persistent)      │      │      (Cache Layer)         │
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

## NoSQL Database Selection & Justification

### Why NoSQL over SQL?

Cloud cost telemetry data is **write-heavy, schema-flexible, and read-hot on recent data** — a poor fit for traditional RDBMS:

| Requirement | RDBMS Limitation | NoSQL Advantage |
|---|---|---|
| High write throughput (thousands of resource usage records per batch) | Row-level locking, index overhead on every write | DynamoDB batch writes with no index blocking |
| Variable attributes per record (tags, metadata differ across services) | Rigid schema requires ALTER TABLE | DynamoDB schema-less items — each record can have different attributes |
| Time-series access patterns (recent data queried 100× more than old) | Full table scans or complex partitioning | Sort key on timestamp enables O(log n) range queries |
| Low-latency dashboard reads (sub-5ms) | Joins across normalized tables | Redis in-memory HASH with O(1) field access |
| Auto-expiration of old data | Manual DELETE jobs or partitioning | DynamoDB TTL automatically removes expired records |

### Why DynamoDB specifically?

1. **Key-Value + Document hybrid**: Stores structured cost summaries AND semi-structured usage records in the same database
2. **Provisioned throughput**: Read/write capacity units allow precise cost control — critical for a cost monitoring platform
3. **Global Secondary Indexes (GSIs)**: Enable efficient cross-account queries by service type and region without data duplication
4. **TTL support**: Alerts and old usage records expire automatically — no cron jobs needed
5. **Scales horizontally**: Partition key distribution means adding more accounts doesn't degrade existing query performance

### Why Redis as the caching layer?

1. **Sub-millisecond reads**: Dashboard snapshots served from Redis HASH in ~0.5ms vs ~15ms from DynamoDB
2. **Purpose-built data structures**: Each use case maps to an optimal Redis structure:
   - `HASH` for dashboard snapshots and anomaly stats (partial field updates without deserialize/reserialize)
   - `ZSET` for cost rankings (built-in score-based sorting, O(log N) insert)
   - `LIST` for recent alerts (LPUSH + LTRIM = bounded FIFO queue)
   - `STRING` (JSON) for trend data and daily summaries (atomic blob read/write)
3. **TTL per key**: Stale cache entries expire automatically

### Why not MongoDB / Cassandra / Neo4j?

| Database | Why it was NOT chosen |
|---|---|
| **MongoDB** | Document model fits, but lacks built-in TTL granularity and provisioned throughput. DynamoDB's key-value access patterns are more efficient for our partition-key-based queries. |
| **Cassandra** | Wide-column store designed for massive write throughput across data centers. Our workload is moderate (~5 accounts, thousands of records) — Cassandra's operational complexity (JVM tuning, compaction) isn't justified. |
| **Neo4j** | Graph database. Cost data has no graph relationships (no traversal queries). Our access patterns are range queries and aggregations — a graph model would be forced and inefficient. |

---

## Data Model & Schema Design

### Design Methodology

The schema follows **single-table-adjacent design** where each table is optimized for a specific set of access patterns. Keys are chosen to support the most frequent queries without requiring scans or joins.

### Table 1: ResourceUsage

```
Partition Key: account_id (String)
Sort Key:      resource_type_timestamp (String) → "EC2#2026-01-15T12:00:00.000000Z"
```

| Access Pattern | How it's served |
|---|---|
| Get usage for account + service type | Sort key `begins_with("EC2#")` — O(log n) |
| Get all usage for an account | Query on partition key only — returns all |
| Get usage across all accounts for a service | GSI `ResourceTypeIndex` (resource_type → timestamp) |
| Get usage by region | GSI `RegionIndex` (region → timestamp) |

**Composite sort key rationale**: `resource_type#timestamp` enables both service-filtered AND time-range queries in a single query operation using `begins_with` and `between` operators. This avoids the need for a filter expression post-query.

**GSI design**: Two GSIs flip the access pattern:
- `ResourceTypeIndex`: partition by service type for "show me all EC2 usage across accounts"
- `RegionIndex`: partition by region for "show me regional cost breakdown"

Both GSIs project ALL attributes to avoid fetching from the base table.

**TTL**: `expires_at` attribute auto-deletes records older than 90 days, keeping the table bounded without manual cleanup.

### Table 2: DailyCostSummary

```
Partition Key: account_id (String)
Sort Key:      date (String) → "2026-01-15"
```

| Access Pattern | How it's served |
|---|---|
| Get cost for a specific date | Exact composite key lookup — O(1) |
| Get costs in a date range | Sort key `between("2026-01-01", "2026-01-31")` |
| Get all costs for an account | Query on partition key — returns all sorted by date |

**ISO 8601 date strings as sort key**: Strings sort lexicographically in the same order as chronological order, so `between()` range queries work correctly without a numeric sort key conversion.

**Denormalization**: `service_breakdown` is stored as a nested map directly on each summary record. In a normalized RDBMS design, this would be a separate `service_costs` table requiring a JOIN. Denormalizing avoids the join and delivers the complete summary in a single read.

### Table 3: OptimizationRecommendations

```
Partition Key: account_id (String)
Sort Key:      rec_id_timestamp (String) → "REC-A1B2C3#2026-01-15T12:00:00Z"
```

**Why composite sort key**: Each recommendation is uniquely identified by `rec_id`, but we also need to retrieve recommendations in chronological order. The composite key `rec_id#timestamp` provides uniqueness AND natural time ordering.

**Denormalization**: `resource_id`, `resource_type`, `rec_type`, and `estimated_monthly_savings` are stored directly on each recommendation item. This avoids joining back to ResourceUsage when listing recommendations in the dashboard.

### Table 4: Alerts

```
Partition Key: account_id (String)
Sort Key:      alert_timestamp (String)
```

**TTL**: `expires_at` auto-deletes alerts after 30 days. Critical for keeping the alerts table bounded in production.

### Redis Data Model — Structure Selection

| Key Pattern | Redis Structure | Why This Structure |
|---|---|---|
| `dashboard:{acct}` | **HASH** | Multiple fields (total_spend, alert_count, top_service). HSET updates individual fields atomically without deserializing the entire object. HINCRBY increments alert_count without read-modify-write race conditions. |
| `anomaly:{acct}` | **HASH** | Same rationale — mean, std_dev, count are updated independently by anomaly detector. |
| `alerts:recent:{acct}` | **LIST** | LPUSH + LTRIM creates a bounded FIFO queue. LRANGE returns newest-first, matching dashboard display order. O(1) push. |
| `cost_rank:{date}` | **ZSET (Sorted Set)** | Built-in score-based ordering. ZADD is O(log N). ZREVRANGE gives highest-cost accounts instantly without sorting. Adding the same account twice just updates its score. |
| `trend:{acct}:7d` | **STRING (JSON)** | Trend data is always read and written as a complete blob. No partial updates needed. STRING with JSON is the simplest and most efficient choice. |
| `cache:summary:{acct}:{date}` | **STRING (JSON)** | Same rationale as trend — atomic blob read/write for daily summary cache. |

---

## Scalability, Performance & Consistency Evaluation

### CAP Theorem Analysis

This system uses **two databases with different CAP trade-offs**, chosen deliberately:

```
                    Consistency
                        │
                        │
           DynamoDB ────┤──── (CP mode for alerts/anomaly)
           (Strong)     │
                        │
                        │──── DynamoDB (AP mode for analytics)
           (Eventual)   │
                        │
    Availability ───────┼─────── Partition Tolerance
                        │
           Redis ───────┤──── (AP, always)
           (Cache)      │
```

| Component | CAP Mode | Justification |
|---|---|---|
| DynamoDB — anomaly reads | **CP** (ConsistentRead=True) | Anomaly detection must evaluate the latest cost data. A stale read could cause a false negative (missed anomaly) or false positive. Strong consistency ensures the value we evaluate is the latest committed write. Trade-off: 2× RCU cost. |
| DynamoDB — analytics reads | **AP** (eventual consistency) | Dashboard charts and trend data tolerate slightly stale reads (seconds). Eventual consistency halves the RCU cost — appropriate for read-heavy analytics views. |
| Redis | **AP** | Redis is a cache layer. If a key expires or Redis restarts, the API falls back to DynamoDB (cache-aside pattern). Availability is prioritized — a Redis outage degrades performance but doesn't break functionality. |

### Cache-Aside Pattern

All API endpoints follow this pattern:

```
Request → Check Redis (fast, ~1ms)
            ├── HIT → Return cached data
            └── MISS → Query DynamoDB (~15ms)
                         └── Populate Redis cache
                              └── Return data
```

This provides:
- **Read latency reduction**: 10-15× faster on cache hits
- **Graceful degradation**: If Redis is down, every request still works via DynamoDB
- **Automatic freshness**: TTLs ensure stale data is purged (60s dashboard, 5min summary, 10min trend)

### Performance Results

Run `python performance_test.py` to reproduce. Typical results on local Docker:

| Metric | Cold (DynamoDB) | Warm (Redis Cache) | Improvement |
|---|---|---|---|
| Avg latency | ~12-18 ms | ~1-3 ms | **6-10× faster** |
| P99 latency | ~45 ms | ~8 ms | **5× faster** |
| Throughput | ~60 req/s | ~300 req/s | **5× higher** |

### Horizontal Scalability

| Component | Scaling Strategy |
|---|---|
| DynamoDB | Horizontal partitioning by `account_id`. Adding 1000 accounts doesn't affect existing query performance — each account's data is co-located in its own partition. |
| Redis | Single-node in dev. In production: Redis Cluster or AWS ElastiCache with read replicas. ZSET keys partition naturally by date. |
| Flask API | Stateless — can be horizontally scaled behind a load balancer. All state lives in DynamoDB/Redis. |
| Background workers | Stateless — anomaly detector and recommendation engine can run as parallel Lambda functions per account. |

### Consistency in Concurrent Scenarios

| Scenario | Solution |
|---|---|
| Two anomaly detectors updating the same account's stats | Redis `HINCRBY` for atomic counter increment. Welford's algorithm update is idempotent for running stats. |
| Dashboard reads during data generator writes | Eventual consistency on analytics. Dashboard shows data as of last cache refresh (60s max). |
| Alert creation during concurrent detection | DynamoDB conditional writes prevent duplicate alerts. Redis LPUSH is atomic. |

---

## Anomaly Detection

Uses **Welford's online algorithm** for numerically stable incremental mean and variance. The naive approach (`E[X²] - E[X]²`) suffers from catastrophic cancellation at large values. Welford's method tracks the sum of squared deviations directly and updates in O(1) per new data point without storing the full history.

```
z = (daily_cost - mean) / std_dev

|z| > 2.0  →  warning   (4.5% chance under normal distribution)
|z| > 3.0  →  critical  (0.3% chance under normal distribution)
```

Stats are persisted in Redis and updated after each detection run. Detection runs **before** the update so the current day's cost is evaluated against past history, not included in its own baseline.

---

## Optimization Engine — 5 rules

| Rule | Trigger | Action | Estimated Savings |
|---|---|---|---|
| Idle resource | Avg usage < 20% of baseline over 7 days | `terminate_idle` | Full resource cost |
| Rightsizing | Avg usage 20–40% of baseline | `rightsize` | 40-50% of resource cost |
| Reserved instances | Resource active ≥ 25 of last 30 days | `switch_to_reserved` | 35-40% discount |
| S3 archiving | Avg access < 30% baseline over 20 days | `move_to_glacier` | 80% storage cost |
| Unused Lambda | Avg invocations < 10% baseline over 14 days | `delete_unused` | Full function cost |

Deduplication prevents the same recommendation firing twice for the same resource across runs.

---

## Stack

| Layer | Technology | Role |
|---|---|---|
| Dashboard | Streamlit, Plotly | Real-time cost intelligence UI |
| API | Flask | RESTful API with cache-aside pattern |
| Primary database | DynamoDB Local (boto3) | Persistent NoSQL store |
| Cache layer | Redis 7 | In-memory caching, ranking, and alerting |
| Data pipeline | Python | Synthetic data generation, anomaly detection, recommendations |
| Containerization | Docker, Docker Compose | Multi-service orchestration |
| CI/CD | GitHub Actions | Automated build → test → deploy pipeline |

---

## CI/CD Pipeline

The project uses a **3-stage GitHub Actions pipeline**:

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│  BUILD  │ ──► │  TEST   │ ──► │ DEPLOY  │
│         │     │         │     │         │
│ Install │     │ DynamoDB │     │ Docker  │
│ deps    │     │ Redis   │     │ build   │
│ Verify  │     │ Pipeline│     │ Tag     │
│ imports │     │ Pytest  │     │ Verify  │
└─────────┘     └─────────┘     └─────────┘
```

### Stage 1 — Build
- Checkout code
- Set up Python 3.10
- Install dependencies from `requirements.txt`
- Verify all imports succeed

### Stage 2 — Test
- Start DynamoDB Local and Redis as **service containers**
- Initialize database schema
- Run full data pipeline (generate → detect → recommend)
- Execute **30+ automated tests** via pytest
- Run performance smoke test (latency < 500ms assertion)

### Stage 3 — Deploy
- Build Docker image (`cloudwatch-app:SHA`)
- Tag as `latest`
- Verify image size
- Run container smoke test
- Print deployment summary

---

## Running locally

**Prerequisites:** Docker Desktop, Python 3.10+, WSL (Windows) or any Unix shell

```bash
# Clone
git clone https://github.com/kvkushal/CloudWatch.git
cd CloudWatch

# Start infrastructure
docker run -d --name dynamodb-local -p 8000:8000 amazon/dynamodb-local
docker run -d --name redis-local -p 6379:6379 redis:7

# Create virtualenv & install
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Initialize and seed
python dynamo_manager.py
python data_generator.py
python anomaly_detector.py
python recommendation_engine.py

# Run tests
pytest test_api.py -v

# Start API (terminal 1)
python api.py

# Start dashboard (terminal 2)
DEMO_MODE=false streamlit run dashboard.py
```

Dashboard: http://localhost:8501
API: http://localhost:5000/api/health

---

## API endpoints

```
GET /api/health                              System health (DynamoDB + Redis)
GET /api/accounts                            List all accounts
GET /api/dashboard/<account_id>              Dashboard snapshot (cache-aside)
GET /api/costs/<account_id>?start=&end=      Daily cost summaries
GET /api/trend/<account_id>                  7-day cost trend
GET /api/rankings?date=&top=                 Cost rankings (Redis ZSET)
GET /api/usage/by-service/<type>?start=&end= Usage by service (GSI query)
GET /api/usage/by-region/<region>?start=&end= Usage by region (GSI query)
GET /api/alerts/<account_id>?source=redis|dynamodb  Recent alerts
GET /api/anomaly-stats/<account_id>          Z-score model statistics
GET /api/recommendations/<account_id>        Cost optimization recommendations
GET /api/summary                             Cross-account summary
GET /api/insights/<account_id>               Account-level insights
```

---

## Project structure

```
CloudWatch/
├── api.py                    # Flask REST API (cache-aside pattern)
├── dashboard.py              # Streamlit UI (cost intelligence dashboard)
├── demo_data.py              # Static data for Streamlit Cloud demo
├── dynamo_manager.py         # DynamoDB access layer (tables, CRUD, GSI queries)
├── redis_manager.py          # Redis caching layer (HASH, ZSET, LIST, STRING)
├── anomaly_detector.py       # Z-score anomaly detection (Welford's algorithm)
├── recommendation_engine.py  # 5-rule cost optimization engine
├── data_generator.py         # Synthetic data seeder (30-day history)
├── config.py                 # Environment-based configuration
├── logger.py                 # Structured logging
├── show_data.py              # Database inspection utility
├── performance_test.py       # Cache vs DB latency benchmark
├── test_api.py               # Pytest test suite (30+ tests)
├── requirements.txt          # Python dependencies
├── Dockerfile                # Container image definition
├── docker-compose.yml        # Multi-service orchestration
├── Makefile                  # Development shortcuts
├── .github/workflows/ci.yml  # CI/CD pipeline (build → test → deploy)
├── .streamlit/config.toml    # Streamlit theme configuration
└── .devcontainer/            # GitHub Codespaces configuration
```
# demo Tue Apr 28 18:44:59 UTC 2026
# Demo Wed Apr 29 05:30:24 UTC 2026
