# config.py
import os

# ================= ENV =================

DYNAMODB_ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT", "http://localhost:8000")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# ================= CACHE =================

TTL_DASHBOARD = 60
TTL_SUMMARY = 300
TTL_TREND = 600

# ================= ANOMALY =================

Z_SCORE_THRESHOLD = 2.0

# ================= PERFORMANCE =================

ENABLE_METRICS = True