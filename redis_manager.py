"""
redis_manager.py

Redis caching layer for the Cloud Cost Monitoring platform.

Design decisions:
- Single module-level client (connection pool reuse). The old get_redis() helper
  was called at module level to create redis_client, but also exported — any
  caller using get_redis() directly would open a new connection each time.
  Removed get_redis(); use redis_client directly everywhere.
- Pipeline used for multi-key writes (dashboard snapshot) to reduce round trips.
- Key naming follows namespace:entity:qualifier pattern for clarity and to
  allow pattern-based flushing (e.g., SCAN MATCH "dashboard:*").
- All TTLs are constants at the top for easy tuning.

Redis data structure choices:
  HASH  — dashboard snapshots, anomaly stats (multiple fields, partial update)
  ZSET  — cost rankings (built-in score-based ordering, O(log N) insert)
  LIST  — recent alerts (LPUSH + LTRIM = bounded FIFO queue, O(1))
  STRING (JSON) — trend data, daily summaries (single blob, atomic read/write)
"""

import redis
import json
from decimal import Decimal


# ==================== TTL CONSTANTS ====================

_TTL_DASHBOARD_SEC  = 60       # dashboard snapshot: 1 minute
_TTL_COST_RANK_SEC  = 86400    # cost ranking: 1 day
_TTL_SUMMARY_SEC    = 300      # daily summary cache: 5 minutes
_TTL_TREND_SEC      = 600      # trend data: 10 minutes
_ALERT_LIST_MAX     = 50       # max recent alerts kept in Redis per account


# ==================== CONNECTION ====================
# Single client with default connection pool (max_connections=10 by default).
# decode_responses=True so all values come back as str, not bytes.

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)


def check_connection():
    try:
        redis_client.ping()
        return True
    except redis.ConnectionError:
        print("Redis connection failed")
        return False


# ==================== SERIALISATION HELPER ====================

def _serialize(obj):
    """Convert Decimal and nested structures to JSON-safe types."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(i) for i in obj]
    return obj


# ==================== HASH: Dashboard Snapshot ====================
# Stored as a Redis HASH so individual fields can be updated atomically
# without deserialising and re-serialising the entire object.
# e.g. HSET dashboard:acct-001 alert_count 5  (after a new alert fires)

def set_dashboard_snapshot(account_id, total_spend, active_resources, top_service, alert_count):
    key = f"dashboard:{account_id}"

    # Pipeline: send HSET + EXPIRE in a single round trip
    pipe = redis_client.pipeline()
    pipe.hset(key, mapping={
        'total_spend':      str(total_spend),
        'active_resources': str(active_resources),
        'top_service':      str(top_service),
        'alert_count':      str(alert_count)
    })
    pipe.expire(key, _TTL_DASHBOARD_SEC)
    pipe.execute()


def get_dashboard_snapshot(account_id):
    key  = f"dashboard:{account_id}"
    data = redis_client.hgetall(key)

    if not data:
        return None

    return {
        'total_spend':      float(data.get('total_spend', 0)),
        'active_resources': int(data.get('active_resources', 0)),
        'top_service':      data.get('top_service', ''),
        'alert_count':      int(data.get('alert_count', 0))
    }


def update_dashboard_alert_count(account_id, delta: int = 1):
    """
    Increment alert_count in-place using HINCRBY — avoids a read-modify-write
    race condition when multiple anomaly detectors run concurrently.
    """
    key = f"dashboard:{account_id}"
    if redis_client.exists(key):
        redis_client.hincrby(key, 'alert_count', delta)


# ==================== SORTED SET: Cost Ranking ====================
# ZSET keyed by date. Score = total cost. ZREVRANGE gives highest-cost accounts.
# Each account is a member; adding the same account twice just updates its score.

def update_cost_ranking(date, account_id, total_cost):
    key = f"cost_rank:{date}"
    pipe = redis_client.pipeline()
    pipe.zadd(key, {account_id: float(total_cost)})
    pipe.expire(key, _TTL_COST_RANK_SEC)
    pipe.execute()


def get_top_accounts_by_cost(date, top_n=10):
    key     = f"cost_rank:{date}"
    results = redis_client.zrevrange(key, 0, top_n - 1, withscores=True)
    return [{'account_id': acc, 'total_cost': score} for acc, score in results]


# ==================== STRING: Daily Summary Cache ====================
# Full daily summary stored as JSON blob. Keyed by account + date.
# Short TTL (5 min) because the anomaly detector may update the summary.

def cache_daily_summary(account_id, date, summary_item):
    key = f"cache:summary:{account_id}:{date}"
    redis_client.setex(key, _TTL_SUMMARY_SEC, json.dumps(_serialize(summary_item)))


def get_cached_daily_summary(account_id, date):
    key = f"cache:summary:{account_id}:{date}"
    raw = redis_client.get(key)
    return json.loads(raw) if raw else None


# ==================== LIST: Recent Alerts ====================
# LPUSH prepends the latest alert; LTRIM keeps list bounded at _ALERT_LIST_MAX.
# LRANGE returns elements in order newest-first, matching dashboard display.

def push_recent_alert(account_id, alert_message):
    key = f"alerts:recent:{account_id}"
    pipe = redis_client.pipeline()
    pipe.lpush(key, alert_message)
    pipe.ltrim(key, 0, _ALERT_LIST_MAX - 1)
    pipe.execute()


def get_recent_alerts(account_id, count=20):
    key = f"alerts:recent:{account_id}"
    return redis_client.lrange(key, 0, count - 1)


# ==================== HASH: Anomaly Stats ====================
# Stored as HASH for the same reason as dashboard — fields update incrementally.
# No TTL: stats should persist across restarts (they're expensive to recompute).

def update_anomaly_stats(account_id, mean, std_dev, count):
    key = f"anomaly:{account_id}"
    redis_client.hset(key, mapping={
        'mean':    str(mean),
        'std_dev': str(std_dev),
        'count':   str(count)
    })


def get_anomaly_stats(account_id):
    key  = f"anomaly:{account_id}"
    data = redis_client.hgetall(key)

    if not data:
        return None

    return {
        'mean':    float(data.get('mean', 0)),
        'std_dev': float(data.get('std_dev', 0)),
        'count':   int(data.get('count', 0))
    }


# ==================== STRING: Trend Cache ====================

def cache_trend_data(account_id, trend_list):
    key = f"trend:{account_id}:7d"
    redis_client.setex(key, _TTL_TREND_SEC, json.dumps(_serialize(trend_list)))


def get_cached_trend_data(account_id):
    key = f"trend:{account_id}:7d"
    raw = redis_client.get(key)
    return json.loads(raw) if raw else None


# ==================== UTILITIES ====================

def flush_all_cache():
    redis_client.flushall()
    print("Redis: all keys flushed")


def get_redis_info():
    info_mem     = redis_client.info('memory')
    info_clients = redis_client.info('clients')
    return {
        'used_memory_human':  info_mem.get('used_memory_human', 'N/A'),
        'connected_clients':  info_clients.get('connected_clients', 0),
        'total_keys':         redis_client.dbsize()
    }


# ==================== MAIN ====================

if __name__ == '__main__':
    print("Running Redis tests...\n")

    if not check_connection():
        exit()

    set_dashboard_snapshot('acct-001', 342.18, 12, 'EC2', 3)
    print("Dashboard:", get_dashboard_snapshot('acct-001'))

    update_cost_ranking('2025-01-15', 'acct-001', 342.18)
    update_cost_ranking('2025-01-15', 'acct-002', 189.50)
    update_cost_ranking('2025-01-15', 'acct-003', 520.00)
    print("Ranking:", get_top_accounts_by_cost('2025-01-15'))

    sample = {
        'account_id': 'acct-001', 'date': '2025-01-15',
        'total_cost': 342.18, 'service_breakdown': {'EC2': 180.5},
        'anomaly_flag': False, 'budget_utilization_pct': 78.5
    }
    cache_daily_summary('acct-001', '2025-01-15', sample)
    print("Cached summary:", get_cached_daily_summary('acct-001', '2025-01-15'))

    push_recent_alert('acct-001', 'Cost spike detected')
    print("Alerts:", get_recent_alerts('acct-001'))

    update_anomaly_stats('acct-001', 300, 45, 30)
    print("Stats:", get_anomaly_stats('acct-001'))

    cache_trend_data('acct-001', [{'date': '2025-01-10', 'total_cost': 300}])
    print("Trend:", get_cached_trend_data('acct-001'))

    print("\nRedis info:", get_redis_info())
