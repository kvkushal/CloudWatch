"""
query_cli.py

Interactive Query Console for the Cloud Cost Monitoring platform.
Demonstrates manual database interaction with DynamoDB and Redis.

Run with: python query_cli.py
Requires: DynamoDB Local (port 8000) + Redis (port 6379) running
"""

import time
from collections import Counter
from decimal import Decimal

import dynamo_manager
import redis_manager

# ==================== TABLE FORMATTING ====================

def fmt_table(headers, rows, col_widths=None):
    """Print a formatted ASCII table."""
    if not rows:
        print("  (no results)")
        return

    if col_widths is None:
        col_widths = []
        for i, h in enumerate(headers):
            max_w = len(str(h))
            for r in rows:
                val = str(r[i]) if i < len(r) else ""
                max_w = max(max_w, len(val))
            col_widths.append(min(max_w + 2, 40))

    def fmt_row(vals, sep="│"):
        parts = []
        for i, v in enumerate(vals):
            w = col_widths[i] if i < len(col_widths) else 20
            parts.append(f" {str(v):<{w-1}}")
        return sep + sep.join(parts) + sep

    border = "┌" + "┬".join("─" * w for w in col_widths) + "┐"
    mid    = "├" + "┼".join("─" * w for w in col_widths) + "┤"
    bottom = "└" + "┴".join("─" * w for w in col_widths) + "┘"

    print(border)
    print(fmt_row(headers))
    print(mid)
    for r in rows:
        print(fmt_row(r))
    print(bottom)
    print(f"  {len(rows)} row(s) returned\n")


def to_float(v):
    try:
        return float(v)
    except:
        return 0.0


# ==================== DYNAMODB QUERIES ====================

def q1_list_tables():
    """List all DynamoDB tables with item counts."""
    print("\n  Query: dynamodb.list_tables()")
    print("  Type:  ListTables API call\n")
    tables = dynamo_manager.list_tables()
    rows = []
    for t in tables:
        try:
            desc = dynamo_manager._client().describe_table(TableName=t)
            count = desc['Table']['ItemCount']
            status = desc['Table']['TableStatus']
            kb = desc['Table'].get('TableSizeBytes', 0) / 1024
        except:
            count, status, kb = "?", "?", 0
        rows.append((t, count, status, f"{kb:.1f} KB"))
    fmt_table(["Table Name", "Item Count", "Status", "Size"], rows)


def q2_daily_costs():
    """Query daily cost summaries by account + date range."""
    acct = input("  Account ID [acct-001]: ").strip() or "acct-001"
    start = input("  Start date (YYYY-MM-DD) [all]: ").strip() or None
    end = input("  End date   (YYYY-MM-DD) [all]: ").strip() or None

    print(f"\n  Query: table.query(KeyConditionExpression="
          f"Key('account_id').eq('{acct}'))")
    if start and end:
        print(f"         + Key('date').between('{start}', '{end}')")
    print("  Table: DailyCostSummary | Key: account_id (PK) + date (SK)\n")

    items = dynamo_manager.query_daily_costs(acct, start, end)
    items = sorted(items, key=lambda x: x['date'])

    rows = []
    for c in items:
        bd = c.get('service_breakdown', {})
        top_svc = max(bd, key=lambda k: to_float(bd[k])) if bd else "N/A"
        rows.append((
            c['account_id'], c['date'],
            f"${to_float(c['total_cost']):.2f}",
            top_svc,
            f"{to_float(c.get('budget_utilization_pct', 0)):.1f}%",
            "YES" if c.get('anomaly_flag') else ""
        ))
    fmt_table(["Account", "Date", "Total Cost", "Top Service",
               "Budget %", "Anomaly"], rows)


def q3_usage_by_service():
    """Query resource usage by service type using GSI."""
    svc = input("  Service [EC2/S3/Lambda/RDS/CloudFront]: ").strip() or "EC2"
    print(f"\n  Query: table.query(IndexName='ResourceTypeIndex', "
          f"KeyConditionExpression=Key('resource_type').eq('{svc}'))")
    print("  GSI:   ResourceTypeIndex (resource_type → timestamp)\n")

    items = dynamo_manager.query_usage_by_resource_type(svc)
    rows = []
    for r in items[:20]:
        rows.append((
            r.get('account_id', ''), r.get('resource_type', ''),
            r.get('resource_id', ''), r.get('region', ''),
            f"{to_float(r.get('usage_quantity', 0)):.2f}",
            f"${to_float(r.get('cost_usd', 0)):.4f}",
            r.get('timestamp', '')[:19]
        ))
    fmt_table(["Account", "Service", "Resource ID", "Region",
               "Usage", "Cost USD", "Timestamp"], rows)
    if len(items) > 20:
        print(f"  ... showing 20 of {len(items)} total records")


def q4_usage_by_region():
    """Query resource usage by region using GSI."""
    region = input("  Region [us-east-1/ap-south-1/eu-west-1]: ").strip() or "us-east-1"
    print(f"\n  Query: table.query(IndexName='RegionIndex', "
          f"KeyConditionExpression=Key('region').eq('{region}'))")
    print("  GSI:   RegionIndex (region → timestamp)\n")

    items = dynamo_manager.query_usage_by_region(region)
    rows = []
    for r in items[:20]:
        rows.append((
            r.get('account_id', ''), r.get('resource_type', ''),
            r.get('resource_id', ''),
            f"{to_float(r.get('usage_quantity', 0)):.2f}",
            f"${to_float(r.get('cost_usd', 0)):.4f}",
            r.get('timestamp', '')[:19]
        ))
    fmt_table(["Account", "Service", "Resource ID",
               "Usage", "Cost USD", "Timestamp"], rows)
    if len(items) > 20:
        print(f"  ... showing 20 of {len(items)} total records")


def q5_alerts():
    """Query alerts by account."""
    acct = input("  Account ID [acct-001]: ").strip() or "acct-001"
    print(f"\n  Query: table.query(KeyConditionExpression="
          f"Key('account_id').eq('{acct}'))")
    print("  Table: Alerts | Key: account_id (PK) + alert_timestamp (SK)\n")

    items = dynamo_manager.query_alerts(acct)
    rows = []
    for a in items:
        msg = a.get('message', '')
        if len(msg) > 50:
            msg = msg[:50] + "..."
        rows.append((
            a.get('account_id', ''), a.get('severity', ''),
            a.get('alert_type', ''), msg,
            a.get('alert_timestamp', '')[:19]
        ))
    fmt_table(["Account", "Severity", "Type", "Message", "Timestamp"], rows)


def q6_recommendations():
    """Query recommendations by account."""
    acct = input("  Account ID [acct-001]: ").strip() or "acct-001"
    print(f"\n  Query: table.query(KeyConditionExpression="
          f"Key('account_id').eq('{acct}'))")
    print("  Table: OptimizationRecommendations\n")

    items = dynamo_manager.query_recommendations(acct)
    items = sorted(items, key=lambda x: -to_float(x.get('estimated_monthly_savings', 0)))
    rows = []
    for r in items:
        rows.append((
            r.get('account_id', ''), r.get('rec_type', ''),
            r.get('resource_id', ''),
            f"${to_float(r.get('estimated_monthly_savings', 0)):.2f}/mo",
            r.get('status', '')
        ))
    fmt_table(["Account", "Type", "Resource", "Savings", "Status"], rows)


def q7_cross_account():
    """Cross-account cost comparison."""
    print("\n  Query: For each account → query_daily_costs() → aggregate")
    print("  Pattern: Scatter-gather across partition keys\n")

    accounts = ['acct-001', 'acct-002', 'acct-003', 'acct-004', 'acct-005']
    rows = []
    for acct in accounts:
        items = dynamo_manager.query_daily_costs(acct)
        if items:
            costs = [to_float(c['total_cost']) for c in items]
            rows.append((
                acct, len(items),
                f"${sum(costs):.2f}", f"${sum(costs)/len(costs):.2f}",
                f"${max(costs):.2f}", f"${min(costs):.2f}"
            ))
    fmt_table(["Account", "Days", "Total Spend", "Avg/Day",
               "Max Day", "Min Day"], rows)


def q8_top_services():
    """Top spending services across all accounts."""
    print("\n  Query: Aggregate service_breakdown across all DailyCostSummary records")
    print("  Pattern: Full partition scan + map-reduce\n")

    accounts = ['acct-001', 'acct-002', 'acct-003', 'acct-004', 'acct-005']
    totals = {}
    for acct in accounts:
        items = dynamo_manager.query_daily_costs(acct)
        for c in items:
            for svc, val in c.get('service_breakdown', {}).items():
                totals[svc] = totals.get(svc, 0) + to_float(val)

    rows = []
    for svc, total in sorted(totals.items(), key=lambda x: -x[1]):
        pct = (total / sum(totals.values()) * 100) if totals else 0
        rows.append((svc, f"${total:.2f}", f"{pct:.1f}%"))
    fmt_table(["Service", "Total Cost", "% of Spend"], rows)


# ==================== REDIS QUERIES ====================

def q9_redis_keys():
    """Show all Redis keys grouped by type."""
    print("\n  Command: KEYS * → TYPE each key")
    print("  Shows Redis data structure choices (HASH, ZSET, LIST, STRING)\n")

    all_keys = redis_manager.redis_client.keys('*')
    type_map = {}
    for k in all_keys:
        t = redis_manager.redis_client.type(k)
        prefix = k.split(':')[0]
        type_map.setdefault(prefix, {'type': t, 'count': 0, 'keys': []})
        type_map[prefix]['count'] += 1
        if len(type_map[prefix]['keys']) < 3:
            type_map[prefix]['keys'].append(k)

    structure_info = {
        'dashboard': 'Dashboard snapshots (TTL 60s)',
        'anomaly': 'Rolling mean/std for z-score',
        'alerts': 'Recent alerts, bounded FIFO',
        'cost_rank': 'Cost rankings by date',
        'trend': '7-day trend cache (TTL 10min)',
        'cache': 'Daily summary cache (TTL 5min)',
    }

    rows = []
    for prefix, info in sorted(type_map.items(), key=lambda x: -x[1]['count']):
        desc = structure_info.get(prefix, '')
        rows.append((prefix, info['type'].upper(), info['count'],
                      desc, ", ".join(info['keys'][:2])))
    fmt_table(["Prefix", "Redis Type", "Count", "Purpose", "Sample Keys"], rows)
    print(f"  Total keys: {len(all_keys)}")


def q10_dashboard_hash():
    """Get dashboard snapshot using HASH → HGETALL."""
    acct = input("  Account ID [acct-001]: ").strip() or "acct-001"
    key = f"dashboard:{acct}"
    print(f"\n  Command: HGETALL {key}")
    print("  Structure: HASH (partial field updates, atomic HINCRBY)\n")

    data = redis_manager.redis_client.hgetall(key)
    if data:
        rows = [(k, v) for k, v in data.items()]
        fmt_table(["Field", "Value"], rows)
    else:
        print("  (key not found or expired — TTL is 60s)")


def q11_cost_rankings():
    """Get cost rankings using ZSET → ZREVRANGE."""
    import datetime
    date = input("  Date [yesterday]: ").strip()
    if not date:
        date = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    key = f"cost_rank:{date}"
    print(f"\n  Command: ZREVRANGE {key} 0 9 WITHSCORES")
    print("  Structure: ZSET (score-based ordering, O(log N) insert)\n")

    results = redis_manager.redis_client.zrevrange(key, 0, 9, withscores=True)
    if results:
        rows = [(i+1, acc, f"${score:.2f}") for i, (acc, score) in enumerate(results)]
        fmt_table(["Rank", "Account", "Total Cost"], rows)
    else:
        print(f"  (no rankings for {date} — TTL is 24h)")


def q12_recent_alerts():
    """Get recent alerts using LIST → LRANGE."""
    acct = input("  Account ID [acct-001]: ").strip() or "acct-001"
    key = f"alerts:recent:{acct}"
    print(f"\n  Command: LRANGE {key} 0 9")
    print("  Structure: LIST (LPUSH + LTRIM = bounded FIFO queue)\n")

    alerts = redis_manager.redis_client.lrange(key, 0, 9)
    if alerts:
        rows = [(i+1, a[:80]) for i, a in enumerate(alerts)]
        fmt_table(["#", "Alert Message"], rows)
    else:
        print("  (no alerts in Redis list)")


def q13_anomaly_stats():
    """Get anomaly stats using HASH → HGETALL."""
    acct = input("  Account ID [acct-001]: ").strip() or "acct-001"
    key = f"anomaly:{acct}"
    print(f"\n  Command: HGETALL {key}")
    print("  Structure: HASH (no TTL — expensive to recompute)\n")

    data = redis_manager.redis_client.hgetall(key)
    if data:
        mean = float(data.get('mean', 0))
        std = float(data.get('std_dev', 0))
        count = int(data.get('count', 0))
        cv = (std / mean * 100) if mean else 0
        threshold = mean + 2 * std

        rows = [
            ("mean", f"${mean:.2f}", "Average daily cost"),
            ("std_dev", f"${std:.2f}", "Standard deviation"),
            ("count", str(count), "Data points analyzed"),
            ("CV", f"{cv:.1f}%", "Coefficient of variation"),
            ("threshold", f"${threshold:.2f}", "Anomaly threshold (mean + 2σ)"),
        ]
        fmt_table(["Stat", "Value", "Description"], rows)
    else:
        print("  (no anomaly stats for this account)")


def q14_trend_cache():
    """Get cached trend data using STRING → GET."""
    acct = input("  Account ID [acct-001]: ").strip() or "acct-001"
    key = f"trend:{acct}:7d"
    print(f"\n  Command: GET {key}")
    print("  Structure: STRING (JSON blob, atomic read/write, TTL 10min)\n")

    import json
    raw = redis_manager.redis_client.get(key)
    if raw:
        data = json.loads(raw)
        rows = [(d['date'], f"${d['total_cost']:.2f}") for d in data]
        fmt_table(["Date", "Total Cost"], rows)
        ttl = redis_manager.redis_client.ttl(key)
        print(f"  TTL remaining: {ttl}s")
    else:
        print("  (trend cache expired or not populated)")


# ==================== ADVANCED QUERIES ====================

def q15_live_anomaly():
    """Run anomaly detection live for a specific account+date."""
    import anomaly_detector
    acct = input("  Account ID [acct-001]: ").strip() or "acct-001"
    date = input("  Date [latest]: ").strip() or None

    if date is None:
        items = dynamo_manager.query_daily_costs(acct)
        if items:
            date = max(items, key=lambda x: x['date'])['date']
        else:
            print("  No data found")
            return

    print(f"\n  Running anomaly detection for {acct} on {date}...")
    print("  Algorithm: Welford's online z-score (numerically stable)\n")

    result = anomaly_detector.detect_cost_anomalies(acct, date)
    if result:
        rows = [
            ("Daily Cost", f"${result['daily_cost']:.2f}"),
            ("Mean", f"${result['mean']:.2f}"),
            ("Std Dev", f"${result['std_dev']:.2f}"),
            ("Z-Score", f"{result['z_score']:.4f}"),
            ("Is Anomaly", "YES ⚠️" if result['is_anomaly'] else "No"),
            ("Alert Created", "YES" if result.get('alert_created') else "No"),
        ]
        fmt_table(["Metric", "Value"], rows)
    else:
        print("  Not enough history for detection (need >= 3 data points)")


def q16_budget_report():
    """Budget utilization report across all accounts."""
    print("\n  Aggregating latest daily costs vs monthly budgets...\n")

    budgets = {
        'acct-001': 10000, 'acct-002': 7500, 'acct-003': 15000,
        'acct-004': 5000, 'acct-005': 12000
    }

    rows = []
    for acct in sorted(budgets.keys()):
        items = dynamo_manager.query_daily_costs(acct)
        if items:
            latest = max(items, key=lambda x: x['date'])
            cost = to_float(latest['total_cost'])
            daily_budget = budgets[acct] / 30
            util = (cost / daily_budget) * 100
            status = "🔴 BREACH" if util > 100 else "🟢 OK"
            rows.append((
                acct, f"${cost:.2f}", f"${daily_budget:.2f}",
                f"{util:.1f}%", status, latest['date']
            ))
    fmt_table(["Account", "Daily Cost", "Daily Budget",
               "Utilization", "Status", "Date"], rows)


def q17_service_trend():
    """Service cost trend comparison."""
    acct = input("  Account ID [acct-001]: ").strip() or "acct-001"
    print(f"\n  Querying service breakdown over time for {acct}...\n")

    items = dynamo_manager.query_daily_costs(acct)
    items = sorted(items, key=lambda x: x['date'])[-7:]

    if not items:
        print("  No data")
        return

    services = set()
    for c in items:
        services.update(c.get('service_breakdown', {}).keys())
    services = sorted(services)

    headers = ["Date"] + services + ["Total"]
    rows = []
    for c in items:
        bd = c.get('service_breakdown', {})
        row = [c['date']]
        for svc in services:
            row.append(f"${to_float(bd.get(svc, 0)):.2f}")
        row.append(f"${to_float(c['total_cost']):.2f}")
        rows.append(tuple(row))
    fmt_table(headers, rows)


def q18_cache_analysis():
    """Cache hit/miss analysis — Redis vs DynamoDB latency."""
    acct = "acct-001"
    print(f"\n  Comparing Redis cache vs DynamoDB direct for {acct}...\n")

    # Redis read
    start = time.time()
    redis_manager.get_dashboard_snapshot(acct)
    redis_ms = (time.time() - start) * 1000

    # DynamoDB read
    start = time.time()
    dynamo_manager.query_daily_costs(acct)
    dynamo_ms = (time.time() - start) * 1000

    speedup = dynamo_ms / redis_ms if redis_ms > 0 else 0

    rows = [
        ("Redis HASH (dashboard)", f"{redis_ms:.2f} ms", "Cache hit"),
        ("DynamoDB Query (costs)", f"{dynamo_ms:.2f} ms", "Direct read"),
        ("Speedup", f"{speedup:.1f}x", "Cache-aside benefit"),
    ]
    fmt_table(["Source", "Latency", "Note"], rows)


# ==================== MENU ====================

MENU = """
═══════════════════════════════════════════════════════
  CloudWatch — Interactive Query Console
  DynamoDB Local (port 8000) + Redis (port 6379)
═══════════════════════════════════════════════════════

  DYNAMODB QUERIES:
    1.  List all tables + item counts
    2.  Query daily costs by account + date range
    3.  Query resource usage by service (GSI: ResourceTypeIndex)
    4.  Query resource usage by region  (GSI: RegionIndex)
    5.  Query alerts by account
    6.  Query recommendations by account
    7.  Cross-account cost comparison
    8.  Top spending services (aggregate)

  REDIS QUERIES:
    9.  Show all Redis keys by type (HASH/ZSET/LIST/STRING)
    10. Dashboard snapshot   (HASH  → HGETALL)
    11. Cost rankings        (ZSET  → ZREVRANGE)
    12. Recent alerts        (LIST  → LRANGE)
    13. Anomaly stats        (HASH  → HGETALL)
    14. Cached trend data    (STRING → GET + JSON)

  ADVANCED / INSIGHT QUERIES:
    15. Anomaly detection live run
    16. Budget utilization report
    17. Service cost trend comparison
    18. Cache hit/miss analysis (Redis vs DynamoDB)

    0.  Exit
"""

HANDLERS = {
    '1': q1_list_tables, '2': q2_daily_costs, '3': q3_usage_by_service,
    '4': q4_usage_by_region, '5': q5_alerts, '6': q6_recommendations,
    '7': q7_cross_account, '8': q8_top_services, '9': q9_redis_keys,
    '10': q10_dashboard_hash, '11': q11_cost_rankings,
    '12': q12_recent_alerts, '13': q13_anomaly_stats,
    '14': q14_trend_cache, '15': q15_live_anomaly,
    '16': q16_budget_report, '17': q17_service_trend,
    '18': q18_cache_analysis,
}


if __name__ == '__main__':
    print(MENU)

    while True:
        choice = input("  Enter option (0-18): ").strip()
        if choice == '0':
            print("\n  Bye!\n")
            break
        handler = HANDLERS.get(choice)
        if handler:
            try:
                handler()
            except Exception as e:
                print(f"\n  Error: {e}\n")
        else:
            print("  Invalid option")
