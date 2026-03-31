import dynamo_manager
import redis_manager

W = 64  # box width

def box(title):
    print("\n" + "─" * W)
    print(f"  {title}")
    print("─" * W)

def row(label, value, indent=2):
    label_str = f"{' ' * indent}{label}"
    print(f"{label_str:<28}{value}")

def divider():
    print("  " + "·" * (W - 4))


# ─────────────────────────────────────────────
print("\n" + "═" * W)
print("  CloudWatch — Live Database Viewer")
print("  DynamoDB Local (port 8000)  +  Redis (port 6379)")
print("═" * W)


# ══════════════ DYNAMODB ══════════════

box("DYNAMODB — Tables")
tables = dynamo_manager.list_tables()
for t in tables:
    print(f"    ✓  {t}")


box("DYNAMODB — DailyCostSummary  (acct-001, last 3 days)")
costs = dynamo_manager.query_daily_costs('acct-001')
for c in sorted(costs, key=lambda x: x['date'])[-3:]:
    print(f"\n  Date: {c['date']}   Total: ${float(c['total_cost']):.2f}   "
          f"Budget util: {float(c.get('budget_utilization_pct', 0)):.1f}%")
    breakdown = sorted(c['service_breakdown'].items(), key=lambda x: -float(x[1]))
    for svc, val in breakdown:
        bar_len = int(float(val) / float(c['total_cost']) * 30)
        bar = "█" * bar_len
        print(f"    {svc:<12} ${float(val):>6.2f}  {bar}")


box("DYNAMODB — OptimizationRecommendations  (acct-001, top 5)")
recs = dynamo_manager.query_recommendations('acct-001')
recs_sorted = sorted(recs, key=lambda x: -float(x['estimated_monthly_savings']))
type_labels = {
    'switch_to_reserved': 'RESERVED',
    'rightsizing':        'RIGHTSIZE',
    'delete_unused':      'DELETE',
    'terminate_idle':     'IDLE',
    'move_to_glacier':    'GLACIER',
}
for r in recs_sorted[:5]:
    label = type_labels.get(r['rec_type'], r['rec_type'].upper()[:9])
    saving = float(r['estimated_monthly_savings'])
    print(f"  [{label:<9}]  {r['resource_id']:<20}  saves ${saving:.2f}/mo")


box("DYNAMODB — Alerts  (acct-001)")
alerts = dynamo_manager.query_alerts('acct-001')
sev_counts = {}
for a in alerts:
    s = a.get('severity', 'info')
    sev_counts[s] = sev_counts.get(s, 0) + 1
print(f"  Total: {len(alerts)} alerts stored")
for sev, count in sev_counts.items():
    print(f"    {sev.upper():<10} {count}")
divider()
for a in alerts[:3]:
    sev = a.get('severity', 'info').upper()
    msg = a.get('message', '')[:58]
    print(f"  [{sev}] {msg}...")


box("DYNAMODB — ResourceUsage  (acct-001, record count)")
records = dynamo_manager.query_usage_by_account('acct-001')
from collections import Counter
by_service = Counter(r.get('resource_type') for r in records)
by_region  = Counter(r.get('region') for r in records)
print(f"  Total records: {len(records)}")
print(f"\n  By service:")
for svc, count in by_service.most_common():
    print(f"    {svc:<14} {count} records")
print(f"\n  By region:")
for region, count in by_region.most_common():
    print(f"    {region:<16} {count} records")


# ══════════════ REDIS ══════════════

box("REDIS — Key Summary")
all_keys = redis_manager.redis_client.keys('*')
key_types = {}
for k in all_keys:
    prefix = k.split(':')[0]
    key_types[prefix] = key_types.get(prefix, 0) + 1
print(f"  Total keys: {len(all_keys)}\n")
structure_map = {
    'dashboard': 'HASH   — dashboard snapshots (TTL 60s)',
    'anomaly':   'HASH   — rolling mean/std for z-score',
    'alerts':    'LIST   — recent alerts, bounded FIFO',
    'cost_rank': 'ZSET   — cost rankings by date',
    'trend':     'STRING — 7-day trend cache (TTL 10min)',
    'cache':     'STRING — daily summary cache (TTL 5min)',
}
for prefix, count in sorted(key_types.items(), key=lambda x: -x[1]):
    structure = structure_map.get(prefix, '')
    print(f"  {prefix:<12} {count:>3} keys   {structure}")


box("REDIS — Anomaly Stats  (all accounts)")
for acct in ['acct-001', 'acct-002', 'acct-003', 'acct-004', 'acct-005']:
    stats = redis_manager.get_anomaly_stats(acct)
    if stats:
        cv = (stats['std_dev'] / stats['mean'] * 100) if stats['mean'] else 0
        threshold = stats['mean'] + 2 * stats['std_dev']
        print(f"  {acct}   mean=${stats['mean']:.2f}   "
              f"std=${stats['std_dev']:.2f}   "
              f"CV={cv:.1f}%   "
              f"threshold=${threshold:.2f}")


box("REDIS — Cost Rankings  (2026-03-19)")
import datetime
date = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
rankings = redis_manager.get_top_accounts_by_cost(date)
if rankings:
    print(f"  Date: {date}\n")
    for i, r in enumerate(rankings, 1):
        bar = "█" * int(r['total_cost'] / 3)
        print(f"  {i}. {r['account_id']}   ${r['total_cost']:>7.2f}  {bar}")
else:
    print(f"  No rankings for {date} (TTL may have expired)")


box("REDIS — Recent Alerts  (acct-001)")
alerts_redis = redis_manager.get_recent_alerts('acct-001', count=3)
if alerts_redis:
    for a in alerts_redis:
        print(f"  {a[:62]}")
else:
    print("  No recent alerts in Redis (list empty or TTL expired)")


print("\n" + "═" * W)
print("  All database checks complete.")
print("═" * W + "\n")