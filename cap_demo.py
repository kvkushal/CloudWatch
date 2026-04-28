"""
cap_demo.py

Live demonstration of CAP theorem trade-offs in the Cloud Cost platform.
Shows consistency models in action — not just comments.

Run with: python cap_demo.py
Requires: DynamoDB Local (port 8000) + Redis (port 6379) running with data seeded.
"""

import time
import json
from decimal import Decimal
from datetime import datetime, timezone

import dynamo_manager
import redis_manager


def banner(title):
    print(f"\n{'═' * 64}")
    print(f"  {title}")
    print(f"{'═' * 64}\n")


def step(msg):
    print(f"  → {msg}")


def result(label, value, color=""):
    print(f"    {label}: {value}")


def pause():
    input("\n  [Press Enter to continue...]\n")


# ==================== DEMO 1 ====================

def demo_strong_vs_eventual():
    """
    Demonstrates DynamoDB strong vs eventual consistency.
    Shows that ConsistentRead=True always returns latest write.
    """
    banner("DEMO 1: Strong vs Eventual Consistency (DynamoDB)")

    print("  DynamoDB offers two read consistency modes:")
    print("    - Eventual (default): May return stale data, costs 0.5 RCU")
    print("    - Strong (ConsistentRead=True): Always latest, costs 1.0 RCU")
    print("    This is the CP vs AP trade-off in action.\n")

    acct = "acct-001"
    test_date = "9999-12-31"  # Use a future date to avoid collision

    # Step 1: Write a record
    step("Writing a cost record to DailyCostSummary...")
    item = {
        'account_id': acct,
        'date': test_date,
        'total_cost': 999.99,
        'service_breakdown': {'EC2': 500.0, 'S3': 499.99},
        'anomaly_flag': False,
        'budget_utilization_pct': 300.0
    }
    dynamo_manager.put_daily_cost_summary(item)
    result("Written", f"account={acct}, date={test_date}, total=$999.99")

    # Step 2: Strong read
    step("Reading with STRONG consistency (ConsistentRead=True)...")
    t1 = time.time()
    table = dynamo_manager._resource().Table('DailyCostSummary')
    strong_resp = table.get_item(
        Key={'account_id': acct, 'date': test_date},
        ConsistentRead=True
    )
    t1_ms = (time.time() - t1) * 1000
    strong_val = strong_resp.get('Item', {}).get('total_cost', 'NOT FOUND')
    result("Strong read result", f"${float(strong_val):.2f}  ({t1_ms:.1f}ms)")
    result("Guarantee", "Always returns the latest committed write")
    result("Cost", "1.0 RCU per read")

    # Step 3: Eventual read
    step("Reading with EVENTUAL consistency (default)...")
    t2 = time.time()
    eventual_resp = table.get_item(
        Key={'account_id': acct, 'date': test_date},
        ConsistentRead=False
    )
    t2_ms = (time.time() - t2) * 1000
    eventual_val = eventual_resp.get('Item', {}).get('total_cost', 'NOT FOUND')
    result("Eventual read result", f"${float(eventual_val):.2f}  ({t2_ms:.1f}ms)")
    result("Guarantee", "May return stale data (but faster, cheaper)")
    result("Cost", "0.5 RCU per read")

    # Step 4: Update and re-read
    step("Now UPDATING the record to $1500.00...")
    item['total_cost'] = 1500.00
    dynamo_manager.put_daily_cost_summary(item)

    step("Immediate STRONG read after update...")
    strong_resp2 = table.get_item(
        Key={'account_id': acct, 'date': test_date},
        ConsistentRead=True
    )
    val2 = float(strong_resp2['Item']['total_cost'])
    result("Strong read", f"${val2:.2f} ← Always sees the update immediately")

    step("Immediate EVENTUAL read after update...")
    eventual_resp2 = table.get_item(
        Key={'account_id': acct, 'date': test_date},
        ConsistentRead=False
    )
    val3 = float(eventual_resp2['Item']['total_cost'])
    result("Eventual read", f"${val3:.2f} ← May or may not see update yet")

    # Cleanup
    table.delete_item(Key={'account_id': acct, 'date': test_date})
    step("Cleaned up test record.\n")

    print("  TAKEAWAY:")
    print("    Our platform uses STRONG reads for anomaly detection")
    print("    (must evaluate latest cost data to avoid false negatives)")
    print("    and EVENTUAL reads for analytics dashboards")
    print("    (acceptable staleness, saves 50% on RCU costs).")

    pause()


# ==================== DEMO 2 ====================

def demo_cache_aside():
    """
    Demonstrates the cache-aside pattern and eventual consistency
    between Redis (cache) and DynamoDB (source of truth).
    """
    banner("DEMO 2: Cache-Aside Pattern (Redis AP Cache)")

    print("  Redis is an AP (Available + Partition-tolerant) cache layer.")
    print("  It sacrifices consistency — cached data may be stale.")
    print("  The cache-aside pattern handles this with TTL expiration.\n")

    acct = "acct-001"

    # Step 1: Clear Redis cache
    step("Flushing Redis dashboard cache for acct-001...")
    redis_manager.redis_client.delete(f"dashboard:{acct}")
    result("Redis cache", "EMPTY (deleted)")

    # Step 2: Show cache miss → DynamoDB fallback
    step("Requesting dashboard snapshot...")
    cached = redis_manager.get_dashboard_snapshot(acct)
    result("Redis lookup", "MISS (key not found)")
    result("Action", "Fall back to DynamoDB query")

    step("Querying DynamoDB for latest cost data...")
    from datetime import timedelta
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
    summaries = dynamo_manager.query_daily_costs(acct, week_ago, today)

    if summaries:
        latest = max(summaries, key=lambda x: x['date'])
        spend = float(latest['total_cost'])
        result("DynamoDB result", f"${spend:.2f} (latest cost for {acct})")

        step("Populating Redis cache from DynamoDB result...")
        redis_manager.set_dashboard_snapshot(acct, spend, 15, 'EC2', 3)
        result("Redis cache", "POPULATED (TTL = 60 seconds)")

    # Step 3: Show cache hit
    step("Requesting dashboard again...")
    t1 = time.time()
    cached = redis_manager.get_dashboard_snapshot(acct)
    redis_ms = (time.time() - t1) * 1000
    result("Redis lookup", f"HIT — ${cached['total_spend']:.2f} in {redis_ms:.1f}ms")

    # Step 4: Show stale data scenario
    step("Simulating stale data: setting Redis value to $9999.00...")
    redis_manager.set_dashboard_snapshot(acct, 9999.00, 15, 'EC2', 3)

    step("Redis now returns stale (wrong) data:")
    stale = redis_manager.get_dashboard_snapshot(acct)
    result("Redis value", f"${stale['total_spend']:.2f} ← STALE (not in DynamoDB)")
    result("DynamoDB value", f"${spend:.2f} ← TRUTH")
    result("Self-healing", "TTL expires in 60s → next read goes to DynamoDB → cache refreshed")

    # Reset
    redis_manager.set_dashboard_snapshot(acct, spend, 15, 'EC2', 3)
    step("Reset cache to correct value.\n")

    print("  TAKEAWAY:")
    print("    Redis (AP) prioritizes availability over consistency.")
    print("    Stale reads are acceptable for dashboards (60s max).")
    print("    TTL-based expiration provides eventual consistency.")

    pause()


# ==================== DEMO 3 ====================

def demo_graceful_degradation():
    """
    Demonstrates that the system still works when Redis is unavailable.
    Shows AP behavior — availability is preserved.
    """
    banner("DEMO 3: Graceful Degradation (Redis Failure)")

    print("  In an AP system, the cache layer can fail without")
    print("  breaking the application. We demonstrate this.\n")

    acct = "acct-001"

    # Step 1: Normal operation with cache
    step("Normal operation — Redis is healthy:")
    t1 = time.time()
    snap = redis_manager.get_dashboard_snapshot(acct)
    redis_ms = (time.time() - t1) * 1000

    if snap:
        result("Redis response", f"${snap['total_spend']:.2f} in {redis_ms:.1f}ms ✓")
    else:
        result("Redis response", f"MISS in {redis_ms:.1f}ms (no cached data)")

    # Step 2: Simulate cache unavailability
    step("Simulating cache failure — deleting all dashboard keys...")
    for a in ['acct-001', 'acct-002', 'acct-003', 'acct-004', 'acct-005']:
        redis_manager.redis_client.delete(f"dashboard:{a}")
    result("Redis dashboard keys", "ALL DELETED (simulates failure)")

    # Step 3: Show DynamoDB fallback
    step("Requesting dashboard — Redis misses, falls back to DynamoDB...")
    t2 = time.time()
    from datetime import timedelta
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
    summaries = dynamo_manager.query_daily_costs(acct, week_ago, today)
    dynamo_ms = (time.time() - t2) * 1000

    if summaries:
        latest = max(summaries, key=lambda x: x['date'])
        cost = float(latest['total_cost'])
        result("DynamoDB fallback", f"${cost:.2f} in {dynamo_ms:.1f}ms ✓")
        result("Status", "System STILL WORKS — just slower")

        # Repopulate cache
        redis_manager.set_dashboard_snapshot(acct, cost, 15, 'EC2', 3)

    # Step 4: Compare latencies
    step("Latency comparison:")
    result("With Redis (cache hit)", f"~{redis_ms:.1f}ms")
    result("Without Redis (DB only)", f"~{dynamo_ms:.1f}ms")
    result("Degradation", f"{dynamo_ms/redis_ms:.1f}x slower, but functional" if redis_ms > 0 else "N/A")

    print("\n  TAKEAWAY:")
    print("    When Redis fails, the system degrades gracefully.")
    print("    Latency increases but availability is preserved.")
    print("    This is the AP guarantee — partition tolerance + availability.")

    pause()


# ==================== SUMMARY ====================

def summary():
    banner("CAP THEOREM SUMMARY FOR THIS PLATFORM")

    print("""
  ┌─────────────────────────────────────────────────────────┐
  │  Component          │ CAP Mode │ Why                    │
  ├─────────────────────┼──────────┼────────────────────────┤
  │ DynamoDB (anomaly)  │ CP       │ Must read latest cost  │
  │ DynamoDB (analytics)│ AP       │ Stale OK, saves RCU    │
  │ Redis (cache)       │ AP       │ Fast reads, TTL heals  │
  └─────────────────────┴──────────┴────────────────────────┘

  Key design decisions demonstrated:
  1. Strong consistency for correctness-critical paths (anomaly detection)
  2. Eventual consistency for performance-critical paths (dashboard)
  3. Cache-aside pattern with TTL for self-healing staleness
  4. Graceful degradation when cache layer fails
""")


# ==================== MAIN ====================

if __name__ == '__main__':
    banner("CAP THEOREM")

    demo_strong_vs_eventual()
    demo_cache_aside()
    demo_graceful_degradation()
    summary()

    print("  All demos complete.\n")
