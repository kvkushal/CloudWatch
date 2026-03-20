"""
anomaly_detector.py

Z-score based cost anomaly detection for the Cloud Cost Monitoring platform.

Algorithm:
  Uses Welford's online algorithm for numerically stable incremental mean and
  variance computation. The naive approach (variance = E[X²] - E[X]²) suffers
  from catastrophic cancellation when values are large relative to their spread.
  Welford's method avoids this by tracking the sum of squared deviations (M2)
  directly. Reference: Welford (1962), "Note on a method for calculating
  corrected sums of squares and products."

Anomaly threshold:
  Z-score > 2.0 → warning (cost is 2 standard deviations above the mean)
  Z-score > 3.0 → critical (cost is 3 standard deviations above the mean)

  Under a normal distribution, |z| > 2 occurs ~4.5% of the time by chance.
  |z| > 3 occurs ~0.3% of the time. These thresholds are tunable via
  Z_SCORE_THRESHOLD.

CAP trade-off:
  Redis (AP, eventual) stores the rolling stats. In the event of a Redis
  failure, detection falls back gracefully (returns None for count < 3).
  DynamoDB reads use ConsistentRead=True (see dynamo_manager) to ensure
  the cost record we're evaluating is the latest write, not a stale replica.
"""

import math
from datetime import datetime, timezone, timedelta

import dynamo_manager
import redis_manager


# ==================== CONFIG ====================

ACCOUNTS = ['acct-001', 'acct-002', 'acct-003', 'acct-004', 'acct-005']

Z_SCORE_THRESHOLD = 2.0

BUDGETS = {
    'acct-001': 10000,
    'acct-002': 7500,
    'acct-003': 15000,
    'acct-004': 5000,
    'acct-005': 12000
}

# Minimum data points before anomaly detection runs.
# Below this threshold we don't have enough history to trust the statistics.
MIN_HISTORY_COUNT = 3


# ==================== Z-SCORE ====================

def compute_z_score(value, mean, std_dev):
    if std_dev == 0:
        return 0.0
    return round((value - mean) / std_dev, 4)


def update_rolling_stats(account_id, new_value):
    """
    Welford's online algorithm for numerically stable incremental mean/variance.

    Standard incremental variance (E[X²] - E[X]²) suffers from catastrophic
    cancellation when values cluster near a large mean. Welford's method tracks
    M2 = sum of squared deviations from the current mean, updating it as each
    new value arrives. Variance = M2 / n, std_dev = sqrt(variance).

    We store (mean, std_dev, count) in Redis and recompute M2 on each call
    since Redis doesn't store M2 directly. This is an approximation — for a
    production system with many concurrent writers, store M2 explicitly.
    """
    stats = redis_manager.get_anomaly_stats(account_id)

    if stats is None or stats['count'] == 0:
        redis_manager.update_anomaly_stats(account_id, new_value, 0.0, 1)
        return {'mean': new_value, 'std_dev': 0.0, 'count': 1}

    old_mean  = stats['mean']
    old_std   = stats['std_dev']
    old_count = stats['count']

    # Welford update
    new_count = old_count + 1
    new_mean  = old_mean + (new_value - old_mean) / new_count

    # Reconstruct M2 from stored std_dev, then update
    old_m2   = (old_std ** 2) * old_count
    new_m2   = old_m2 + (new_value - old_mean) * (new_value - new_mean)
    new_std  = round(math.sqrt(new_m2 / new_count), 4)

    redis_manager.update_anomaly_stats(
        account_id,
        round(new_mean, 4),
        new_std,
        new_count
    )

    return {'mean': new_mean, 'std_dev': new_std, 'count': new_count}


# ==================== ALERT ====================

def create_alert(account_id, alert_type, severity, message):
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    alert_item = {
        'account_id':      account_id,
        'alert_timestamp': now,
        'alert_type':      alert_type,
        'severity':        severity,
        'message':         message,
        'acknowledged':    False
    }

    dynamo_manager.put_alert(alert_item)

    # Also push to Redis list for low-latency dashboard reads
    alert_display = f"[{severity.upper()}] {alert_type}: {message}"
    redis_manager.push_recent_alert(account_id, alert_display)

    # Increment the dashboard alert count without a full snapshot refresh
    redis_manager.update_dashboard_alert_count(account_id, delta=1)

    return alert_item


# ==================== DETECTION ====================

def detect_cost_anomalies(account_id, date=None):
    """
    Detects whether the given day's cost is a statistical anomaly relative
    to historical data. Detection runs BEFORE updating stats so the current
    day's value is evaluated against past history, not included in the baseline.
    """
    if date is None:
        date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    summaries = dynamo_manager.query_daily_costs(account_id, date, date)
    if not summaries:
        return None

    summary    = summaries[0]
    daily_cost = float(summary['total_cost'])

    stats = redis_manager.get_anomaly_stats(account_id)

    if stats is None or stats['count'] < MIN_HISTORY_COUNT:
        # Not enough history — update stats and skip detection
        update_rolling_stats(account_id, daily_cost)
        return None

    z_score = compute_z_score(daily_cost, stats['mean'], stats['std_dev'])

    result = {
        'account_id': account_id,
        'date':       date,
        'daily_cost': daily_cost,
        'mean':       stats['mean'],
        'std_dev':    stats['std_dev'],
        'z_score':    z_score,
        'is_anomaly': abs(z_score) > Z_SCORE_THRESHOLD
    }

    if result['is_anomaly']:
        direction = "above" if z_score > 0 else "below"
        pct_diff  = round(
            abs(daily_cost - stats['mean']) / stats['mean'] * 100, 1
        ) if stats['mean'] != 0 else 0

        breakdown   = summary.get('service_breakdown', {})
        top_service = (
            max(breakdown, key=lambda k: float(breakdown[k]))
            if breakdown else 'Unknown'
        )

        message  = (
            f"Daily cost ${daily_cost:.2f} is {pct_diff}% {direction} average "
            f"(mean=${stats['mean']:.2f}, z={z_score:.2f}). "
            f"Top contributor: {top_service}"
        )
        severity = 'critical' if abs(z_score) > 3.0 else 'warning'
        create_alert(account_id, 'anomaly', severity, message)
        result['alert_created'] = True
    else:
        result['alert_created'] = False

    # Update stats AFTER detection so current value doesn't inflate the baseline
    update_rolling_stats(account_id, daily_cost)

    return result


def detect_budget_breach(account_id, date=None):
    if date is None:
        date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    summaries = dynamo_manager.query_daily_costs(account_id, date, date)
    if not summaries:
        return None

    summary      = summaries[0]
    daily_cost   = float(summary['total_cost'])
    daily_budget = BUDGETS.get(account_id, 10000) / 30
    utilization  = round((daily_cost / daily_budget) * 100, 2)

    result = {
        'account_id':   account_id,
        'date':         date,
        'daily_cost':   daily_cost,
        'daily_budget': round(daily_budget, 2),
        'utilization':  utilization,
        'breached':     utilization > 100
    }

    if result['breached']:
        message  = (
            f"Budget breach: spent ${daily_cost:.2f} vs "
            f"daily budget ${daily_budget:.2f} ({utilization}% utilization)"
        )
        severity = 'critical' if utilization > 150 else 'warning'
        create_alert(account_id, 'budget_breach', severity, message)
        result['alert_created'] = True
    else:
        result['alert_created'] = False

    return result


# ==================== RUN ====================

def run_full_detection():
    print("Running anomaly detection...\n")

    anomaly_count      = 0
    budget_breach_count = 0
    total_checked      = 0

    for account in ACCOUNTS:
        all_summaries = dynamo_manager.query_daily_costs(account)
        if not all_summaries:
            continue

        all_summaries   = sorted(all_summaries, key=lambda x: x['date'])
        account_anomalies = 0

        for summary in all_summaries:
            date = summary['date']
            total_checked += 1

            result = detect_cost_anomalies(account, date)
            if result and result['is_anomaly']:
                anomaly_count += 1
                account_anomalies += 1
                print(f"  ANOMALY {account} {date}: "
                      f"${result['daily_cost']:.2f} z={result['z_score']:.2f}")

            budget_result = detect_budget_breach(account, date)
            if budget_result and budget_result['breached']:
                budget_breach_count += 1
                print(f"  BUDGET  {account} {date}: "
                      f"{budget_result['utilization']}%")

        print(f"  {account}: {account_anomalies} anomalies")

    print(f"\nTotal checked: {total_checked}")
    print(f"Anomalies: {anomaly_count}")
    print(f"Budget breaches: {budget_breach_count}")


# ==================== DASHBOARD REFRESH ====================

def refresh_dashboards():
    print("\nRefreshing dashboards...")

    for account in ACCOUNTS:
        alerts    = dynamo_manager.query_alerts(account)
        today     = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        week_ago  = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
        summaries = dynamo_manager.query_daily_costs(account, week_ago, today)

        if summaries:
            latest      = max(summaries, key=lambda x: x['date'])
            breakdown   = latest.get('service_breakdown', {})
            top_service = (
                max(breakdown, key=lambda k: float(breakdown[k]))
                if breakdown else 'N/A'
            )

            redis_manager.set_dashboard_snapshot(
                account,
                float(latest['total_cost']),
                15,
                top_service,
                len(alerts)
            )
            print(f"  {account}: {len(alerts)} alerts")


# ==================== VERIFY ====================

def verify_alerts():
    print("\nAlert verification:\n")

    for account in ACCOUNTS:
        db_alerts    = dynamo_manager.query_alerts(account)
        redis_alerts = redis_manager.get_recent_alerts(account)

        print(f"{account}: DB={len(db_alerts)}, Redis={len(redis_alerts)}")
        if redis_alerts:
            print(f"  Latest: {redis_alerts[0]}")


# ==================== MAIN ====================

if __name__ == '__main__':
    print("=" * 50)
    print("ANOMALY DETECTOR")
    print("=" * 50 + "\n")

    run_full_detection()
    refresh_dashboards()
    verify_alerts()
