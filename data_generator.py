import random
import uuid
from datetime import datetime, timedelta

import dynamo_manager
import redis_manager

# ==================== CONFIG ====================

ACCOUNTS = ['acct-001', 'acct-002', 'acct-003', 'acct-004', 'acct-005']

SERVICES = {
    'EC2':        {'unit': 'hours',       'base_cost_per_unit': 0.085,      'base_usage': 24.0},
    'S3':         {'unit': 'GB',          'base_cost_per_unit': 0.023,      'base_usage': 50.0},
    'Lambda':     {'unit': 'invocations', 'base_cost_per_unit': 0.0000002,  'base_usage': 500000.0},
    'RDS':        {'unit': 'hours',       'base_cost_per_unit': 0.145,      'base_usage': 24.0},
    'CloudFront': {'unit': 'GB',          'base_cost_per_unit': 0.085,      'base_usage': 100.0}
}

REGIONS = ['us-east-1', 'ap-south-1', 'eu-west-1']
DAYS = 30

BUDGETS = {
    'acct-001': 10000,
    'acct-002': 7500,
    'acct-003': 15000,
    'acct-004': 5000,
    'acct-005': 12000
}

RESOURCE_MAP = {}

# ==================== HELPERS ====================

def _generate_resource_ids():
    for account in ACCOUNTS:
        RESOURCE_MAP[account] = {}
        for service in SERVICES:
            count = random.randint(2, 4)
            prefix = {
                'EC2': 'i-',
                'S3': 'bucket-',
                'Lambda': 'fn-',
                'RDS': 'db-',
                'CloudFront': 'dist-'
            }[service]

            RESOURCE_MAP[account][service] = [
                f"{prefix}{uuid.uuid4().hex[:8]}"
                for _ in range(count)
            ]

def _pick_anomaly_days():
    anomaly_days = {}
    for account in ACCOUNTS:
        # FIXED: always recent days for demo visibility
        anomaly_days[account] = [DAYS - 2, DAYS - 5]
    return anomaly_days

def _random_variation(base):
    return base * random.uniform(0.7, 1.3)

def _anomaly_multiplier():
    return random.uniform(3.5, 6.0)

# ==================== MAIN ====================

def generate_all_data():
    print("Generating resource IDs...")
    _generate_resource_ids()

    anomaly_days = _pick_anomaly_days()
    print(f"Anomaly days: {anomaly_days}\n")

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = today - timedelta(days=DAYS)

    total_records = 0

    for day_offset in range(DAYS):
        current_date = start_date + timedelta(days=day_offset)
        date_str = current_date.strftime('%Y-%m-%d')

        print(f"Day {day_offset+1}/{DAYS}: {date_str}", end='')

        for account in ACCOUNTS:
            is_anomaly_day = day_offset in anomaly_days[account]
            anomaly_service = random.choice(list(SERVICES)) if is_anomaly_day else None

            daily_costs = {}
            records = []

            for service, config in SERVICES.items():
                for resource_id in RESOURCE_MAP[account][service]:

                    region = random.choice(REGIONS)

                    usage = _random_variation(config['base_usage'])

                    if is_anomaly_day and service == anomaly_service:
                        usage *= _anomaly_multiplier()

                    cost = round(usage * config['base_cost_per_unit'], 4)

                    timestamp = current_date.replace(
    hour=random.randint(0, 23),
    minute=random.randint(0, 59),
    second=random.randint(0, 59),
    microsecond=random.randint(0, 999999)
).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

                    record = {
                        'account_id': account,
                        'resource_type': service,
                        'timestamp': timestamp,
                        'day': date_str,
                        'resource_id': resource_id,
                        'region': region,
                        'usage_quantity': round(usage, 2),
                        'usage_unit': config['unit'],
                        'cost_usd': cost,
                        'tags': {
                            'team': random.choice(['backend', 'frontend', 'data', 'devops']),
                            'env': random.choice(['prod', 'staging', 'dev'])
                        }
                    }

                    records.append(record)
                    daily_costs[service] = daily_costs.get(service, 0) + cost

            dynamo_manager.batch_write_resource_usage(records)
            total_records += len(records)

            total_daily = round(sum(daily_costs.values()), 2)
            budget = BUDGETS[account]

            summary = {
                'account_id': account,
                'date': date_str,
                'total_cost': total_daily,
                'service_breakdown': {k: round(v, 2) for k, v in daily_costs.items()},
                'anomaly_flag': is_anomaly_day,
                'budget_utilization_pct': round((total_daily / (budget / 30)) * 100, 2)
            }

            dynamo_manager.put_daily_cost_summary(summary)

            # Ranking (always filled)
            redis_manager.update_cost_ranking(date_str, account, total_daily)

            # Recommendations (always created on anomaly days)
            if is_anomaly_day:
                recommendation = {
    "rec_id": f"rec-{uuid.uuid4().hex[:8]}",
    "account_id": account,
    "timestamp": current_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
    "date": date_str,
    "type": random.choice(["rightsizing", "delete_unused", "switch_to_reserved"]),
    "resource_id": random.choice(RESOURCE_MAP[account][anomaly_service]),
    "estimated_monthly_savings": round(random.uniform(50, 200), 2),
    "details": {
        "days_active": random.randint(1, 30),
        "savings_pct": random.randint(10, 60)
    }
}
                dynamo_manager.put_recommendation(recommendation)

        print(" ✓")

    print(f"\nTotal records: {total_records}")

# ==================== REDIS ====================

def populate_redis_caches():
    print("\nPopulating Redis...")

    today = datetime.utcnow().strftime('%Y-%m-%d')
    week_ago = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')

    for account in ACCOUNTS:

        summaries = dynamo_manager.query_daily_costs(account, week_ago, today)

        if summaries:
            latest = max(summaries, key=lambda x: x['date'])

            breakdown = latest.get('service_breakdown', {})
            top_service = max(breakdown, key=lambda k: float(breakdown[k])) if breakdown else 'N/A'

            redis_manager.set_dashboard_snapshot(
                account,
                float(latest['total_cost']),
                10,
                top_service,
                random.randint(1, 5)
            )

            trend = [
                {'date': s['date'], 'total_cost': float(s['total_cost'])}
                for s in summaries
            ]

            redis_manager.cache_trend_data(account, trend)

            for s in summaries:
                redis_manager.cache_daily_summary(account, s['date'], s)

        # Strong anomaly stats
        all_costs = dynamo_manager.query_daily_costs(account)
        if all_costs:
            costs = [float(c['total_cost']) for c in all_costs]
            mean = sum(costs) / len(costs)
            variance = sum((c - mean) ** 2 for c in costs) / len(costs)
            std_dev = max((variance ** 0.5), 5)

            redis_manager.update_anomaly_stats(account, round(mean, 2), round(std_dev, 2), len(costs))

        print(f"{account} cached")

    print("Redis ready")

# ==================== MAIN ====================

if __name__ == '__main__':
    print("===== DATA GENERATOR =====\n")

    redis_manager.flush_all_cache()

    if len(dynamo_manager.list_tables()) < 4:
        dynamo_manager.create_all_tables()

    generate_all_data()
    populate_redis_caches()