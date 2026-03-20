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
        anomaly_days[account] = random.sample(range(3, DAYS), random.randint(2, 3))
    return anomaly_days


def _random_variation(base):
    return base * random.uniform(0.7, 1.3)


def _anomaly_multiplier():
    return random.uniform(3.0, 6.0)


# ==================== MAIN GENERATION ====================

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
            is_anomaly_day = day_offset in anomaly_days.get(account, [])
            anomaly_service = random.choice(list(SERVICES)) if is_anomaly_day else None

            daily_costs = {}
            records = []

            for service, config in SERVICES.items():
                resources = RESOURCE_MAP[account][service]

                for resource_id in resources:

                    # FIX 1: region per resource (more realistic)
                    region = random.choice(REGIONS)

                    usage = _random_variation(config['base_usage'])

                    if is_anomaly_day and service == anomaly_service:
                        usage *= _anomaly_multiplier()

                    cost = round(usage * config['base_cost_per_unit'], 4)

                    timestamp = current_date.replace(
                        hour=random.randint(0, 23),
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59)
                    ).strftime('%Y-%m-%dT%H:%M:%SZ')

                    record = {
                        'account_id': account,
                        'resource_type': service,
                        'timestamp': timestamp,
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

            # batch write
            dynamo_manager.batch_write_resource_usage(records)
            total_records += len(records)

            # summary (2 decimal precision)
            total_daily = round(sum(daily_costs.values()), 2)
            budget = BUDGETS[account]
            utilization = round((total_daily / (budget / 30)) * 100, 2)

            summary = {
                'account_id': account,
                'date': date_str,
                'total_cost': total_daily,
                'service_breakdown': {k: round(v, 2) for k, v in daily_costs.items()},
                'anomaly_flag': is_anomaly_day,
                'budget_utilization_pct': utilization
            }

            dynamo_manager.put_daily_cost_summary(summary)

            redis_manager.update_cost_ranking(date_str, account, total_daily)

        print(" ✓")

    print(f"\nTotal records: {total_records}")


# ==================== REDIS POPULATION ====================

def populate_redis_caches():
    print("\nPopulating Redis...")

    today = datetime.utcnow().strftime('%Y-%m-%d')
    week_ago = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')

    for account in ACCOUNTS:

        summaries = dynamo_manager.query_daily_costs(account, week_ago, today)

        if summaries:

            # FIX 2: correct latest selection
            latest = max(summaries, key=lambda x: x['date'])

            breakdown = latest.get('service_breakdown', {})
            top_service = max(breakdown, key=lambda k: float(breakdown[k])) if breakdown else 'N/A'

            alerts = dynamo_manager.query_alerts(account)

            redis_manager.set_dashboard_snapshot(
                account,
                float(latest['total_cost']),
                sum(len(RESOURCE_MAP[account][s]) for s in SERVICES),
                top_service,
                len(alerts)
            )

            trend = [
                {'date': s['date'], 'total_cost': float(s['total_cost'])}
                for s in summaries
            ]

            redis_manager.cache_trend_data(account, trend)

            for s in summaries:
                redis_manager.cache_daily_summary(account, s['date'], s)

        # anomaly stats
        all_costs = dynamo_manager.query_daily_costs(account)
        if all_costs:
            costs = [float(c['total_cost']) for c in all_costs]
            mean = sum(costs) / len(costs)
            variance = sum((c - mean) ** 2 for c in costs) / len(costs)
            std_dev = variance ** 0.5

            redis_manager.update_anomaly_stats(account, round(mean, 2), round(std_dev, 2), len(costs))

        print(f"{account} cached")

    print("Redis ready")


# ==================== VERIFY ====================

def verify_data():
    print("\nVerification:\n")

    for table in ['ResourceUsage', 'DailyCostSummary']:
        print(f"{table}: {dynamo_manager.get_table_item_count(table)} items")

    acc = ACCOUNTS[0]
    print(f"\nSample usage ({acc}):", len(dynamo_manager.query_usage_by_account(acc)))

    print("Redis keys:", redis_manager.get_redis_info()['total_keys'])
    print("Dashboard:", redis_manager.get_dashboard_snapshot(acc))
    print("Anomaly stats:", redis_manager.get_anomaly_stats(acc))


# ==================== MAIN ====================

if __name__ == '__main__':
    print("===== DATA GENERATOR =====\n")

    redis_manager.flush_all_cache()

    if len(dynamo_manager.list_tables()) < 4:
        dynamo_manager.create_all_tables()

    generate_all_data()
    populate_redis_caches()
    verify_data()