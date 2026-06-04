# recommendation_engine.py

import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import dynamo_manager
import redis_manager


# ==================== CONFIG ====================

ACCOUNTS = ['acct-001', 'acct-002', 'acct-003', 'acct-004', 'acct-005']

SERVICES = {
    'EC2':        {'base_cost_per_unit': 0.085,     'base_usage': 24.0},
    'S3':         {'base_cost_per_unit': 0.023,     'base_usage': 50.0},
    'Lambda':     {'base_cost_per_unit': 0.0000002, 'base_usage': 500000.0},
    'RDS':        {'base_cost_per_unit': 0.145,     'base_usage': 24.0},
    'CloudFront': {'base_cost_per_unit': 0.085,     'base_usage': 100.0}
}

IDLE_THRESHOLD_PCT = 20
RIGHTSIZE_THRESHOLD_PCT = 40
RESERVED_DAYS = 25
LOW_ACCESS_DAYS = 20

MIN_SAVINGS_USD = 1  # optional noise filter


# ==================== HELPERS ====================

_USAGE_CACHE = {}
_DEDUP_CACHE = set()


def _generate_rec_id():
    return f"REC-{uuid.uuid4().hex[:6].upper()}"


def _parse_ts(ts):
    try:
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except Exception:
        return None


def _get_usage(account_id, service):
    key = (account_id, service)
    if key not in _USAGE_CACHE:
        records = dynamo_manager.query_usage_by_account_and_type(account_id, service)

        # parse timestamps once
        for r in records:
            r['_parsed_ts'] = _parse_ts(r.get('timestamp'))

        # safe sort
        records.sort(key=lambda r: r.get('_parsed_ts') or datetime.min)

        _USAGE_CACHE[key] = records

    return _USAGE_CACHE[key]


def _build_dedup_cache(account_id):
    recs = dynamo_manager.query_recommendations(account_id)
    for r in recs:
        if r.get('status') == 'open':
            key = (account_id, r.get('resource_id'), r.get('rec_type'))
            _DEDUP_CACHE.add(key)


def _recommendation_exists(account_id, resource_id, rec_type):
    return (account_id, resource_id, rec_type) in _DEDUP_CACHE


def _save_recommendation(account_id, resource_id, resource_type, rec_type,
                         estimated_savings, details):

    if estimated_savings < MIN_SAVINGS_USD:
        return None

    if _recommendation_exists(account_id, resource_id, rec_type):
        return None

    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    rec_id = _generate_rec_id()

    item = {
        'account_id':               account_id,
        'rec_id':                   rec_id,
        'rec_id_timestamp':         f"{rec_id}#{now}",
        'timestamp':                now,
        'resource_id':              resource_id,
        'resource_type':            resource_type,
        'rec_type':                 rec_type,
        'estimated_monthly_savings': Decimal(str(estimated_savings)),
        'status':                   'open',
        'details':                  details
    }

    dynamo_manager.put_recommendation(item)

    _DEDUP_CACHE.add((account_id, resource_id, rec_type))

    return item


# ==================== RULE 1 ====================

def detect_idle_resources(account_id):
    recommendations = []

    today = datetime.now(timezone.utc)
    week_ago_dt = today - timedelta(days=7)

    for service, config in SERVICES.items():
        records = _get_usage(account_id, service)

        if not records:
            continue

        recent = [
            r for r in records
            if r.get('_parsed_ts') and r['_parsed_ts'] >= week_ago_dt
        ]

        if len(recent) < 5:
            continue

        resource_usage = {}
        for r in recent:
            rid = r['resource_id']
            resource_usage.setdefault(rid, []).append(float(r['usage_quantity']))

        base = config['base_usage']
        threshold = base * (IDLE_THRESHOLD_PCT / 100)

        for rid, usages in resource_usage.items():
            avg_usage = sum(usages) / len(usages)

            if avg_usage < threshold:
                avg_cost = avg_usage * config['base_cost_per_unit']
                monthly_savings = round(avg_cost * 30, 2)

                rec = _save_recommendation(
                    account_id, rid, service, 'terminate_idle',
                    monthly_savings,
                    {
                        'avg_usage': round(avg_usage, 2),
                        'base_usage': base,
                        'usage_pct': round((avg_usage / base) * 100, 1),
                        'reason': f'Average usage {round((avg_usage/base)*100,1)}% over last 7 days'
                    }
                )
                if rec:
                    recommendations.append(rec)

    return recommendations


# ==================== RULE 2 ====================

def detect_rightsize_candidates(account_id):
    recommendations = []
    applicable_services = ['EC2', 'RDS']

    size_map = {
        'EC2': {'current': 'm5.xlarge', 'suggested': 'm5.large', 'savings_pct': 50},
        'RDS': {'current': 'db.r5.large', 'suggested': 'db.r5.medium', 'savings_pct': 45}
    }

    for service in applicable_services:
        config = SERVICES[service]
        records = _get_usage(account_id, service)

        if not records:
            continue

        resource_usage = {}
        for r in records:
            rid = r['resource_id']
            resource_usage.setdefault(rid, []).append(float(r['usage_quantity']))

        base = config['base_usage']
        threshold = base * (RIGHTSIZE_THRESHOLD_PCT / 100)
        idle_thresh = base * (IDLE_THRESHOLD_PCT / 100)

        for rid, usages in resource_usage.items():
            avg_usage = sum(usages) / len(usages)

            if idle_thresh <= avg_usage < threshold:
                avg_cost = avg_usage * config['base_cost_per_unit']
                savings_pct = size_map[service]['savings_pct']
                monthly_savings = round(avg_cost * 30 * (savings_pct / 100), 2)

                rec = _save_recommendation(
                    account_id, rid, service, 'rightsize',
                    monthly_savings,
                    {
                        'current_instance': size_map[service]['current'],
                        'suggested_instance': size_map[service]['suggested'],
                        'avg_usage': round(avg_usage, 2),
                        'usage_pct': round((avg_usage / base) * 100, 1),
                        'reason': f'Usage at {round((avg_usage/base)*100,1)}%'
                    }
                )
                if rec:
                    recommendations.append(rec)

    return recommendations


# ==================== RULE 3 ====================

def detect_reserved_candidates(account_id):
    recommendations = []
    applicable_services = ['EC2', 'RDS']

    reserved_savings = {'EC2': 40, 'RDS': 35}

    for service in applicable_services:
        records = _get_usage(account_id, service)

        if not records:
            continue

        resource_days = {}
        for r in records:
            rid = r['resource_id']
            day = r.get('timestamp', '')[:10]
            resource_days.setdefault(rid, set()).add(day)

        config = SERVICES[service]

        for rid, days in resource_days.items():
            if len(days) >= RESERVED_DAYS:
                avg_daily_cost = config['base_usage'] * config['base_cost_per_unit']
                monthly_cost = avg_daily_cost * 30
                savings_pct = reserved_savings[service]
                monthly_savings = round(monthly_cost * (savings_pct / 100), 2)

                rec = _save_recommendation(
                    account_id, rid, service, 'switch_to_reserved',
                    monthly_savings,
                    {
                        'days_active': len(days),
                        'savings_pct': savings_pct
                    }
                )
                if rec:
                    recommendations.append(rec)

    return recommendations


# ==================== RULE 4 ====================

def detect_archive_candidates(account_id):
    recommendations = []

    records = _get_usage(account_id, 'S3')

    if not records:
        return recommendations

    resource_usage = {}
    for r in records:
        rid = r['resource_id']
        resource_usage.setdefault(rid, []).append(float(r['usage_quantity']))

    for rid, usages in resource_usage.items():
        if len(usages) < LOW_ACCESS_DAYS:
            continue

        avg_usage = sum(usages) / len(usages)
        base = SERVICES['S3']['base_usage']

        if avg_usage < base * 0.3:
            avg_daily_cost = avg_usage * SERVICES['S3']['base_cost_per_unit']
            monthly_savings = round(avg_daily_cost * 30 * 0.80, 2)

            rec = _save_recommendation(
                account_id, rid, 'S3', 'move_to_glacier',
                monthly_savings,
                {
                    'avg_usage_gb': round(avg_usage, 2),
                    'days_tracked': len(usages)
                }
            )
            if rec:
                recommendations.append(rec)

    return recommendations


# ==================== RULE 5 ====================

def detect_unused_lambdas(account_id):
    recommendations = []

    records = _get_usage(account_id, 'Lambda')

    if not records:
        return recommendations

    resource_usage = {}
    for r in records:
        rid = r['resource_id']
        resource_usage.setdefault(rid, []).append(float(r['usage_quantity']))

    for rid, usages in resource_usage.items():
        if len(usages) < 14:
            continue

        avg_invocations = sum(usages) / len(usages)
        base = SERVICES['Lambda']['base_usage']

        if avg_invocations < base * 0.10:
            avg_daily_cost = avg_invocations * SERVICES['Lambda']['base_cost_per_unit']
            monthly_savings = round(avg_daily_cost * 30, 4)

            rec = _save_recommendation(
                account_id, rid, 'Lambda', 'delete_unused',
                monthly_savings,
                {
                    'avg_invocations': round(avg_invocations, 0),
                    'days_tracked': len(usages)
                }
            )
            if rec:
                recommendations.append(rec)

    return recommendations


# ==================== RUN ====================

def run_all_recommendations():
    print("Running recommendation engine...\n")

    _USAGE_CACHE.clear()
    _DEDUP_CACHE.clear()

    total_recs = 0
    summary = {
        'terminate_idle': 0,
        'rightsize': 0,
        'switch_to_reserved': 0,
        'move_to_glacier': 0,
        'delete_unused': 0
    }

    for account in ACCOUNTS:
        print(f"  Analyzing {account}...")

        _build_dedup_cache(account)

        recs = []

        idle = detect_idle_resources(account)
        rightsize = detect_rightsize_candidates(account)
        reserved = detect_reserved_candidates(account)
        archive = detect_archive_candidates(account)
        unused = detect_unused_lambdas(account)

        recs.extend(idle + rightsize + reserved + archive + unused)

        summary['terminate_idle'] += len(idle)
        summary['rightsize'] += len(rightsize)
        summary['switch_to_reserved'] += len(reserved)
        summary['move_to_glacier'] += len(archive)
        summary['delete_unused'] += len(unused)

        total_savings = sum(float(r['estimated_monthly_savings']) for r in recs)

        print(f"    {len(recs)} recommendations, est. savings: ${total_savings:.2f}/month")

        total_recs += len(recs)

    print(f"\n{'='*50}")
    print(f"Total recommendations: {total_recs}")

    return summary


def verify_recommendations():
    print("\nRecommendation verification:\n")

    total_savings = 0

    for account in ACCOUNTS:
        recs = dynamo_manager.query_recommendations(account)
        print(f"  {account}: {len(recs)} recommendations")

        if recs:
            r = recs[0]
            print(f"    Sample: [{r.get('rec_type')}] {r.get('resource_id')}")

        total_savings += sum(float(r.get('estimated_monthly_savings', 0)) for r in recs)

    print(f"\n  Total estimated monthly savings: ${total_savings:.2f}")


# ==================== MAIN ====================

if __name__ == '__main__':
    print("=" * 50)
    print("RECOMMENDATION ENGINE")
    print("=" * 50 + "\n")

    run_all_recommendations()
    verify_recommendations()