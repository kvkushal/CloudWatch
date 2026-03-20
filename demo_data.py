"""
demo_data.py

Realistic static data for the Streamlit Community Cloud deployment.
Used when DEMO_MODE=True so the dashboard renders fully without a
running Flask API, DynamoDB, or Redis.

Data is seeded with a fixed random seed so it looks consistent across
page refreshes but varies per account selection.
"""

import random
from datetime import datetime, timedelta


ACCOUNTS = ['acct-001', 'acct-002', 'acct-003', 'acct-004', 'acct-005']

SERVICES = ['EC2', 'S3', 'Lambda', 'RDS', 'CloudFront']

# Per-account personality so switching accounts feels meaningful
_ACCOUNT_PROFILES = {
    'acct-001': {'base': 33,  'anomaly_days': [8, 19],      'top': 'CloudFront', 'resources': 38, 'alerts': 4},
    'acct-002': {'base': 22,  'anomaly_days': [5, 14, 27],  'top': 'RDS',        'resources': 24, 'alerts': 2},
    'acct-003': {'base': 48,  'anomaly_days': [3, 21],      'top': 'EC2',        'resources': 61, 'alerts': 1},
    'acct-004': {'base': 15,  'anomaly_days': [11, 25],     'top': 'S3',         'resources': 17, 'alerts': 0},
    'acct-005': {'base': 39,  'anomaly_days': [7, 16, 29],  'top': 'CloudFront', 'resources': 44, 'alerts': 3},
}

_SERVICE_WEIGHTS = {
    'EC2':        0.28,
    'CloudFront': 0.24,
    'RDS':        0.22,
    'S3':         0.16,
    'Lambda':     0.10,
}


def _rng(account_id):
    seed = sum(ord(c) for c in account_id)
    return random.Random(seed)


def get_dashboard(account_id):
    p = _ACCOUNT_PROFILES.get(account_id, _ACCOUNT_PROFILES['acct-001'])
    rng = _rng(account_id)
    spend = round(p['base'] + rng.uniform(-3, 3), 2)
    return {
        "data": {
            "total_spend":      spend,
            "active_resources": p['resources'],
            "top_service":      p['top'],
            "alert_count":      p['alerts'],
        }
    }


def get_trend(account_id):
    p   = _ACCOUNT_PROFILES.get(account_id, _ACCOUNT_PROFILES['acct-001'])
    rng = _rng(account_id)

    today  = datetime.utcnow().date()
    result = []

    for i in range(30):
        day  = today - timedelta(days=29 - i)
        base = p['base']

        if i in p['anomaly_days']:
            cost = round(base * rng.uniform(2.8, 4.5), 2)
        else:
            cost = round(base + rng.gauss(0, base * 0.12), 2)
            cost = max(cost, base * 0.5)

        result.append({'date': str(day), 'total_cost': cost})

    return {"data": result}


def get_costs(account_id):
    p   = _ACCOUNT_PROFILES.get(account_id, _ACCOUNT_PROFILES['acct-001'])
    rng = _rng(account_id)

    today  = datetime.utcnow().date()
    result = []

    for i in range(30):
        day   = today - timedelta(days=29 - i)
        base  = p['base']
        total = round(base + rng.gauss(0, base * 0.1), 2)
        total = max(total, base * 0.5)

        # Distribute total across services by weight
        breakdown = {}
        remaining = total
        services  = list(_SERVICE_WEIGHTS.items())
        for j, (svc, weight) in enumerate(services):
            if j == len(services) - 1:
                breakdown[svc] = round(remaining, 2)
            else:
                v = round(total * weight * rng.uniform(0.85, 1.15), 2)
                v = min(v, remaining - 0.01)
                breakdown[svc] = v
                remaining -= v

        result.append({
            'date':              str(day),
            'total_cost':        total,
            'service_breakdown': breakdown,
            'anomaly_flag':      i in p['anomaly_days'],
        })

    return {"data": result}


def get_recommendations(account_id):
    rng  = _rng(account_id)
    p    = _ACCOUNT_PROFILES.get(account_id, _ACCOUNT_PROFILES['acct-001'])
    base = p['base']

    templates = [
        # (rec_type, resource_prefix, savings_range, details)
        ('switch_to_reserved', 'i-',      (20, 40), {'days_active': 30, 'savings_pct': 40}),
        ('switch_to_reserved', 'db-',     (15, 35), {'days_active': 30, 'savings_pct': 35}),
        ('rightsizing',        'i-',      (8,  18), {'days_active': 14, 'savings_pct': 50}),
        ('rightsizing',        'db-',     (6,  14), {'days_active': 21, 'savings_pct': 45}),
        ('delete_unused',      'fn-',     (0.5, 2), {'days_active': 7,  'savings_pct': 100}),
        ('switch_to_reserved', 'i-',      (18, 38), {'days_active': 28, 'savings_pct': 40}),
        ('rightsizing',        'i-',      (9,  20), {'days_active': 18, 'savings_pct': 50}),
        ('switch_to_reserved', 'db-',     (12, 28), {'days_active': 25, 'savings_pct': 35}),
        ('delete_unused',      'fn-',     (0.3, 1), {'days_active': 5,  'savings_pct': 100}),
        ('switch_to_reserved', 'i-',      (22, 42), {'days_active': 30, 'savings_pct': 40}),
    ]

    data       = []
    by_type    = {}
    total_sav  = 0

    for rec_type, prefix, (lo, hi), details in templates:
        rid     = f"{prefix}{rng.randint(10000000, 99999999):x}"
        savings = round(rng.uniform(lo, hi), 2)

        data.append({
            'rec_type':                 rec_type,
            'resource_id':              rid,
            'estimated_monthly_savings': savings,
            'details':                  details,
            'status':                   'open',
        })
        by_type[rec_type] = by_type.get(rec_type, 0) + 1
        total_sav += savings

    return {
        "data":                  data,
        "by_type":               by_type,
        "total_monthly_savings": round(total_sav, 2),
    }


def get_alerts(account_id):
    p   = _ACCOUNT_PROFILES.get(account_id, _ACCOUNT_PROFILES['acct-001'])
    rng = _rng(account_id)

    if p['alerts'] == 0:
        return {"data": []}

    base = p['base']
    mean = base
    std  = base * 0.12

    templates = [
        f"[CRITICAL] anomaly: Daily cost ${base * rng.uniform(2.8,3.5):.2f} is "
        f"{rng.randint(150,200)}% above average (mean=${mean:.2f}, z={rng.uniform(2.8,3.8):.2f})",

        f"[WARNING] budget_breach: Spent ${base * rng.uniform(1.1,1.3):.2f} vs "
        f"daily budget ${base:.2f} ({rng.randint(110,140)}% utilization)",

        f"[CRITICAL] anomaly: Daily cost ${base * rng.uniform(3.0,4.0):.2f} is "
        f"{rng.randint(180,220)}% above average (mean=${mean:.2f}, z={rng.uniform(3.1,4.2):.2f})",

        f"[WARNING] anomaly: Daily cost ${base * rng.uniform(1.15,1.4):.2f} is "
        f"{rng.randint(15,40)}% above average (mean=${mean:.2f}, z={rng.uniform(2.0,2.5):.2f})",
    ]

    return {"data": templates[:p['alerts']]}


def get_anomaly_stats(account_id):
    p   = _ACCOUNT_PROFILES.get(account_id, _ACCOUNT_PROFILES['acct-001'])
    rng = _rng(account_id)
    mean = p['base'] + rng.uniform(-1, 1)
    std  = round(mean * rng.uniform(0.10, 0.35), 4)

    return {
        "data": {
            "count":   30,
            "mean":    round(mean, 4),
            "std_dev": std,
        }
    }


def get_region_data():
    rng = random.Random(42)
    costs = {svc: round(rng.uniform(200, 4000), 2) for svc in SERVICES}
    return {"service_cost_summary": costs}