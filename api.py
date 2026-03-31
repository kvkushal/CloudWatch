"""
api.py

Flask REST API for the Cloud Cost Monitoring platform.

Caching strategy:
  All endpoints follow a cache-aside (lazy loading) pattern:
    1. Check Redis (low latency, ~1ms)
    2. On miss, query DynamoDB (~5–20ms)
    3. Populate Redis cache before returning

  TTLs are managed in redis_manager constants. This means the API layer
  never sets TTLs directly — they're determined by the caching layer.

Consistency model:
  Dashboard and alert endpoints use ConsistentRead=True in dynamo_manager
  (see that module for rationale). Trend and ranking endpoints use eventual
  consistency (lower RCU cost, acceptable for read-heavy analytics views).

Scalability notes:
  DynamoDB scales horizontally via partition key distribution. With 5 accounts
  as partition keys the partition count is low, but each account's data is
  co-located for efficient range queries. Under real load with thousands of
  accounts the same schema scales without modification.

  Redis operates as a single node here (local dev). In production, Redis
  Cluster or ElastiCache with read replicas would be used. The sorted set
  for cost rankings is particularly suited to Redis Cluster's hash slot model
  since each date key maps to a single slot.
"""

from flask import Flask, jsonify, request
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import dynamo_manager
import redis_manager
import anomaly_detector
import recommendation_engine


# ==================== APP ====================

app = Flask(__name__)

VALID_ACCOUNTS = {'acct-001', 'acct-002', 'acct-003', 'acct-004', 'acct-005'}
VALID_SERVICES = {'EC2', 'S3', 'Lambda', 'RDS', 'CloudFront'}
VALID_REGIONS  = {'us-east-1', 'ap-south-1', 'eu-west-1'}


# ==================== HELPERS ====================

def _convert_decimals(obj):
    if isinstance(obj, Decimal):  return float(obj)
    if isinstance(obj, dict):     return {k: _convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):     return [_convert_decimals(i) for i in obj]
    if isinstance(obj, set):      return [_convert_decimals(i) for i in obj]
    if isinstance(obj, bool):     return obj
    return obj


def _validate_account(account_id):
    return account_id in VALID_ACCOUNTS


def _validate_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except (ValueError, TypeError):
        return False


def _count_active_resources(account_id):
    """Count unique resource IDs from the past 7 days of usage records."""
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%dT00:00:00Z')
    resource_ids = set()
    for service in VALID_SERVICES:
        records = dynamo_manager.query_usage_by_account_and_type(account_id, service)
        for r in records:
            if r.get('timestamp', '') >= week_ago:
                resource_ids.add(r.get('resource_id'))
    return len(resource_ids)


# ==================== HEALTH ====================

@app.route('/api/health', methods=['GET'])
def health():
    dynamo_ok = redis_ok = False
    try:
        dynamo_ok = len(dynamo_manager.list_tables()) >= 4
    except Exception:
        pass
    try:
        redis_ok = redis_manager.redis_client.ping()
    except Exception:
        pass

    return jsonify({
        'status':   'healthy' if (dynamo_ok and redis_ok) else 'degraded',
        'dynamodb': dynamo_ok,
        'redis':    redis_ok
    })


# ==================== ACCOUNTS ====================

@app.route('/api/accounts', methods=['GET'])
def get_accounts():
    return jsonify({'accounts': sorted(list(VALID_ACCOUNTS))})


# ==================== DASHBOARD ====================

@app.route('/api/dashboard/<account_id>', methods=['GET'])
def get_dashboard(account_id):
    if not _validate_account(account_id):
        return jsonify({'error': f'Invalid account: {account_id}'}), 400

    snapshot = redis_manager.get_dashboard_snapshot(account_id)
    source   = 'redis'

    if snapshot is None:
        source   = 'dynamodb'
        today    = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')

        summaries = dynamo_manager.query_daily_costs(account_id, week_ago, today)
        if not summaries:
            return jsonify({'error': 'No data found', 'account_id': account_id}), 404

        latest      = max(summaries, key=lambda x: x['date'])
        breakdown   = latest.get('service_breakdown', {})
        top_service = max(breakdown, key=lambda k: float(breakdown[k])) if breakdown else 'N/A'
        alerts      = dynamo_manager.query_alerts(account_id)
        active      = _count_active_resources(account_id)

        snapshot = {
            'total_spend':      float(latest['total_cost']),
            'active_resources': active,
            'top_service':      top_service,
            'alert_count':      len(alerts)
        }
        redis_manager.set_dashboard_snapshot(
            account_id,
            snapshot['total_spend'],
            snapshot['active_resources'],
            snapshot['top_service'],
            snapshot['alert_count']
        )

    return jsonify({'account_id': account_id, 'source': source, 'data': snapshot})


# ==================== DAILY COST SUMMARY ====================

@app.route('/api/costs/<account_id>', methods=['GET'])
def get_costs(account_id):
    if not _validate_account(account_id):
        return jsonify({'error': f'Invalid account: {account_id}'}), 400

    start_date = request.args.get('start')
    end_date   = request.args.get('end')

    if start_date and not _validate_date(start_date):
        return jsonify({'error': f'Invalid start date: {start_date}'}), 400
    if end_date and not _validate_date(end_date):
        return jsonify({'error': f'Invalid end date: {end_date}'}), 400

    if not start_date:
        start_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    if start_date == end_date:
        cached = redis_manager.get_cached_daily_summary(account_id, start_date)
        if cached:
            return jsonify({'account_id': account_id, 'source': 'redis', 'count': 1, 'data': [cached]})

    summaries = dynamo_manager.query_daily_costs(account_id, start_date, end_date)

    if start_date == end_date and summaries:
        redis_manager.cache_daily_summary(account_id, start_date, summaries[0])

    return jsonify({
        'account_id': account_id,
        'source':     'dynamodb',
        'count':      len(summaries),
        'data':       _convert_decimals(summaries)
    })


# ==================== COST TREND ====================

@app.route('/api/trend/<account_id>', methods=['GET'])
def get_trend(account_id):
    if not _validate_account(account_id):
        return jsonify({'error': f'Invalid account: {account_id}'}), 400

    trend  = redis_manager.get_cached_trend_data(account_id)
    source = 'redis'

    if trend is None:
        source   = 'dynamodb'
        today    = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')

        # Use projection-optimised query — only fetches date + total_cost,
        # skipping service_breakdown to reduce RCU consumption on this hot path.
        summaries = dynamo_manager.query_daily_costs_trend(account_id, week_ago, today)

        trend = [
            {'date': s['date'], 'total_cost': float(s['total_cost'])}
            for s in summaries
        ]
        redis_manager.cache_trend_data(account_id, trend)

    return jsonify({'account_id': account_id, 'source': source, 'data': trend})


# ==================== COST RANKINGS ====================

@app.route('/api/rankings', methods=['GET'])
def get_rankings():
    date = request.args.get('date')
    if date and not _validate_date(date):
        return jsonify({'error': f'Invalid date: {date}'}), 400
    if not date:
        date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    top_n    = request.args.get('top', 10, type=int)
    rankings = redis_manager.get_top_accounts_by_cost(date, top_n)

    return jsonify({'date': date, 'source': 'redis', 'data': rankings})


# ==================== USAGE BY SERVICE ====================

@app.route('/api/usage/by-service/<resource_type>', methods=['GET'])
def get_usage_by_service(resource_type):
    if resource_type not in VALID_SERVICES:
        return jsonify({'error': f'Invalid service: {resource_type}'}), 400

    start   = request.args.get('start')
    end     = request.args.get('end')
    records = dynamo_manager.query_usage_by_resource_type(resource_type, start, end)

    return jsonify({
        'resource_type':  resource_type,
        'source':         'dynamodb_gsi',
        'total_count':    len(records),
        'returned_count': min(len(records), 50),
        'data':           _convert_decimals(records[:50])
    })


# ==================== USAGE BY REGION ====================

@app.route('/api/usage/by-region/<region>', methods=['GET'])
def get_usage_by_region(region):
    if region not in VALID_REGIONS:
        return jsonify({'error': f'Invalid region: {region}'}), 400

    start   = request.args.get('start')
    end     = request.args.get('end')
    records = dynamo_manager.query_usage_by_region(region, start, end)

    service_costs = {}
    for r in records:
        svc  = r.get('resource_type', 'Unknown')
        cost = float(r.get('cost_usd', 0))
        service_costs[svc] = service_costs.get(svc, 0) + cost

    return jsonify({
        'region':               region,
        'source':               'dynamodb_gsi',
        'total_count':          len(records),
        'returned_count':       min(len(records), 50),
        'service_cost_summary': {k: round(v, 2) for k, v in service_costs.items()},
        'data':                 _convert_decimals(records[:50])
    })


# ==================== ALERTS ====================

@app.route('/api/alerts/<account_id>', methods=['GET'])
def get_alerts(account_id):
    if not _validate_account(account_id):
        return jsonify({'error': f'Invalid account: {account_id}'}), 400

    source_param = request.args.get('source', 'redis')

    if source_param == 'redis':
        alerts = redis_manager.get_recent_alerts(account_id, count=50)
        return jsonify({'account_id': account_id, 'source': 'redis', 'count': len(alerts), 'data': alerts})
    else:
        alerts = dynamo_manager.query_alerts(account_id)
        return jsonify({'account_id': account_id, 'source': 'dynamodb', 'count': len(alerts), 'data': _convert_decimals(alerts)})


# ==================== ANOMALY STATS ====================

@app.route('/api/anomaly-stats/<account_id>', methods=['GET'])
def get_anomaly_stats(account_id):
    if not _validate_account(account_id):
        return jsonify({'error': f'Invalid account: {account_id}'}), 400

    stats = redis_manager.get_anomaly_stats(account_id)
    if stats is None:
        return jsonify({'error': 'No anomaly stats found'}), 404

    return jsonify({'account_id': account_id, 'source': 'redis', 'data': stats})


# ==================== RECOMMENDATIONS ====================

@app.route('/api/recommendations/<account_id>', methods=['GET'])
def get_recommendations(account_id):
    if not _validate_account(account_id):
        return jsonify({'error': f'Invalid account: {account_id}'}), 400

    recs = dynamo_manager.query_recommendations(account_id)

    by_type       = {}
    total_savings = 0
    for r in recs:
        rt = r.get('rec_type', 'unknown')
        by_type[rt] = by_type.get(rt, 0) + 1
        total_savings += float(r.get('estimated_monthly_savings', 0))

    return jsonify({
        'account_id':           account_id,
        'source':               'dynamodb',
        'count':                len(recs),
        'total_monthly_savings': round(total_savings, 2),
        'by_type':              by_type,
        'data':                 _convert_decimals(recs)
    })


# ==================== SUMMARY ====================

@app.route('/api/summary', methods=['GET'])
def get_summary():
    account_summaries = []

    for account in sorted(VALID_ACCOUNTS):
        snap = redis_manager.get_dashboard_snapshot(account)

        if snap is None:
            today    = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
            summaries = dynamo_manager.query_daily_costs(account, week_ago, today)

            if summaries:
                latest    = max(summaries, key=lambda x: x['date'])
                breakdown = latest.get('service_breakdown', {})
                top_svc   = max(breakdown, key=lambda k: float(breakdown[k])) if breakdown else 'N/A'
                alerts    = dynamo_manager.query_alerts(account)

                snap = {
                    'total_spend':      float(latest['total_cost']),
                    'alert_count':      len(alerts),
                    'top_service':      top_svc,
                    'active_resources': _count_active_resources(account)
                }
                redis_manager.set_dashboard_snapshot(
                    account, snap['total_spend'], snap['active_resources'],
                    snap['top_service'], snap['alert_count']
                )
            else:
                snap = {'total_spend': 0, 'alert_count': 0, 'top_service': 'N/A', 'active_resources': 0}

        stats = redis_manager.get_anomaly_stats(account)
        recs  = dynamo_manager.query_recommendations(account)
        total_savings = sum(float(r.get('estimated_monthly_savings', 0)) for r in recs)

        account_summaries.append({
            'account_id':       account,
            'total_spend':      snap['total_spend'],
            'alert_count':      snap['alert_count'],
            'top_service':      snap['top_service'],
            'mean_daily_cost':  stats['mean'] if stats else 0,
            'recommendations':  len(recs),
            'potential_savings': round(total_savings, 2)
        })

    return jsonify({'source': 'redis+dynamodb', 'data': account_summaries})

#new
@app.route('/api/insights/<account_id>', methods=['GET'])
def get_insights(account_id):
    summaries = dynamo_manager.query_daily_costs(account_id)
    
    if not summaries:
        return jsonify({'error': 'No data'}), 404

    total = sum(float(s['total_cost']) for s in summaries)
    avg   = total / len(summaries)

    max_day = max(summaries, key=lambda x: float(x['total_cost']))

    return jsonify({
        'account_id': account_id,
        'total_spend': round(total, 2),
        'average_daily_cost': round(avg, 2),
        'highest_spend_day': max_day
    })


# ==================== MAIN ====================

if __name__ == '__main__':
    print("=" * 50)
    print("CLOUD COST API SERVER")
    print("=" * 50)
    print("\nEndpoints:")
    for route in [
        "GET /api/health",
        "GET /api/accounts",
        "GET /api/dashboard/<account_id>",
        "GET /api/costs/<account_id>?start=&end=",
        "GET /api/trend/<account_id>",
        "GET /api/rankings?date=&top=",
        "GET /api/usage/by-service/<type>?start=&end=",
        "GET /api/usage/by-region/<region>?start=&end=",
        "GET /api/alerts/<account_id>?source=redis|dynamodb",
        "GET /api/anomaly-stats/<account_id>",
        "GET /api/recommendations/<account_id>",
        "GET /api/summary",
    ]:
        print(f"  {route}")

    app.run(host='0.0.0.0', port=5000, debug=True)
