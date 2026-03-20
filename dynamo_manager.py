"""
dynamo_manager.py

DynamoDB access layer for the Cloud Cost Monitoring platform.

Design decisions:
- Single module-level resource/client (connection reuse, not per-call).
- All multi-page queries use paginate() to handle DynamoDB's 1MB response limit.
- TTL attribute added to Alerts and ResourceUsage for automatic data expiry.
- ConsistentRead=True on dashboard/anomaly reads where stale data causes alerts.
- ReturnConsumedCapacity='TOTAL' on writes for capacity monitoring.
- ProjectionExpression on hot-path reads (trend, dashboard) to reduce RCU cost.
- Proper error handling with botocore ClientError, not bare except.

CAP trade-off note:
  DynamoDB is AP by default (eventual consistency). We opt into strong consistency
  (ConsistentRead=True) only for anomaly detection and dashboard reads where
  reading stale cost data could produce false alerts. All other reads use eventual
  consistency for lower latency and half the RCU cost.
"""

import time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from decimal import Decimal


# ==================== CONNECTION (module-level singleton) ====================
# Creating a new boto3 resource per function call re-establishes HTTP connections
# on every invocation. A module-level resource reuses the underlying urllib3
# connection pool, which matters under load.

_dynamodb_resource = boto3.resource(
    'dynamodb',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
    aws_access_key_id='dummy',
    aws_secret_access_key='dummy'
)

_dynamodb_client = boto3.client(
    'dynamodb',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
    aws_access_key_id='dummy',
    aws_secret_access_key='dummy'
)


def _resource():
    return _dynamodb_resource


def _client():
    return _dynamodb_client


# ==================== TABLE CREATION ====================

def create_resource_usage_table():
    """
    Schema design:
      PK: account_id (HASH)   — all access is per-account, hot partition risk
          mitigated by low account count (5) and per-service prefix in sort key.
      SK: resource_type#timestamp (RANGE) — composite key enables begins_with()
          queries for service-scoped time ranges without a GSI.

    GSI 1 - ResourceTypeIndex:
      Supports /usage/by-service/<type> queries. Hash=resource_type allows
      scatter-gather across all accounts for a given service type.

    GSI 2 - RegionIndex:
      Supports /usage/by-region/<region> queries. Same scatter-gather pattern.

    TTL: expires_at (epoch seconds) — items auto-deleted after 90 days,
         keeping the table from growing unboundedly without a manual purge job.
    """
    try:
        table = _resource().create_table(
            TableName='ResourceUsage',
            KeySchema=[
                {'AttributeName': 'account_id',              'KeyType': 'HASH'},
                {'AttributeName': 'resource_type_timestamp', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'account_id',              'AttributeType': 'S'},
                {'AttributeName': 'resource_type_timestamp', 'AttributeType': 'S'},
                {'AttributeName': 'resource_type',           'AttributeType': 'S'},
                {'AttributeName': 'timestamp',               'AttributeType': 'S'},
                {'AttributeName': 'region',                  'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'ResourceTypeIndex',
                    'KeySchema': [
                        {'AttributeName': 'resource_type', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp',     'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                },
                {
                    'IndexName': 'RegionIndex',
                    'KeySchema': [
                        {'AttributeName': 'region',    'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                }
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()

        # Enable TTL — items with expires_at < now() are auto-deleted by DynamoDB
        _client().update_time_to_live(
            TableName='ResourceUsage',
            TimeToLiveSpecification={'Enabled': True, 'AttributeName': 'expires_at'}
        )
        print("Created: ResourceUsage (TTL=expires_at)")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("ResourceUsage already exists")
        else:
            raise


def create_daily_cost_summary_table():
    """
    Schema design:
      PK: account_id (HASH)
      SK: date (RANGE, ISO 8601 string)

    Date strings sort lexicographically, so between('2025-01-01', '2025-01-31')
    works correctly without a numeric sort key. No GSI needed — all access
    patterns are account + date range.

    No TTL here: cost summaries are the source of truth for billing reports and
    should be retained indefinitely (or per org policy).
    """
    try:
        table = _resource().create_table(
            TableName='DailyCostSummary',
            KeySchema=[
                {'AttributeName': 'account_id', 'KeyType': 'HASH'},
                {'AttributeName': 'date',        'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'account_id', 'AttributeType': 'S'},
                {'AttributeName': 'date',        'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()
        print("Created: DailyCostSummary")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("DailyCostSummary already exists")
        else:
            raise


def create_recommendations_table():
    """
    Schema design:
      PK: account_id (HASH)
      SK: rec_id#timestamp (RANGE) — composite prevents collision if the same
          recommendation fires twice in different runs (different timestamps).

    Denormalization: resource_id, rec_type, and estimated_monthly_savings are
    stored directly on the item rather than normalising into a separate resource
    table. This avoids a second query on every recommendation list read.
    """
    try:
        table = _resource().create_table(
            TableName='OptimizationRecommendations',
            KeySchema=[
                {'AttributeName': 'account_id',     'KeyType': 'HASH'},
                {'AttributeName': 'rec_id_timestamp','KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'account_id',      'AttributeType': 'S'},
                {'AttributeName': 'rec_id_timestamp', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()
        print("Created: OptimizationRecommendations")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("OptimizationRecommendations already exists")
        else:
            raise


def create_alerts_table():
    """
    Schema design:
      PK: account_id (HASH)
      SK: alert_timestamp (RANGE, ISO 8601)

    TTL: expires_at — alerts older than 30 days are expired automatically.
    This keeps the Alerts table small and prevents historical noise from
    polluting the recent alert count shown on the dashboard.
    """
    try:
        table = _resource().create_table(
            TableName='Alerts',
            KeySchema=[
                {'AttributeName': 'account_id',    'KeyType': 'HASH'},
                {'AttributeName': 'alert_timestamp','KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'account_id',    'AttributeType': 'S'},
                {'AttributeName': 'alert_timestamp','AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()

        _client().update_time_to_live(
            TableName='Alerts',
            TimeToLiveSpecification={'Enabled': True, 'AttributeName': 'expires_at'}
        )
        print("Created: Alerts (TTL=expires_at)")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("Alerts already exists")
        else:
            raise


def create_all_tables():
    create_resource_usage_table()
    create_daily_cost_summary_table()
    create_recommendations_table()
    create_alerts_table()


# ==================== PAGINATION HELPER ====================

def _paginate_query(table, **kwargs):
    """
    DynamoDB returns at most 1MB of data per Query call. Without pagination,
    any result set larger than 1MB is silently truncated. This helper follows
    LastEvaluatedKey until all pages are consumed.

    For this project's data volume (5 accounts × 30 days × 5 services × ~4
    resources = ~3,000 items per table) this rarely triggers, but it is
    required for correctness and demonstrates production-readiness.
    """
    items = []
    response = table.query(**kwargs)
    items.extend(response.get('Items', []))

    while 'LastEvaluatedKey' in response:
        kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        response = table.query(**kwargs)
        items.extend(response.get('Items', []))

    return items


# ==================== TTL HELPER ====================

_ALERT_TTL_DAYS     = 30
_USAGE_TTL_DAYS     = 90


def _ttl_epoch(days: int) -> int:
    """Return Unix epoch seconds for `days` from now. Used for DynamoDB TTL."""
    return int(time.time()) + days * 86400


# ==================== CRUD: ResourceUsage ====================

def put_resource_usage(item):
    table = _resource().Table('ResourceUsage')

    if 'timestamp' not in item:
        raise ValueError("timestamp is required for ResourceUsage")

    item['resource_type_timestamp'] = f"{item['resource_type']}#{item['timestamp']}"
    item['usage_quantity'] = Decimal(str(item['usage_quantity']))
    item['cost_usd']       = Decimal(str(item['cost_usd']))
    item['expires_at']     = _ttl_epoch(_USAGE_TTL_DAYS)

    table.put_item(
        Item=item,
        ReturnConsumedCapacity='TOTAL'
    )


def batch_write_resource_usage(items):
    """
    Batch writes in groups of 25 (DynamoDB maximum per batch request).
    boto3's batch_writer handles this automatically but we add TTL here.
    """
    table = _resource().Table('ResourceUsage')

    with table.batch_writer() as batch:
        for item in items:
            if 'timestamp' not in item:
                continue
            item['resource_type_timestamp'] = f"{item['resource_type']}#{item['timestamp']}"
            item['usage_quantity'] = Decimal(str(item['usage_quantity']))
            item['cost_usd']       = Decimal(str(item['cost_usd']))
            item['expires_at']     = _ttl_epoch(_USAGE_TTL_DAYS)
            batch.put_item(Item=item)


def query_usage_by_account(account_id):
    table = _resource().Table('ResourceUsage')
    return _paginate_query(
        table,
        KeyConditionExpression=Key('account_id').eq(account_id)
    )


def query_usage_by_account_and_type(account_id, resource_type):
    table = _resource().Table('ResourceUsage')
    return _paginate_query(
        table,
        KeyConditionExpression=(
            Key('account_id').eq(account_id) &
            Key('resource_type_timestamp').begins_with(resource_type + '#')
        )
    )


def query_usage_by_resource_type(resource_type, start_time=None, end_time=None):
    table = _resource().Table('ResourceUsage')

    key_expr = Key('resource_type').eq(resource_type)
    if start_time and end_time:
        key_expr &= Key('timestamp').between(start_time, end_time)

    return _paginate_query(
        table,
        IndexName='ResourceTypeIndex',
        KeyConditionExpression=key_expr
    )


def query_usage_by_region(region, start_time=None, end_time=None):
    table = _resource().Table('ResourceUsage')

    key_expr = Key('region').eq(region)
    if start_time and end_time:
        key_expr &= Key('timestamp').between(start_time, end_time)

    return _paginate_query(
        table,
        IndexName='RegionIndex',
        KeyConditionExpression=key_expr
    )


# ==================== CRUD: DailyCostSummary ====================

def put_daily_cost_summary(item):
    table = _resource().Table('DailyCostSummary')

    item['total_cost']            = Decimal(str(item['total_cost']))
    item['budget_utilization_pct']= Decimal(str(item['budget_utilization_pct']))
    item['service_breakdown']     = {
        k: Decimal(str(v)) for k, v in item['service_breakdown'].items()
    }

    table.put_item(
        Item=item,
        ReturnConsumedCapacity='TOTAL'
    )


def query_daily_costs(account_id, start_date=None, end_date=None):
    """
    ConsistentRead=True: the anomaly detector reads this immediately after the
    data generator writes. With eventual consistency (default), the read might
    return stale data and miss the day's record, producing a false negative.
    Strong consistency costs 2x RCU but is necessary here for correctness.
    """
    table = _resource().Table('DailyCostSummary')

    key_expr = Key('account_id').eq(account_id)
    if start_date and end_date:
        key_expr &= Key('date').between(start_date, end_date)

    return _paginate_query(
        table,
        KeyConditionExpression=key_expr,
        ConsistentRead=True
    )


def query_daily_costs_trend(account_id, start_date, end_date):
    """
    Projection-optimised version for the trend chart.
    Only fetches date and total_cost — saves RCU by not reading
    service_breakdown (a large nested map) when we don't need it.
    """
    table = _resource().Table('DailyCostSummary')

    return _paginate_query(
        table,
        KeyConditionExpression=(
            Key('account_id').eq(account_id) &
            Key('date').between(start_date, end_date)
        ),
        ProjectionExpression='#d, total_cost',
        # 'date' is a DynamoDB reserved word, so we alias it
        ExpressionAttributeNames={'#d': 'date'},
        ConsistentRead=False    # trend chart can tolerate eventual consistency
    )


# ==================== CRUD: Recommendations ====================

def put_recommendation(item):
    table = _resource().Table('OptimizationRecommendations')

    item['rec_id_timestamp']         = f"{item['rec_id']}#{item['timestamp']}"
    item['estimated_monthly_savings'] = Decimal(str(item['estimated_monthly_savings']))

    table.put_item(
        Item=item,
        ReturnConsumedCapacity='TOTAL'
    )


def query_recommendations(account_id):
    table = _resource().Table('OptimizationRecommendations')
    return _paginate_query(
        table,
        KeyConditionExpression=Key('account_id').eq(account_id)
    )


# ==================== CRUD: Alerts ====================

def put_alert(item):
    table = _resource().Table('Alerts')

    # TTL: alerts expire after 30 days automatically
    item['expires_at'] = _ttl_epoch(_ALERT_TTL_DAYS)

    table.put_item(
        Item=item,
        ReturnConsumedCapacity='TOTAL'
    )


def query_alerts(account_id, start_time=None, end_time=None):
    table = _resource().Table('Alerts')

    key_expr = Key('account_id').eq(account_id)
    if start_time and end_time:
        key_expr &= Key('alert_timestamp').between(start_time, end_time)

    # ConsistentRead=True: alert count shown on dashboard must reflect writes
    # from the anomaly detector that may have just completed.
    return _paginate_query(
        table,
        KeyConditionExpression=key_expr,
        ConsistentRead=True
    )


# ==================== UTILITIES ====================

def list_tables():
    return _client().list_tables()['TableNames']


def get_table_item_count(table_name):
    """
    Uses table.scan(Select='COUNT') which reads all items but returns
    no attribute data, minimising RCU cost for a count operation.
    In production, prefer table.item_count (updated every ~6h by AWS)
    or maintain a counter in Redis for real-time counts.
    """
    table = _resource().Table(table_name)
    try:
        count = 0
        response = table.scan(Select='COUNT')
        count += response['Count']
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                Select='COUNT',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            count += response['Count']
        return count
    except ClientError as e:
        print(f"Error counting {table_name}: {e.response['Error']['Message']}")
        return 0


# ==================== MAIN ====================

if __name__ == '__main__':
    print("Creating all DynamoDB tables...\n")
    create_all_tables()
