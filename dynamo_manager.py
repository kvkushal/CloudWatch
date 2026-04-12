"""
dynamo_manager.py

DynamoDB access layer for the Cloud Cost Monitoring platform.

CAP THEOREM JUSTIFICATION:
- DynamoDB supports both AP and CP modes
- Strong consistency used for anomaly + alerts (correctness critical)
- Eventual consistency used for analytics (performance optimized)

Redis (used separately) is AP and optimized for low-latency reads.
"""

import time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from decimal import Decimal

from config import DYNAMODB_ENDPOINT
from logger import get_logger

logger = get_logger("DynamoDB")


# ==================== CONNECTION ====================

_dynamodb_resource = boto3.resource(
    'dynamodb',
    endpoint_url=DYNAMODB_ENDPOINT,
    region_name='us-east-1',
    aws_access_key_id='dummy',
    aws_secret_access_key='dummy'
)

_dynamodb_client = boto3.client(
    'dynamodb',
    endpoint_url=DYNAMODB_ENDPOINT,
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
    try:
        table = _resource().create_table(
            TableName='ResourceUsage',
            KeySchema=[
                {'AttributeName': 'account_id', 'KeyType': 'HASH'},
                {'AttributeName': 'resource_type_timestamp', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'account_id', 'AttributeType': 'S'},
                {'AttributeName': 'resource_type_timestamp', 'AttributeType': 'S'},
                {'AttributeName': 'resource_type', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'},
                {'AttributeName': 'region', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'ResourceTypeIndex',
                    'KeySchema': [
                        {'AttributeName': 'resource_type', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                },
                {
                    'IndexName': 'RegionIndex',
                    'KeySchema': [
                        {'AttributeName': 'region', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                }
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()

        _client().update_time_to_live(
            TableName='ResourceUsage',
            TimeToLiveSpecification={'Enabled': True, 'AttributeName': 'expires_at'}
        )

        logger.info("Created ResourceUsage table")

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            logger.info("ResourceUsage already exists")
        else:
            logger.error(f"Error creating ResourceUsage: {e}")
            raise


def create_daily_cost_summary_table():
    try:
        table = _resource().create_table(
            TableName='DailyCostSummary',
            KeySchema=[
                {'AttributeName': 'account_id', 'KeyType': 'HASH'},
                {'AttributeName': 'date', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'account_id', 'AttributeType': 'S'},
                {'AttributeName': 'date', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()
        logger.info("Created DailyCostSummary table")

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            logger.info("DailyCostSummary already exists")
        else:
            logger.error(f"Error creating DailyCostSummary: {e}")
            raise


def create_recommendations_table():
    try:
        table = _resource().create_table(
            TableName='OptimizationRecommendations',
            KeySchema=[
                {'AttributeName': 'account_id', 'KeyType': 'HASH'},
                {'AttributeName': 'rec_id_timestamp', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'account_id', 'AttributeType': 'S'},
                {'AttributeName': 'rec_id_timestamp', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()
        logger.info("Created OptimizationRecommendations table")

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            logger.info("OptimizationRecommendations already exists")
        else:
            logger.error(f"Error creating recommendations table: {e}")
            raise


def create_alerts_table():
    try:
        table = _resource().create_table(
            TableName='Alerts',
            KeySchema=[
                {'AttributeName': 'account_id', 'KeyType': 'HASH'},
                {'AttributeName': 'alert_timestamp', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'account_id', 'AttributeType': 'S'},
                {'AttributeName': 'alert_timestamp', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()

        _client().update_time_to_live(
            TableName='Alerts',
            TimeToLiveSpecification={'Enabled': True, 'AttributeName': 'expires_at'}
        )

        logger.info("Created Alerts table")

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            logger.info("Alerts already exists")
        else:
            logger.error(f"Error creating Alerts table: {e}")
            raise


def create_all_tables():
    create_resource_usage_table()
    create_daily_cost_summary_table()
    create_recommendations_table()
    create_alerts_table()


# ==================== CRUD ====================

def put_daily_cost_summary(item):
    try:
        table = _resource().Table('DailyCostSummary')

        item['total_cost'] = Decimal(str(item['total_cost']))
        item['budget_utilization_pct'] = Decimal(str(item['budget_utilization_pct']))
        item['service_breakdown'] = {
            k: Decimal(str(v)) for k, v in item['service_breakdown'].items()
        }

        table.put_item(Item=item)

        logger.info(f"Inserted summary: {item['account_id']} {item['date']}")

    except Exception as e:
        logger.error(f"Error inserting summary: {e}")
        raise


def put_alert(item):
    try:
        table = _resource().Table('Alerts')

        item['expires_at'] = int(time.time()) + 30 * 86400

        table.put_item(Item=item)

        logger.info(f"Alert created for {item['account_id']}")

    except Exception as e:
        logger.error(f"Error inserting alert: {e}")
        raise


def query_daily_costs(account_id, start_date=None, end_date=None):
    """
    Query daily cost summaries for an account.
    If start_date and end_date are provided, filters by date range.
    Otherwise returns ALL records for the account.
    """
    try:
        table = _resource().Table('DailyCostSummary')

        if start_date and end_date:
            response = table.query(
                KeyConditionExpression=Key('account_id').eq(account_id) &
                                       Key('date').between(start_date, end_date)
            )
        else:
            response = table.query(
                KeyConditionExpression=Key('account_id').eq(account_id)
            )

        items = response.get('Items', [])

        logger.info(f"Fetched {len(items)} cost records for {account_id}")

        return items

    except Exception as e:
        logger.error(f"Error querying daily costs: {e}")
        return []


def query_daily_costs_trend(account_id, start_date, end_date):
    """
    Optimised query that only projects date + total_cost.
    Reduces RCU consumption on the trend endpoint hot path.
    """
    try:
        table = _resource().Table('DailyCostSummary')

        response = table.query(
            KeyConditionExpression=Key('account_id').eq(account_id) &
                                   Key('date').between(start_date, end_date),
            ProjectionExpression='#d, total_cost',
            ExpressionAttributeNames={'#d': 'date'}
        )

        items = response.get('Items', [])
        logger.info(f"Fetched {len(items)} trend records for {account_id}")
        return items

    except Exception as e:
        logger.error(f"Error querying trend data: {e}")
        return []


def list_tables():
    try:
        response = _client().list_tables()
        tables = response.get('TableNames', [])
        logger.info(f"Existing tables: {tables}")
        return tables
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return []


def batch_write_resource_usage(records):
    try:
        table = _resource().Table('ResourceUsage')

        def convert(item):
            if isinstance(item, float):
                return Decimal(str(item))
            elif isinstance(item, dict):
                return {k: convert(v) for k, v in item.items()}
            elif isinstance(item, list):
                return [convert(i) for i in item]
            return item

        with table.batch_writer() as batch:
            for item in records:
                # Ensure composite key exists
                if "resource_type_timestamp" not in item:
                    item["resource_type_timestamp"] = f"{item['resource_type']}#{item['timestamp']}"

                # Convert floats to Decimal for DynamoDB
                item = convert(item)

                batch.put_item(Item=item)

        logger.info(f"Batch inserted {len(records)} resource usage records")

    except Exception as e:
        logger.error(f"Error in batch write: {e}")
        raise


def put_recommendation(item):
    try:
        table = _resource().Table('OptimizationRecommendations')

        def convert(obj):
            if isinstance(obj, float):
                return Decimal(str(obj))
            elif isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert(i) for i in obj]
            return obj

        # Convert floats
        item = convert(item)

        # Ensure composite sort key exists
        if 'rec_id_timestamp' not in item:
            rec_id = item.get('rec_id', 'unknown')
            timestamp = item.get('timestamp', '')
            item['rec_id_timestamp'] = f"{rec_id}#{timestamp}"

        # TTL
        item['expires_at'] = int(time.time()) + 30 * 86400

        table.put_item(Item=item)

        logger.info(f"Inserted recommendation for {item['account_id']}")

    except Exception as e:
        logger.error(f"Error inserting recommendation: {e}")
        raise


def query_alerts(account_id):
    try:
        table = _resource().Table('Alerts')

        response = table.query(
            KeyConditionExpression=Key('account_id').eq(account_id)
        )

        items = response.get('Items', [])

        logger.info(f"Fetched {len(items)} alerts for {account_id}")

        return items

    except Exception as e:
        logger.error(f"Error querying alerts: {e}")
        return []


def query_recommendations(account_id):
    try:
        table = _resource().Table('OptimizationRecommendations')

        response = table.query(
            KeyConditionExpression=Key('account_id').eq(account_id)
        )

        items = response.get('Items', [])

        logger.info(f"Fetched {len(items)} recommendations for {account_id}")

        return items

    except Exception as e:
        logger.error(f"Error querying recommendations: {e}")
        return []


# ==================== GSI QUERIES ====================

def query_usage_by_account(account_id):
    """
    Query ALL resource usage records for an account using the primary partition key.
    Handles pagination for large result sets.
    """
    try:
        table = _resource().Table('ResourceUsage')
        items = []
        last_key = None

        while True:
            kwargs = {
                'KeyConditionExpression': Key('account_id').eq(account_id)
            }
            if last_key:
                kwargs['ExclusiveStartKey'] = last_key

            response = table.query(**kwargs)
            items.extend(response.get('Items', []))
            last_key = response.get('LastEvaluatedKey')

            if not last_key:
                break

        logger.info(f"Fetched {len(items)} usage records for {account_id}")
        return items

    except Exception as e:
        logger.error(f"Error querying usage for account: {e}")
        return []


def query_usage_by_account_and_type(account_id, resource_type):
    """
    Query resource usage for a specific account and service type.
    Uses the primary table with sort key prefix filtering.
    """
    try:
        table = _resource().Table('ResourceUsage')
        items = []
        last_key = None

        while True:
            kwargs = {
                'KeyConditionExpression': Key('account_id').eq(account_id) &
                                          Key('resource_type_timestamp').begins_with(f"{resource_type}#")
            }
            if last_key:
                kwargs['ExclusiveStartKey'] = last_key

            response = table.query(**kwargs)
            items.extend(response.get('Items', []))
            last_key = response.get('LastEvaluatedKey')

            if not last_key:
                break

        logger.info(f"Fetched {len(items)} {resource_type} records for {account_id}")
        return items

    except Exception as e:
        logger.error(f"Error querying usage by account+type: {e}")
        return []


def query_usage_by_resource_type(resource_type, start=None, end=None):
    """
    Query resource usage by service type using the ResourceTypeIndex GSI.
    Scatter-gather query across all accounts for a given service.
    """
    try:
        table = _resource().Table('ResourceUsage')

        if start and end:
            key_expr = Key('resource_type').eq(resource_type) & \
                       Key('timestamp').between(start, end)
        else:
            key_expr = Key('resource_type').eq(resource_type)

        response = table.query(
            IndexName='ResourceTypeIndex',
            KeyConditionExpression=key_expr
        )

        items = response.get('Items', [])
        logger.info(f"Fetched {len(items)} records from ResourceTypeIndex for {resource_type}")
        return items

    except Exception as e:
        logger.error(f"Error querying by resource type: {e}")
        return []


def query_usage_by_region(region, start=None, end=None):
    """
    Query resource usage by region using the RegionIndex GSI.
    Returns cost breakdown by service within a region.
    """
    try:
        table = _resource().Table('ResourceUsage')

        if start and end:
            key_expr = Key('region').eq(region) & \
                       Key('timestamp').between(start, end)
        else:
            key_expr = Key('region').eq(region)

        response = table.query(
            IndexName='RegionIndex',
            KeyConditionExpression=key_expr
        )

        items = response.get('Items', [])
        logger.info(f"Fetched {len(items)} records from RegionIndex for {region}")
        return items

    except Exception as e:
        logger.error(f"Error querying by region: {e}")
        return []


# ==================== MAIN ====================

if __name__ == '__main__':
    print("=" * 50)
    print("DYNAMO MANAGER — Table Creation")
    print("=" * 50)
    create_all_tables()
    print("\nTables:", list_tables())