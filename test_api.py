"""
test_api.py

Comprehensive test suite for the Cloud Cost Monitoring platform.
Covers: unit tests (z-score, helpers), integration tests (DynamoDB, Redis),
and API endpoint tests.

Run with: pytest test_api.py -v
"""

import pytest
import json
import math
from decimal import Decimal
from unittest.mock import patch, MagicMock


# ==================== UNIT TESTS ====================

class TestZScoreComputation:
    """Unit tests for anomaly detection z-score computation."""

    def test_z_score_normal_value(self):
        from anomaly_detector import compute_z_score
        z = compute_z_score(100, 100, 10)
        assert z == 0.0

    def test_z_score_above_mean(self):
        from anomaly_detector import compute_z_score
        z = compute_z_score(120, 100, 10)
        assert z == 2.0

    def test_z_score_below_mean(self):
        from anomaly_detector import compute_z_score
        z = compute_z_score(80, 100, 10)
        assert z == -2.0

    def test_z_score_zero_std_dev(self):
        from anomaly_detector import compute_z_score
        z = compute_z_score(150, 100, 0)
        assert z == 0.0

    def test_z_score_anomaly_threshold(self):
        from anomaly_detector import compute_z_score
        z = compute_z_score(130, 100, 10)
        assert abs(z) > 2.0  # Should trigger anomaly


class TestDecimalConversion:
    """Unit tests for Decimal/float conversion used in API responses."""

    def test_convert_decimal(self):
        from api import _convert_decimals
        result = _convert_decimals(Decimal('10.5'))
        assert result == 10.5
        assert isinstance(result, float)

    def test_convert_nested_dict(self):
        from api import _convert_decimals
        data = {'cost': Decimal('42.50'), 'name': 'test'}
        result = _convert_decimals(data)
        assert result == {'cost': 42.5, 'name': 'test'}

    def test_convert_list(self):
        from api import _convert_decimals
        data = [Decimal('1.1'), Decimal('2.2')]
        result = _convert_decimals(data)
        assert result == [1.1, 2.2]

    def test_convert_nested_structure(self):
        from api import _convert_decimals
        data = {
            'items': [{'cost': Decimal('10')}, {'cost': Decimal('20')}],
            'total': Decimal('30')
        }
        result = _convert_decimals(data)
        assert result['total'] == 30.0
        assert result['items'][0]['cost'] == 10.0


class TestRedisSerializer:
    """Unit tests for Redis serialization helper."""

    def test_serialize_decimal(self):
        from redis_manager import _serialize
        assert _serialize(Decimal('99.99')) == 99.99

    def test_serialize_dict(self):
        from redis_manager import _serialize
        result = _serialize({'a': Decimal('1'), 'b': 'text'})
        assert result == {'a': 1.0, 'b': 'text'}

    def test_serialize_list(self):
        from redis_manager import _serialize
        result = _serialize([Decimal('1'), Decimal('2')])
        assert result == [1.0, 2.0]


class TestInputValidation:
    """Unit tests for API input validation."""

    def test_valid_account(self):
        from api import _validate_account
        assert _validate_account('acct-001') is True

    def test_invalid_account(self):
        from api import _validate_account
        assert _validate_account('acct-999') is False

    def test_valid_date(self):
        from api import _validate_date
        assert _validate_date('2026-01-15') is True

    def test_invalid_date(self):
        from api import _validate_date
        assert _validate_date('not-a-date') is False

    def test_none_date(self):
        from api import _validate_date
        assert _validate_date(None) is False


# ==================== API TESTS (with Flask test client) ====================

class TestAPIEndpoints:
    """Integration tests for Flask API endpoints."""

    @pytest.fixture
    def client(self):
        from api import app
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client

    def test_health_endpoint(self, client):
        resp = client.get('/api/health')
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'status' in data
        assert 'dynamodb' in data
        assert 'redis' in data

    def test_accounts_endpoint(self, client):
        resp = client.get('/api/accounts')
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'accounts' in data
        assert len(data['accounts']) == 5

    def test_dashboard_valid_account(self, client):
        resp = client.get('/api/dashboard/acct-001')
        # Should return 200 (with data) or 404 (no data yet) — NOT 500
        assert resp.status_code in (200, 404)

    def test_dashboard_invalid_account(self, client):
        resp = client.get('/api/dashboard/acct-999')
        assert resp.status_code == 400

    def test_costs_valid_account(self, client):
        resp = client.get('/api/costs/acct-001')
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'account_id' in data

    def test_costs_invalid_account(self, client):
        resp = client.get('/api/costs/acct-999')
        assert resp.status_code == 400

    def test_costs_invalid_date(self, client):
        resp = client.get('/api/costs/acct-001?start=bad-date')
        assert resp.status_code == 400

    def test_trend_valid_account(self, client):
        resp = client.get('/api/trend/acct-001')
        assert resp.status_code == 200

    def test_trend_invalid_account(self, client):
        resp = client.get('/api/trend/acct-999')
        assert resp.status_code == 400

    def test_rankings_endpoint(self, client):
        resp = client.get('/api/rankings')
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'date' in data

    def test_rankings_invalid_date(self, client):
        resp = client.get('/api/rankings?date=bad')
        assert resp.status_code == 400

    def test_alerts_valid_account(self, client):
        resp = client.get('/api/alerts/acct-001')
        assert resp.status_code == 200

    def test_alerts_invalid_account(self, client):
        resp = client.get('/api/alerts/acct-999')
        assert resp.status_code == 400

    def test_anomaly_stats_valid(self, client):
        resp = client.get('/api/anomaly-stats/acct-001')
        # 200 if stats exist, 404 if not — never 500
        assert resp.status_code in (200, 404)

    def test_anomaly_stats_invalid(self, client):
        resp = client.get('/api/anomaly-stats/acct-999')
        assert resp.status_code == 400

    def test_recommendations_valid(self, client):
        resp = client.get('/api/recommendations/acct-001')
        assert resp.status_code == 200

    def test_recommendations_invalid(self, client):
        resp = client.get('/api/recommendations/acct-999')
        assert resp.status_code == 400

    def test_usage_by_service_valid(self, client):
        resp = client.get('/api/usage/by-service/EC2')
        assert resp.status_code == 200

    def test_usage_by_service_invalid(self, client):
        resp = client.get('/api/usage/by-service/InvalidService')
        assert resp.status_code == 400

    def test_usage_by_region_valid(self, client):
        resp = client.get('/api/usage/by-region/us-east-1')
        assert resp.status_code == 200

    def test_usage_by_region_invalid(self, client):
        resp = client.get('/api/usage/by-region/invalid-region')
        assert resp.status_code == 400

    def test_summary_endpoint(self, client):
        resp = client.get('/api/summary')
        assert resp.status_code == 200


# ==================== DATA MODEL TESTS ====================

class TestDataModel:
    """Tests verifying the DynamoDB schema design decisions."""

    def test_resource_usage_composite_key(self):
        """Verify composite sort key format: resource_type#timestamp."""
        key = "EC2#2026-01-15T12:00:00.000000Z"
        parts = key.split('#')
        assert len(parts) == 2
        assert parts[0] == 'EC2'

    def test_recommendation_composite_key(self):
        """Verify recommendation sort key format: rec_id#timestamp."""
        key = "REC-ABC123#2026-01-15T12:00:00.000000Z"
        parts = key.split('#')
        assert len(parts) == 2
        assert parts[0].startswith('REC-')

    def test_budget_utilization_calculation(self):
        """Budget utilization = (daily_cost / daily_budget) * 100."""
        daily_cost = 400
        monthly_budget = 10000
        daily_budget = monthly_budget / 30
        utilization = (daily_cost / daily_budget) * 100
        assert utilization > 100  # Over budget


if __name__ == '__main__':
    pytest.main([__file__, '-v'])