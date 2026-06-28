"""
Tests for the data generator module.
Uses moto to mock AWS services (no real AWS calls).
"""

import pytest
import json
import sys
import os

# Ensure src is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ──────────────────────────────────────────────
# DATA GENERATOR TESTS
# ──────────────────────────────────────────────
from src.data_generator.generate_data import (
    make_customers, make_products, make_orders
)

class TestMakeCustomers:
    def test_returns_correct_count(self):
        result = make_customers(50)
        assert len(result) == 50

    def test_required_fields(self):
        c = make_customers(1)[0]
        for field in ("customer_id", "name", "email", "country",
                      "signup_date", "segment", "phone", "city"):
            assert field in c, f"Missing field: {field}"

    def test_unique_emails(self):
        customers = make_customers(100)
        emails = [c["email"] for c in customers]
        assert len(set(emails)) == 100

    def test_valid_segments(self):
        valid = {"Bronze", "Silver", "Gold", "Platinum"}
        for c in make_customers(50):
            assert c["segment"] in valid

    def test_valid_countries(self):
        valid = {"US","UK","CA","AU","DE","FR","IN","JP","BR","MX"}
        for c in make_customers(50):
            assert c["country"] in valid


class TestMakeProducts:
    def test_returns_correct_count(self):
        assert len(make_products(30)) == 30

    def test_positive_price(self):
        for p in make_products(50):
            assert p["price"] > 0
            assert p["cost"] > 0

    def test_cost_less_than_price(self):
        for p in make_products(100):
            assert p["cost"] < p["price"], \
                f"cost {p['cost']} >= price {p['price']}"

    def test_non_negative_stock(self):
        for p in make_products(50):
            assert p["stock_qty"] >= 0


class TestMakeOrders:
    def setup_method(self):
        self.customers = make_customers(20)
        self.products  = make_products(10)

    def test_returns_correct_count(self):
        orders, _ = make_orders(100, self.customers, self.products)
        assert len(orders) == 100

    def test_total_gte_subtotal(self):
        orders, _ = make_orders(50, self.customers, self.products)
        for o in orders:
            assert o["total"] >= o["subtotal"] - 0.01, \
                f"total {o['total']} < subtotal {o['subtotal']}"

    def test_valid_status(self):
        valid = {"pending","processing","shipped","delivered","cancelled","returned"}
        orders, _ = make_orders(50, self.customers, self.products)
        for o in orders:
            assert o["status"] in valid

    def test_order_items_have_order_ids(self):
        orders, items = make_orders(20, self.customers, self.products)
        order_ids = {o["order_id"] for o in orders}
        for item in items:
            assert item["order_id"] in order_ids

    def test_no_negative_totals(self):
        orders, _ = make_orders(100, self.customers, self.products)
        for o in orders:
            assert o["total"] >= 0

    def test_valid_channels(self):
        valid = {"web", "mobile", "api"}
        orders, _ = make_orders(50, self.customers, self.products)
        for o in orders:
            assert o["channel"] in valid

    def test_line_total_calculation(self):
        _, items = make_orders(50, self.customers, self.products)
        for item in items:
            expected = round(item["quantity"] * item["unit_price"] * (1 - item["discount"]), 2)
            assert abs(item["line_total"] - expected) < 0.02, \
                f"Line total mismatch: {item['line_total']} vs {expected}"


# ──────────────────────────────────────────────
# LAMBDA HANDLER TESTS (mocked)
# ──────────────────────────────────────────────
import unittest.mock as mock

class TestBatchIngestionLambda:
    def _get_event(self, bucket, key):
        return {
            "Records": [{
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key}
                }
            }]
        }

    @mock.patch("src.lambda.batch_ingestion.handler.s3")
    @mock.patch("src.lambda.batch_ingestion.handler.cw")
    @mock.patch("src.lambda.batch_ingestion.handler.sfn")
    def test_valid_key_processed(self, mock_sfn, mock_cw, mock_s3):
        os.environ["STEP_FUNCTIONS_ARN"] = ""
        os.environ["AUTO_TRIGGER_PIPELINE"] = "false"
        mock_s3.head_object.return_value = {
            "ContentLength": 1024,
            "LastModified": __import__("datetime").datetime.utcnow(),
            "ETag": '"abc123"',
        }
        mock_cw.put_metric_data.return_value = {}

        from src.lambda.batch_ingestion.handler import handler
        event = self._get_event("my-bucket", "bronze/orders/dt=2024/11/01/orders.csv")
        result = handler(event, {})
        assert result["statusCode"] == 200
        assert result["body"]["summary"]["processed_count"] == 1

    @mock.patch("src.lambda.batch_ingestion.handler.s3")
    @mock.patch("src.lambda.batch_ingestion.handler.cw")
    @mock.patch("src.lambda.batch_ingestion.handler.sfn")
    def test_unknown_table_skipped(self, mock_sfn, mock_cw, mock_s3):
        from src.lambda.batch_ingestion.handler import handler
        event = self._get_event("my-bucket", "bronze/unknown_table/dt=2024/11/01/data.csv")
        result = handler(event, {})
        assert result["body"]["summary"]["processed_count"] == 0


class TestParseS3Key:
    def test_valid_key(self):
        from src.lambda.batch_ingestion.handler import parse_s3_key
        meta = parse_s3_key("bronze/orders/dt=2024/11/01/orders.csv")
        assert meta["table"] == "orders"
        assert meta["date"]  == "2024-11-01"
        assert meta["ext"]   == ".csv"

    def test_invalid_key(self):
        from src.lambda.batch_ingestion.handler import parse_s3_key
        meta = parse_s3_key("some/random/path.csv")
        assert meta["table"] is None

    def test_json_extension(self):
        from src.lambda.batch_ingestion.handler import parse_s3_key
        meta = parse_s3_key("bronze/products/dt=2024/01/15/products.json")
        assert meta["ext"] == ".json"
