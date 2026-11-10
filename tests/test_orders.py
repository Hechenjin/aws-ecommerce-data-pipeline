
import csv
from collections import Counter
from datetime import date
from decimal import Decimal
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[1] / "sample_data"
EXPECTED_COLUMNS = [
    "order_id", "customer_id", "amount", "order_date", "country"
]


def load_orders():
    files = sorted(DATA_DIR.glob("synthetic_orders_batch_*.csv"))
    assert len(files) == 3, "Expected exactly three CSV batches"

    orders = []
    for path in files:
        with path.open(newline="") as file:
            reader = csv.DictReader(file)
            assert reader.fieldnames == EXPECTED_COLUMNS
            orders.extend(reader)

    return orders


def test_total_order_count():
    assert len(load_orders()) == 800


def test_order_ids_are_unique():
    orders = load_orders()
    ids = [order["order_id"] for order in orders]

    assert len(ids) == len(set(ids))


def test_order_amounts_are_positive():
    for order in load_orders():
        amount = Decimal(order["amount"])
        assert amount > 0


def test_required_fields_are_present():
    for order in load_orders():
        for column in EXPECTED_COLUMNS:
            assert order[column].strip(), (
                f"Missing {column} in order {order}"
            )


def test_reporting_dates():
    dates = {
        date.fromisoformat(order["order_date"])
        for order in load_orders()
    }

    assert len(dates) == 40


def test_order_ids_are_in_expected_range():
    ids = {int(order["order_id"]) for order in load_orders()}

    assert ids == set(range(20001, 20801))


def test_batch_distribution():
    counts = Counter()

    for path in DATA_DIR.glob("synthetic_orders_batch_*.csv"):
        with path.open(newline="") as file:
            counts[path.name] = sum(1 for _ in csv.DictReader(file))

    assert sorted(counts.values()) == [266, 267, 267]
