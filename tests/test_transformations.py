
import pytest

from pyspark.sql import SparkSession
from src.order_transformations import validate_orders


COLUMNS = [
    "order_id",
    "customer_id",
    "amount",
    "order_date",
    "country",
]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("order-validation-tests")
        .getOrCreate()
    )

    yield session
    session.stop()


def make_orders(spark, rows):
    return spark.createDataFrame(rows, COLUMNS)


def test_valid_order(spark):
    df = make_orders(spark, [
        ("20001", "C264", "146.49", "2026-10-19", "US")
    ])

    result = validate_orders(df).collect()

    assert len(result) == 1
    assert str(result[0]["amount"]) == "146.49"


def test_duplicate_order_ids_rejected(spark):
    df = make_orders(spark, [
        ("20001", "C264", "146.49", "2026-10-19", "US"),
        ("20001", "C264", "200.00", "2026-10-19", "US"),
    ])

    with pytest.raises(ValueError, match="duplicate"):
        validate_orders(df)


@pytest.mark.parametrize(
    "amount",
    ["-10.00", "0", "invalid", ""]
)
def test_invalid_amount_rejected(spark, amount):
    df = make_orders(spark, [
        ("20001", "C264", amount, "2026-10-19", "US")
    ])

    with pytest.raises(ValueError, match="invalid"):
        validate_orders(df)


def test_invalid_date_rejected(spark):
    df = make_orders(spark, [
        ("20001", "C264", "146.49", "not-a-date", "US")
    ])

    with pytest.raises(ValueError, match="invalid"):
        validate_orders(df)


def test_missing_customer_rejected(spark):
    df = make_orders(spark, [
        ("20001", "", "146.49", "2026-10-19", "US")
    ])

    with pytest.raises(ValueError, match="invalid"):
        validate_orders(df)


def test_missing_column_rejected(spark):
    df = spark.createDataFrame(
        [("20001", "146.49")],
        ["order_id", "amount"],
    )

    with pytest.raises(ValueError, match="Missing source columns"):
        validate_orders(df)
