from pyspark.sql import functions as F

REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "amount",
    "order_date",
    "country",
}


def validate_orders(df):
    """Validate incoming orders before Iceberg MERGE."""

    missing = REQUIRED_COLUMNS - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing source columns: {sorted(missing)}"
        )

    cleaned = df.select(
        F.trim(F.col("order_id")).alias("order_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.expr(
            "try_cast(trim(amount) AS DECIMAL(18,2))"
        ).alias("amount"),
        F.expr(
            "try_cast(trim(order_date) AS DATE)"
        ).alias("parsed_date"),
        F.trim(F.col("country")).alias("country"),
    )

    invalid = cleaned.filter(
        F.col("order_id").isNull()
        | (F.col("order_id") == "")
        | F.col("customer_id").isNull()
        | (F.col("customer_id") == "")
        | F.col("amount").isNull()
        | (F.col("amount") <= 0)
        | F.col("parsed_date").isNull()
        | F.col("country").isNull()
        | (F.col("country") == "")
    )

    invalid_count = invalid.count()

    if invalid_count:
        raise ValueError(
            f"Incoming batch contains {invalid_count} invalid records"
        )

    duplicates = (
        cleaned.groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count():
        raise ValueError(
            "Incoming batch contains duplicate order IDs"
        )

    return cleaned.select(
        "order_id",
        "customer_id",
        "amount",
        F.date_format(
            "parsed_date", "yyyy-MM-dd"
        ).alias("order_date"),
        "country",
    )
