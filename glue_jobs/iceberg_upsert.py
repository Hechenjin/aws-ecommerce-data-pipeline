
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ---------------------------------------------------
# 1. Initialize AWS Glue and job bookmarks
# ---------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------
# 2. Configuration
# ---------------------------------------------------

SOURCE_PATH = (
    "s3://etl-pineline-project1-753397111997-"
    "us-east-1-an/raw/iceberg-updates/"
)

DATABASE = "project1-ecommerce-etl-data-catalog"
TABLE = "orders_iceberg"

TARGET = f"glue_catalog.`{DATABASE}`.`{TABLE}`"


# ---------------------------------------------------
# 3. Verify Iceberg catalog configuration
# ---------------------------------------------------

catalog_class = spark.conf.get(
    "spark.sql.catalog.glue_catalog",
    "NOT CONFIGURED"
)

print("Iceberg catalog:", catalog_class)

if catalog_class == "NOT CONFIGURED":
    raise RuntimeError(
        "The Iceberg Spark catalog is not configured."
    )

print("Checking available tables...")


spark.sql(
    f"SHOW TABLES IN glue_catalog.`{DATABASE}`"
).show(truncate=False)

# Fail early if the target table is not accessible.
spark.table(TARGET).printSchema()

print("Iceberg target verified:", TARGET)


# ---------------------------------------------------
# 4. Read incremental CSV input
# ---------------------------------------------------

source = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ",",
        "quoteChar": '"'
    },
    connection_options={
        "paths": [SOURCE_PATH],
        "recurse": True
    },
    transformation_ctx="iceberg_order_updates_source"
)

df = source.toDF()


# ---------------------------------------------------
# 5. Handle empty input
# ---------------------------------------------------

if not df.columns or df.isEmpty():

    print("No new order records. Skipping MERGE.")

else:

    required = {
        "order_id",
        "customer_id",
        "amount",
        "order_date",
        "country"
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing source columns: {sorted(missing)}"
        )

    # -----------------------------------------------
    # 6. Clean and validate incoming records
    # -----------------------------------------------

    cleaned = (
        df.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.col("amount").cast("double").alias("amount"),
            F.trim(F.col("order_date")).alias("order_date"),
            F.trim(F.col("country")).alias("country")
        )
        .filter(
            F.col("order_id").isNotNull()
            & (F.col("order_id") != "")
            & F.col("amount").isNotNull()
            & (F.col("amount") > 0)
        )
    )

    # Reject duplicate order IDs within this batch.
    duplicate_ids = (
        cleaned.groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
    )

    if not duplicate_ids.isEmpty():
        raise ValueError(
            "Incoming batch contains duplicate order IDs."
        )

    if cleaned.isEmpty():

        print("No valid order records to merge.")

    else:

        # -------------------------------------------
        # 7. Register a temporary Spark SQL view
        # -------------------------------------------

        cleaned.createOrReplaceTempView(
            "incoming_orders"
        )

        print("Incoming orders prepared:")
        cleaned.show(truncate=False)

        # -------------------------------------------
        # 8. Perform an Iceberg MERGE
        # -------------------------------------------

        spark.sql(f"""
            MERGE INTO {TARGET} AS target
            USING incoming_orders AS source

            ON target.order_id = source.order_id

            WHEN MATCHED THEN UPDATE SET
                target.customer_id = source.customer_id,
                target.amount = source.amount,
                target.order_date = source.order_date,
                target.country = source.country

            WHEN NOT MATCHED THEN INSERT (
                order_id,
                customer_id,
                amount,
                order_date,
                country
            )
            VALUES (
                source.order_id,
                source.customer_id,
                source.amount,
                source.order_date,
                source.country
            )
        """)

        print("Iceberg MERGE completed successfully.")


# ---------------------------------------------------
# 9. Commit bookmark state after successful work
# ---------------------------------------------------

job.commit()

print("Glue job completed.")
