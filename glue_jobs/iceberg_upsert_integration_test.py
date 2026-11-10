
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from src.order_transformations import validate_orders


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

SOURCE_PATH = "s3://etl-pineline-project1-753397111997-us-east-1-an/raw/integration-test-orders/"

DATABASE = "project1-ecommerce-etl-data-catalog"
TABLE = "orders_iceberg_integration_test"

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

    cleaned = validate_orders(df)
    cleaned = cleaned.withColumn("amount", F.col("amount").cast("double"))

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
