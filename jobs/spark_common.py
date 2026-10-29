from jobs.config import env, identifier
from jobs.observability import OBSERVATIONS, LINEAGE


def session(name):
    from pyspark.sql import SparkSession

    catalog = "spark.sql.catalog.lake"
    settings = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        catalog: "org.apache.iceberg.spark.SparkCatalog",
        catalog + ".type": "jdbc",
        catalog
        + ".uri": f"jdbc:postgresql://{env('SOURCE_DB_HOST')}:{env('SOURCE_DB_PORT')}/{env('SOURCE_DB_NAME')}",
        catalog + ".jdbc.user": env("SOURCE_DB_USER"),
        catalog + ".jdbc.password": env("SOURCE_DB_PASSWORD"),
        catalog + ".warehouse": f"s3://{env('LAKE_BUCKET')}/warehouse",
        catalog + ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        catalog + ".s3.endpoint": env("S3_ENDPOINT"),
        catalog + ".s3.path-style-access": "true",
        catalog + ".s3.access-key-id": env("MINIO_ROOT_USER"),
        catalog + ".s3.secret-access-key": env("MINIO_ROOT_PASSWORD"),
        catalog + ".client.region": env("AWS_REGION"),
        "spark.sql.shuffle.partitions": "2",
        "spark.default.parallelism": "2",
        "spark.sql.session.timeZone": "UTC",
        "spark.ui.enabled": "false",
        "spark.driver.bindAddress": "127.0.0.1",
    }
    builder = SparkSession.builder.appName(name).master("local[2]")
    for key, value in settings.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    namespace = identifier("LAKE_NAMESPACE")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{namespace}")
    return spark


def table(name):
    return f"lake.{identifier('LAKE_NAMESPACE')}.{name}"


def create_tables(spark):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table("raw_events")} (
        topic STRING, event_partition INT, event_offset BIGINT, payload STRING
        ) USING iceberg PARTITIONED BY (topic)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table("transactions")} (
        txn_id BIGINT, card_id BIGINT, customer_id BIGINT, card_type STRING,
        merchant STRING, amount DECIMAL(18,2), currency STRING, status STRING,
        txn_time TIMESTAMP, txn_date DATE
        ) USING iceberg PARTITIONED BY (txn_date)""")

    # Additive migrations preserve existing data. Unknown historical times stay null
    # until the original Kafka record is replayed; no historical time is fabricated.
    for name, fields in [("raw_events", OBSERVATIONS), ("transactions", LINEAGE)]:
        existing = set(spark.table(table(name)).columns)
        additions = ", ".join(
            f"{column} {kind}" for column, kind in fields.items() if column not in existing
        )
        if additions:
            spark.sql(f"ALTER TABLE {table(name)} ADD COLUMNS ({additions})")
            spark.catalog.refreshTable(table(name))
