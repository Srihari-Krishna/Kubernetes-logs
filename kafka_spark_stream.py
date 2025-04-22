from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unix_timestamp, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaLogStreaming") \
    .getOrCreate()

# Step 2: Define schema of the Kafka outer JSON
outer_schema = StructType([
    StructField("@timestamp", DoubleType()),
    StructField("log", StringType()),
    StructField("stream", StringType())
])

# Step 3: Define schema for inner JSON inside 'log'
inner_schema = StructType([
    StructField("level", StringType()),
    StructField("ts", StringType()),
    StructField("caller", StringType()),
    StructField("msg", StringType()),
    StructField("hash", LongType()),
    StructField("revision", LongType()),
    StructField("compact-revision", LongType())
])

# Step 4: Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "info-logs,warn-logs,error-logs") \
    .load()

# Step 5: Convert Kafka value to string and parse outer JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), outer_schema).alias("data")) \
    .select("data.*")

# Step 6: Parse the inner JSON string in 'log' field
parsed_df = json_df \
    .withColumn("log_data", from_json(col("log"), inner_schema)) \
    .withColumn("timestamp", to_timestamp(col("@timestamp").cast("timestamp"))) \
    .select(
        col("timestamp"),
        col("stream"),
        col("log_data.level").alias("level"),
        to_timestamp("log_data.ts").alias("ts"),
        col("log_data.caller").alias("caller"),
        col("log_data.msg").alias("msg"),
        col("log_data.hash").alias("hash"),
        col("log_data.revision").alias("revision"),
        col("log_data.`compact-revision`").alias("compact_revision")
    )

# Step 7: Define writing to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/sparklogs") \
        .option("dbtable", "logs_v2") \
        .option("user", "sparkuser") \
        .option("password", "sparkpass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Step 8: Also write each batch to local file (for checking)
def write_to_file(batch_df, batch_id):
    batch_df.write \
        .mode("append") \
        .json(f"/tmp/logs_batch_{batch_id}")

# Step 8.5: Spark SQL logic
def write_sql_outputs(batch_df, batch_id):
    # Register the DataFrame as a temporary SQL view
    session = batch_df.sparkSession
    batch_df.createOrReplaceTempView("logs_view")

    # === Query 1: Count by log level ===
    level_count = session.sql("""
        SELECT level, COUNT(*) as count
        FROM logs_view
        GROUP BY level
        ORDER BY count DESC
    """)
    level_count.write.mode("overwrite").json(f"/tmp/sql_results_{batch_id}/level_counts")

    # === Query 2: Top 5 most frequent callers ===
    top_callers = session.sql("""
        SELECT caller, COUNT(*) as count
        FROM logs_view
        GROUP BY caller
        ORDER BY count DESC
        LIMIT 5
    """)
    top_callers.write.mode("overwrite").json(f"/tmp/sql_results_{batch_id}/top_callers")

    # === Query 3: Recent warning/error logs (last 10 minutes) ===
    recent_warns = session.sql("""
        SELECT *
        FROM logs_view
        WHERE level IN ('error', 'warn')
          AND ts > current_timestamp() - INTERVAL 10 MINUTES
    """)
    recent_warns.write.mode("overwrite").json(f"/tmp/sql_results_{batch_id}/recent_warnings")


# Step 9: Combined writer
def write_to_targets(batch_df, batch_id):
    write_to_postgres(batch_df, batch_id)
    write_to_file(batch_df, batch_id)
    write_sql_outputs(batch_df, batch_id)

# Step 10: Start streaming
query = parsed_df \
    .writeStream \
    .foreachBatch(write_to_targets) \
    .outputMode("append") \
    .start()

query.awaitTermination()
