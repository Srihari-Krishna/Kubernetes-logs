from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BatchProcessingFromPostgres") \
    .getOrCreate()

# PostgreSQL Connection Details (Not used for writing output)
jdbc_url = "jdbc:postgresql://localhost:5432/sparklogs"
jdbc_properties = {
    "user": "sparkuser",
    "password": "sparkpass",
    "driver": "org.postgresql.Driver"
}

# Step 1: Define the SQL query to fetch data (adjust the batch size as needed)
# Example: Fetch logs from a specific time range
query = """
    SELECT * 
    FROM logs_v2 
    WHERE timestamp >= '2025-04-21 00:00:00' AND timestamp <= '2025-04-21 23:59:59'
    LIMIT 1000
"""

# Step 2: Read the data from PostgreSQL
start_time = time.time()  # Start time for batch processing

batch_df = spark.read \
    .jdbc(url=jdbc_url, table=f"({query}) AS logs", properties=jdbc_properties)

# Step 3: Perform transformations or aggregations (e.g., count by log level)
transformed_df = batch_df.groupBy("level").count()

# Step 4: Write the transformed data to a local file in JSON format
output_path = "/tmp/logs_batch_processing_results"  # Change the path as needed
transformed_df.write \
    .json(output_path)  # Writing the result as a JSON file

# Step 5: Measure time taken for batch processing
end_time = time.time()  # End time for batch processing
elapsed_time = end_time - start_time
print(f"Batch processing took {elapsed_time} seconds")

