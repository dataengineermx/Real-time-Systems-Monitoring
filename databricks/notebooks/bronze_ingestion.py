from pyspark.sql import functions as F

print("Real-time Systems Monitoring")
print("Bronze ingestion started")

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

SOURCE_PATH = "/Volumes/dbacademy/get_started_de/myfiles/*.csv"
BRONZE_PATH = "/Volumes/dbacademy/get_started_de/bronze/employees"

# ---------------------------------------------------------
# Read source CSV files
# ---------------------------------------------------------

print(f"Reading source files from: {SOURCE_PATH}")

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(SOURCE_PATH)
)

# ---------------------------------------------------------
# Add Bronze metadata
# ---------------------------------------------------------

df_bronze = (
    df
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
)

# ---------------------------------------------------------
# Show Bronze data
# ---------------------------------------------------------

print("Bronze DataFrame:")
display(df_bronze)

# ---------------------------------------------------------
# Write Bronze as Delta
# ---------------------------------------------------------

print(f"Writing Bronze Delta data to: {BRONZE_PATH}")

(
    df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_PATH)
)

print("Bronze ingestion completed successfully")
