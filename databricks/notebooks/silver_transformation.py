from pyspark.sql import functions as F

print("Real-time Systems Monitoring")
print("Silver transformation started")

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

BRONZE_PATH = "/Volumes/dbacademy/get_started_de/bronze/employees"
SILVER_PATH = "/Volumes/dbacademy/get_started_de/silver/employees"

# ---------------------------------------------------------
# Read Bronze
# ---------------------------------------------------------

print(f"Reading Bronze data from: {BRONZE_PATH}")

df_bronze = (
    spark.read
        .format("delta")
        .load(BRONZE_PATH)
)

print("Bronze DataFrame:")
display(df_bronze)

# ---------------------------------------------------------
# Silver transformation
# ---------------------------------------------------------
# Basic data quality and standardization:
# - Remove records without ID
# - Remove records without FirstName
# - Normalize text fields
# - Preserve Bronze metadata
# - Add Silver processing timestamp
# ---------------------------------------------------------

df_silver = (
    df_bronze
    .filter(F.col("ID").isNotNull())
    .filter(F.col("FirstName").isNotNull())
    .withColumn("FirstName", F.trim(F.col("FirstName")))
    .withColumn("Country", F.trim(F.col("Country")))
    .withColumn("Role", F.trim(F.col("Role")))
    .withColumn("silver_processed_timestamp", F.current_timestamp())
)

print("Silver DataFrame:")
display(df_silver)

# ---------------------------------------------------------
# Write Silver as Delta
# ---------------------------------------------------------

print(f"Writing Silver Delta data to: {SILVER_PATH}")

(
    df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_PATH)
)

print("Silver transformation completed successfully")
