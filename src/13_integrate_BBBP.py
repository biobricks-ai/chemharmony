import os
import json
import biobricks
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# SETUP =================================================================
spark = SparkSession.builder \
    .appName("BBBP") \
    .config("spark.executor.memory", "50g") \
    .config("spark.driver.memory", "100g")  \
    .config("spark.executor.memoryOverhead", "20g")  \
    .config("spark.driver.memoryOverhead", "20g")  \
    .config("spark.sql.shuffle.partitions", "200")  \
    .config("spark.default.parallelism", "40")  \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")  \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.executor.heartbeatInterval","20000ms") \
    .config("spark.network.timeout","10000000ms") \
    .getOrCreate()

stg = os.makedirs("staging/BBBP", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# BUILD COMPOUNDS =========================================================
bbbp_raw = spark.read.parquet(data.BBBP_parquet)

# Filter out rows with null SMILES values
bbbp_filtered = bbbp_raw.filter(F.col('smiles').isNotNull())
bbbp_with_sid = bbbp_filtered.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# Combine metadata into the data column
subjson = bbbp_with_sid.select(
    "sid", 
    F.to_json(
        F.struct(
            F.col("num"),
            F.col("name"),
            F.col("p_np"),
            F.col("smiles")
        )
    ).alias("data")
)

# Write the substances table to Parquet
subjson.write.mode("overwrite").parquet("staging/BBBP/substances.parquet")

# WRITE PROPERTIES =====================================================
# Define the JSON data for properties
data_json = json.dumps({
    'p_np = 1': 'permeable',
    'p_np = 0': 'non-permeable'
})

# Create the DataFrame with a single row
properties = spark.createDataFrame([
    (0, data_json)
], ["pid", "data"])

# Write the properties table to a Parquet file
properties.write.mode("overwrite").parquet("staging/BBBP/properties.parquet")

# BUILD ACTIVITIES ======================================================
# Set the source and PID for the activity table
bbbp_with_ids = bbbp_with_sid.withColumn("aid", F.col("sid")) \
                             .withColumn("pid", F.lit(0)) \
                             .withColumn("source", F.lit("BBBP"))

# Final Activity Table: select required columns including smiles
activity_table = bbbp_with_ids.select("aid", "pid", "sid", "smiles", "source", "p_np")

# Write the activities table to a Parquet file
activity_table.write.mode("overwrite").parquet("staging/BBBP/activities.parquet")
