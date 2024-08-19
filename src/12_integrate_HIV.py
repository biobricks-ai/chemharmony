import os
import json
import biobricks
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# SETUP =================================================================
spark = SparkSession.builder \
    .appName("HIV") \
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

stg = os.makedirs("staging/HIV", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# BUILD COMPOUNDS =========================================================
cmpraw = spark.read.parquet(data.HIV_parquet)

# Filter out rows with null SMILES values
cmp1 = cmpraw.filter(F.col('smiles').isNotNull())
cmp2 = cmp1.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# Create the substances table in JSON format
subjson = cmp2.select("sid", F.to_json(F.struct("smiles", "activity", "HIV_active")).alias("data"))
subjson.write.mode("overwrite").parquet("staging/HIV/substances.parquet")

# WRITE PROPERTIES =====================================================
# Define the JSON data for properties
data_json = json.dumps({
    'HIV_active = 1': 'positive',
    'HIV_active = 0': 'negative'
})

# Create the DataFrame with a single row
properties = spark.createDataFrame([
    (0, data_json)
], ["pid", "data"])

# Write the properties table to a Parquet file
properties.write.mode("overwrite").parquet("staging/HIV/properties.parquet")

# BUILD ACTIVITIES ======================================================
# Set the source and PID for the activity table
cmp_with_ids = cmp2.withColumn("aid", F.col("sid")) \
                   .withColumn("pid", F.lit(0)) \
                   .withColumn("source", F.lit("HIV"))

# Final Activity Table: select required columns including smiles
activity_table = cmp_with_ids.select("aid", "pid", "sid", "smiles", "source", "HIV_active")

# Write the activities table to a Parquet file
activity_table.write.mode("overwrite").parquet("staging/HIV/activities.parquet")