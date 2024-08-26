import os
import json
import biobricks
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from helper.udf import get_smiles_to_inchi_udf
from rdkit import RDLogger

# Suppress RDKit warnings
RDLogger.DisableLog('rdApp.*')

# Spark setup
spark = SparkSession.builder \
    .appName("BACE") \
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

os.makedirs("staging/BACE", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# Build compounds
cmpraw = spark.read.parquet(data.bace_parquet)
cmp_filtered = cmpraw.select(F.col("mol").alias("smiles"), "CID", "Class") \
                     .filter(F.col('smiles').isNotNull())
cmp_with_sid = cmp_filtered.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# Convert SMILES to InChI
smiles_to_inchi_udf = get_smiles_to_inchi_udf()
cmp_with_inchi = cmp_with_sid.withColumn('inchi', smiles_to_inchi_udf(F.col('smiles'))).cache()

# Substances table (structural + metadata, excluding property information)
subjson = cmp_with_inchi.select(
    "sid", 
    F.to_json(F.struct("smiles", "inchi", "CID")).alias("data")
)
subjson.write.mode("overwrite").parquet("staging/BACE/substances.parquet")

# Properties table
data_json = json.dumps({
    'property': 'Class',
    'active_value': 1,
    'inactive_value': 0
})
properties = spark.createDataFrame([("0", data_json)], ["pid", "data"])
properties.write.mode("overwrite").parquet("staging/BACE/properties.parquet")

# Activities table
cmp_with_ids = cmp_with_inchi.withColumn("aid", F.col("sid")) \
                             .withColumn("pid", F.lit("0")) \
                             .withColumn("source", F.lit("BACE")) \
                             .withColumnRenamed("Class", "value")
activity_table = cmp_with_ids.select("aid", "pid", "sid", "smiles", "inchi", "source", "value")
activity_table.write.mode("overwrite").parquet("staging/BACE/activities.parquet")

# Clean up
cmp_with_inchi.unpersist()
