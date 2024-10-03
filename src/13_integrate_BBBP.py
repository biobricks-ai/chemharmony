import os
import json
import biobricks
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from helper.udf import get_smiles_to_inchi_udf

# Spark setup
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

os.makedirs("staging/BBBP", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# Build compounds
bbbp_raw = spark.read.parquet(data.BBBP_parquet)
bbbp_filtered = bbbp_raw.filter(F.col('smiles').isNotNull())
bbbp_with_sid = bbbp_filtered.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# Convert SMILES to InChI
smiles_to_inchi_udf = get_smiles_to_inchi_udf()
bbbp_with_inchi = bbbp_with_sid.withColumn("inchi", smiles_to_inchi_udf(F.col("smiles"))).cache()

# Substances table
subjson = bbbp_with_inchi.select(
    "sid", 
    F.to_json(F.struct("num", "name", "smiles", "inchi")).alias("data")
)
subjson.write.mode("overwrite").parquet("staging/BBBP/substances.parquet")

# Properties table
data_json = json.dumps({
    'property': 'p_np',
    'description': 'Blood-brain barrier permeability: ability of a compound to cross the blood-brain barrier',
    'active_value': 1,
    'inactive_value': 0,
    'active_label': 'Permeable (P)',
    'inactive_label': 'Non-permeable (NP)'
})
properties = spark.createDataFrame([("0", data_json)], ["pid", "data"])
properties.write.mode("overwrite").parquet("staging/BBBP/properties.parquet")

# Activities table
bbbp_with_ids = bbbp_with_inchi.withColumn("aid", F.monotonically_increasing_id().cast('string')) \
                               .withColumn("pid", F.lit("0")) \
                               .withColumn("source", F.lit("BBBP")) \
                               .withColumnRenamed("p_np", "value")
activity_table = bbbp_with_ids.select("aid", "pid", "sid", "smiles", "inchi", "source", "value")
activity_table = activity_table.withColumn("value", F.when(F.col("value") == 0, "negative").otherwise("positive"))
activity_table.write.mode("overwrite").parquet("staging/BBBP/activities.parquet")


# add a test here
for table in ["substances", "properties", "activities"]:
    df = spark.read.parquet(f"staging/BBBP/{table}.parquet")
    assert df.count() > 0, f"{table.capitalize()} table is empty"

print("All tables exist and contain data.")

