import os
import pandas as pd
import biobricks as bb
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, isnan
from pyspark.sql.window import Window

# Create staging directory
stg = os.makedirs("staging/pubchem", exist_ok=True)

# Load data
pc = bb.assets("pubchem")

# Spark session with optimized settings
spark = SparkSession.builder \
    .appName("pubchem") \
    .config("spark.executor.memory", "50g") \
    .config("spark.driver.memory", "50g")  \
    .config("spark.executor.memoryOverhead", "10g")  \
    .config("spark.driver.memoryOverhead", "10g")  \
    .config('spark.driver.maxResultSize', '0')  \
    .config("spark.sql.shuffle.partitions", "200")  \
    .config("spark.default.parallelism", "200")  \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")  \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .getOrCreate()

def generate_uuid():
    return str(uuid.uuid4())

generate_uuid_udf = F.udf(generate_uuid)

# BUILD COMPOUNDS =========================================================
cmpraw = spark.read.parquet(pc.compound_sdf_parquet)
cmp1 = cmpraw \
    .filter(col('property') == "PUBCHEM_IUPAC_INCHI") \
    .withColumnRenamed('value', 'inchi')

## Filter out any cids with more than one entry and generate a sid
cmp2 = cmp1.withColumn('count', F.count('id').over(Window.partitionBy('id')))
cmp3 = cmp2.filter(col('count') == 1).drop('count')
cmp4 = cmp3.withColumn('sid', generate_uuid_udf())

# Select and rename columns
cmp = cmp4.select("sid", F.col('id').alias('pubchem_cid'), "inchi")

# BUILD ACTIVITIES ======================================================

# Read and process data in Spark, avoid collecting large data to the driver
actraw = spark.read.parquet(pc.bioassay_concise_parquet)
act1 = actraw \
    .filter(col('property') == "pubchem_activity_outcome") \
    .filter(col('value').isin(["Active", "Inactive"])) \
    .filter(~isnan(col("pubchem_cid"))) \
    .select("pubchem_cid", "pubchem_sid", "aid", "property", "value")
    
# build pids
bea = spark.read.parquet(pc.bioassay_extra_bioassay_parquet) \
    .withColumnRenamed('AID', 'aid') \
    .select(
        "aid", "BioAssay Name", "Deposit Date", "Modify Date",
        "Source Name", "Source ID", "Substance Type", "Outcome Type",
        "Project Category", "BioAssay Group", "BioAssay Types",
        "Protein Accessions", "UniProts IDs", "Gene IDs",
        "Target TaxIDs", "Taxonomy IDs"
    ).withColumn('pid', generate_uuid_udf())

## join bioassay_extra_bioassay `bea` name info with activities `act`
## and join activities with compounds
act2 = act1.join(bea, ["aid"], how='left')
act = act2.join(cmp, act2.pubchem_cid == cmp.pubchem_cid, how='left')

# WRITE PROPERTIES =====================================================
# Add a JSON column for the property data
group_cols = ["aid", "BioAssay Name", "Deposit Date", "Modify Date",
              "Source Name", "Source ID", "Substance Type", "Outcome Type",
              "Project Category", "BioAssay Group", "BioAssay Types",
              "Protein Accessions", "UniProts IDs", "Gene IDs",
              "Target TaxIDs", "Taxonomy IDs"]
struct_cols = F.struct(*group_cols)
propjson = act \
    .withColumn("data", F.to_json(struct_cols)) \
    .select("pid", "data")

propjson.write.mode("overwrite").parquet("staging/pubchem/properties.parquet")

# WRITE COMPOUNDS =======================================================
struct_cols = F.struct("pubchem_cid", "inchi")
subjson = cmp.select("sid", F.to_json(struct_cols).alias("data"))
subjson.write.mode("overwrite").parquet("staging/pubchem/substances.parquet")

# WRITE ACTIVITIES =====================================================
gen_aid = F.udf(lambda x: f"pubchem_{x}")
actout = act \
    .select("sid", "pid", "inchi", "value") \
    .distinct() \
    .withColumn("aid", gen_aid(F.monotonically_increasing_id())) \
    .select("aid", "sid", "pid", "inchi", "value")

actout.write.mode("overwrite").parquet("staging/pubchem/activities.parquet")