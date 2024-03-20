import os, biobricks, pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# SETUP =================================================================
spark = pyspark.sql.SparkSession.builder \
    .appName("pubchem") \
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

stg = os.makedirs("staging/pubchem", exist_ok=True)
pc = biobricks.assets("pubchem")

# BUILD COMPOUNDS =========================================================
cmpraw = spark.read.parquet(pc.compound_sdf_parquet)
cmp1 = cmpraw.filter(F.col('property') == "PUBCHEM_IUPAC_INCHI").withColumnRenamed('value', 'inchi')

## Filter out any cids with more than one entry and generate a sid
cmp2 = cmp1.withColumn('count', F.count('id').over(Window.partitionBy('id')))
cmp3 = cmp2.filter(F.col('count') == 1).drop('count')
cmp4 = cmp3.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# Select and rename columns
cmp = cmp4.select("sid", F.col('id').alias('pubchem_cid'), "inchi")
subjson = cmp.select("sid", F.to_json(F.struct("pubchem_cid", "inchi")).alias("data"))
subjson.write.mode("overwrite").parquet("staging/pubchem/substances.parquet")

# WRITE PROPERTIES =====================================================
bea = spark.read.parquet(pc.bioassay_extra_bioassay_parquet) \
    .withColumnRenamed('AID', 'aid') \
    .select("aid", "BioAssay Name", "Deposit Date", "Modify Date","Source Name", "Source ID", "Substance Type", "Outcome Type","Project Category", "BioAssay Group", "BioAssay Types","Protein Accessions", "UniProts IDs", "Gene IDs","Target TaxIDs", "Taxonomy IDs")\
    .withColumn('pid', F.monotonically_increasing_id().cast('string'))

group_cols = ["aid", "BioAssay Name", "Deposit Date", "Modify Date","Source Name", "Source ID", "Substance Type", "Outcome Type", "Project Category", "BioAssay Group", "BioAssay Types", "Protein Accessions", "UniProts IDs", "Gene IDs", "Target TaxIDs", "Taxonomy IDs"]
propjson = bea.withColumn("data", F.to_json(F.struct(*group_cols))).select("pid", "data")
propjson.write.mode("overwrite").parquet("staging/pubchem/properties.parquet")

# BUILD ACTIVITIES ======================================================
actraw = spark.read.parquet(pc.bioassay_concise_parquet)
act1 = actraw \
    .filter(F.col('property') == "pubchem_activity_outcome") \
    .filter(F.col('value').isin(["Active", "Inactive"])) \
    .withColumn('value', F.when(F.col('value') == "Active", "positive").otherwise("negative")) \
    .filter(~F.isnan(F.col("pubchem_cid"))) \
    .select("pubchem_cid", "pubchem_sid", "aid", "property", "value")
    
act2 = act1.join(bea, ["aid"], how='inner').join(cmp, ['pubchem_cid'], how='inner')
act3 = act2.select("sid", "pid", "inchi", "value").distinct() \
    .withColumn("aid", F.monotonically_increasing_id().cast('string')) \
    .select("aid", "sid", "pid", "inchi", "value")

act3.write.mode("overwrite").parquet("staging/pubchem/activities.parquet")
    
# TESTS =================================================================
# How many properties have more than 100 positives and negatives?
act = spark.read.parquet("staging/pubchem/activities.parquet")
res = act.groupBy("pid").pivot("value").count().filter(F.col("positive") > 100).filter(F.col("negative") > 100)
valid_properties = res.count() # 2160
assert valid_properties > 1000

# Among the valid properties, how many positives and negatives?
res2 = act.join(res, ["pid"], how='inner').groupBy("value").count().toPandas()
assert sum(res2['count']) > 1e6

# check that there are no repeats
res3 = act.join(res, ["pid"], how='inner').distinct().groupBy("value").count().toPandas()
assert res2.equals(res3)

# joining substances with activities yields some substances
sub = spark.read.parquet("staging/pubchem/substances.parquet")
act = spark.read.parquet("staging/pubchem/activities.parquet")
res = sub.join(act, ["sid"], how='inner').count()
assert res > 1e6