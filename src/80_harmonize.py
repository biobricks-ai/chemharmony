from helper import udf, util

import pyspark.sql 
import pyspark.sql.types as T
import pyspark.sql.functions as F
import glob, pathlib, logging, shutil

# SETUP =================================================================
logging.basicConfig(filename="log/harmonize.log", level=logging.INFO)
pathlib.Path("log").mkdir(exist_ok=True)
logging.info("Starting the script...")

spark = pyspark.sql.SparkSession.builder.appName("Harmonize Script")\
    .config("spark.driver.memory", "64g")\
    .config("spark.executor.memory", "16g")\
    .config("spark.executor.memoryOverhead", "4g")\
    .getOrCreate()

stgdir = util.mk_empty_dir('staging2')

# Define a UDF that checks each part of the split path for a match in the source list
source = [pathlib.Path(f).name for f in glob.glob("staging/*")]
def match_source(split_path):
    matches = [part for part in split_path if part in source]
    return matches[-1] if matches else None

match_source_udf = F.udf(match_source, T.StringType())

# CREATE PROPERTIES =====================================================
logging.info("building staging2/properties.parquet")

pdf1 = spark.read.parquet(f"staging/**/*properties.parquet")\
    .withColumn("source_split", F.split(F.input_file_name(), "/")) \
    .withColumn("source", match_source_udf(F.col("source_split"))) \
    .drop("source_split") \
    .distinct()

canonicalize_json_udf = udf.get_canonicalize_json_udf()
pdf2 = pdf1.withColumn("data", canonicalize_json_udf("data"))
pdf3 = pdf2.withColumn("newpid", F.md5("data"))

prpdir = util.mk_empty_dir(stgdir / "properties.parquet")
pdf3.write.mode("overwrite").parquet(prpdir.as_posix())

# CREATE SUBSTANCES =====================================================
logging.info("building brick/substances.parquet")

sdf = spark.read.parquet("staging/**/*substances.parquet") \
    .withColumn("source_split", F.split(F.input_file_name(), "/")) \
    .withColumn("source", match_source_udf(F.col("source_split"))) \
    .drop("source_split") \
    .distinct()

canonicalize_json_udf = udf.get_canonicalize_json_udf()
sdf2 = sdf.withColumn("data", canonicalize_json_udf("data"))
sdf3 = sdf2.withColumn("newsid", F.md5("data"))

subdir = util.mk_empty_dir(stgdir / "substances.parquet")
sdf3.write.mode("overwrite").parquet(subdir.as_posix())

# CREATE ACTIVITIES =====================================================
logging.info("processing staged activities")

adf1 = spark.read.parquet(f"staging/**/*activities.parquet")\
    .withColumn("source_split", F.split(F.input_file_name(), "/")) \
    .withColumn("source", match_source_udf(F.col("source_split"))) \
    .drop("source_split") \
    .distinct() \
    .withColumn("binary_value", F.when(F.col("value") == "positive", 1).otherwise(0))

# Build smiles
inchi2smi = udf.get_inch2smi_udf()
inchidf = adf1.select("inchi").distinct().withColumn("smiles", inchi2smi(F.col("inchi")))
adf2 = adf1.join(inchidf, on="inchi")

# Build pid and sid
padf = spark.read.parquet(prpdir.as_posix()).select('newpid','pid').distinct()
sadf = spark.read.parquet(subdir.as_posix()).select('newsid','sid').distinct()
adf4 = adf2.join(sadf, on="sid").join(padf, on="pid").drop('pid','sid')

# Create new md5 based aid
adf5 = adf4.withColumnRenamed('newpid','pid').withColumnRenamed('newsid','sid')\
    .select("aid","sid","pid",'source',"inchi","smiles",'value',"binary_value")\
    .withColumn('aid', F.md5(F.concat(F.col('sid'),F.col('pid'),F.col('inchi'),F.col('value'))))\
    .distinct()

adf5.write.mode("overwrite").parquet("brick/activities.parquet")

# CREATE BRICK ========================================================================
pdf = spark.read.parquet(prpdir.as_posix()).drop('pid')\
    .select(F.col('newpid').alias('pid'), 'source', 'data')
sdf = spark.read.parquet(subdir.as_posix()).drop('sid')\
    .select(F.col('newsid').alias('sid'), 'source', 'data')

pdf.write.mode("overwrite").parquet("brick/properties.parquet")
sdf.write.mode("overwrite").parquet("brick/substances.parquet")

# ASSERTIONS ========================================================================
assert spark.read.parquet("brick/substances.parquet").count() > 1e6
assert spark.read.parquet("brick/properties.parquet").count() > 1e3
assert spark.read.parquet("brick/activities.parquet").count() > 1e7

# CLEAN UP ==========================================================================
shutil.rmtree(stgdir.as_posix())