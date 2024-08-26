import os
import json
import biobricks
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from helper.udf import get_smiles_to_inchi_udf

# Spark setup
spark = SparkSession.builder \
    .appName("SIDER") \
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

os.makedirs("staging/sider", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# Build compounds
cmpraw = spark.read.parquet(data.sider_parquet)
cmp_filtered = cmpraw.filter(F.col('smiles').isNotNull())
cmp_with_sid = cmp_filtered.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# Convert SMILES to InChI
smiles_to_inchi_udf = get_smiles_to_inchi_udf()
cmp_with_inchi = cmp_with_sid.withColumn("inchi", smiles_to_inchi_udf(F.col("smiles"))).cache()

# Substances table (include structural + metadata, excluding property information)
subjson = cmp_with_inchi.select(
    "sid", 
    F.to_json(F.struct("smiles", "inchi")).alias("data")
)
subjson.write.mode("overwrite").parquet("staging/sider/substances.parquet")

# Properties table
property_columns = cmpraw.columns[1:]

properties_list = []
for idx, col_name in enumerate(property_columns):
    data_json = json.dumps({
        'property': col_name,
        'active_value': 1,
        'inactive_value': 0
    })
    properties_list.append((str(idx), data_json))  # Ensure pid is a string

properties_df = spark.createDataFrame(properties_list, ["pid", "data"])
properties_df.write.mode("overwrite").parquet("staging/sider/properties.parquet")

# Activities table
activities = []
for idx, col_name in enumerate(property_columns, start=0):
    activity_table = cmp_with_inchi.withColumn("pid", F.lit(str(idx))) \
                                   .withColumn("activity", F.col(col_name)) \
                                   .withColumnRenamed("activity", "value") \
                                   .select("pid", "sid", "smiles", "inchi", "value")
    activities.append(activity_table)

final_activity_table = activities[0]
for activity_table in activities[1:]:
    final_activity_table = final_activity_table.union(activity_table)

# Ensure unique 'aid' by assigning row numbers
window_spec = Window.orderBy(F.monotonically_increasing_id())
final_activity_table = final_activity_table.withColumn("aid", row_number().over(window_spec).cast("string"))

# Reorder columns to have 'aid' and 'pid' in front
final_activity_table = final_activity_table.select("aid", "pid", "sid", "smiles", "inchi", "value")

final_activity_table.write.mode("overwrite").parquet("staging/sider/activities.parquet")
