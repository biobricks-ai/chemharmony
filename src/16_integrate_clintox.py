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
    .appName("CLINTOX") \
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

os.makedirs("staging/CLINTOX", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# Build compounds
clintox_raw = spark.read.parquet(data.clintox_parquet)
clintox_filtered = clintox_raw.filter(F.col('smiles').isNotNull())
clintox_with_sid = clintox_filtered.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# Convert SMILES to InChI
smiles_to_inchi_udf = get_smiles_to_inchi_udf()
clintox_with_inchi = clintox_with_sid.withColumn("inchi", smiles_to_inchi_udf(F.col("smiles"))).cache()

# Filter out rows where InChI could not be generated (NULL or empty)
clintox_with_inchi = clintox_with_inchi.filter(F.col("inchi").isNotNull() & (F.col("inchi") != ""))

# Substances table (include structural + metadata, excluding property information)
subjson = clintox_with_inchi.select(
    "sid", 
    F.to_json(F.struct("smiles", "inchi")).alias("data")
)
subjson.write.mode("overwrite").parquet("staging/CLINTOX/substances.parquet")

# Properties table
properties_list = []

# For FDA_APPROVED column
data_json_fda = json.dumps({
    'property': 'FDA_APPROVED',
    'description': 'FDA approval status of the compound',
    'active_value': 1,
    'inactive_value': 0,
    'active_label': 'Approved',
    'inactive_label': 'Not Approved'
})
properties_list.append(("0", data_json_fda))  # Ensure pid is a string

# For CT_TOX column
data_json_ct_tox = json.dumps({
    'property': 'CT_TOX',
    'description': 'Clinical trial toxicity: indicates whether the compound was found to be toxic in clinical trials',
    'active_value': 1,
    'inactive_value': 0,
    'active_label': 'Toxic',
    'inactive_label': 'Non-toxic'
})
properties_list.append(("1", data_json_ct_tox))  # Ensure pid is a string

properties_df = spark.createDataFrame(properties_list, ["pid", "data"])
properties_df.write.mode("overwrite").parquet("staging/CLINTOX/properties.parquet")

# Activities table--
activities = []
property_columns = ["FDA_APPROVED", "CT_TOX"]

for idx, col_name in enumerate(property_columns):
    activity_table = clintox_with_inchi.withColumn("pid", F.lit(str(idx))) \
                                       .withColumn("value", F.col(col_name)) \
                                       .withColumn("source", F.lit("CLINTOX")) \
                                       .select("sid", "pid", "smiles", "inchi", "source", "value")
    activities.append(activity_table)

final_activity_table = activities[0].unionAll(activities[1])

# Generate unique 'aid' for the entire table
final_activity_table = final_activity_table.withColumn("aid", F.monotonically_increasing_id().cast("string"))

final_activity_table = final_activity_table.select("aid", "sid", "pid", "smiles", "inchi", "source", "value")

final_activity_table.write.mode("overwrite").parquet("staging/CLINTOX/activities.parquet")
