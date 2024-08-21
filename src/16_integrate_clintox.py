import os
import json
import biobricks
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, udf
from rdkit import Chem
from rdkit.Chem import inchi

# SETUP =================================================================
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

stg = os.makedirs("staging/CLINTOX", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# BUILD COMPOUNDS =========================================================
clintox_raw = spark.read.parquet(data.clintox_parquet)

# Filter out rows with null SMILES values
clintox_filtered = clintox_raw.filter(F.col('smiles').isNotNull())
clintox_with_sid = clintox_filtered.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# UDF to convert SMILES to InChI
def smiles_to_inchi(smiles):
    mol = Chem.MolFromSmiles(smiles)
    return inchi.MolToInchi(mol) if mol else None

smiles_to_inchi_udf = udf(smiles_to_inchi, StringType())

# Add InChI column to the DataFrame
clintox_with_inchi = clintox_with_sid.withColumn("inchi", smiles_to_inchi_udf(F.col("smiles")))

# Filter out rows where InChI could not be generated (NULL or empty)
clintox_with_inchi = clintox_with_inchi.filter(F.col("inchi").isNotNull() & (F.col("inchi") != ""))

# SUBSTANCES TABLE =======================================================
subjson = clintox_with_inchi.select("sid", F.to_json(F.struct("smiles")).alias("data"))
subjson.write.mode("overwrite").parquet("staging/CLINTOX/substances.parquet")

# PROPERTIES TABLE =======================================================
# Create property rows for each column (FDA_APPROVED, CT_TOX)
properties_list = []

# For FDA_APPROVED column
data_json_fda = json.dumps({
    "FDA_APPROVED = 1": "positive toxic",
    "FDA_APPROVED = 0": "negative toxic"
})
properties_list.append((0, data_json_fda))

# For CT_TOX column
data_json_ct_tox = json.dumps({
    "CT_TOX = 1": "toxic",
    "CT_TOX = 0": "non-toxic"
})
properties_list.append((1, data_json_ct_tox))

# Create DataFrame from the list of properties
properties_df = spark.createDataFrame(properties_list, ["pid", "data"])

# Write the properties table to a Parquet file
properties_df.write.mode("overwrite").parquet("staging/CLINTOX/properties.parquet")

# ACTIVITIES TABLE =======================================================
activities = []

property_columns = ["FDA_APPROVED", "CT_TOX"]

for idx, col_name in enumerate(property_columns):
    # Create aid, pid, and select columns for the activity table
    activity_table = clintox_with_inchi.withColumn("pid", F.lit(idx)) \
                                       .withColumn("activity", F.col(col_name)) \
                                       .withColumn("aid", F.monotonically_increasing_id().cast("string")) \
                                       .withColumn("source", F.lit("CLINTOX")) \
                                       .select("aid", "sid", "pid", "smiles", "inchi", "source", "activity")
    activities.append(activity_table)

# Union all activities into a single DataFrame
final_activity_table = activities[0]
for activity_table in activities[1:]:
    final_activity_table = final_activity_table.union(activity_table)

# Ensure unique 'aid' by assigning row numbers
window_spec = Window.orderBy(F.monotonically_increasing_id())
final_activity_table = final_activity_table.withColumn("aid", row_number().over(window_spec).cast("string"))

# Write the activities table to a Parquet file
final_activity_table.write.mode("overwrite").parquet("staging/CLINTOX/activities.parquet")
