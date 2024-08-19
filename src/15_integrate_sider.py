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

stg = os.makedirs("staging/sider", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# BUILD COMPOUNDS =========================================================
cmpraw = spark.read.parquet(data.sider_parquet)

# Filter out rows with null SMILES values
cmp1 = cmpraw.filter(F.col('smiles').isNotNull())
cmp2 = cmp1.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# UDF to convert SMILES to InChI
def smiles_to_inchi(smiles):
    mol = Chem.MolFromSmiles(smiles)
    return inchi.MolToInchi(mol) if mol else None

smiles_to_inchi_udf = udf(smiles_to_inchi, StringType())

# Add InChI column to the DataFrame
cmp_with_inchi = cmp2.withColumn("inchi", smiles_to_inchi_udf(F.col("smiles")))

# SUBSTANCES TABLE =======================================================
subjson = cmp_with_inchi.select("sid", F.to_json(F.struct("smiles")).alias("data"))
subjson.write.mode("overwrite").parquet("staging/sider/substances.parquet")

# PROPERTIES TABLE =======================================================
# Extract column names for side effects (exclude 'smiles')
property_columns = cmpraw.columns[1:]

properties_list = []
for idx, col_name in enumerate(property_columns):
    data_json = json.dumps({col_name: 'side effect present'})
    properties_list.append((idx, data_json))

# Create DataFrame from the list of properties
properties_df = spark.createDataFrame(properties_list, ["pid", "data"])

# Write the properties table to a Parquet file
properties_df.write.mode("overwrite").parquet("staging/sider/properties.parquet")

# ACTIVITIES TABLE =======================================================
activities = []

for idx, col_name in enumerate(property_columns, start=0):
    activity_table = cmp_with_inchi.withColumn("pid", F.lit(idx)) \
                                   .withColumn("activity", F.col(col_name)) \
                                   .select("pid", "sid", "smiles", "inchi", "activity")
    activities.append(activity_table)

# Union all activities into a single DataFrame
final_activity_table = activities[0]
for activity_table in activities[1:]:
    final_activity_table = final_activity_table.union(activity_table)

# Ensure unique 'aid' by assigning row numbers
window_spec = Window.orderBy(F.monotonically_increasing_id())
final_activity_table = final_activity_table.withColumn("aid", row_number().over(window_spec).cast("string"))

# Reorder columns to have 'aid' and 'pid' in front
final_activity_table = final_activity_table.select("aid", "pid", "sid", "smiles", "inchi", "activity")

# Write the activities table to a Parquet file
final_activity_table.write.mode("overwrite").parquet("staging/sider/activities.parquet")
