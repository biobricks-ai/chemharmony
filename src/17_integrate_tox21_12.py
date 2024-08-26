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
    .appName("Tox21") \
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

os.makedirs("staging/Tox21", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# Build compounds
tox21_raw = spark.read.parquet(data.tox21_parquet)
tox21_filtered = tox21_raw.filter(F.col('smiles').isNotNull())
tox21_with_sid = tox21_filtered.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# Convert SMILES to InChI
smiles_to_inchi_udf = get_smiles_to_inchi_udf()
tox21_with_inchi = tox21_with_sid.withColumn("inchi", smiles_to_inchi_udf(F.col("smiles"))).cache()

# Filter out rows where InChI could not be generated (NULL or empty)
tox21_with_inchi = tox21_with_inchi.filter(F.col("inchi").isNotNull() & (F.col("inchi") != ""))

# Substances table (include structural + metadata, excluding property information)
subjson = tox21_with_inchi.select(
    "sid", 
    F.to_json(F.struct("mol_id", "smiles", "inchi")).alias("data")
)
subjson.write.mode("overwrite").parquet("staging/Tox21/substances.parquet")

# Properties table
column_descriptions = {
    "NR-AR": "Androgen Receptor assay tests for agonists of the androgen receptor.",
    "NR-AR-LBD": "Androgen Receptor Ligand Binding Domain assay assesses the ability of chemicals to bind to the androgen receptor.",
    "NR-AhR": "Aryl Hydrocarbon Receptor assay tests for activation of the aryl hydrocarbon receptor.",
    "NR-Aromatase": "Aromatase assay tests for inhibition of aromatase, an enzyme responsible for a key step in estrogen biosynthesis.",
    "NR-ER": "Estrogen Receptor assay tests for agonists of the estrogen receptor.",
    "NR-ER-LBD": "Estrogen Receptor Ligand Binding Domain assay assesses the binding ability of chemicals to the estrogen receptor.",
    "NR-PPAR-gamma": "Peroxisome Proliferator-Activated Receptor Gamma assay targets PPAR-gamma receptor, regulating fatty acid storage and glucose metabolism.",
    "SR-ARE": "Antioxidant Response Element assay identifies chemicals that activate ARE, involved in response to oxidative stress.",
    "SR-ATAD5": "ATAD5 assay assesses DNA damage by identifying chemicals that increase ATAD5 expression, involved in DNA repair.",
    "SR-HSE": "Heat Shock Element assay identifies chemicals that activate HSE, involved in protecting cells from stress.",
    "SR-MMP": "Matrix Metalloproteinase assay tests for inhibition of matrix metalloproteinases, involved in extracellular matrix breakdown.",
    "SR-p53": "p53 assay identifies chemicals that activate p53, a protein crucial for controlling cell division and apoptosis."
}

properties_list = []
for idx, (col_name, description) in enumerate(column_descriptions.items()):
    data_json = json.dumps({
        "property": col_name,
        "active_value": 1,
        "inactive_value": 0
    })
    properties_list.append((str(idx), data_json))  # Ensure pid is a string

properties_df = spark.createDataFrame(properties_list, ["pid", "data"])
properties_df.write.mode("overwrite").parquet("staging/Tox21/properties.parquet")

# Activities table
activities = []
for idx, col_name in enumerate(column_descriptions.keys()):
    activity_table = tox21_with_inchi.withColumn("pid", F.lit(str(idx))) \
                                     .withColumn("value", F.col(col_name)) \
                                     .withColumn("aid", F.monotonically_increasing_id().cast("string")) \
                                     .withColumn("source", F.lit("Tox21")) \
                                     .select("aid", "sid", "pid", "smiles", "inchi", "source", "value")
    activities.append(activity_table)

final_activity_table = activities[0]
for activity_table in activities[1:]:
    final_activity_table = final_activity_table.union(activity_table)

# Ensure unique 'aid' by assigning row numbers
window_spec = Window.orderBy(F.monotonically_increasing_id())
final_activity_table = final_activity_table.withColumn("aid", row_number().over(window_spec).cast("string"))

final_activity_table.write.mode("overwrite").parquet("staging/Tox21/activities.parquet")
