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

stg = os.makedirs("staging/Tox21", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# BUILD COMPOUNDS =========================================================
tox21_raw = spark.read.parquet(data.tox21_parquet)

# Filter out rows with null SMILES values
tox21_filtered = tox21_raw.filter(F.col('smiles').isNotNull())
tox21_with_sid = tox21_filtered.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# UDF to convert SMILES to InChI
def smiles_to_inchi(smiles):
    mol = Chem.MolFromSmiles(smiles)
    return inchi.MolToInchi(mol) if mol else None

smiles_to_inchi_udf = udf(smiles_to_inchi, StringType())

# Add InChI column to the DataFrame
tox21_with_inchi = tox21_with_sid.withColumn("inchi", smiles_to_inchi_udf(F.col("smiles")))

# Filter out rows where InChI could not be generated (NULL or empty)
tox21_with_inchi = tox21_with_inchi.filter(F.col("inchi").isNotNull() & (F.col("inchi") != ""))

# SUBSTANCES TABLE =======================================================
subjson = tox21_with_inchi.select(
    "sid", 
    F.to_json(F.struct("mol_id", "smiles")).alias("data")
)
subjson.write.mode("overwrite").parquet("staging/Tox21/substances.parquet")

# PROPERTIES TABLE =======================================================
# Create a list with descriptions and labels for each property
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
        "description": description,
        f"{col_name} = 1": "toxic",
        f"{col_name} = 0": "non-toxic"
    })
    properties_list.append((idx, data_json))

# Create DataFrame from the list of properties
properties_df = spark.createDataFrame(properties_list, ["pid", "data"])

# Write the properties table to a Parquet file
properties_df.write.mode("overwrite").parquet("staging/Tox21/properties.parquet")

# ACTIVITIES TABLE =======================================================
activities = []

for idx, col_name in enumerate(column_descriptions.keys()):
    # Create aid, pid, and select columns for the activity table
    activity_table = tox21_with_inchi.withColumn("pid", F.lit(idx)) \
                                     .withColumn("activity", F.col(col_name)) \
                                     .withColumn("aid", F.monotonically_increasing_id().cast("string")) \
                                     .withColumn("source", F.lit("Tox21")) \
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
final_activity_table.write.mode("overwrite").parquet("staging/Tox21/activities.parquet")
