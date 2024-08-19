import os
import json
import biobricks
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from rdkit import Chem
from rdkit import RDLogger

# Suppress RDKit warnings
RDLogger.DisableLog('rdApp.*')

# SETUP =================================================================
spark = SparkSession.builder \
    .appName("BACE") \
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

stg = os.makedirs("staging/bace", exist_ok=True)
data = biobricks.assets("MoleculeNet")

# BUILD COMPOUNDS =========================================================
cmpraw = spark.read.parquet(data.bace_parquet)

# Select relevant columns and rename `mol` to `smiles`
cmp1 = cmpraw.select(F.col("mol").alias("smiles"), "CID", "Class") \
             .filter(F.col('smiles').isNotNull())
cmp2 = cmp1.withColumn('sid', F.monotonically_increasing_id().cast('string'))

# UDF to convert SMILES to InChI
def smiles_to_inchi(smiles):
    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol:
            return Chem.MolToInchi(mol)
        else:
            return None
    except Exception as e:
        print(f"Error processing SMILES: {smiles} - {e}")
        return None

smiles_to_inchi_udf = udf(smiles_to_inchi, StringType())

# Add InChI column
cmp_with_inchi = cmp2.withColumn('inchi', smiles_to_inchi_udf(F.col('smiles')))

# Create the substances table in JSON format
subjson = cmp_with_inchi.select("sid", F.to_json(F.struct("smiles", "CID", "Class", "inchi")).alias("data"))
subjson.write.mode("overwrite").parquet("staging/BACE/substances.parquet")

# WRITE PROPERTIES =====================================================
data_json = json.dumps({
    'Class = 1': 'inhibitor',
    'Class = 0': 'non-inhibitor'
})

properties = spark.createDataFrame([
    (0, data_json)
], ["pid", "data"])

properties.write.mode("overwrite").parquet("staging/BACE/properties.parquet")

# BUILD ACTIVITIES ======================================================
cmp_with_ids = cmp_with_inchi.withColumn("aid", F.col("sid")) \
                             .withColumn("pid", F.lit(0)) \
                             .withColumn("source", F.lit("BACE"))

activity_table = cmp_with_ids.select("aid", "pid", "sid", "smiles", "inchi", "source", "Class")

activity_table.write.mode("overwrite").parquet("staging/BACE/activities.parquet")
