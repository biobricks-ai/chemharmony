import os, json, tqdm, pandas as pd

from pyspark.sql import SparkSession, types as T
from src.helper.cache_helper_titles import assign_titles

# SET UP ========================================================
spark = SparkSession.builder.appName("llm_property_titles")\
    .config("spark.driver.memory", "64g")\
    .getOrCreate()

propcats = spark.read.parquet("brick/property_categories.parquet")
prev_pids = spark.createDataFrame([], T.StructType([T.StructField("pid", T.StringType())]))
if os.path.exists("brick/property_titles.parquet"):
    prev_pids = spark.read.parquet("brick/property_titles.parquet").select("pid")

properties = spark.read.parquet("brick/properties.parquet").join(propcats, "pid", "inner")
properties = properties.join(prev_pids, "pid", "left_anti")

# GENERATE PROPERTY TITLES =========================================

props = properties.rdd.collect()
results_df, titles = [], []
for prop in tqdm.tqdm(props):
    prop_data = prop["data"]
    prop_json_data = json.loads(prop_data)
    # remove any keys with more than 1000 characters
    prop_json_data = {k: v for k, v in prop_json_data.items() if len(str(v)) < 1000 and v is not None}
    prop_json = json.dumps(prop_json_data, indent=4, sort_keys=True)
    prop_id = prop["pid"]
    title = assign_titles(prop_json, titles)
    titles.append(title)
    
    print(prop_json)
    print(title)
    results_df.append({"pid": prop_id, "title": title})

if len(results_df) > 0:
    df = pd.DataFrame(results_df)
    sdf = spark.createDataFrame(df)
    sdf.write.mode("append").parquet("brick/property_titles.parquet")