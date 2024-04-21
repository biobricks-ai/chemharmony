import os, json, tqdm, pandas as pd

from pyspark.sql import SparkSession, functions as F, types as T
from src.helper.cache_helper import assign_categories

# SET UP ========================================================
spark = SparkSession.builder.appName("prpcat").config("spark.driver.memory", "64g").getOrCreate()

## build properties for category assignment
big_pids = spark.read.parquet("brick/activities.parquet")\
    .groupBy("pid", "binary_value").count() \
    .groupBy("pid").agg(F.min("count").alias("min_count")) \
    .filter("min_count >= 100")\
    .select('pid')

prev_pids = spark.createDataFrame([], T.StructType([T.StructField("pid", T.StringType())]))
if os.path.exists("brick/property_categories.parquet"):
    prev_pids = spark.read.parquet("brick/property_categories.parquet").select("pid")
    
properties = spark.read.parquet("brick/properties.parquet").join(big_pids, "pid")
properties = properties.join(prev_pids, "pid", "left_anti")
    
# GENERATE PROPERTY CATEGORIES ================================
props = [row.asDict() for row in properties.collect()]
results_df = []
for prop in tqdm.tqdm(props):
    prop_json_data = json.loads(prop["data"])
    prop_json = json.dumps(prop_json_data, indent=4, sort_keys=True)
    prop_id = prop["pid"]
    cat_reasons = assign_categories(prop_json)
    print(prop_json)
    for cat, reason, strength in cat_reasons:
        print(f"{cat}\t{strength}")
        print(reason)
        print('====================')
        results_df.append({"pid": prop_id, "category": cat, "reason": reason, "strength": strength})

if len(results_df) > 0:    
    df = pd.DataFrame(results_df)
    df['strength'] = df['strength'].astype(float)

    sdf = spark.createDataFrame(df)
    sdf.write.mode("append").parquet("brick/property_categories.parquet")