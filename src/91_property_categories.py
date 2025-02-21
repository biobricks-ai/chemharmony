import os, json, pandas as pd, sys, joblib
import tqdm
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
sys.path.append("./")

from pyspark.sql import SparkSession, functions as F, types as T
from src.helper.cache_helper import assign_categories

# SET UP ========================================================
spark = SparkSession.builder.appName("prpcat").config("spark.driver.memory", "64g").getOrCreate()
cache_dir = pathlib.Path("cache") / 'property_categories'
cache_dir.mkdir(parents=True, exist_ok=True)

## build properties for category assignment
properties = spark.read.parquet("brick/properties.parquet")
big_pids = spark.read.parquet("brick/activities.parquet")\
    .groupBy("pid", "binary_value").count() \
    .groupBy("pid").agg(F.min("count").alias("min_count")) \
    .filter("min_count >= 100")\
    .select('pid')

active_props = big_pids.join(properties, on="pid", how="inner")
props = [row.asDict() for row in active_props.collect()]
props.sort(key=lambda x: x['pid'])

def process_property(prop):
    prop_json_data = json.loads(prop["data"])
    prop_json = json.dumps(prop_json_data, indent=4, sort_keys=True)
    prop_id = prop["pid"]
    cat_reasons = assign_categories(prop_json)
    results_df = []
    for cat, reason, strength in cat_reasons:
        results_df.append({"pid": prop_id, "category": cat, "reason": reason, "strength": strength})
    return results_df

def safe_process_property(prop):
    try:
        return process_property(prop)
    except Exception as e:
        with open((cache_dir / 'log.txt').as_posix(), "a") as f:
            f.write(f"Error processing property {prop['pid']}: {e}\n")
        return []

results_df = []
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(safe_process_property, p) for p in props]
    
    for future in tqdm.tqdm(as_completed(futures), total=len(props), position=0, ncols=90):
        results = future.result()
        results_df.extend(results)

df = pd.DataFrame(results_df)
df['strength'] = df['strength'].astype(float)

sdf = spark.createDataFrame(df)
sdf.write.mode("append").parquet("brick/property_categories.parquet")