import os, json, tqdm, pandas as pd, sys, pickle
import hashlib
sys.path.append(".")

from src.helper.cache_helper_titles import assign_titles
from pyspark.sql import SparkSession, types as T


# SET UP ========================================================
spark = SparkSession.builder.appName("llm_property_titles")\
    .config("spark.driver.memory", "64g")\
    .getOrCreate()

# only assign titles to properties with a category
propcats = spark.read.parquet("brick/property_categories.parquet")
propcat_pids = propcats.select("pid").distinct()
properties = spark.read.parquet("brick/properties.parquet").join(propcat_pids, "pid", "inner")

# check that pids are all unique
assert properties.select("pid").distinct().count() == properties.count(), "pids are not unique"

# GENERATE PROPERTY TITLES =========================================

# Create cache directory if it doesn't exist
cache_dir = os.path.join("joblib_cache", "assign_titles")
os.makedirs(cache_dir, exist_ok=True)
def cache_assign_titles(prop_json_data, titles=[]):
    cache_input = json.dumps(prop_json_data, sort_keys=True) + (','.join(sorted(titles)) if titles else '')
    cache_key = hashlib.md5(cache_input.encode('utf-8')).hexdigest()[:16]
    cache_file = os.path.join(cache_dir, f"{cache_key}.pkl")
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            title = pickle.load(f)
    else:
        prop_json = json.dumps(prop_json_data, indent=4, sort_keys=True)
        title = assign_titles(prop_json, titles=titles)
        pickle.dump(title, open(cache_file, 'wb'))
    return title
    
props = sorted(properties.rdd.collect(), key=lambda x: x["pid"])
results_arr = []
for prop in tqdm.tqdm(props):
    prop_data = prop["data"]
    prop_json_data = json.loads(prop_data)
    
    # remove any keys with more than 1000 characters
    prop_json_data = {k: v for k, v in prop_json_data.items() if len(str(v)) < 1000 and v is not None}
    title = cache_assign_titles(prop_json_data)
    results_arr.append({"pid": prop['pid'], "title": title, "prop_json_data": prop_json_data})

# look for duplicates and assert that pids are unique
results_df = pd.DataFrame(results_arr)
assert len(results_df['pid'].unique()) == len(results_df), "pids are not unique"
dup_results_df = results_df[results_df.duplicated(subset=["title"])][['pid','title','prop_json_data']]
newtitles_by_title = {t:[t] for t in results_df["title"].unique()}
all_titles = list(newtitles_by_title.keys())

for i, row in tqdm.tqdm(dup_results_df.iterrows(), desc="Generating unique titles"):
    prior_titles = newtitles_by_title[row["title"]]
    newtitle = cache_assign_titles(row["prop_json_data"], titles = prior_titles)
    print(newtitle)
    while newtitle in all_titles:
        prior_titles.append(newtitle)
        newtitle = cache_assign_titles(row["prop_json_data"], titles = prior_titles)
    all_titles.append(newtitle)
    newtitles_by_title[row["title"]].append(newtitle)
    results_df.loc[results_df['pid'] == row['pid'], 'title'] = newtitle

# assert that all titles are unique
assert len(results_df['title'].unique()) == len(results_df), "titles are not unique"
spark.createDataFrame(results_df).write.mode("overwrite").parquet("brick/property_titles.parquet")

# TESTING ========================================================
# check that all titles are unique
title_df = spark.read.parquet("brick/property_titles.parquet").toPandas()
title_counts = title_df.groupby("title").size().reset_index(name="count")
maxcount = title_counts["count"].max()
assert maxcount == 1, "Each title should be unique"

# Check that each property has at most one title
property_counts = title_df.groupby("pid").size().reset_index(name="count")
maxcount = property_counts["count"].max()
assert maxcount == 1, "Each property should have exactly one title"
