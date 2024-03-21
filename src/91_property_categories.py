import os, dotenv, json,re, tqdm, pathlib, joblib, random
import pandas as pd, biobricks as bb

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

from openai import OpenAI

# ADD CACHE =====================================================
cachedir = pathlib.Path("./joblib_cache")
cachedir.mkdir(exist_ok=True)
memory = joblib.Memory(cachedir, verbose=0)

# SET UP ========================================================
dotenv.load_dotenv()
openai = OpenAI(api_key= os.environ["OPENAI_API_KEY"])
spark = SparkSession.builder.appName("pubchem")\
    .config("spark.driver.memory", "64g")\
    .getOrCreate()

## build properties for category assignment
big_pids = spark.read.parquet("brick/activities.parquet")\
    .groupBy("pid", "binary_value").count() \
    .groupBy("pid").agg(F.min("count").alias("min_count")) \
    .filter("min_count >= 100")\
    .select('pid')

prev_pids = spark.createDataFrame([], StructType([StructField("pid", StringType())]))
if os.path.exists("brick/property_categories.parquet"):
    prev_pids = spark.read.parquet("brick/property_categories.parquet").select("pid")
    
properties = spark.read.parquet("brick/properties.parquet").join(big_pids, "pid")
properties = properties.join(prev_pids, "pid", "left_anti")

# GENERATE PROPERTY CATEGORIES ================================
categories = pathlib.Path("src/resources/property_categories.txt").read_text().splitlines()
template = pathlib.Path("src/resources/property_categories_prompt.txt").read_text()

def process_gpt_response(text : str) -> list[(str, str)]:
    
    matches = re.findall(r"CATEGORY=(.*?)\nREASON=(.*?)\nSTRENGTH=(.*?)(?:\n|$)", text, re.DOTALL)
    matches = [(c.lower().strip(),r.strip(),s.strip()) for c, r, s in matches]
    if not matches: return (False, "I could not parse any CATEGORY, REASON, STRENGTH results from your response, try again.")
    
    badmsg = lambda x: f"In the above response, category {x} is not in the predefined list of categories, try again."
    badmatch = [(cat, badmsg(cat.lower().strip())) for cat, _, _ in matches if cat not in categories]
    if badmatch: return (False, badmatch[0][1])
    
    return (True, matches)

@memory.cache
def assign_categories(prop_json, inmessages = [], attempts = 0):
    
    if attempts > 3: return [("unknown", "too many attempts")]
    
    catstring = '\n'.join(categories)    
    prompt = template.format(prop_json=prop_json, catstring=catstring)
    
    messages = inmessages + [{"role": "user", "content": prompt,}]
    response = openai.chat.completions.create(messages=messages, model="gpt-4")
    response_text = response.choices[0].message.content
    
    cat_reasons = process_gpt_response(response_text)
    
    if not cat_reasons[0]:
        messages = messages + [{"role": "user", "content": cat_reasons[1]}]
        return assign_categories(prop_json, messages, attempts + 1)
    
    return cat_reasons[1]

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

df = pd.DataFrame(results_df)
df['strength'] = df['strength'].astype(float)

sdf = spark.createDataFrame(df)
sdf.write.mode("append").parquet("brick/property_categories.parquet")