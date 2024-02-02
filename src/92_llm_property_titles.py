import os, dotenv, json,re, tqdm, pathlib, joblib
import pandas as pd, biobricks as bb

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from openai import OpenAI


dotenv.load_dotenv()
openai = OpenAI(api_key= os.environ["OPENAI_API_KEY"])
spark = SparkSession.builder.appName("pubchem").getOrCreate()

properties = spark.read.parquet("brick/properties.parquet")

def process_gpt_response(text : str, titles) -> [(str, str)]:
    
    title = re.findall(r"title=(.*)", text.lower())
    
    if len(title) == 0:
        return (False, "I could not parse any title, try again.")
    
    if title in titles:
        return (False, "This title has already been used. choose a more unique and perhaps descriptive title.")
    
    return (True, title[0])

def assign_titles(prop_json, titles, inmessages = [], attempts = 0):
    
    if attempts > 3:
        return [("unknown", "too many attempts")]
    
    prompt = f"you are an expert toxicologist.\n\n{prop_json}\n\nthe above is a json description of an assay that measures chemicals."
    prompt += f"Create a title for this json, make sure it is distinct from the existing titles. It should be a title a toxicologist would understand."
    prompt += "your response should read\n\nTITLE=[a short descriptive title]\n\n"
    prompt += "DO NOT OUTPUT ANYTHING OTHER THAN THE TITLE LINE." 
    
    messages = inmessages + [{"role": "user", "content": prompt,}]
    response = openai.chat.completions.create(messages=messages, model="gpt-3.5-turbo")
    response_text = response.choices[0].message.content
    
    title = process_gpt_response(response_text, titles)
    
    if not title[0]:
        print('title failure: ', title[1])
        messages = messages + [{"role": "user", "content": title[1]}]
        return assign_titles(prop_data, titles, messages, attempts + 1)
    
    return title[1]

# import random
allprops = properties.rdd.collect()
output_path = "brick/property_titles.parquet"
prev_pids = []
if os.path.exists(output_path):
    prev_pids = spark.read.parquet(output_path).select("pid").rdd.map(lambda x: x['pid']).collect()
props = [prop for prop in allprops if prop['pid'] not in prev_pids]

results_df = []
titles = []
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

df = pd.DataFrame(results_df)

pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)
max_i = len(os.listdir(output_path))
df_chunks = [df[i:i+int(1e6)] for i in range(0, len(df), int(1e6))]
for i,chunk in enumerate(df_chunks):
    path = f"{output_path}/{i + max_i}.parquet"
    chunk.to_parquet(path)
    
# test that all pids are in results_df
pids = spark.read.parquet(output_path).select("pid").rdd.map(lambda x: x['pid']).collect()
assert(len(set(pids).difference(set([x['pid'] for x in allprops]))) == 0)