import os
import pandas as pd
import biobricks as bb
import uuid
import dotenv
import json
import re
import tqdm
import functools
import pathlib
from joblib import Memory

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, isnan
from pyspark.sql.window import Window

from openai import OpenAI


dotenv.load_dotenv()
openai = OpenAI(api_key= os.environ["OPENAI_API_KEY"])
spark = SparkSession.builder.appName("pubchem").getOrCreate()

properties = spark.read.parquet("brick/properties.parquet")
categories = ["acute oral toxicity", "acute inhalation toxicity", "reproductive toxicity", "skin irritation", "eye irritation", 
              "skin sensitization", "mutagenicity", "carcinogenicity", "sub-chronic toxicity", "chronic toxicity", "developmental toxicity", 
              "genotoxicity", "neurotoxicity", "immunotoxicity", "endocrine disruption", "environmental toxicity", "other"]

def process_gpt_response(text : str) -> [(str, str)]:
    
    matches = re.findall(r"CATEGORY=(.*?)\nREASON=(.*?)\nSTRENGTH=(.*?)(?:\n|$)", text, re.DOTALL)
    results = []
    for match in matches:
        category, reason, strength = match
        category = category.lower().strip()

        # Check if the category is in the predefined list of categories
        if category not in categories:
            return (False, f"In the above response, Category '{category}' is not in the predefined list of categories., try again.")

        results.append((category, reason.strip(), strength.strip()))

    if len(results) == 0:
        return (False, "I could not parse any CATEGORY, REASON, STRENGTH results from your response, try again.")
    
    return (True, results)

cache_dir = "./cache"
memory = Memory(cache_dir, verbose=0)

def assign_categories(prop_json, inmessages = [], attempts = 0):
    
    if attempts > 3:
        return [("unknown", "too many attempts")]
    
    catstring = '\n'.join(categories)
    prompt = f"you are an expert toxicologist.\n\n{prop_json}\n\nthe above is a json description of an assay that measures chemicals."
    prompt += f"Assign a category from the below list, it is ok and encouraged to make logical links between assays in categories, "
    prompt += f"for example an ER binding assay could be categorized as a reproductive toxicity assay. Just explain your logic in the REASON:\n{catstring}\n\n"
    prompt += "your response should read\n\nCATEGORY=[selected category]\nREASON=[your reason for selecting this category]\nSTRENGTH=[1-10 with 1 being a weak categorization and 10 being a direct and obvious relationship between the assay and the category]\n\n"
    prompt += "DO NOT OUTPUT ANYTHING OTHER THAN THE ABOVE TWO LINES. You may output multiple responses separated by a newline if desired." 
    
    messages = inmessages + [{"role": "user", "content": prompt,}]
    response = openai.chat.completions.create(messages=messages, model="gpt-4")
    response_text = response.choices[0].message.content
    
    cat_reasons = process_gpt_response(response_text)
    
    if not cat_reasons[0]:
        messages = messages + [{"role": "user", "content": cat_reasons[1]}]
        return assign_categories(prop_data, messages, attempts + 1)
    
    return cat_reasons[1]

# import random
allprops = properties.rdd.collect()
prev_pids = spark.read.parquet("brick/property_categories.parquet").select("pid").rdd.map(lambda x: x['pid']).collect()
props = [prop for prop in allprops if prop['pid'] not in prev_pids]

results_df = []
results_pid = [x['pid'] for x in results_df]
for prop in tqdm.tqdm(props):
    if prop['pid'] in results_pid: continue
    prop_data = prop["data"]
    prop_json_data = json.loads(prop_data)
    # remove any keys with more than 1000 characters
    prop_json_data = {k: v for k, v in prop_json_data.items() if len(str(v)) < 1000 and v is not None}
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

pathlib.Path("brick/property_categories.parquet").mkdir(parents=True, exist_ok=True)
max_i = len(os.listdir("brick/property_categories.parquet"))    
df_chunks = [df[i:i+int(1e6)] for i in range(0, len(df), int(1e6))]
for i,chunk in enumerate(df_chunks):
    path = f"brick/property_categories.parquet/{i + max_i}.parquet"
    chunk.to_parquet(path)
    
# test that all pids are in results_df
pids = spark.read.parquet("brick/property_categories.parquet").select("pid").rdd.map(lambda x: x['pid']).collect()
assert(len(set(pids).difference(set([x['pid'] for x in allprops]))) == 0)