import pathlib, re, joblib, dotenv, os
from openai import OpenAI

cachedir = pathlib.Path("./joblib_cache/assign_categories")
cachedir.mkdir(exist_ok=True)
memory = joblib.Memory(cachedir, verbose=0)

dotenv.load_dotenv()
openai = OpenAI(api_key= os.environ["OPENAI_API_KEY"])

def process_gpt_response(text : str) -> list[(str, str)]:
    
    matches = re.findall(r"CATEGORY=(.*?)\nREASON=(.*?)\nSTRENGTH=(.*?)(?:\n|$)", text, re.DOTALL)
    matches = [(c.lower().strip(),r.strip(),s.strip()) for c, r, s in matches]
    if not matches: return (False, "I could not parse any CATEGORY, REASON, STRENGTH results from your response, try again.")
    
    badmsg = lambda x: f"In the above response, category {x} is not in the predefined list of categories, try again."
    badmatch = [(cat, badmsg(cat.lower().strip())) for cat, _, _ in matches if cat not in categories]
    if badmatch: return (False, badmatch[0][1])
    
    return (True, matches)

template = pathlib.Path("src/resources/property_categories_prompt.txt").read_text()
categories = pathlib.Path("src/resources/property_categories.txt").read_text().splitlines()

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