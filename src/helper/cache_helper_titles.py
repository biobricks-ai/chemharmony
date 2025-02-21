import pathlib, joblib, re, dotenv, os
from openai import OpenAI

cachedir = pathlib.Path("./joblib_cache/assign_titles")
cachedir.mkdir(exist_ok=True)
memory = joblib.Memory(cachedir, verbose=0)

dotenv.load_dotenv()
openai = OpenAI(api_key= os.environ["OPENAI_API_KEY"])

def process_gpt_response(text : str, titles) -> list[(str, str)]:
    title = re.findall(r"title=(.*)", text.lower())
    if len(title) == 0: 
        return (False, "I could not parse any title, try again.")
    if title in titles:
        return (False, "This title has already been used. choose a more unique and perhaps descriptive title.")
    return (True, title[0])

@memory.cache
def assign_titles(prop_json, titles, inmessages = [], attempts = 0, model="gpt-3.5-turbo"):
    
    if attempts > 3:
        return [("unknown", "too many attempts")]
    
    prompt = f"you are an expert toxicologist.\n\n{prop_json}\n\nthe above is a json description of an assay that measures chemicals."
    prompt += f"Create a title for this json, make sure it is distinct from the existing titles. It should be a title a toxicologist would understand."
    prompt += "your response should read\n\nTITLE=[a short descriptive title]\n\n"
    prompt += "DO NOT OUTPUT ANYTHING OTHER THAN THE TITLE LINE." 
    
    messages = inmessages + [{"role": "user", "content": prompt,}]
    response = openai.chat.completions.create(messages=messages, model=model)
    response_text = response.choices[0].message.content
    
    title = process_gpt_response(response_text, titles)
    
    if not title[0]:
        print('title failure: ', title[1])
        messages = messages + [{"role": "user", "content": title[1]}]
        return assign_titles(prop_json, titles, messages, attempts + 1, model)
    
    return title[1]