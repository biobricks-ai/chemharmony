from magentic import chatprompt, prompt, AssistantMessage, UserMessage, SystemMessage
import dotenv, os, openai, pandas as pd
from Levenshtein import distance as levenshtein_distance
from pyarrow import parquet as pq

dotenv.load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')

labels = []

def find_similar_labels(proposed_label: str) -> [str]:
    return [label for label in labels if levenshtein_distance(label, proposed_label) < 3]

@prompt("""You are a programmer creating human readable descriptions for property descriptions.
        Invent a human reable description for the below property description. 
        It should be something people can read and understand what the property is measuring. 
        This description should differentiate the project description from other, perhaps very similar,
        property descriptions. This description will later be used in a text index.\n\n{description}""")
def invent_property_description(description: str) -> str:
    ...

base_prompt = """You are a programmer creating label names for property descriptions.
        Invent a label for the below property description. 
        It should be something people can read and understand what the property is measuring, 
        but it should also be very concise, less than 200 characters. It should 
        differentiate this property from other, perhaps very similar, properties.
        It should directly contain any important descriptors, like gene names, or species name.\n\n{description}"""

@prompt(base_prompt)
def invent_label_prompt(description: str) -> str:
    ...  # No function body as this is never executed

@chatprompt(
    SystemMessage("You are a programmer creating label names for property descriptions."),
    UserMessage(base_prompt),
    AssistantMessage("{proposed_label}"),
    UserMessage("This label is too similar to these existing labels: {similar_labels}. Please differentiate."),
)
def invent_differentiated_label(description: str, proposed_label: str, similar_labels: [str]) -> str:
    ...

def generate_label(description: str) -> str:
    proposed_label = invent_label_prompt(description).lower()
    similar_labels = find_similar_labels(proposed_label)
    while similar_labels:
        proposed_label = invent_differentiated_label(description, proposed_label, similar_labels).lower()
        similar_labels = find_similar_labels(proposed_label)
    return proposed_label


dataset = pq.ParquetDataset('./brick/properties.parquet/')
df = dataset.read().to_pandas()

property_descriptions = df['data'].tolist()
property_source = df['source'].tolist()

for description in property_descriptions:
    new_label = generate_label(description)
    new_description = invent_property_description(description)
    print(new_label)
    print(new_description)
    print("============")
    labels.append(new_label)
