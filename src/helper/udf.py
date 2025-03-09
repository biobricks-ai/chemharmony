import pyspark
import pyspark.sql.functions as F
import json
from rdkit import Chem, RDLogger, logging
from pyspark.sql.types import StringType 

def get_inch2smi_udf():
    def inchi2smi(inch):
        try:
            RDLogger.DisableLog('rdApp.*')
            mol = Chem.MolFromInchi(inch, sanitize=True, removeHs=True)
            return Chem.MolToSmiles(mol)
        except:
            return None

    return F.udf(inchi2smi)

def get_canonicalize_json_udf(max_str_len=None, float_precision=4):
    def canonicalize_json(json_string):
        def process_value(item):
            if isinstance(item, dict):
                return {key: process_value(value) for key, value in item.items() if value is not None and value != '' and value != []}
            elif isinstance(item, list):
                return [process_value(element) for element in item if element is not None and element != '' and element != []]
            elif isinstance(item, float):
                return round(item, float_precision)
            elif isinstance(item, str) and max_str_len is not None and len(item) > max_str_len:
                return item[:max_str_len] + '...'  # Truncate long strings
            else:
                return item

        parsed_json = json.loads(json_string)
        processed_json = process_value(parsed_json)
        canonical_json = json.dumps(processed_json, sort_keys=True)
        return canonical_json

    return F.udf(canonicalize_json)

def get_smiles_to_inchi_udf():
    def smiles_to_inchi(smiles):
        try:
            mol = Chem.MolFromSmiles(smiles)
            return Chem.MolToInchi(mol) if mol else None
        except:
            return None

    return F.udf(smiles_to_inchi, StringType())
