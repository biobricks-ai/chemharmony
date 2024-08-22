import os, biobricks as bb, pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

ssdb = bb.assets('skinsensdb').skinsens_parquet

chem_cols = ['Chemical_Name', 'CAS No', 'PubChem CID', 'Canonical SMILES']
df = pd.read_parquet(ssdb).melt(id_vars=chem_cols, var_name='assay', value_name='value')

# filter out NaN
df = df.dropna(subset=['value'])
df = df[df['value'] != 'nan']

# count unique values
df.groupby(['assay']).size().reset_index(name='count').sort_values('count', ascending=False)
df.groupby(['assay','value']).size().reset_index(name='count').sort_values('count', ascending=False)

# thresholds 
