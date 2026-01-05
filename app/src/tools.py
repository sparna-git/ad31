import yaml as py
import pandas as pd
import numpy as np
import json
# https://docs.python.org/3/library/uuid.html
'''  UUID objects according to RFC 9562 '''
import uuid


def read_yaml(YamlFile):
    __fileYml = open(YamlFile,'r')
    return py.safe_load(__fileYml)        

# Pandas

def load_df(__source:str) -> pd.DataFrame:
    
    import warnings
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    
    # Load data in dataframe
    df = pd.read_excel(__source,engine="openpyxl",dtype=pd.StringDtype())
    # Replaces NaN values for empty
    df.replace(np.nan,'',inplace=True)
    
    return df

# Generate ID WITH uuid

""" Generate an Identifier unique 
    Shacl: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}
    Format: 00ac42ad-99d0-4922-985a-df32927776e8
"""
def generate_id():
    return uuid.uuid1()


def convert_dict_str(__data:dict) -> str:
    return json.dumps(__data)

def eval_type_data(__data:str):
    """
    Args:
        __data (str): String

    Returns:
        If data content "/" then return list
        Then return str
    """
    if "/" in __data:
        return __data.split("/")
    else:
        return __data.strip()

# JSON

def generate_json_URI(value):
    """
        Generate a format { "@id" : "values link"} json 

    Args:
        value (_type_): this is a link Exemple: "https://data.archives.haute-garonne.fr/instanciation/6bf370bb-dcde-11f0-8cf1-94e70b70a1ec"

    Returns: { "@id": "https://data.archives.haute-garonne.fr/instanciation/6bf370bb-dcde-11f0-8cf1-94e70b70a1ec" }
    """
    return { "@id" : value }

def read_json(__inputJsonFile) -> dict:
    return json.loads(open(__inputJsonFile,'r'))

def read_json_str(__inputJsonFile) -> dict:
    return json.load(open(__inputJsonFile,'r'))