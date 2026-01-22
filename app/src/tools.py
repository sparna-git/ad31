import yaml as py
import pandas as pd
import numpy as np
from rdflib import Graph
import json
# https://docs.python.org/3/library/uuid.html
'''  UUID objects according to RFC 9562 '''
import uuid
from pathlib import Path
import shutil

# OS

def create_directory(path_dir:str):
    """
        Create directory if not exists
    """
    if not Path(path_dir).exists():
        Path(path_dir).mkdir(parents=True, exist_ok=True)
    else:
        shutil.rmtree(path_dir)
        Path(path_dir).mkdir(parents=True, exist_ok=True)
    

# Yaml

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


# RDF Lib Serialize in turtle file
def convert_to_turtle(file_name:str,json_ld_data_string):
    # Parse the data in the 'normal' RDFLib way, setting the format parameter to "json-ld"
    g = Graph()
    g.parse(data=json_ld_data_string, format="json-ld")
    # save in file
    g.serialize(file_name,format="turtle")

def write_json_file(file_name:str, json_data:str):
    """
        Ecriture de fichier JSON
        arguments:
            file_name: nom de fichier sans extension
            json_data: contenu de code à ecrir dans le fichier
    """
    # Write Json File
    with open(file_name, "w") as f:
        json.dump(json_data, f)

def convert_json_context(graph):
    """
        Créer une code JSON avec de context
    """

    json_lieux = {}
    # Add context
    json_lieux["@context"] = {
                                "rico": "https://www.ica.org/standards/RiC/ontology#",
                                "crm": "http://www.cidoc-crm.org/cidoc-crm/",
                                "frbroo": "http://iflastandards.info/ns/fr/frbr/frbroo/",
                                "owl": "http://www.w3.org/2002/07/owl#",
                                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                                "xsd": "http://www.w3.org/2001/XMLSchema#",
                                "type": "https://data.archives.haute-garonne.fr/type/",
                                "xsd": "http://www.w3.org/2001/XMLSchema#",
                                "geo": "http://www.opengis.net/ont/geosparql#"
                            }
    json_lieux["@graph"] = graph

    return  json_lieux



# Generate ID WITH uuid

""" Generate an Identifier unique 
    Shacl: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}
    Format: 00ac42ad-99d0-4922-985a-df32927776e8
"""
def generate_id():
    return uuid.uuid1()

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