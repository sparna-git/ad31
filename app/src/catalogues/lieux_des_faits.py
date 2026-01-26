from src.tools import convert_to_turtle, write_json_file, convert_json_context, generate_id
from src.template.template_notices import templatesJSON
import logging
import pandas as pd

class convert_lieux:

    # https://pandas.pydata.org/docs/user_guide/copy_on_write.html#
    pd.options.mode.copy_on_write = True

    def __init__(self,datamarts):
        
        # 
        __templateJsonLD = datamarts.get_conf_notices()
        self.__json_notice = templatesJSON(__templateJsonLD)
        # Login
        self.logger = logging.getLogger(__name__)
        # Directory Output
        self.__get_directory_lieux = datamarts.get_directory_lieux


        # Vocabulaire Type de Lieux
        self.__type_of_lieux = datamarts.get_voc_Lieux()

        # DataFrames Lieux
        __dflieux_juridiction = datamarts.get_catalogue_liuex_juridiction()
        
        __dflieux_juridiction.drop(["provenance"], axis=1,inplace=True)
        __dflieux_juridiction["latitude"] = ""
        __dflieux_juridiction["longitude"] = ""
        
        __dflieux_geo_code_input = datamarts.get_catalogue_liuex_GeoCode() # Lieux Geo Code
        
        # Chercher les lieux qui n'existe pas dans le fichier de geo code
        __dfJur = __dflieux_juridiction[~__dflieux_juridiction["vedette"].isin(__dflieux_geo_code_input["vedette"].to_list())]
        # Ajouter les lieux qui ne sont pas dans le geo code
        __dflieux_geo_code = pd.concat([__dflieux_geo_code_input,__dfJur])
        
        __dflieux_geo_code.drop_duplicates(inplace=True) 
        __dflieux_geo_code["id"] = __dflieux_geo_code.apply(lambda _: generate_id(),axis=1) 

        # Lieux Dataset
        __dflieux_geo_code["departement"] = __dflieux_geo_code["departement"].str.strip()
        __dflieux_geo_code["commune"] = __dflieux_geo_code["commune"].str.strip()
        __dflieux_geo_code["code_INSEE"] = __dflieux_geo_code["code_INSEE"].str.strip()
        __dflieux_geo_code["type"] = __dflieux_geo_code["type_lieu"].apply(lambda x: self.__find_voc_lieux(x)) # Type of Lieux
        
        # DEPARTEMENT
        __geo_code_departement = __dflieux_geo_code[__dflieux_geo_code["type_lieu"] == "département"]
        #            Get code INSEE JSON
        __geo_code_departement["json"] = __geo_code_departement.apply( lambda x : self.__json_notice.get_departement(x), axis=1)
        self.__dfDepartement = __geo_code_departement
        # Fichier Log
        #__geo_code_departement.to_csv("geo_departement.csv",index=False)

        # COMMUNE
        __geo_code_commune = __dflieux_geo_code[__dflieux_geo_code["type_lieu"] == "commune"]
        __geo_code_commune["json_isDirectlyContainedBy"] = __geo_code_commune["departement"].apply(lambda x : self.__find_departement(x) if x != "" else "")
        __geo_code_commune["others"] = ""
        __geo_code_commune["geocode"] = __geo_code_commune.apply( lambda x : f'POINT({x["longitude"]} {x["latitude"]})' if x["latitude"] != "" and x["longitude"] != "" else "", axis=1)
        
        __geo_code_commune["json"] = __geo_code_commune.apply(lambda x : self.__json_notice.get_lieux_des_faits(x), axis=1)
        self.__dfCommune = __geo_code_commune
        # Fichier Log
        #__geo_code_commune.to_csv("geo_commune.csv",index=False)


        # Tous les lieux different du type Département et Commune
        __geo_Lieux = __dflieux_geo_code[(__dflieux_geo_code["type_lieu"] != "département") & (__dflieux_geo_code["type_lieu"] != "commune")]
        __geo_Lieux["json_isDirectlyContainedBy"] = __geo_Lieux["commune"].apply(lambda x : self.__find_commune(x))
        __geo_Lieux["others"] = ""
        __geo_Lieux["geocode"] = ""
        __geo_Lieux["json"] = __geo_Lieux.apply(lambda x : self.__json_notice.get_lieux_des_faits(x), axis=1)
        __geo_Lieux["json_directlyContains"] = __geo_Lieux.apply(lambda x : self.__json_notice.get_lieux_des_faits_directlyContains(x) if x["commune"] != "" else "", axis=1)
        self.__dfLieux = __geo_Lieux
        # Fichier Log
        #__geo_Lieux.to_csv("geo_lieux.csv",index=False)

    
    def __find_voc_lieux(self,typeId):
        
        try:
            result = self.__type_of_lieux[typeId]
            if result:
                return result["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Lieux: On ne trouve pas le type de Lieux {typeId}")
            return None

    def __find_departement(self,depId):

        df = self.__dfDepartement[self.__dfDepartement["departement"] == depId]
        if len(df) > 0:
            return df["json"].iloc[0]["@id"]
        else:
            return ""

    def __find_code_insee(self,codeInsee):
        
        df = self.__dfInsee[self.__dfInsee["code_INSEE"] == codeInsee]
        if len(df) > 0:
            return df["json"].iloc[0]
        else:
            return ""

    def __find_commune(self, communeId):

        df = self.__dfCommune[self.__dfCommune["commune"] == communeId]
        if len(df) > 0:
            return df["json"].iloc[0]["@id"]
        else:
            return ""

    def convert_to_rdf(self):

        # Generate une seule resultat json Département + Commune + Lieux
        json_lieux_full = []
        [json_lieux_full.append(j) for j in self.__dfDepartement["json"].to_list() ]
        [json_lieux_full.append(j) for j in self.__dfCommune["json"].to_list() ]
        [json_lieux_full.append(j) for j in self.__dfLieux["json"].to_list() ]
        [json_lieux_full.append(j) for j in self.__dfInsee["json"].to_list() ]

        json_output = convert_json_context(json_lieux_full)
        write_json_file(f"{self.__get_directory_lieux}/lieux_result.json",json_output)
        convert_to_turtle(f"{self.__get_directory_lieux}/lieux_result.ttl",json_output)
        
        # Concat all lieux des faits        
        dfLieuxResult = pd.concat([self.__dfLieux[["vedette","departement","commune","type_lieu","json"]],self.__dfCommune[["vedette","departement","commune","type_lieu","json"]],self.__dfDepartement[["vedette","departement","commune","type_lieu","json"]]])
        
        return dfLieuxResult