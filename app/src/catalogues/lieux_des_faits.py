from src.tools import load_df, convert_to_turtle, write_json_file, convert_json_context
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


        # Vocabulaire Type de Lieux
        self.__type_of_lieux = datamarts.get_voc_Lieux()

        # DataFrames Lieux
        __dflieux_juridiction = datamarts.get_catalogue_liuex_juridiction()
        __dflieux_geo_code_input = datamarts.get_catalogue_liuex_GeoCode() # Lieux Geo Code

        __dflieux_geo_code = pd.concat([__dflieux_geo_code_input,__dflieux_juridiction])
        __dflieux_geo_code.drop_duplicates(inplace=True) 

        # Lieux Dataset
        
        __dflieux_geo_code["departement"] = __dflieux_geo_code["departement"].str.strip()
        __dflieux_geo_code["commune"] = __dflieux_geo_code["commune"].str.strip()
        __dflieux_geo_code["code_INSEE"] = __dflieux_geo_code["code_INSEE"].str.strip()
        __dflieux_geo_code["type"] = __dflieux_geo_code["type_lieu"].apply(lambda x: self.__find_voc_lieux(x)) # Type of Lieux
        
        
        # CODE INSEE
        __geo_code_insee = __dflieux_geo_code["code_INSEE"].to_list()
        __list_code_insee = [cod_insee for cod_insee in __geo_code_insee if cod_insee != ""]
        __dfCodeInsee = pd.DataFrame(__list_code_insee,columns=["code_INSEE"])
        __dfCodeInsee.drop_duplicates(inplace=True)
        __dfCodeInsee["json"] = __dfCodeInsee["code_INSEE"].apply( lambda x : self.__json_notice.get_insee(x))
        self.__dfInsee = __dfCodeInsee
        # Fichier Log
        #__dfCodeInsee.to_csv("code_insee_geocode.csv",index=False)

        # DEPARTEMENT
        __geo_code_departement = __dflieux_geo_code[__dflieux_geo_code["type_lieu"] == "département"]
        #            Get code INSEE JSON
        __geo_code_departement["json_insee"] = __geo_code_departement["code_INSEE"].apply( lambda x : __dfCodeInsee[__dfCodeInsee["code_INSEE"] == x]["json"].iloc[0] if x != "" else "")
        __geo_code_departement["json"] = __geo_code_departement.apply( lambda x : self.__json_notice.get_departement(x), axis=1)
        self.__dfDepartement = __geo_code_departement
        # Fichier Log
        #__geo_code_departement.to_csv("geo_departement.csv",index=False)

        # COMMUNE
        __geo_code_commune = __dflieux_geo_code[__dflieux_geo_code["type_lieu"] == "commune"]
        __geo_code_commune["json_isDirectlyContainedBy"] = __geo_code_commune["departement"].apply(lambda x : self.__find_departement(x) if x != "" else "")
        __geo_code_commune["json_insee"] = __geo_code_commune["code_INSEE"].apply( lambda x : __dfCodeInsee[__dfCodeInsee["code_INSEE"] == x]["json"].iloc[0] if x != "" else "")
        __geo_code_commune["others"] = ""
        __geo_code_commune["geocode"] = __geo_code_commune.apply( lambda x : f'POINT({x["longitude"]} {x["latitude"]})' if x["latitude"] != "" and x["longitude"] != "" else "", axis=1)
        #__geo_code_commune.to_csv("geo_commune_debug.csv",index=False)
        __geo_code_commune["json"] = __geo_code_commune.apply(lambda x : self.__json_notice.get_lieux_des_faits(x), axis=1)
        self.__dfCommune = __geo_code_commune
        # Fichier Log
        #__geo_code_commune.to_csv("geo_commune.csv",index=False)


        # Tous les lieux different du type Département et Commune
        __geo_Lieux = __dflieux_geo_code[(__dflieux_geo_code["type_lieu"] != "département") & (__dflieux_geo_code["type_lieu"] != "commune")]
        __geo_Lieux["json_isDirectlyContainedBy"] = __geo_Lieux["commune"].apply(lambda x : self.__find_commune(x))
        __geo_Lieux["json_insee"] = __geo_Lieux["code_INSEE"].apply( lambda x : __dfCodeInsee[__dfCodeInsee["code_INSEE"] == x]["json"].iloc[0] if x != "" else "")
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
        write_json_file("output/lieux/lieux_result.json",json_output)
        convert_to_turtle("output/lieux/lieux_result.ttl",json_output)

        # Concat all lieux des faits        
        dfLieuxResult = pd.concat([self.__dfLieux[["vedette","departement","commune","type_lieu","json"]],self.__dfCommune[["vedette","departement","commune","type_lieu","json"]],self.__dfDepartement[["vedette","departement","commune","type_lieu","json"]]])
        
        return dfLieuxResult