from src.tools import load_df
from src.template.template_notices import templatesJSON
import pandas as pd
import logging
import re

class convert_pmorales:


    def __init__(self,datamarts, dfLieux: pd.DataFrame):

        # Login
        self.logger = logging.getLogger(__name__)
        # 
        __templateJsonLD = datamarts.get_conf_notices()
        self.__json_notice = templatesJSON(__templateJsonLD)
        self.__voc_collectivite = datamarts.get_voc_collectivite()

        dfLieuxCommune = dfLieux[dfLieux["type_lieu"] == "commune"]

        # Data Personnes Morales
        __dfdata = datamarts.get_catalogue_personnes_morales()
        __dfdata["short_pm"] = __dfdata["personnes_morales"].apply(lambda x : x.split("(")[0])
        __dfdata["short_pm"] = __dfdata["short_pm"].str.strip()

        # Type Personne Morale
        __dfdata["type_pm"] = __dfdata["type_personne_morale"].apply(lambda x : self.__find_voc_collectivite(x))
        # Get the json Lieux
        __dfdata["json_location"] = __dfdata.apply(lambda x : self.__find_lieux(x,dfLieuxCommune),axis=1)
        __dfdata["json"] = __dfdata.apply(lambda x : self.__json_notice.get_personnes_morales(x) if x["type_personne_morale"] != "juridiction" else "",axis=1)

        self.__personnes_morales = __dfdata

    def __find_lieux(self,data,dfLieux:pd.DataFrame):

        df = dfLieux[dfLieux["commune"] == data["commune"]]
        if len(df) > 0:
            return df["json"].iloc[0]["@id"]
        else:
            self.logger.warning(f"Personnes Morales: Ne se trouve pas le lieux avec la personne morale: {data["personnes_morales"]} - Commune:  <<{data["commune"]}>>")
            return ""            

    def __find_voc_collectivite(self,typePersonneMorale):
        
        try:
            if self.__voc_collectivite[typePersonneMorale]:
                return self.__voc_collectivite[typePersonneMorale]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Personnes Morales: On ne trove pas le type de Personne Morale: {typeId}")
            return None
    
    def get_personnes_morales(self) -> pd.DataFrame:
        return self.__personnes_morales
