from src.tools import load_df
from src.template.template_notices import templatesJSON
import pandas as pd
import logging
import re

class convert_juridictions:

    def __init__(self,datamarts, dfLieux: pd.DataFrame):

        # Login
        self.logger = logging.getLogger(__name__)
        # 
        __templateJsonLD = datamarts.get_conf_notices()
        self.__json_notice = templatesJSON(__templateJsonLD)
        self.__voc_type_juridiction = datamarts.get_voc_Juridiction()

        # Data
        __dfdata = datamarts.get_catalogue_juridiction()
        # Type de juridiction
        __dfdata["type_voc"] = __dfdata["type_juridiction"].apply(lambda x: self.__find_voc_juridiction(x))
        # Nom curtdfdata
        dfLieux["vedette_short"] = dfLieux["vedette"].apply(lambda x : re.search("(.+) (\\()",x).group(1))
        # find Lieux siege
        __dfdata["lieu_siege"] = __dfdata["lieu_siege"].str.split("/")
        __dfOutput = __dfdata.explode("lieu_siege").reset_index(drop=False)
        __dfOutput["lieu_siege"] = __dfOutput["lieu_siege"].str.strip()
        __dfOutput["lieu_siege_qd"] = __dfOutput["lieu_siege"].apply(lambda x : re.search("(.+) (\\()",x).group(1) if "(" in x else x )
        __dfOutput["lieu_siege_qd"] = __dfOutput["lieu_siege_qd"].str.replace(")","")
        __dfOutput["json_location"] = __dfOutput.apply(lambda x : self.__find_lieu_siege(x["lieu_siege"],x["lieu_siege_qd"],dfLieux),axis=1)
        
        __dfOutput["json"] = __dfOutput.apply(lambda x : self.__json_notice.get_Juridiction(x),axis=1)
        self.__juridiction = __dfOutput

    def __find_lieu_siege(self,ls,lsQd,dfLieux):

        df = dfLieux[dfLieux["vedette"] == ls]
        if len(df) == 0:
            df = dfLieux[dfLieux["vedette_short"].str.contains(lsQd)]
            if len(df) == 0:
                self.logger.warning(f"Juridiction: On ne trouve pas le Lieu de siege: {ls} dans le lieux")
            if len(df) == 1:
                return df["json"].iloc[0]["@id"]
            if len(df) > 1:    
                df = dfLieux[dfLieux["vedette"].str.contains(lsQd)]
                if len(df) > 0:
                    return df["json"].iloc[0]["@id"]
        else:
            return df["json"].iloc[0]["@id"]

    def __get_type_juridiction(self, typeId):

        if typeId != "":
            try:
                if self.__voc_type_juridiction[typeId]:
                    return self.__voc_type_juridiction[typeId]["Concept URI"]
            except Exception as e:
                self.logger.warning(f"Juridiction: On ne trove pas le type de juridiction: {typeId}, la valeur par défaut type:junon")
                return "type:junon"
        else:
            return "type:junon"
    
    def __find_voc_juridiction(self,typeId):
        
        if "/" in typeId:
            return [self.__get_type_juridiction(str(t).lower().strip()) for t in str(typeId).split("/")]    
        else:
            return self.__get_type_juridiction(str(typeId).lower().strip())
    
    def read_data(self) -> pd.DataFrame:

        return self.__juridiction