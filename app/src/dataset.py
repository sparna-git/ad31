from .tools import load_df, read_json_str, generate_json_URI, generate_id
import logging
import pandas as pd

class metadata:
        
    def __init__(self, configuration_notices: dict):
        
        __sourceNotices = configuration_notices["source"]
        __template = configuration_notices["template"]     
        __vocabularies = configuration_notices["vocabularies"]
        
        self.__notice = __sourceNotices["notice"]
        self.__personPhysiques = __sourceNotices["personphysique"]
        self.__personnesmorales = __sourceNotices["personnesmorales"]
        self.__juridictions = __sourceNotices["juri"]
        self.__lieux = __sourceNotices["lieux"]
        
        self.__conf_notices = __template["conf_notice"]
        # Vocabulaires
        self.__voc_typeProfession = __vocabularies["voc_typeProfession"]
        self.__voc_sexPersonnes = __vocabularies["voc_sexePersonnes"]
        self.__voc_typeJuridiction = __vocabularies["voc_typeJuridiction"]
        self.__voc_typeProcedure = __vocabularies["voc_typeProcedure"]
        self.__voc_typeLieu = __vocabularies["voc_typeLieu"]
        self.__voc_qualifFaits = __vocabularies["voc_qualifFaits"]
        self.__voc_collectivite = __vocabularies["voc_collectivite"]
        #
        self.logger = logging.basicConfig(
                            filename='app.log',  # Nom du fichier de log
                            level=logging.DEBUG,  # Niveau de log
                            format='%(asctime)s - %(levelname)s - %(message)s'  # Format du log
                        ) 
        self.logger = logging.getLogger(__name__)
        self.logger.info("Start")
    
    def __df_notices(self):
        """
        Populate a dataframe with information of Notices
        Returns:
            Dataframe
        """
        return load_df(self.__notice)
        
    def __df_pp(self):
        """
        Populate a dataframe with information of Personnes Physiques
        Returns:
            Dataframe
        """
        return load_df(self.__personPhysiques) 
    
    def __df_personnesmorales(self):
       
        return load_df(self.__personnesmorales) 
    
    def __df_juri(self):
        """
        Populate a dataframe with information of Jurisdictions
        Returns:
            Dataframe
        """
        return load_df(self.__juridictions)
    
    def __df_lieux(self):
        return load_df(self.__lieux)
    
    def __set_type_profession(self) -> dict:
        return read_json_str(self.__voc_typeProfession)

    def __set_sexPersonnes(self) -> dict:
        return read_json_str(self.__voc_sexPersonnes)
    
    def __set_typeJuridiction(self) -> dict:
        return read_json_str(self.__voc_typeJuridiction)
    
    def __set_typeProcedure(self) -> dict:
        return read_json_str(self.__voc_typeProcedure)
    
    def __set_typeCollectivite(self) -> dict:
        return read_json_str(self.__voc_collectivite)

    def __set_typeLieu(self) -> dict:
        return read_json_str(self.__voc_typeLieu)
    
    def __set_qualifFaits(self) -> dict:
        return read_json_str(self.__voc_qualifFaits)
    
    def __set_read_config_notices(self) -> dict:
        return read_json_str(self.__conf_notices)
    
    def get_conf_notices(self) -> dict:
        return self.__set_read_config_notices()
    
    def get_voc_profession(self) -> dict:
        return self.__set_type_profession()

    def get_voc_sexe(self) -> dict:
        return self.__set_sexPersonnes()
    
    def get_voc_Juridiction(self):
        return self.__set_typeJuridiction()

    def get_voc_typeProcedure(self) -> dict:
        return self.__set_typeProcedure()

    def get_voc_Lieux(self):
        return self.__set_typeLieu()
    
    def get_voc_qualifFaits(self):
        return self.__set_qualifFaits()
    
    def get_voc_collectivite(self):
        return self.__set_typeCollectivite()
    
    def get_noticies_db(self):
        return self.__df_notices()
    
    def get_personnePhysique(self):
        return self.__df_pp()

    def get_personnesmorales(self):
        return self.__df_personnesmorales()
    
    def get_jurisdictions(self):
        return self.__df_juri()
    
    def get_lieux(self):
        return self.__df_lieux()
    
    # Function for find in the sources    
    # Personnes Physiques
    def find_personnes_physiques(self,coteId:str) -> pd.DataFrame:
        df = self.__df_pp()
        return df[df["cote"] == coteId]

    # Personnes Morales
    def find_personnes_morales(self,personName:str) -> pd.DataFrame:
        df = self.get_personnesmorales()
        return df[df["personnes_morales"] == personName.strip()]

    # Jurisdictions
    def find_jurisdictions(self,forme) -> pd.DataFrame:
        df = self.__df_juri()
        return df[df["forme_autorisee"] == forme]

    # Vocabularies
    def find_voc_sexe(self,sexeId):
        
        try:
            if self.get_voc_sexe()[sexeId]:
                return self.get_voc_sexe()[sexeId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the sexe {sexeId}")
            return None
    
    def find_voc_profession(self,professionId):
        
        try:
            if self.get_voc_profession()[professionId]:
                return self.get_voc_profession()[professionId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the profession {professionId}")
            return None  
    
    def find_voc_collectivite(self,typePersonneMorale):
        
        try:
            if self.get_voc_collectivite()[typePersonneMorale]:
                return self.get_voc_collectivite()[typePersonneMorale]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the collectivite {typePersonneMorale}")
            return None
    
    def __get_type_juridiction(self, typeId):
        
        try:
            if self.get_voc_Juridiction()[typeId]:
                return self.get_voc_Juridiction()[typeId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the Juridiction {typeId}")
            return None
    
    def find_voc_juridiction(self,typeId):
        
        if "/" in typeId:
            return [self.__get_type_juridiction(str(t).lower().strip()) for t in str(typeId).split("/")]    
        else:
            return self.__get_type_juridiction(str(typeId).lower().strip())
    
    def find_voc_lieux(self,typeId):
        
        try:
            result = self.get_voc_Lieux()[typeId]
            if result:
                return result["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the Lieux {typeId}")
            return None
    
    def find_voc_qualification_faits(self,typeId):
        
        try:
            if self.get_voc_qualifFaits()[typeId]:
                return self.get_voc_qualifFaits()[typeId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the qualification faits {typeId}")
            return None
    
    def find_voc_typeProcedure(self, typeId:str):
        
        procId = typeId.replace("affaire","procédure")
        try:
            if self.get_voc_typeProcedure()[procId]:
                return self.get_voc_typeProcedure()[procId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the qualification faits {typeId}")
            return None
    
    # dataset 
    