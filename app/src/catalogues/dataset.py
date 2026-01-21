from src.tools import load_df, read_json_str, generate_json_URI, generate_id
import logging
import pandas as pd

class metadata:
        
    def __init__(self, configuration_notices: dict):
        
        __sourceNotices = configuration_notices["inputs"]
        __template = configuration_notices["template"]     
        __vocabularies = configuration_notices["vocabularies"]
        
        self.__notice = __sourceNotices["notice"]
        self.__personPhysiques = __sourceNotices["personphysique"]
        self.__arkcode = __sourceNotices["arkCode"]
        self.__list_juridiction = __sourceNotices["juridiction"]
        self.__personnes_morales = __sourceNotices["personnesmorales"]
        # Lieux
        self.__lieux_Geocode = __sourceNotices["lieux_Geocode"]

        self.__conf_notices = __template["conf_notice"]
        # Vocabulaires
        self.__voc_typeProfession = __vocabularies["voc_typeProfession"]
        self.__voc_sexPersonnes = __vocabularies["voc_sexePersonnes"]
        self.__voc_typeJuridiction = __vocabularies["voc_typeJuridiction"]
        self.__voc_typeProcedure = __vocabularies["voc_typeProcedure"]
        self.__voc_typeLieu = __vocabularies["voc_typeLieu"]
        self.__voc_qualifFaits = __vocabularies["voc_qualifFaits"]
        self.__voc_collectivite = __vocabularies["voc_collectivite"]
        # Logging
        self.logger = logging.getLogger(__name__)
        
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
    
    def __set_catalogue_personnes_morales(self) -> pd.DataFrame:
        return load_df(self.__personnes_morales)
    
    def get_catalogue_personnes_morales(self) -> pd.DataFrame:
        return self.__set_catalogue_personnes_morales()

    def __set_catalogue_juridiction(self) -> pd.DataFrame:
        return load_df(self.__list_juridiction)

    def get_catalogue_juridiction(self) -> pd.DataFrame:
        return self.__set_catalogue_juridiction()

    # Lieux
    def __set_catalogue_liuex_Geocode(self) -> pd.DataFrame:
        return load_df(self.__lieux_Geocode)

    def get_catalogue_liuex_GeoCode(self) -> pd.DataFrame:
        return self.__set_catalogue_liuex_Geocode()

    # Code ARK    
    def __set_df_ark_code(self) -> pd.DataFrame:
        return load_df(self.__arkcode)

    def get_ark_code(self) -> pd.DataFrame:
        return self.__set_df_ark_code()
    
    # Vocabulaires
    def __set_type_profession(self) -> dict:
        return read_json_str(self.__voc_typeProfession)

    def get_voc_profession(self) -> dict:
        return self.__set_type_profession()

    def __set_sexPersonnes(self) -> dict:
        return read_json_str(self.__voc_sexPersonnes)
    
    def get_voc_sexe(self) -> dict:
        return self.__set_sexPersonnes()
    
    def __set_typeJuridiction(self) -> dict:
        return read_json_str(self.__voc_typeJuridiction)
    
    def get_voc_Juridiction(self):
        return self.__set_typeJuridiction()

    def __set_typeProcedure(self) -> dict:
        return read_json_str(self.__voc_typeProcedure)
    
    def get_voc_typeProcedure(self) -> dict:
        return self.__set_typeProcedure()
    
    def __set_typeCollectivite(self) -> dict:
        return read_json_str(self.__voc_collectivite)

    def get_voc_collectivite(self):
        return self.__set_typeCollectivite()

    def __set_typeLieu(self) -> dict:
        return read_json_str(self.__voc_typeLieu)
    
    def get_voc_Lieux(self):
        return self.__set_typeLieu()

    def __set_qualifFaits(self) -> dict:
        return read_json_str(self.__voc_qualifFaits)
    
    def get_voc_qualifFaits(self):
        return self.__set_qualifFaits()
    
    def __set_read_config_notices(self) -> dict:
        return read_json_str(self.__conf_notices)
    
    # Template JSON pour chaque notices
    def get_conf_notices(self) -> dict:
        return self.__set_read_config_notices()

    # Lecture des notices
    def get_noticies_db(self):

        # Table aux notices
        df = self.__df_notices()
        # Add Id's
        df["sac"] = df.apply(lambda _: generate_id(),axis=1) 
        df["procedure"] = df.apply(lambda _: generate_id(),axis=1)
        df["document"] = df.apply(lambda _: generate_id(),axis=1)
        df["fait"] = df.apply(lambda _: generate_id(),axis=1)

        return df
    
    def get_personnePhysique(self):
        return self.__df_pp()

    # Personnes Physiques
    def find_personnes_physiques(self,coteId:str) -> pd.DataFrame:
        df = self.__df_pp()
        return df[df["cote"] == coteId]