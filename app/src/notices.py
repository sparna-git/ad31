from .tools import generate_id, eval_type_data, generate_json_URI
import pandas as pd
import logging
from .sacs_notices import sacsnotices

class readNotices():
    
    def __init__(self, datamarts):
        
        self.logger = logging.getLogger(__name__)        
        # 
        self.datamarts = datamarts
        self.df_notices = datamarts.get_noticies_db()        
        
        
        # New Dataset
        self.output_list = []
        self.__df_tmp = pd.DataFrame()
        # 
        self.templateJsonLD = datamarts.get_conf_notices()
        self.__generate_notice = sacsnotices(self.templateJsonLD, datamarts)
        
    
    # Populate new dataframe with jsonld result
    def __get_notices(self,dataset):
        
        output = {}
        # For columns
        codeURI = generate_id()
        
        output["coteId"] = dataset["cote"]
        output["id_notice"] = codeURI
        
        # Generate Cote
        output["cote"] = self.__generate_notice.get_cote(dataset["cote"],codeURI)
        # Generate Sac
        output["sac"] = self.__generate_notice.generate_sacs(codeURI,dataset["cote"],output["cote"]["@id"])
        
        if dataset["castan"]:
            output["castan"] = self.__generate_notice.get_castan(dataset["castan"])            
        else:
            output["castan"] = ""
            
        if str(dataset["date_debut"]):
            output["date_debut"] = self.__generate_notice.get_date_debut(str(dataset["date_debut"]))
        else:
            output["date_debut"] = ""
            
        if str(dataset["date_fin"]):
            output["date_fin"] = self.__generate_notice.get_date_fin(str(dataset["date_fin"]))
        else:
            output["date_fin"] = ""
        
        if str(dataset["intitule"]):
            output["intitule"] = self.__generate_notice.get_intitule(str(dataset["intitule"]))
        else:
            output["intitule"] = ""
        
        # Persons
        if dataset["personnes_physiques"]:
            output["personnes_physiques"] = self.__generate_notice.get_personnes_physiques(dataset["cote"])
        else:
            output["personnes_physiques"] = ""
        
        if dataset["personnes_morales"]:
            output["personnes_morales"] = self.__generate_notice.get_personnes_morales(dataset["personnes_morales"])
        else:
            output["personnes_morales"] = ""
            
        # Juridiction
        if dataset["Juridiction_1"]:
            output["Juridiction_1"] = self.__generate_notice.get_Juridiction("Juridiction_1",dataset["Juridiction_1"])
        else:
            output["Juridiction_1"] = ""
        
        if dataset["Juridiction_2"]:
            output["Juridiction_2"] = self.__generate_notice.get_Juridiction("Juridiction_2",dataset["Juridiction_2"])
        else:
            output["Juridiction_2"] = ""
        
        if dataset["Juridiction_3"]:
            output["Juridiction_3"] = self.__generate_notice.get_Juridiction("Juridiction_3",dataset["Juridiction_3"])
        else:
            output["Juridiction_3"] = ""
        
        #
        output["lieux_des_faits_data"] = dataset["lieux_des_faits"]
        if dataset["lieux_des_faits"]:
            output["lieux_des_faits"] = self.__generate_notice.get_lieux_des_faits(dataset["lieux_des_faits"])
        else:
            output["lieux_des_faits"] = ""
        
        if dataset["qualification_faits"]:
            output["qualification_faits"] = self.__generate_notice.get_qualification_faits(dataset["qualification_faits"])
        else:
            output["qualification_faits"] = ""
            
        if dataset["nb_pieces"] and dataset["nb_pieces"] is not None:
            output["nb_pieces"] = self.__generate_notice.get_nb_pieces(dataset["nb_pieces"])
        else:
            output["nb_pieces"] = ""
        
        output["reference_sacs_liasse"] = dataset["reference_sacs_liasse"]
        output["intitule_liasse"] = dataset["intitule_liasse"]
        
        self.output_list.append(output) 
    
    # Generate liasse
    def __get_sac_uri(self,sac):
        
        if sac:
            data = eval_type_data(sac)
            if isinstance(data,list):
                sacs = []
                for sacId in data:
                    sacsId = self.__df_tmp[self.__df_tmp["coteId"] == sacId]["sac"].values[0]
                    codeJson = generate_json_URI(sacsId["@id"])
                    sacs.append(codeJson)
                return sacs
            if isinstance(data,str):
                return generate_json_URI(self.__df_tmp[self.__df_tmp["coteId"] == sac]["sac"].values)
        else:
            ""
    
    def __set_generate_liasses(self,ref, intitule):
    
        if ref and intitule:            
            return self.__generate_notice.get_liasse(ref,intitule)
        
    def __post_processing_liasse(self) -> pd.DataFrame:
    
        # Save in Dataframe Tmp
        df = pd.DataFrame(self.output_list)
        # S
        self.__df_tmp = df
        # Get all intitule_liasse
        df["intitule_liasse_output"] = df["intitule_liasse"].apply(self.__get_sac_uri)
        
        # Generate Liasses
        df["liasse"] = df.apply(lambda x: self.__set_generate_liasses(x.reference_sacs_liasse, x.intitule_liasse_output), axis=1)
        
        return df
    
    # Update Sac 
    def __update_sacs(self,notices):
        
        # Get Sac Dict
        sacsJSON = notices["sac"]
        return self.__generate_notice.update_sacs(notices,sacsJSON)        
    
    # Generate Procedure
    def __generate_procedure(self, notices):
        
        return ""
        
    def generate_json_ld(self):
        
        """
            Pour chaque ligne (notices), on va recuperer les résultat en format JSON-LD
            Premier etape, traitement de notices simples sans lien
        """
        self.df_notices.apply(self.__get_notices,axis=1)
        # Laisse generate information
        dfOutput = self.__post_processing_liasse()
        # Procedure
        dfOutput["procedure"] = dfOutput.apply(self.__generate_procedure,axis=1)
        # Update Sac
        dfOutput["sac"] = dfOutput.apply(lambda x: self.__update_sacs(x), axis=1)
        
        
        dfOutput.to_excel("result.xlsx",index=False)