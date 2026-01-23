from .tools import read_yaml as ry
from .tools import generate_id
# Lecture de tous les fichiers
from .catalogues.dataset import metadata
# Catalogue des Lieux
from .catalogues.lieux_des_faits import convert_lieux
#from .lieux_des_faits import convert_lieux
# Catalogue des Juridictions
from .catalogues.juridictions import convert_juridictions
# Catalogue des Personnes Morales
from .catalogues.personnes_morales import convert_pmorales
# template notices
from .template.template_notices import templatesJSON
# Logging
import logging
import pandas as pd
import numpy as np
import re

class semsac:

    # https://pandas.pydata.org/docs/user_guide/copy_on_write.html#
    pd.options.mode.copy_on_write = True

    def __init__(self):

        #
        self.logger = logging.basicConfig(
                            filename='app.log',  # Nom du fichier de log
                            filemode='w',
                            level=logging.DEBUG,  # Niveau de log
                            format='%(asctime)s - %(name)s  - %(levelname)s - %(message)s'  # Format du log
                        ) 
        self.logger = logging.getLogger(__name__)
        self.logger.info("Start")

        # Lire le fichier de configuration
        configuration = ry("config.yml")

        # Lire les fichiers sources
        datamarts = metadata(configuration)
        self.datamarts = datamarts

        # Directory output notices
        self.get_directory_notices = datamarts.get_directory_notices
        
        # Templates JSON-LD 
        __templateJsonLD = datamarts.get_conf_notices()
        self.__json_notice = templatesJSON(__templateJsonLD)
        self.notices = datamarts.get_noticies_db()
        
        # Chercher dans les vocabularies
        self.filter_voc = datamarts 
        # Personnes Physiques
        self.__personnesPhysiques = datamarts.get_personnePhysique()
        # Code ARK
        self.__ark_code = datamarts.get_ark_code()

        # Vocabulaires
        self.__personnes_physiques_sexe = datamarts.get_voc_sexe()
        self.__personnes_physiques_profession = datamarts.get_voc_profession()
        self.__qualification_faits_type = datamarts.get_voc_qualifFaits()
        self.__type_of_procedure = datamarts.get_voc_typeProcedure()
        
        # Lecture de autres données
        # Créer resource de Lieux
        print("Créer table Lieux")
        lieux = convert_lieux(datamarts)
        dflieux = lieux.convert_to_rdf()
        dflieux["short_vedette"] = dflieux["vedette"].apply(lambda x : re.search("(.+) (\\()",x).group(1))
        self.__lieux = dflieux

        # Créer resource Juridictions
        print("Créer graph de Juridiction")
        juridiction = convert_juridictions(datamarts,dflieux)
        self.__juridiction = juridiction.read_data()

        # Créer resource Personnes Morales
        print("Personnes Morales")
        pmorales = convert_pmorales(datamarts,dflieux).get_personnes_morales()        
        pmorales["json"] = pmorales.apply(lambda x : self.__fin_personne_morale_in_juridiction(x),axis=1)
        self.__personnesmorales = pmorales

    ## Chercher dans les vocabulaires
    def __find_voc_sexe(self,sexeId):
        
        try:
            if self.__personnes_physiques_sexe[sexeId]:
                return self.__personnes_physiques_sexe[sexeId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Sexe: On ne trove pas le sexe: {sexeId}")
            return None
    
    def __find_voc_profession(self,professionId):
        
        try:
            if self.__personnes_physiques_profession[professionId]:
                return self.__personnes_physiques_profession[professionId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Profession: On ne trove pas la Profession: {professionId}, la valeur par défaut sera: <<type:prnon>>")
            # Pour le rico:hasOrHadOccupationOfType des Person, utiliser la valeur par défaut si ce n'est pas connu : "type:prnon"
            return "type:prnon"

    def __find_voc_qualification_faits(self,typeId):
        
        if typeId != "":
            try:
                if self.__qualification_faits_type[typeId]:
                    return self.__qualification_faits_type[typeId]["Concept URI"]
            except Exception as e:
                self.logger.warning(f"Qualification Fait: On ne trouve pas le type de qualification faits {typeId}.")
                return "type:fanod"
        else:
            return "type:fanod"
    
    def __find_voc_typeProcedure(self, typeId:str):
        
        result = ["type:acpro"]        
        if typeId != "":

            if "," in typeId:
                list_types = typeId.split(",")
                for lt in list_types:
                    procId = lt.replace("affaire","procédure")
                    try:
                        if self.__type_of_procedure[procId]:
                            result.append(self.__type_of_procedure[procId]["Concept URI"])
                    except Exception as e:
                        self.logger.warning(f"Qualification Fait: On ne trouve pas la procedure {procId}.")
                return result
            else:
                procId = typeId.replace("affaire","procédure")
                try:
                    if self.__type_of_procedure[procId]:
                        result.append(self.__type_of_procedure[procId]["Concept URI"])
                        return result
                except Exception as e:
                    self.logger.warning(f"Qualification Fait: On ne trouve pas la procedure {typeId}.")
                    return result
        else:
            return result

    # Uiliser apres de reçevoir le fichier de notice 
    def __liasses_ref_sort(self,data):
        
        if data != "":
            # Convert to list
            list_reference = data.split("/")
            references = [i.strip() for i in list_reference]
            # Sort the list
            sort_reference =  sorted(references)
            return "|".join(sort_reference)
        else:
            return ""

    
    ###### 

    def __fin_personne_morale_in_juridiction(self,data:dict):

        if data["json"] == "":
            json = self.__find_juridictions(data["personnes_morales"])
            if json != "":
                return json
            else:
                return ""
        else:
            return data["json"]

    def __find_juridictions(self,juridictionId):
        
        df = self.__juridiction[self.__juridiction["forme_autorisee"] == juridictionId]
        if len(df) > 0:
            return df["json"].iloc[0]
        else:
            return "" 

    def __find_lieux(self,lieuxName:str):

        df = self.__lieux[self.__lieux["vedette"] == lieuxName]
        if len(df) == 1:
            return df["json"].iloc[0]["@id"]
        if len(df) > 1:
            self.logger.info(f"Lieux: On a trouve plus de 1 lieux avec le nom: {lieuxName}")
            return df["json"][0].iloc[0]["@id"]
        if len(df) == 0:
            self.logger.warning(f"Lieux: On n'a pas trouve lieux avec le nom: {lieuxName}")
            return ""

    def __find_procedure_cote(self,coteAssocie:str):

        df = self.get_titles()[self.get_titles()["cote"] == coteAssocie]
        if len(df) > 0:
            return df["procedure"].iloc[0]
        else:
            return ""
    
    def __physicalCharacteristicsNote(self,intitule_liasse:str):

        if "/" in intitule_liasse:
            split_data = intitule_liasse.strip().split("/")
            del split_data[0]
            return "".join(split_data).strip()
        else:
            return ""

    # Notices
    
    def __set_sac_cote(self) -> pd.DataFrame:

        df = self.notices[["sac","cote"]]
        df["json"] = df.apply(lambda x: self.__json_notice.get_cote(x),axis=1)
        return df

    def get_sac_cote(self) -> pd.DataFrame:
        return self.__set_sac_cote()

    def __set_castan(self) -> pd.DataFrame:

        df = self.notices[["sac","cote","castan"]]
        df["castan"] = df["castan"].str.split("/")
        dfCastanOper = df.explode("castan").reset_index(drop=False)
        # Generate code JSON-LD pour chaque ligne
        dfCastan = dfCastanOper[dfCastanOper["castan"] != ""]
        dfCastan["sequence"] = dfCastan.groupby("cote").cumcount()+1
        dfCastan["json"] = dfCastan.apply(lambda x: self.__json_notice.get_castan(x.castan,x.sac,x.sequence),axis=1)

        return dfCastan

    def get_castan(self) -> pd.DataFrame:
        return self.__set_castan()

    # Date dans la procedure
    
    def __set_date_debut(self) -> pd.DataFrame:

        __df = self.notices[["cote","procedure","date_debut"]]
        __dfOper = __df[__df["date_debut"] != ""]
        __dfOper["json"] = __dfOper.apply(lambda x : self.__json_notice.get_date_debut(str(x["procedure"]),x["date_debut"]),axis=1)
        
        return __dfOper
    
    def get_date_debut(self) -> pd.DataFrame:
        return self.__set_date_debut()

    def __set_date_fin(self) -> pd.DataFrame:

        __df = self.notices[["cote","procedure","date_fin"]]
        __df = __df[__df["date_fin"] != ""]
        __df["json"] = __df.apply(lambda x : self.__json_notice.get_date_fin(str(x["procedure"]),x["date_fin"]) if x["date_fin"] != "" else "" ,axis=1)
        
        return __df

    def get_date_fin(self) -> pd.DataFrame:
        return self.__set_date_fin()

    # Personnes

    def __set_personnes_physiques(self) -> pd.DataFrame:

        # find the Personnes Physiques in the sources
        dfpp = pd.merge(self.notices[["cote"]], self.__personnesPhysiques, how="inner",on="cote")
        dfPersonnesPhysiques = dfpp[["cote","nom","sexe_supposé","type_profession"]]
        
        dfPersonnesPhysiques["id"] = dfPersonnesPhysiques.apply(lambda _: generate_id(),axis=1)
        
        # Find sexe type
        dfPersonnesPhysiques["type_sexe"] = dfPersonnesPhysiques["sexe_supposé"].replace('F','Femme').replace('H','Homme').replace('Indéterminé','Indéterminé')
        dfPersonnesPhysiques["type_sexe"] = dfPersonnesPhysiques["type_sexe"].apply(lambda x : self.__find_voc_sexe(x))
        
        # Find profesion
        dfPersonnesPhysiques["type_profession"] = dfPersonnesPhysiques["type_profession"].apply(lambda x : self.__find_voc_profession(x))
        # Créer code JSON
        dfPersonnesPhysiques["json"] = dfPersonnesPhysiques.apply(lambda x: self.__json_notice.get_personnes_physiques(x),axis=1)     
        
        return dfPersonnesPhysiques[["cote","json"]]

    def get_personnes_physiques(self) -> pd.DataFrame:
        return self.__set_personnes_physiques()
    
    def __set_personnes_morales(self) -> pd.DataFrame:

        df = self.notices[["cote","personnes_morales"]]
        __dfPM = df[df["personnes_morales"] != ""]
        # Split
        __dfPM["personnes_morales"] = __dfPM["personnes_morales"].str.split("/")
        dfOutput = __dfPM.explode("personnes_morales").reset_index(drop=False)
        dfOutput["personnes_morales"] = dfOutput["personnes_morales"].str.strip()
        # Join with the Personnes Morales
        dfpm = pd.merge(dfOutput, self.__personnesmorales[["personnes_morales","type_pm","json"]], on="personnes_morales", how="left")
        # Read dataframe
        dfpm.replace(np.nan,"",inplace=True)
        # Log
        dfpm.apply(lambda x : self.logger.warning(f"Personne Morale: On ne trouve pas le code Json de <<{x["personnes_morales"]}>>") if x["json"] == "" else "",axis=1)
        
        return dfpm[["cote","personnes_morales","type_pm","json"]]

    def get_personnes_morales(self) -> pd.DataFrame:
        return self.__set_personnes_morales()
    
    # Juridictions

    def __set_juridiction_1(self) -> pd.DataFrame:
        	
        df = self.notices[["cote","Juridiction_1","Role_juridiction_1"]]
        if len(df) > 0:
            __df = df[df["Juridiction_1"] != ""]
            # Chercher dans la table de juridiction
            __df["json"] = __df["Juridiction_1"].apply(lambda x : self.__find_juridictions(x))
            # Log
            __df.apply(lambda x : self.logger.warning(f"Juridiction 1: On ne trouve pas le code Json de <<{x["Juridiction_1"]}>>") if x["json"] == "" else "",axis=1)
            return __df
        else:
            return pd.DataFrame()
    
    def get_juridiction_1(self) -> pd.DataFrame:
        return self.__set_juridiction_1()
    
    def __set_juridiction_2(self) -> pd.DataFrame:
        
        df = self.notices[["cote","Juridiction_2","Role_juridiction_2"]]
        if len(df) > 0:
            __df = df[df["Juridiction_2"] != ""]
            # Chercher dans la table de juridiction
            __df["json"] = __df["Juridiction_2"].apply(lambda x : self.__find_juridictions(x))
            # Log
            __df.apply(lambda x : self.logger.warning(f"Juridiction 2: On ne trouve pas le code Json de <<{x["Juridiction_2"]}>>") if x["json"] == "" else "",axis=1)
            return __df
        else:
            return pd.DataFrame()

    def get_juridiction_2(self) -> pd.DataFrame:
        return self.__set_juridiction_2()

    def __set_juridiction_3(self) -> pd.DataFrame:
        
        df = self.notices[["cote","Juridiction_3","Role_juridiction_3"]]
        if len(df) > 0:
            __df = df[df["Juridiction_3"] != ""]
            # Chercher dans la table de juridiction
            __df["json"] = __df["Juridiction_3"].apply(lambda x : self.__find_juridictions(x))
            # Log
            __df.apply(lambda x : self.logger.warning(f"Juridiction 3: On ne trouve pas le code Json de <<{x["Juridiction_3"]}>>") if x["json"] == "" else "",axis=1)
            return __df
        else:
            return pd.DataFrame()

    def get_juridiction_3(self) -> pd.DataFrame:
        return self.__set_juridiction_3()
    
    def __json_instruction(self,data:dict) -> list:

        json_instruction = []
        # Id"s
        j1_id = str(generate_id())
        j2_id = str(generate_id())
        j3_id = str(generate_id())
        
        title = self.get_titles()[self.get_titles()["cote"] == data["cote"]]["intitule"].iloc[0]
        # Juridiction 1
        if data["Juridiction_1"] != "":
            j1 = {}

            j1["id"] = j1_id
            j1["name"] = f"Première instruction de \"{title}\""
            # Valider si on a le json de la juridiction
            j1["isOrWasPerformedBy"] = data["json_j1"]

            if data["Juridiction_2"] != "":
                j1["precedesInTime"] = f"https://data.archives.haute-garonne.fr/evenement/{j2_id}"
            elif data["Juridiction_2"] == "" and data["Juridiction_3"] != "":
                j1["precedesInTime"] = f"https://data.archives.haute-garonne.fr/evenement/{j3_id}"
            else:
                j1["precedesInTime"] = ""

            j1["followsInTime"] = ""
            
            json_instruction.append(self.__json_notice.get_instruction(j1))

        # Juridiction 2
        if data["Juridiction_2"] != "":
            j2 = {}

            j2["id"] = j2_id
            j2["name"] = f"Deuxième instruction de \"{title}\""
            if data["json_j2"] != "":
                j2["isOrWasPerformedBy"] = data["json_j2"]
            else:
                self.logger.warning(f"Instruction: Error dans la cote {data["cote"]} ne se troue pas l'information de la Juridiction 2 : {data["Juridiction_2"]}")
                j2["isOrWasPerformedBy"] = ""

            if data["Juridiction_3"] != "":
                j2["precedesInTime"] = f"https://data.archives.haute-garonne.fr/evenement/{j3_id}"
            else:
                j2["precedesInTime"] = ""

            if data["Juridiction_1"] != "":
                j2["followsInTime"] = f"https://data.archives.haute-garonne.fr/evenement/{j1_id}"
            else:
                j2["followsInTime"] = ""

            json_instruction.append(self.__json_notice.get_instruction(j2))

        # Juridiction 3
        if data["Juridiction_3"] != "":
            j3 = {}

            j3["id"] = j3_id
            j3["name"] = f"Troisième instruction de \"{title}\""
            if data["json_j3"] != "":
                self.logger.warning(f"Instruction: Error dans la cote {data["cote"]} ne se troue pas l'information de la Juridiction 3 : {data["Juridiction_3"]}")
                j3["isOrWasPerformedBy"] = data["json_j3"]
            else:
                j3["isOrWasPerformedBy"] = ""

            j3["precedesInTime"] = ""

            if data["Juridiction_2"] != "":
                j3["followsInTime"] = f"https://data.archives.haute-garonne.fr/evenement/{j2_id}"                
            elif data["Juridiction_2"] == "" and data["Juridiction_1"] != "":
                j3["followsInTime"] = f"https://data.archives.haute-garonne.fr/evenement/{j1_id}"
            else:
                j3["followsInTime"] = ""

            json_instruction.append(self.__json_notice.get_instruction(j3))

        return json_instruction

    def __set_instruction_juridiction(self) -> pd.DataFrame:

        dfCotes = self.notices[["cote"]]
        
        dfJ1 = self.get_juridiction_1()
        dfJ1.rename(columns={"json": "json_j1"},inplace= True)
        dfJ1 = dfJ1[dfJ1["json_j1"] != ""]
        dfJ2 = self.get_juridiction_2()
        dfJ2.rename(columns={"json": "json_j2"},inplace= True)
        dfJ3 = self.get_juridiction_3()
        dfJ3.rename(columns={"json": "json_j3"},inplace= True)

        dfCotes_JURIC1 = pd.merge(dfCotes,dfJ1,on="cote",how="left")
        dfCotes_JURIC2 = pd.merge(dfCotes_JURIC1,dfJ2,on="cote",how="left")
        __dfJuridiction = pd.merge(dfCotes_JURIC2,dfJ3,on="cote",how="left")
        __dfJuridiction.replace(np.nan,"",inplace=True)
        __dfOperation = __dfJuridiction[__dfJuridiction["Juridiction_1"] != ""]
        __dfOperation["json"] = __dfOperation.apply(lambda x : self.__json_instruction(x), axis=1)
        #__dfOperation.to_csv("catalogo de instruccion.csv",index=False)

        return __dfOperation

    def get_instruction_juridiction(self) -> pd.DataFrame:
        return self.__set_instruction_juridiction()
    
    # Lieux

    def __set_lieux_des_faits(self) -> pd.DataFrame:

        #
        df = self.notices[["cote","lieux_des_faits"]]
        __df = df[df["lieux_des_faits"] != ""] 
        __df["lieux_des_faits"] = __df["lieux_des_faits"].str.split("/")
        __df = __df.explode("lieux_des_faits").reset_index(drop=False)
        #
        keys_cote = pd.Series(__df.cote.to_list()).unique()
        data_lieux = []
        for key in keys_cote:
            dfAux = __df[__df["cote"] == key]
            
            dfAux["lieux_des_faits"] = dfAux["lieux_des_faits"].str.strip()
            if len(dfAux) == 1:
                data_lieux.append(dfAux.squeeze())
            else:        
                for idx, row in dfAux.iterrows():
                    if "département" not in row["lieux_des_faits"]:
                        data_lieux.append(row.to_dict())
        dfLF = pd.DataFrame(data_lieux)        
        
        # Chercher dans le tableau de Lieux
        dfLF["json"] = dfLF["lieux_des_faits"].apply(lambda x : self.__find_lieux(x))
        return dfLF

    def get_lieux_des_faits(self) -> pd.DataFrame:
        return self.__set_lieux_des_faits()

    def __set_qualification_faits(self):

        df = self.notices[["cote","qualification_faits"]]
        df["id"] = df.apply(lambda _: generate_id(),axis=1)
        df["qualification_faits"] = df["qualification_faits"].str.split("/")
        __df = df.explode("qualification_faits").reset_index(drop=False)
        __df["qualification_faits"] = __df["qualification_faits"].str.strip()

        # Recupere tous les qualite des faires
        __dfEventType = __df[~__df["qualification_faits"].isin(["affaire criminelle","affaire civile"])]
        __dfEventType.drop_duplicates(inplace=True) 
        
        # Recupere tous les affaires XXXX pour la procedure
        __dfProc = __df[__df["qualification_faits"].isin(["affaire criminelle","affaire civile"])]
        __dfProc.drop_duplicates(inplace=True)
        __dfProcGpo = __dfProc.groupby(["cote"]).agg({"qualification_faits": lambda x: ",".join(x)}).reset_index()
        
        qfEvents = pd.Series(__dfEventType["cote"]).unique()
        __cotes_add_proc = []
        for qfeCote in qfEvents:
            __dfProcId = __dfProcGpo[__dfProcGpo["cote"] == qfeCote]
            if len(__dfProcId) == 0:
                __cotes_add_proc.append((qfeCote,""))

        __dfProcedure = None
        if len(__cotes_add_proc) > 0:
            __dfCotesAditional = pd.DataFrame(__cotes_add_proc,columns=["cote","qualification_faits"])
            __dfProcedure = pd.concat([__dfProcGpo,__dfCotesAditional])
        else:
            __dfProcedure = __dfProcGpo

        __dfProcedure["procType"] = __dfProcedure["qualification_faits"].apply(lambda x : self.__find_voc_typeProcedure(x))
        

        # Chercher dans les cotes de procedures que n'existe pas dans le qualifications faits
        __cotes_proc = pd.Series(__dfProc["cote"]).unique()
        __cotes_add = []
        for coteid in __cotes_proc:
            __dfOper = __dfEventType[__dfEventType["cote"] == coteid]
            if len(__dfOper) == 0:
                __cotes_add.append((coteid,""))
        
        __dfQF = None
        if len(__cotes_add) > 0:
            __dfCotesAditional = pd.DataFrame(__cotes_add,columns=["cote","qualification_faits"])
            __dfQF = pd.concat([__dfEventType,__dfCotesAditional])
        else:
            __dfQF = __dfEventType
        
        # Add le type de Qualification faits        
        __dfQF["EventType"] = __dfQF["qualification_faits"].apply(lambda x : self.__find_voc_qualification_faits(x))
      
        return __dfQF[["cote","qualification_faits","EventType"]], __dfProcedure[["cote","qualification_faits","procType"]]

    def get_qualification_faits(self):
        return self.__set_qualification_faits()

    def __set_liasses(self) -> pd.DataFrame:
    
        df = self.notices[["intitule_liasse","reference_sacs_liasse"]]
        
        df["reference_sacs_liasse"] = df["reference_sacs_liasse"].str.split('/')
        df_ = df.explode("reference_sacs_liasse").reset_index(drop=False)
        df_["reference_sacs_liasse"] = df_["reference_sacs_liasse"].str.strip()
        # Remove duplicates
        __dfAli = df_[["intitule_liasse","reference_sacs_liasse"]].drop_duplicates()
        __dfAliasses = pd.merge(__dfAli,self.notices[["cote","sac"]],how='left',left_on="reference_sacs_liasse",right_on="cote")
        # Add uri de la procedure
        __dfAliasses["sac"] =  __dfAliasses["sac"].apply(lambda x : f"https://data.archives.haute-garonne.fr/instanciation/{x}")

        # 
        list_aliasses = pd.Series(df_["intitule_liasse"]).unique()
        data = []
        for aliasses in list_aliasses:
            #
            dfAux = __dfAliasses[__dfAliasses["intitule_liasse"] == aliasses]
            if len(dfAux) > 0:
                tmp = {}
                tmp["title"] = dfAux["intitule_liasse"].iloc[0]
                
                pcn = self.__physicalCharacteristicsNote(dfAux["intitule_liasse"].iloc[0])
                if pcn != "":
                    tmp["physicalCharacteristicsNote"] = pcn
                else:
                    tmp["physicalCharacteristicsNote"] = ""


                tmp["hadComponent"] = dfAux["sac"].to_list()
                
                json = self.__json_notice.get_liasse(tmp)

                data.append((dfAux["intitule_liasse"].iloc[0], json))
        
        if len(data) > 0:
            return pd.DataFrame(data,columns=["intitule_liasse","json"])
        else:
            return pd.DataFrame()

    def get_liasses(self) -> pd.DataFrame:
        return self.__set_liasses()
    
    def __set_cotes_associees(self) -> pd.DataFrame:

        __df = self.notices[["cote","cotes_associees"]]
        __dfca = __df[__df["cotes_associees"] != ""]

        __dfca["cotes_associees"] = __dfca["cotes_associees"].str.split('/')
        __dfOper = __dfca.explode("cotes_associees").reset_index(drop=False)
        __dfOper["cotes_associees"] = __dfOper["cotes_associees"].str.strip()

        # Chercher la procedure de la cote
        __dfOper["procedure_cote"] = __dfOper["cotes_associees"].apply(lambda x : self.__find_procedure_cote(x))
        #dfOutput = __dfOper.replace(np.nan,"")
        # Log
        __dfOper.apply(lambda x : self.logger.warning(f"Cote Associees: {x["cotes_associees"]} avec la cote {x["cote"]} Ne se trouve pas la cote") if x["procedure_cote"] == "" else x["procedure_cote"],axis=1)

        __dfOper["procedure_cote"] = __dfOper["procedure_cote"].apply(lambda x : f"https://data.archives.haute-garonne.fr/evenement/{x}" if x != "" else "")
        # 
        #__dfOper["json"] = __dfOper.apply(lambda x : self.__json_notice.get_cotes_associees(x) if str(x["procedure_cote"]) != "" else "", axis=1)
        
        dfCA = __dfOper[__dfOper["procedure_cote"] != ""]

        return dfCA

    def get_cotes_associees(self) -> pd.DataFrame:
        return self.__set_cotes_associees()
    
    def __set_titles(self) -> pd.DataFrame:
        return self.notices[["cote","procedure","document","intitule","presentation_contenu","intitule_liasse","nb_pieces","odd"]]
    
    def get_titles(self) -> pd.DataFrame:
        return self.__set_titles()

    def __set_code_ark(self):

        __df = self.__ark_code
        __dfTitles = self.get_titles()
        __df["unitid"] = __df["unitid"].str.strip()
        __dfArkCote = pd.merge(__df,__dfTitles[["cote","document"]], left_on="unitid",right_on="cote",how="left")
        __dfArkCote["json"] = __dfArkCote.apply(lambda x : self.__json_notice.get_ark(x) if x["ark"] != "" else "",axis=1)
        
        return __dfArkCote

    def get_ark_code(self) -> pd.DataFrame:
        return self.__set_code_ark()