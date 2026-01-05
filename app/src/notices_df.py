from .tools import generate_id, generate_json_URI, convert_dict_str
import pandas as pd
import numpy as np
import json
from rdflib import Graph, Namespace, URIRef
from .sacs_notices import sacsnotices
import re

class relation:
    
    pd.options.mode.copy_on_write = True
    
    def __init__(self,datamarts):
        self.datamarts = datamarts
        self.notices = datamarts.get_noticies_db()
        self.personnesPhysiques = datamarts.get_personnePhysique()
        self.personnesmorales = datamarts.get_personnesmorales()
        self.juridiction = datamarts.get_jurisdictions()
        self.lieux = datamarts.get_lieux()
        
        # Vocabularies
        self.filter_voc = datamarts
        # 
        self.templateJsonLD = datamarts.get_conf_notices()
        self.__json_notice = sacsnotices(self.templateJsonLD, datamarts)        
    
    def __df_sacs(self, df:pd.DataFrame) -> pd.DataFrame:
        
        # id Sac & Cote
        df["sac"] = df.apply(lambda _: generate_id(),axis=1) 
        df["procedure"] = df.apply(lambda _: generate_id(),axis=1)
        df["document"] = df.apply(lambda _: generate_id(),axis=1)
        df["fait"] = df.apply(lambda _: generate_id(),axis=1)
        
        df["castan"] = df["castan"].str.split("/")
        dfOutput = df.explode("castan").reset_index(drop=False)
        
        return dfOutput 
        
    def __df_personnes_physiques(self, df: pd.DataFrame) -> pd.DataFrame:
        
        # find the Personnes Physiques in the sources
        dfpp = pd.merge(df, self.personnesPhysiques, how="inner",on="cote")
        dfPersonnesPhysiques = dfpp[["cote","nom","sexe_supposé","type_profession"]]
        dfPersonnesPhysiques["id"] = dfPersonnesPhysiques.apply(lambda _: generate_id(),axis=1)
        
        # Find sexe type
        dfPersonnesPhysiques["type_sexe"] = dfPersonnesPhysiques["sexe_supposé"].replace('F','Femme').replace('H','Homme').replace('Indéterminé','Indéterminé')
        dfPersonnesPhysiques["type_sexe"] = dfPersonnesPhysiques["type_sexe"].apply(lambda x : self.filter_voc.find_voc_sexe(x))
        
        # Find profesion
        dfPersonnesPhysiques["type_profession"] = dfPersonnesPhysiques["type_profession"].apply(lambda x : self.filter_voc.find_voc_profession(x))        
        
        return dfPersonnesPhysiques[["cote","id","nom","type_sexe","type_profession"]]
    
    def __df_personnes_morales(self, df: pd.DataFrame) -> pd.DataFrame:
        
        # Split
        df["personnes_morales"] = df["personnes_morales"].str.split("/")
        dfOutput = df.explode("personnes_morales").reset_index(drop=False)
        # id Personnes Morales
        dfOutput["id"] = dfOutput.apply(lambda _: generate_id(),axis=1)
        
        # Join with the Personnes Morales 
        dfpm = pd.merge(dfOutput, self.personnesmorales, left_on="personnes_morales", right_on="personnes_morales", how="inner")
        # Type Personne Morale
        dfpm["type_pm"] = dfpm["type_personne_morale"].apply(lambda x : self.filter_voc.find_voc_collectivite(x))
        
        return dfpm[["cote","id","personnes_morales","commune","departement","etat","type_personne_morale","type_pm"]]
        
    def __df_juridiction(self,df: pd.DataFrame) -> pd.DataFrame:
        
        j1 = df[["Juridiction_1"]]
        j1.rename(columns={"Juridiction_1":"Juridiction"},inplace=True)
        s1 = j1.squeeze()
        s1_ = s1[s1.apply(len) > 0]
        j2 = df[["Juridiction_2"]]
        j2.rename(columns={"Juridiction_2":"Juridiction"},inplace=True)
        s2 = j2.squeeze()
        s2_ = s2[s2.apply(len) > 0]
        j3 = df[["Juridiction_3"]]
        j3.rename(columns={"Juridiction_3":"Juridiction"},inplace=True)
        s3 = j3.squeeze()
        s3_ = s3[s3.apply(len) > 0]
        
        data = [s1_,s2_,s3_]
        
        sJuridiction = pd.concat(data,axis=0)
        df = pd.DataFrame(sJuridiction)
        df.drop_duplicates(inplace=True)
        # Merge with list juridiction
        dfOutput = pd.merge(df, self.juridiction,how="inner",left_on="Juridiction", right_on="forme_autorisee")
        # find type juridiction
        dfOutput["type_voc"] = dfOutput["type_juridiction"].apply(lambda x : self.filter_voc.find_voc_juridiction(str(x).strip()))   
        
        return dfOutput
    
    def __find_departement(self,depId):
        
        df = self.departements[self.departements["departement"].str.contains(depId)]
        if len(df) > 0:
            result = df["json"].iloc[0]
            if result:
                return result["@id"]
        else:
            return None
        
    def __df_lieux_des_faits(self, df: pd.DataFrame) -> pd.DataFrame:
        
        #
        df["lieux_des_faits"] = df["lieux_des_faits"].str.split("/")
        df = df.explode("lieux_des_faits").reset_index(drop=False)
        #
        keys_cote = pd.Series(df.cote.to_list()).unique()
        data_lieux = []
        for key in keys_cote:
            dfAux = df[df["cote"] == key]
            dfAux["lieux_des_faits"] = dfAux["lieux_des_faits"].str.strip()
            if len(dfAux) == 1:
                data_lieux.append(dfAux.squeeze())
            else:        
                for idx, row in dfAux.iterrows():
                    if "département" not in row["lieux_des_faits"]:
                        data_lieux.append(row.to_dict())
        dfOutput = pd.DataFrame(data_lieux)        
        # join with the lieux
        dfOperation = pd.merge(dfOutput,self.lieux, left_on="lieux_des_faits", right_on="vedette", how="inner")
        # find the type de lieu dans le vocabulaire
        dfOperation["typeLieu"] = dfOperation["type_lieu"].apply(lambda x : self.filter_voc.find_voc_lieux(x))
        
        return dfOperation
        
    def __df_qualification_faits(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df["id"] = df.apply(lambda _: generate_id(),axis=1)
        df["qualification_faits"] = df["qualification_faits"].str.split("/")
        df_ = df.explode("qualification_faits").reset_index(drop=False)
        df_["qualification_faits"] = df_["qualification_faits"].str.strip()
        # find the qualification fait voc
        df_["EventType"] = df_["qualification_faits"].apply(lambda x : self.filter_voc.find_voc_qualification_faits(x))
        # find in the procedures
        dfEventType = df_[df_["EventType"].apply(lambda x: x is not None)] 
        
        dfProc = df_[df_["EventType"].apply(lambda x: x is None)]
        dfProc["EventType"] = dfProc["qualification_faits"].apply(lambda x : self.filter_voc.find_voc_typeProcedure(x))
        
        return dfEventType[["cote","id","qualification_faits","EventType"]], dfProc[["cote","id","qualification_faits","EventType"]]
    
    def __df_liasses(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df["id"] = df.apply(lambda _: generate_id(),axis=1)
        df["intitule_liasse"] = df["intitule_liasse"].str.split('/')
        df_ = df.explode("intitule_liasse").reset_index(drop=False)
        
        # Join with cote and get Id
        dfResult = pd.merge(df_,self.__dataset_Sacs[["cote","sac"]],how='left',left_on="intitule_liasse",right_on="cote")
        #
        dfOutput = dfResult.drop("cote_y",axis=1)
        dfliases = dfOutput.rename({"cote_x": "cote"},axis=1)
        dfliases.replace(np.nan,"",inplace=True)
        
        return dfliases[["cote","id","reference_sacs_liasse","intitule_liasse","sac"]]
    
    def __df_cotes_associees(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df["cotes_associees"] = df["cotes_associees"].str.split("/")
        dfOutput = df.explode("cotes_associees").reset_index(drop=False)
        dfOutput["id"] = dfOutput.apply(lambda _: generate_id(),axis=1)
        
        return dfOutput
    
    def __json_instruction(self, data) -> dict:
        
        inst = {}
        return inst
    
    def __json_procedure(self, data:dict) -> dict:
        
        procedure_ = {}
        
        procedure_["id"] = data["procedure"]
        procedure_["name"] = data["intitule"]
        # Une procédure est ouverte suite à des faits
        procedure_["hasActivityType"] = self.__dataset_QF_Proc[self.__dataset_QF_Proc["cote"] == data["cote"]]["json"].to_list()
        procedure_["resultsOrResultedFrom"] = self.__json_fait(data) 
        # Une procédure est un ensemble d’instructions
        procedure_["hasOrHadSubevent"] = self.__json_instruction(data)
        # Une procédure implique des parties
        
        # Personnes
        participant = []
        dfpp = self.__dataset_PersonnesPhysiques[self.__dataset_PersonnesPhysiques["cote"] == data["cote"]]
        if len(dfpp) > 0:
            [participant.append(j) for j in dfpp["json"].to_list()]
        
        dfmorales = self.__dataset_PersonnesMorales[self.__dataset_PersonnesMorales["cote"] == data["cote"]]
        if len(dfmorales) > 0:
            [participant.append(j) for j in dfmorales["json"].to_list()]            
        procedure_["hasOrHadParticipant"] = participant
                
        # Date
        procedure_["hasBeginningDate"] = self.__json_notice.get_date_debut(data["procedure"],data["date_debut"])
        procedure_["hasEndDate"] = self.__json_notice.get_date_fin(data["procedure"],data["date_fin"])
        
        json_procedure = self.__json_notice.get_procedure(procedure_)
        
        return json_procedure
    
    def __json_fait(self,data: dict) -> dict:
        
        fait = {}
        
        fait["name"] = data["presentation_contenu"]
        fait["EventType"] = self.__dataset_QF_EventType[self.__dataset_QF_EventType["cote"] == data["cote"]]["json"].to_list()
        fait["Location"] = ""
        
        return self.__json_notice.get_fait(fait)
    
    def __json_document(self,data:dict) -> dict:
        
        doc = {}
        
        doc["id"] = data["document"]
        doc["name"] = data["cote"]
        doc["generalDescription"] = data["presentation_contenu"] 
        doc["nb_pieces"] = data["nb_pieces"]
        doc["hasOrHadInstantiation"] = generate_json_URI(f"https://data.archives.haute-garonne.fr/instanciation/{data["sac"]}")
        doc["procedure"] = generate_json_URI(f"https://data.archives.haute-garonne.fr/evenement/{data["procedure"]}")
        
        return self.__json_notice.get_document(doc)
    
    def __set_wasComponentOf(self, coteId:str):
        
        df = self.__dataset_Liasses[self.__dataset_Liasses["cote"] == coteId]
        if df.size > 0:
            ids = [idLaisse["@id"] for idLaisse in df["json"].to_list()]
            code_json = df.json.to_list()
            return code_json,list(dict.fromkeys(ids))
        else:
            return "",""
    
    def __json_sac(self,data:dict):
        
        json_code = []
        identifier_uris = []
        
        json_Notices = {}
        dictSac = {}
        # generate context
        json_Notices["@context"] = {
                                    "sacs": "https://data.archives.haute-garonne.fr/modeles/sacsaproces",
                                    "rico": "https://www.ica.org/standards/RiC/ontology#",
                                    "crm": "http://www.cidoc-crm.org/cidoc-crm/",
                                    "dash": "http://datashapes.org/dash#",
                                    "frbroo": "http://iflastandards.info/ns/fr/frbr/frbroo/",
                                    "owl": "http://www.w3.org/2002/07/owl#",
                                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                                    "xsd": "http://www.w3.org/2001/XMLSchema#",
                                    "vocad": "https://data.archives.haute-garonne.fr/vocabulaires",
                                    "type": "https://data.archives.haute-garonne.fr/type/",
                                    "xsd": "https://www.w3.org/2001/XMLSchema#"
                                }
        
        dictSac["id"] = data["sac"]
        dictSac["name"] = data["cote"]
        
        # Cote
        cote_json = self.__json_notice.get_cote(data["cote"],data["sac"])
        identifier_uris.append(cote_json["@id"])
        json_code.append(cote_json)
        
        # Castan
        castan = self.__dataset_castan[self.__dataset_castan["cote"] == data["cote"]]["json"].to_list()
        if len(castan) > 0:
            [identifier_uris.append(generate_json_URI(d["@id"])) for d in castan if d != ""]
            [json_code.append(d) for d in castan]
        # Identifiers        
        dictSac["identifier"] = identifier_uris
        
        wasComponentOf_code_json, wasComponentOf_ids = self.__set_wasComponentOf(data["cote"])
        if wasComponentOf_code_json != "":
            dictSac["wasComponentOf_ids"] = wasComponentOf_ids
            [json_code.append(d) for d in wasComponentOf_code_json]
        else:
            dictSac["wasComponentOf_ids"] = ""
        
        juridiction_Id = []
        dfJ1 = self.__dataset_Jurisdiction1[self.__dataset_Jurisdiction1["cote"] == data["cote"]]
        if len(dfJ1) > 0:
            juridiction_Id.append(dfJ1["json"].iloc[0]["@id"])
            [json_code.append(j) for j in dfJ1["json"].to_list()]
        
        if len(self.__dataset_Jurisdiction2) > 0:
            dfJ2 = self.__dataset_Jurisdiction2[self.__dataset_Jurisdiction2["cote"] == data["cote"]]
            if len(dfJ2) > 0:
                juridiction_Id.append(dfJ2["json"].iloc[0]["@id"])
                [json_code.append(j) for j in dfJ2["json"].to_list()]
        
        if len(self.__dataset_Jurisdiction3) > 0:
            dfJ3 = self.__dataset_Jurisdiction3[self.__dataset_Jurisdiction3["cote"] == data["cote"]]
            if len(dfJ3) > 0:
                juridiction_Id.append(dfJ3["json"].iloc[0]["@id"])
                [json_code.append(j) for j in dfJ3["json"].to_list()]
        # 
        dictSac["hasOrganicProvenance"] = juridiction_Id
        
        # Procedure
        dictSac["procedure"] = self.__json_procedure(data)
        
        # Document  
        dictSac["document"] = self.__json_document(data)
        
        # Generate Template complete du SAC
        json_code.append(self.__json_notice.generate_sacs(dictSac))
        
        # Notices
        json_code.append(self.__json_document(data))
        
        json_Notices["@graph"] = json_code
        
        
        name_file = str(data["cote"]).replace(" ","_")
        # Write Json File
        with open(f"output/json/semsac_{name_file}.json", "w") as f:
            json.dump(json_Notices, f)
        # Write RDF
        self.convert_rdf(name_file,json.dumps(json_Notices))
        
    def convert_rdf(self,cote:str,json_ld_data_string: dict):
        
        print(cote)
        # Parse the data in the 'normal' RDFLib way, setting the format parameter to "json-ld"
        g = Graph()
        g.parse(data=json_ld_data_string, format="json-ld")
        # save in file
        g.serialize(f"output/turtle/semsac_{cote}.ttl",format="turtle")
    
    def __generate_json(self) -> dict:
        
        for idx, row in self.__dataset_Sacs.iterrows():
            # Convert to dict
            data = row.to_dict()
            sacs = self.__json_sac(data)
    
    def rowIndex(self,row):
        return row.name

    def __find_location(self,pm,dep,commun):
        
        dfAux = self.__dfLieux_db
        dfAux["short_vedette"] = dfAux["vedette"].apply(lambda x : re.search("(.+) (\\()",x).group(1))
        df = dfAux[dfAux["short_vedette"].str.contains(pm)]
        if len(df) == 1:
            return df["json"].iloc[0]
        if len(df) > 1:
            return df[(df["departement"] == dep) & (df["commune"] == commun)]["json"].iloc[0]
        else:
            return ""

    def read_notices(self):
        
        # Sacs
        self.__dataset_Sacs = self.__df_sacs(self.notices)
        
        # Castan
        _dfCastan = self.__dataset_Sacs[["cote","sac","castan"]]
        # Generate code JSON-LD pour chaque ligne
        dfCastan = _dfCastan[_dfCastan["castan"] != ""]
        dfCastan["sequence"] = dfCastan.groupby("cote").cumcount()+1
        dfCastan["json"] = dfCastan.apply(lambda x: self.__json_notice.get_castan(x.castan,x.sac,x.sequence),axis=1)
        self.__dataset_castan = dfCastan
        
        # Lieux des faits
        """ Insee """
        dfInsee_ = self.lieux[self.lieux["code_INSEE"] != ""]
        _dfinsee = dfInsee_[["code_INSEE"]]
        insee_unique = pd.Series(_dfinsee["code_INSEE"].to_list()).unique()
        dfInsee = pd.DataFrame(insee_unique, columns=["code_INSEE"])
        dfInsee["json"] = dfInsee["code_INSEE"].apply(lambda x: self.__json_notice.get_insee(x))
                
        """ Departemet """
        dfDepartement = self.lieux[self.lieux["type_lieu"] == "département"]
        dfOper_departament = pd.merge(dfDepartement, dfInsee, on="code_INSEE", how="inner")
        dfOper_departament.rename(columns={"json": "json_insee"},inplace=True)
        dfOper_departament["json"] = dfOper_departament.apply(lambda x : self.__json_notice.get_departement(x),axis=1)
        # Remove columns
        dfOper_departament.drop(columns=["code_INSEE","departement","commune","autre_lieu","type_lieu"],inplace=True)
        # Rename
        dfOper_departament.rename(columns={"vedette": "departement"},inplace=True)
        self.departements = dfOper_departament
        # Lieux
        dfLieux = self.lieux[self.lieux["type_lieu"] != "département"]
        dfLieux_insee = pd.merge(dfLieux, dfInsee, on="code_INSEE", how="left")
        dfLieux_insee.rename(columns={"json": "json_insee"},inplace=True)
        dfLieux_insee.replace(np.nan,"",inplace=True)
        dfLieux_insee["insee_uri"] = dfLieux_insee["json_insee"].apply(lambda x : x["@id"] if x != "" else "" )
        dfLieux_insee["typeLieu"] = dfLieux_insee["type_lieu"].apply(lambda x : self.filter_voc.find_voc_lieux(x))
        dfLieux_insee["departement_uri"] = dfLieux_insee["departement"].apply(lambda x : self.__find_departement(x))
        dfLieux_insee["json"] = dfLieux_insee.apply(lambda x : self.__json_notice.get_lieux_des_faits(x),axis=1)
        dfLieux_db = dfLieux_insee
        self.__dfLieux_db = dfLieux_db
        
        """ lieux_des_faits """
        df_lieux_des_faits = self.__df_lieux_des_faits(self.notices[["cote","lieux_des_faits"]])
        df_lieux_des_faits["departement_uri"] = df_lieux_des_faits["departement"].apply(lambda x : self.__find_departement(x))
        
        dfLieux = pd.merge(df_lieux_des_faits, dfInsee,on="code_INSEE",how="left")
        dfLieux.rename(columns={"json":"insee_uri"},inplace=True)
        
        if dfLieux["insee_uri"].iloc[0]:
            dfLieux["insee_uri"] = dfLieux["insee_uri"].iloc[0]["@id"]
        dfLieux["json"] = dfLieux.apply(lambda x : self.__json_notice.get_lieux_des_faits(x),axis=1)
        self.__dataset_lieux_des_faits = dfLieux
        
        """ Personnes """
        # Personnes Physiques
        dfPersonnesPhysiques = self.__df_personnes_physiques(self.notices[["cote"]])
        # Generate code JSON-LD pour chaque ligne
        dfPersonnesPhysiques["json"] = dfPersonnesPhysiques.apply(lambda x: self.__json_notice.get_personnes_physiques(x),axis=1)
        self.__dataset_PersonnesPhysiques = dfPersonnesPhysiques
        
        # Personnes Morales
        _dfPersonnesMorales = self.__df_personnes_morales(self.notices[["cote","personnes_morales"]])
        dfPersonnesMorales = _dfPersonnesMorales[_dfPersonnesMorales["personnes_morales"] != ""]
        if len(dfPersonnesMorales) > 0:
            # Get the json location
            dfPersonnesMorales["short_pm"] = dfPersonnesMorales["personnes_morales"].apply(lambda x : re.search("(.+) (\\()",x).group(1))
            # Read dataframe
            dfPersonnesMorales["json_location"] = dfPersonnesMorales.apply(lambda x : self.__find_location(x["short_pm"],x["departement"],x["commune"]),axis=1)
            
            #for i, row in dfPersonnesMorales.iterrows():
            #    row["json_location"] = self.__find_location(row["short_pm"],row["departement"],row["commune"])
                
            print(dfPersonnesMorales)
            dfPersonnesMorales.replace(np.nan,"")
            dfPersonnesMorales["json"] = dfPersonnesMorales.apply(lambda x : self.__json_notice.get_personnes_morales(x),axis=1)
            self.__dataset_PersonnesMorales = dfPersonnesMorales
        else:
            self.__dataset_PersonnesMorales = pd.DataFrame()
        
        # Jurisdiction
        dfJuridiction = self.__df_juridiction(self.notices[["Juridiction_1","Juridiction_2","Juridiction_3"]])
        dfJuridiction["lieu_siege"] = dfJuridiction["lieu_siege"].str.strip()
        dfJuridiction_lieux = pd.merge(dfJuridiction,dfLieux_db[["vedette","json"]], left_on="lieu_siege",right_on="vedette",how="left")
        dfJuridiction_lieux.rename(columns={"json":"json_location"},inplace=True)
        dfJuridiction_lieux.replace(np.nan,'',inplace=True)
        dfJuridiction_lieux["json"] = dfJuridiction_lieux.apply(lambda x: self.__json_notice.get_Juridiction(x),axis=1)
                
        # Jurisdiction 1
        dfJuridiction1 = self.notices[["cote","Juridiction_1"]]
        if len(dfJuridiction1) > 0:
            dfJuridiction_lieux.head()
            dfJ1 = dfJuridiction1[dfJuridiction1["Juridiction_1"] != ""]
            dfOutputJ1 = pd.merge(dfJ1,dfJuridiction_lieux[["Juridiction","json"]],left_on="Juridiction_1", right_on="Juridiction",how="left")
            self.__dataset_Jurisdiction1 = dfOutputJ1
        else:
            self.__dataset_Jurisdiction1 = pd.DataFrame()
        
        # Jurisdiction 2
        dfJuridiction2 = self.notices[["cote","Juridiction_2"]]
        if len(dfJuridiction2) > 0:
            dfJ2 = dfJuridiction2[dfJuridiction2["Juridiction_2"] != ""]
            dfOutputJ2 = pd.merge(dfJ2,dfJuridiction_lieux[["Juridiction","json"]],left_on="Juridiction_2", right_on="Juridiction",how="left")
            self.__dataset_Jurisdiction2 = dfOutputJ2
        else:
            self.__dataset_Jurisdiction2 = pd.DataFrame()
        
        # Jurisdiction 3
        dfJuridiction3 = self.notices[["cote","Juridiction_3"]]
        if len(dfJuridiction3) > 0:
            df3 = dfJuridiction3[dfJuridiction3["Juridiction_3"] != ""]
            dfOutputJ3 = pd.merge(df3,dfJuridiction_lieux[["Juridiction","json"]],left_on="Juridiction_3", right_on="Juridiction",how="left")
            self.__dataset_Jurisdiction3 = dfOutputJ3            
        else:
            self.__dataset_Jurisdiction3 = pd.DataFrame()
        
        # qualification_faits
        dfEventType, dfProc = self.__df_qualification_faits(self.notices[["cote","qualification_faits"]])
        if len(dfEventType) > 0:
            dfEventType["json"] = dfEventType.apply(lambda x: self.__json_notice.get_qualification_faits(x),axis=1)
            self.__dataset_QF_EventType = dfEventType
        else:
            self.__dataset_QF_EventType = pd.DataFrame()
        if len(dfProc) > 0:
            dfProc["json"] = dfProc.apply(lambda x: self.__json_notice.get_qualification_faits(x),axis=1)
            self.__dataset_QF_Proc = dfProc
        else:
            self.__dataset_QF_Proc = pd.DataFrame()
        
        # Liasses
        _dfLiasses = self.__df_liasses(self.notices[["cote","reference_sacs_liasse","intitule_liasse"]])
        dfLiasses = _dfLiasses[_dfLiasses["reference_sacs_liasse"] != ""]
        if len(dfLiasses) > 0:
            dfLiasses["json"] = dfLiasses.apply(self.__json_notice.get_liasse, axis=1)
            self.__dataset_Liasses = dfLiasses
        else:
            self.__dataset_Liasses = pd.DataFrame()
        
        # cotes_associees
        self.__dataset_CotesAssociees = self.__df_cotes_associees(self.notices[["cote","cotes_associees"]])
        
        self.__generate_json()