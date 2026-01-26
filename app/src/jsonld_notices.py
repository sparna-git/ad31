from .tools import generate_id, generate_json_URI, convert_to_turtle, write_json_file, convert_json_context
from .semsac_notices import semsac
from .template.template_notices import templatesJSON
import pandas as pd
import numpy as np
import logging

class convert:
    
    # https://pandas.pydata.org/docs/user_guide/copy_on_write.html#
    pd.options.mode.copy_on_write = True
    
    def __init__(self):

        
        self.logger = logging.getLogger(__name__)   

        self.__resource = semsac()
        self.__dfSacCote = self.__resource.get_sac_cote()
        self.__dfCastan = self.__resource.get_castan()
        self.__dfTitle = self.__resource.get_titles()
        self.__dfj1 = self.__resource.get_juridiction_1()
        self.__dfj2 = self.__resource.get_juridiction_2()
        self.__dfj3 = self.__resource.get_juridiction_3()
        self.__dfDateDebut = self.__resource.get_date_debut()
        self.__dfDatefin = self.__resource.get_date_fin()
        self.__dfpersonnes_physiques = self.__resource.get_personnes_physiques()
        self.__dfpersonnes_morale = self.__resource.get_personnes_morales()
        self.__dfQFaits,self.__dfQFaitsProc = self.__resource.get_qualification_faits()
        self.__dfLieuxdesFaits = self.__resource.get_lieux_des_faits()
        self.__dfcoteAssociees = self.__resource.get_cotes_associees()
        self.__dfLaisses = self.__resource.get_liasses()
        self.__dfInstructions = self.__resource.get_instruction_juridiction()
        self.__ark_code = self.__resource.get_ark_code()        

        # Directory output notices
        self.__get_directory_notices = self.__resource.get_directory_notices

        # Templates JSON-LD
        __templateJsonLD = self.__resource.datamarts.get_conf_notices()
        self.__json_notice = templatesJSON(__templateJsonLD) 
    
    def __physicalCharacteristicsNote(self,intitule_liasse:str):

        if "/" in intitule_liasse:
            split_data = intitule_liasse.strip().split("/")
            del split_data[0]
            return "".join(split_data).strip()
        else:
            return ""
        
    def __procedure_participant(self,coteId:str):

        json_code = []
        json_ids = []

        pp = self.__dfpersonnes_physiques[self.__dfpersonnes_physiques["cote"] == coteId]["json"].to_list()
        if len(pp) > 0:
            [json_code.append(c) for c in pp]
            [json_ids.append(c["@id"]) for c in pp]

        pm = self.__dfpersonnes_morale[self.__dfpersonnes_morale["cote"] == coteId]["json"].to_list()
        if len(pm) > 0:
            [json_code.append(c) for c in pm if c != ""]
            [json_ids.append(c["@id"]) for c in pm if c != ""]
        
        return json_code,json_ids

    def __procedure_fait(self,coteId:str) -> dict:
        
        fait = {}
        
        name = self.__dfTitle[self.__dfTitle["cote"] == coteId]["intitule"].iloc[0]
        if name != "":
            fait["name"] = f"Fait de la procédure \"{name}\""
        else:
            fait["name"] = ""

        EventType = self.__dfQFaits
        fait["EventType"] = EventType[EventType["cote"] == coteId]["EventType"].to_list()
        # 
        lieux_des_faits = self.__dfLieuxdesFaits[self.__dfLieuxdesFaits["cote"]== coteId]
        if len(lieux_des_faits) > 0:
            fait["hasOrHadLocation"] = lieux_des_faits["json"].iloc[0]     
        else:
            fait["hasOrHadLocation"] = ""
        
        return self.__json_notice.get_fait(fait)
        
    def __sac_procedure(self,coteId:str) -> dict:

        procedure_ = {}
        
        procedure_["id"] = str(self.__dfTitle[self.__dfTitle["cote"] == coteId]["procedure"].iloc[0])

        intitule = self.__dfTitle[self.__dfTitle["cote"] == coteId]["intitule"].iloc[0]
        if intitule != "":
            procedure_["name"] = intitule
        else:
            procedure_["name"] = ""

        # Une procédure est ouverte suite à des faits,index=False
        type_Of_procedure = self.__dfQFaitsProc[self.__dfQFaitsProc["cote"] == coteId]
        if len(type_Of_procedure) > 0:
            procedure_["hasActivityType"] = type_Of_procedure["procType"].iloc[0]
        else:
            procedure_["hasActivityType"] = ""
        
        procedure_["resultsOrResultedFrom"] = self.__procedure_fait(coteId)
        
        # Une procédure est un ensemble d’instructions
        resultInstruction = self.__dfInstructions[self.__dfInstructions["cote"] == coteId]
        if len(resultInstruction) > 0:
            procedure_["hasOrHadSubevent"] = resultInstruction["json"].iloc[0]
        else:
            procedure_["hasOrHadSubevent"] = ""
        # Une procédure implique des parties

        # Une procédure peut être liée à une autre procédure
        ca = self.__dfcoteAssociees[self.__dfcoteAssociees["cote"] == coteId]["procedure_cote"].to_list()
        if len(ca) > 0:
            procedure_["isAssociatedWithEvent"] = ca
        else:
            procedure_["isAssociatedWithEvent"] = ""

        # Participant
        participan_code, participan_ids = self.__procedure_participant(coteId)
        if len(participan_code) > 0:
            procedure_["hasOrHadParticipant"] = participan_code
        else:
            procedure_["hasOrHadParticipant"] = ""

        # Date
        date_debut = self.__dfDateDebut[self.__dfDateDebut["cote"] == coteId]
        if len(date_debut) > 0:
            procedure_["hasBeginningDate"] = date_debut["json"].iloc[0]
        else:
            procedure_["hasBeginningDate"] = ""
        
        date_fin = self.__dfDatefin[self.__dfDatefin["cote"] == coteId]
        if len(date_fin) > 0:
            procedure_["hasEndDate"] = date_fin["json"].iloc[0]            
        else:
            procedure_["hasEndDate"] = ""
        
        json_procedure = self.__json_notice.get_procedure(procedure_)
        return json_procedure

    def __sac_identifiers(self,coteId:str):

        json_code = []
        json_id = []

        # Cote
        cote_json = self.__dfSacCote[self.__dfSacCote["cote"] == coteId]["json"].iloc[0]
        json_code.append(cote_json)
        json_id.append(cote_json["@id"])

        # Castan
        castan = self.__dfCastan[self.__dfCastan["cote"] == coteId]["json"].to_list()
        if len(castan) > 0:
            [json_code.append(castanjson) for castanjson in castan]
            [json_id.append(castanjson["@id"]) for castanjson in castan]

        return json_code, json_id

    def __sac_wasComponentOf(self, coteId:str):
        
        intitule_liasse = self.__dfTitle[self.__dfTitle["cote"] == coteId]["intitule_liasse"].iloc[0]
        if intitule_liasse != "":
            df = self.__dfLaisses[self.__dfLaisses["intitule_liasse"] == intitule_liasse]
            if len(df) > 0:
                ids = [idLaisse["@id"] for idLaisse in df["json"].to_list()]
                code_json = df.json.to_list()
                return code_json,list(dict.fromkeys(ids))
            else:
                return [],[]
        else:
            return [],[]

    def __sac_hasOrganicProvenance(self,coteId:str):

        json_code = []
        json_ids = []
        # Juridiction 1
        dfJ1 = self.__dfj1[(self.__dfj1["cote"] == coteId) & (self.__dfj1["Role_juridiction_1"] == "Producteur")]["json"].to_list()
        if len(dfJ1) > 0:
            [json_code.append(j1_code) for j1_code in dfJ1 if j1_code != ""]
            [json_ids.append(j1_code["@id"]) for j1_code in dfJ1 if j1_code != ""]

        # Juridiction 2
        dfJ2 = self.__dfj2[(self.__dfj2["cote"] == coteId) & (self.__dfj2["Role_juridiction_2"] == "Producteur")]["json"].to_list()
        if len(dfJ2) > 0:
            [json_code.append(j2_code) for j2_code in dfJ2 if j2_code != ""]
            [json_ids.append(j2_code["@id"]) for j2_code in dfJ2 if j2_code != ""]

        # Juridiction 3
        dfJ3 = self.__dfj3[(self.__dfj3["cote"] == coteId) & (self.__dfj3["Role_juridiction_3"] == "Producteur")]["json"].to_list()
        if len(dfJ3) > 0:
            [json_code.append(j3_code) for j3_code in dfJ3 if j3_code != ""]
            [json_ids.append(j3_code["@id"]) for j3_code in dfJ3 if j3_code != ""]

        return json_code, json_ids
    
    def __sac_document(self,sac:str,coteId:str) -> dict:
        
        doc = {}
        
        doc["id"] = str(self.__dfTitle[self.__dfTitle["cote"] == coteId]["document"].iloc[0])
        doc["name"] = f"Notice {coteId}"

        presentation_contenu = self.__dfTitle[self.__dfTitle["cote"] == coteId]["presentation_contenu"].iloc[0]
        if presentation_contenu != "":
            doc["generalDescription"] = presentation_contenu
        else:
            doc["generalDescription"] = ""

        nb_pieces = self.__dfTitle[self.__dfTitle["cote"] == coteId]["nb_pieces"].iloc[0]
        if nb_pieces != "":
            doc["nb_pieces"] = nb_pieces
        else:
            doc["nb_pieces"] = ""

        ark_code_identifier = self.__ark_code[self.__ark_code["unitid"] == coteId]
        if len(ark_code_identifier) > 0:
            if ark_code_identifier["json"].iloc[0] != "":
                doc["ark_identifier"] = ark_code_identifier["json"].iloc[0]["@id"]
            else:
                doc["ark_identifier"] = ""
        else:
            doc["ark_identifier"] = ""

        doc["hasOrHadInstantiation"] = str(sac)
        
        doc["procedure"] = str(self.__dfTitle[self.__dfTitle["cote"] == coteId]["procedure"].iloc[0])
        
        return self.__json_notice.get_document(doc)

    # Creer SAC
    def __generate_sac(self,notice:dict) -> dict:
        
        print(f"Sac {notice["cote"]}")
        
        json_code = []
        dictSac = {}

        dictSac["id"] = notice["sac"]
        dictSac["name"] = f"Sac {notice["cote"]}"

        # Identifiers
        identifier_json,identifiers_id = self.__sac_identifiers(notice["cote"])
        [json_code.append(c) for c in identifier_json]
        dictSac["identifier"] = identifiers_id

        wasComponentOf_code_json, wasComponentOf_ids = self.__sac_wasComponentOf(notice["cote"])
        if len(wasComponentOf_code_json) > 0:
            [json_code.append(c) for c in wasComponentOf_code_json]
            dictSac["wasComponentOf_ids"] = wasComponentOf_ids
        else:
            dictSac["wasComponentOf_ids"] = ""
        
        # État de conservation des documents
        #pcn = self.__physicalCharacteristicsNote(self.__dfTitle[self.__dfTitle["cote"] == notice["cote"]]["intitule_liasse"].iloc[0])
        #if pcn != "":
        #    dictSac["physicalCharacteristicsNote"] = pcn
        #else:
        dictSac["physicalCharacteristicsNote"] = ""
        
        # Caractéristiques matérielles du sac STRUCTURE
        odd = self.__dfTitle[self.__dfTitle["cote"] == notice["cote"]]["odd"].iloc[0]
        if odd != "":
            dictSac["structure"] = odd
        else:
            dictSac["structure"] = ""
        
        juridiction_code,juridiction_Ids = self.__sac_hasOrganicProvenance(notice["cote"])
        if len(juridiction_Ids) > 0:
            [json_code.append(c) for c in juridiction_code]
            dictSac["hasOrganicProvenance"] = juridiction_Ids
        else:
            dictSac["hasOrganicProvenance"] = ""

        
        # Procedure
        dictSac["procedure"] = self.__sac_procedure(notice["cote"])["@id"]
        json_code.append(self.__sac_procedure(notice["cote"]))
        
        # Document or Notices
        dictSac["document"] = self.__sac_document(notice["sac"],notice["cote"])
        json_code.append(self.__sac_document(notice["sac"],notice["cote"]))


        # Identifier Document
        ark_code_identifier = self.__ark_code[self.__ark_code["unitid"] == notice["cote"]]
        if len(ark_code_identifier) > 0:
            if ark_code_identifier["json"].iloc[0] != "":
                json_code.append(ark_code_identifier["json"].iloc[0])

        # Generate JSON du SAC
        json_code.append(self.__json_notice.generate_sacs(dictSac))

        # Ecrir fichier JSON pour chaque notice
        name_file = str(notice["cote"]).replace(" ","_")
        
        # Generate JSON 
        json_output_string = convert_json_context(json_code)
        # Ecrir un fichier JSON
        #write_json_file(f"{self.__get_directory_notices}/semsac_{name_file}.json",json_output_string)
        # Ecrir un fichier graph
        convert_to_turtle(f"{self.__get_directory_notices}/semsac_{name_file}.ttl",json_output_string)

    # Leture de notices
    def read_notices(self):
        # Read Notices
        print("Creation de documents")
        self.__dfSacCote.apply(lambda x : self.__generate_sac(x),axis=1)