from .tools import generate_id, eval_type_data, generate_json_URI
import logging
import numpy as np

class sacsnotices:
    
    def __init__(self,tempate_notice:dict, filter):
        self.template = tempate_notice
        self.logger = logging.getLogger(__name__)
        # List of functions
        self.filters = filter
        
        # Update Lieux des Faits
        dfLF = filter.get_lieux()
        dfLF["type_voc"] = dfLF.apply(self.__add_type_lieux,axis=1)
        dfLF["departement_URI"] = dfLF.apply(self.__update_lieux_des_faits,axis=1)
        dfLF["insee_URI"] = dfLF.apply(self.__add_insee,axis=1)
        self.__db_lieux_des_faits = dfLF
    
    # update Lieux with Departamente Id
    def __add_type_lieux(self,data):
        
        if data["type_lieu"] is not None or data["type_lieu"] != "":
            return self.filters.find_voc_lieux(data["type_lieu"])
    
    def __update_lieux_des_faits(self, data):
        
        if data["type_lieu"] == "département":
            
            # Template
            templateJsonLd = self.template["lieux_des_faits"].copy()
            
            templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
            # Name
            templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],str(data["vedette"]))
            #
            if data["type_voc"]:
                templateJsonLd["rico:hasOrHadPlaceType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadPlaceType"],data["type_voc"])
            else:
                del templateJsonLd["rico:hasOrHadPlaceType"]
            
            return templateJsonLd
        else:
            return ""
    
    def __add_insee(self,data):
        
        if data["code_INSEE"]:
            # insee
            templateJsonLd = self.template["insee"].copy()
            
            templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
            templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["@id"],data["code_INSEE"])
            
            return templateJsonLd
        else:
            return ""
    
    def __find_lieux(self, lieu:str):
        return self.__db_lieux_des_faits[self.__db_lieux_des_faits["vedette"] == lieu.strip()]
    
    # JSON LD Construct
    def __eval_type_element(self,element):
        
        if isinstance(element,str):
            return element
        
        if isinstance(element,dict):
            return list(element.items())

    def __eval_values(self,Values,data):
    
        if isinstance(Values,str):
            return Values.format(data)
        
        if isinstance(Values,list):
            list_result = []
            for k, v in Values:
                if "{}" in v:
                    v = data
                list_result.append((k,v))
            
            newdict={}
            for k,v in list_result:
                if k not in newdict: 
                    newdict[k]=v
                else: 
                    newdict[k].append(v)
            return newdict

    def __update_value_jsonLd(self,jsonValue,dataValue):
        return self.__eval_values(self.__eval_type_element(jsonValue),dataValue)

    # SAC
    def _set_sacs(self,data:dict,templateJsonLd: dict):
        
        # Id 
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],data["id"])
        # Cote value
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["name"])
        # Identifiers
        templateJsonLd["rico:hasOrHadIdentifier"] = [self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadIdentifier"],identifier) for identifier in data["identifier"]]
        # Les sacs peuvent être issus d’une liasse
        if data["wasComponentOf_ids"]:
            templateJsonLd["rico:wasComponentOf"] = [self.__update_value_jsonLd(templateJsonLd["rico:wasComponentOf"],identifier) for identifier in data["wasComponentOf_ids"]]
        else:
            del templateJsonLd["rico:wasComponentOf"]
            
        #Les sacs sont le support d’un contenu informationnel (est une instanciation de)
        templateJsonLd["rico:isOrWasInstantiationOf"] = data["document"]
        # Les sacs ont été produits par une ou plusieurs juridictions
        if data["hasOrganicProvenance"]:
            templateJsonLd["rico:hasOrganicProvenance"] = [self.__update_value_jsonLd(templateJsonLd["rico:hasOrganicProvenance"],identifier) for identifier in data["hasOrganicProvenance"]]
        else:
            del templateJsonLd["rico:hasOrganicProvenance"]
            
        # Les documents documentent une procédure
        templateJsonLd["rico:documents"] =  data["procedure"]
    
        return templateJsonLd
    
    def generate_sacs(self,data: dict):
        return self._set_sacs(data,self.template["sacs"].copy())
    
    def __set_get_hasOrganicProvenance(self,notices:dict):
    
        output = []
        if notices["Juridiction_1"]:
            output.append(notices["Juridiction_1"]["@id"])
        if notices["Juridiction_2"]:
            output.append(notices["Juridiction_2"]["@id"])
        if notices["Juridiction_3"]:
            output.append(notices["Juridiction_3"]["@id"])
                
        if len(output) > 0:
            return [generate_json_URI(juridiction) for juridiction in output]
        return []
        
    def update_sacs(self, notices:dict, templateSac: dict):
        
        if notices["liasse"] is not None:
            URI = generate_json_URI(notices["liasse"]["@id"])
            
            templateSac["rico:wasComponentOf"] = URI #self.__update_value_jsonLd(templateSac["rico:wasComponentOf"],URI)
        else:
            del templateSac["rico:wasComponentOf"]
    
        #
        """
        juridictions = self.__set_get_hasOrganicProvenance(notices)
        if len(juridictions) > 0:
            templateSac["rico:hasOrganicProvenance"] = juridictions
        else:
            del templateSac["rico:hasOrganicProvenance"]
        """
    
        return templateSac
    
    def __get_URI_qualification_faits(self,qf):
        
        if isinstance(qf,list):
            return [generate_json_URI(q["@id"]) for q in qf]
        if isinstance(qf,str):
            return generate_json_URI(qf["@id"])
    
    # PROCEDURE
    def __get_uris_json(self, input):
        
        if isinstance(input,list):
            return [generate_json_URI(i["@id"]) for i in input]
            
        if isinstance(input,str):
            return generate_json_URI(input["@id"])
        
    def __set_procedure(self,data:dict,templateProcedure: dict):
        
        
        templateProcedure["@id"] = self.__update_value_jsonLd(templateProcedure["@id"],data["id"])
        templateProcedure["rico:name"] = self.__update_value_jsonLd(templateProcedure["rico:name"],data["name"])
        
        if data["hasActivityType"]:
            templateProcedure["rico:hasActivityType"] = data["hasActivityType"]
        else:
            del templateProcedure["rico:hasActivityType"]
            
        
        if data["resultsOrResultedFrom"]:
            templateProcedure["rico:resultsOrResultedFrom"] = data["resultsOrResultedFrom"]
        else:
            del templateProcedure["rico:resultsOrResultedFrom"]
        
        
        # Instruction 
        templateProcedure["rico:hasOrHadSubevent"] = "??"
        # Une procédure peut être liée à une autre procédure ??
        templateProcedure["rico:isAssociatedWithEvent"] = "???"        
        
        if data["hasOrHadParticipant"]:
            templateProcedure["rico:hasOrHadParticipant"] = data["hasOrHadParticipant"]
        else:
            del templateProcedure["rico:hasOrHadParticipant"]
        
        # Dates
        if data["hasBeginningDate"]:
            templateProcedure["rico:hasBeginningDate"] = data["hasBeginningDate"]
        else:
            del templateProcedure["rico:hasBeginningDate"]
            
        if data["hasEndDate"]:
            templateProcedure["rico:hasEndDate"] = data["hasEndDate"]
        else:
            templateProcedure["rico:hasEndDate"]
        
        return templateProcedure
    
    def get_procedure(self, data:dict):
        return self.__set_procedure(data,self.template["procedure"].copy())
  
    # Document
    def __set_document(self, data:dict,templateJsonLd: dict) -> dict:
        
        #
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],data["id"])
        #
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["name"])
        #
        if data["generalDescription"]:
            templateJsonLd["rico:generalDescription"] = self.__update_value_jsonLd(templateJsonLd["rico:generalDescription"],data["generalDescription"])
        else:
            del templateJsonLd["rico:generalDescription"]
            
        #
        if data["nb_pieces"]:
            templateJsonLd["rico:recordResourceExtent"] = self.__update_value_jsonLd(templateJsonLd["rico:recordResourceExtent"],data["generalDescription"])
        else:
            del templateJsonLd["rico:recordResourceExtent"]
            
        #
        templateJsonLd["rico:hasOrHadInstantiation"] = data["hasOrHadInstantiation"]
        #
        templateJsonLd["rico:documents"] = data["hasOrHadInstantiation"]
        
        return templateJsonLd
        
    def get_document(self, data:dict):
        return self.__set_document(data,self.template["document"].copy())
    
    # Fait
    def __set_fait(self, data:dict,templateJsonLd: dict) -> dict:
        
        #
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        #
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["name"])
        #
        if data["EventType"]:
            templateJsonLd["rico:hasEventType"] = data["EventType"]
        else:
            del templateJsonLd["rico:hasEventType"]
            
        #
        if data["Location"]:
            templateJsonLd["rico:hasOrHadLocation"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadLocation"],data["Location"])
        else:
            del templateJsonLd["rico:hasOrHadLocation"]
            
        return templateJsonLd
        
    def get_fait(self, data:dict):
        return self.__set_fait(data,self.template["fait"].copy())
    
    
    # Cote    
    def _set_cote(self, cote_name:str,cote_uri,templateJsonLd: dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],cote_uri)
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],cote_name)
        
        return templateJsonLd
        
    def get_cote(self, cote:str, cote_Id) -> dict:
        return self._set_cote(cote,cote_Id,self.template["cote"].copy())
        
    # Castan
    def __set_castan(self, castan:str, castanId:str,idx:str,templateJsonLd: dict):
        
        # For castan notice set 2 values 
        templateJsonLd["@id"] = templateJsonLd["@id"].format(castanId,idx)
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],castan)
        
        return templateJsonLd
    
    def get_castan(self, castan:str, castanId, idx):
        
        if castan != "": 
            return self.__set_castan(castan, castanId,str(idx),self.template["castan"].copy() )

    # Notices Dates
    def __set_date(self,procedureId,data,templateJsonLd:dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],procedureId)
        templateJsonLd["rico:expressedDate"] = self.__update_value_jsonLd(templateJsonLd["rico:expressedDate"],data)
        templateJsonLd["rico:normalizedDateValue"] = self.__update_value_jsonLd(templateJsonLd["rico:normalizedDateValue"],data)
        
        return templateJsonLd
    
    def get_date_debut(self, procedureId:str,date_debut:str):
        if date_debut:
            return self.__set_date(procedureId,date_debut,self.template["date_debut"].copy())
    
    def get_date_fin(self, procedureId:str,date_fin:str):
        if date_fin:
            return self.__set_date(procedureId,date_fin,self.template["date_fin"].copy())
    
    # Notice intitule
    def __set_intitule(self,data,templateJsonLd: dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data)
        
        return templateJsonLd
        
    def get_intitule(self,intitule:str):
        
        templateJsonLd = self.template["intitule"].copy()
        if intitule:
            return self.__set_intitule(intitule,templateJsonLd)
        
    def __set_presentation_contenu(self): 
        return ""
    
    def get_presentation_contenu(self):	
        return self.__set_presentation_contenu()
    
    # Notices Personnes
    def __set_personnes_physiques(self,person: dict, templateJsonLd:dict):
        
        # @id
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],person["id"])
        # rico:name
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],person["nom"])
        # Sexe
        if person["type_sexe"] != "":
            templateJsonLd["rico:hasOrHadDemographicGroup"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadDemographicGroup"],person["type_sexe"])
        else:
            # remove the key dictionary
            del templateJsonLd["rico:hasOrHadDemographicGroup"]
            
        # find Type of Profession
        if person["type_profession"] != "":
            templateJsonLd["rico:hasOrHadOccupationOfType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadOccupationOfType"],person["type_profession"])
        else:
            # remove the key dictionary
            del templateJsonLd["rico:hasOrHadOccupationOfType"]
            
        return templateJsonLd
        
    def get_personnes_physiques(self, pp:dict):
        
        templateJsonLd = self.template["personnes_physiques"].copy()
        # Read Dataframe
        if len(pp) > 0:
            return self.__set_personnes_physiques(pp,templateJsonLd.copy())
    
    def __set_personnes_morales(self, data:dict,templateJsonLd: dict ) -> dict:
        
        # @id
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        # rico:name
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["personnes_morales"])
        # Set type of personnes morales
        typePersonneMorale = data["type_pm"]
        if typePersonneMorale != "":
            templateJsonLd["rico:hasOrHadCorporateBodyType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadCorporateBodyType"],typePersonneMorale)                                                            
        else:
            del templateJsonLd["rico:hasOrHadCorporateBodyType"]
            
        if data["json_location"]:
            templateJsonLd["rico:agentHasOrHadLocation"] = data["json_location"]
        else:
            del templateJsonLd["rico:agentHasOrHadLocation"]
            
        return templateJsonLd
        
    def get_personnes_morales(self, data:dict):        
        return self.__set_personnes_morales(data,self.template["personnes_morales"].copy())
    
    # Notices Juridiction
    def __set_Juridiction(self, data:dict,templateJsonLd: dict) -> dict:
        
        # @id
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        # rico:name
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["Juridiction"])
        
        # find type
        typeOfJuridiction = data["type_voc"]
        if typeOfJuridiction != "":
            templateJsonLd["rico:hasOrHadLegalStatus"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadLegalStatus"],typeOfJuridiction)
        else:
            del templateJsonLd["rico:hasOrHadLegalStatus"]
        
        # Had location
        location = data["json_location"]
        if location:
            templateJsonLd["rico:agentHasOrHadLocation"] = location
        else:
            del templateJsonLd["rico:agentHasOrHadLocation"]
        
        return templateJsonLd
    
    def get_Juridiction(self, data):
        return self.__set_Juridiction(data,self.template["Juridiction_1"].copy())
    
    # lieux_des_faits
    def __set_lieux_des_faits(self, data:dict,templateJsonLd: dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        # Name
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["vedette"])
        # Type Lieu
        if data["typeLieu"] is not None:
            templateJsonLd["rico:hasOrHadPlaceType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadPlaceType"],data["typeLieu"])
        else:
            del templateJsonLd["rico:hasOrHadPlaceType"]
        
        # find departement
        if data["departement_uri"]:
            templateJsonLd["rico:isDirectlyContainedBy"] = self.__update_value_jsonLd(templateJsonLd["rico:isDirectlyContainedBy"],data["departement_uri"])
        else:
            del templateJsonLd["rico:isDirectlyContainedBy"]
                
        if data["insee_uri"]:
            templateJsonLd["rico:directlyContains"] = self.__update_value_jsonLd(templateJsonLd["rico:directlyContains"],data["insee_uri"])
        else:
            del templateJsonLd["rico:directlyContains"]                
        return templateJsonLd
        
    def get_lieux_des_faits(self,data:dict):
        return self.__set_lieux_des_faits(data,self.template["lieux_des_faits"].copy())
    
    # Lieux insee
    def __set_insee(self,data, templateJsonLd: dict):
        
        templateJsonLd["@id"] =  self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data)
        return templateJsonLd
        
    def get_insee(self,data):
        return self.__set_insee(data,self.template["insee"].copy())
    
    # Lieu département
    def __set_departement(self, data:dict, templateJsonLd: dict):
        
        templateJsonLd["@id"] =  self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["vedette"])
        
        get_insee_id = data["json_insee"]["@id"]
        if get_insee_id:
            templateJsonLd["rico:hasOrHadIdentifier"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadIdentifier"],get_insee_id)
        else:
            del templateJsonLd["rico:hasOrHadIdentifier"]
        
        return templateJsonLd
    
    def get_departement(self, data: dict):
        return self.__set_departement(data,self.template["departement"].copy())
        
    
    # Notice qualification_faits
    def __value__qualification_faits_type(self,qf:str):
        
        # find in the vocabulary
        if isinstance(eval_type_data(qf),list):
            new_value = [e.strip() for e in eval_type_data(qf)]
            return [self.filters.find_voc_qualification_faits(typeQf.strip()) for typeQf in new_value if self.filters.find_voc_qualification_faits(typeQf) is not None]
        if isinstance(eval_type_data(qf),str):
            return self.filters.find_voc_qualification_faits(qf.strip())        
    
    def __set_qualification_faits(self,data:dict,templateJsonLd: dict) -> dict:
        
        # Generate URI
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],data["id"])
        # find in the vocabulary
        if data["EventType"]:
            templateJsonLd["rico:hasEventType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasEventType"],data["EventType"])
        else:
            del templateJsonLd["rico:hasEventType"]
        
        return templateJsonLd
    
    def get_qualification_faits(self,qualification_faits):
        #
        if qualification_faits is not None:
            return self.__set_qualification_faits(qualification_faits,self.template["qualification_faits"].copy())
    
    # Liasses
    def __set_liasse(self,data:dict,templateJsonLd: dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],data["id"])
        templateJsonLd["rico:title"] = self.__update_value_jsonLd(templateJsonLd["rico:title"],data["reference_sacs_liasse"])
        
        if data["sac"]:
            templateJsonLd["rico:hadComponent"] = generate_json_URI(templateJsonLd["rico:hadComponent"].format(str(data["sac"])))
        else:
            del templateJsonLd["rico:hadComponent"]
            
        return templateJsonLd
    
    def get_liasse(self,data:dict):
        return self.__set_liasse(data,self.template["liasse"].copy())
    
    # Pieces      
    def __set_nb_pieces(self,data, templateJsonLd: dict):
        
        # 
        templateJsonLd["rico:recordResourceExtent"] = self.__update_value_jsonLd(templateJsonLd["rico:recordResourceExtent"],data)
        return templateJsonLd
        
    def get_nb_pieces(self, documentId:str,pieces):
        
        # Template
        if pieces is not None or pieces != "":
            return self.__set_nb_pieces(documentId,pieces, self.template["nb_pieces"].copy())
            
    
    
    
    """    	    	
    def __set_odd
    def get_odd	
    def __set_cotes_associees
    def get_cotes_associees	
    def __set_Role_juridiction_1
    def get_Role_juridiction_1	
    def __set_Role_juridiction_2
    def get_Role_juridiction_2	
    def __set_Role_juridiction_3
    def get_Role_juridiction_3
    """