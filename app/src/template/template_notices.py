from src.tools import generate_id, generate_json_URI
import logging
import numpy as np

class templatesJSON:
    
    def __init__(self,tempate_notice:dict):
        
        self.template = tempate_notice
        self.logger = logging.getLogger(__name__)
        
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

        #
        if data["physicalCharacteristicsNote"] != "":
            templateJsonLd["rico:physicalCharacteristicsNote"] = self.__update_value_jsonLd(templateJsonLd["rico:physicalCharacteristicsNote"],data["physicalCharacteristicsNote"])
        else:
            del templateJsonLd["rico:physicalCharacteristicsNote"]

        #
        if data["structure"]:
            templateJsonLd["rico:structure"] = self.__update_value_jsonLd(templateJsonLd["rico:structure"],data["structure"])
        else:
            del templateJsonLd["rico:structure"]

        # Les sacs peuvent être issus d’une liasse
        if data["wasComponentOf_ids"]:
            templateJsonLd["rico:wasComponentOf"] = [self.__update_value_jsonLd(templateJsonLd["rico:wasComponentOf"],identifier) for identifier in data["wasComponentOf_ids"]]
        else:
            del templateJsonLd["rico:wasComponentOf"]
            
        #Les sacs sont le support d’un contenu informationnel (est une instanciation de)
        if data["document"]:
            templateJsonLd["rico:isOrWasInstantiationOf"] = self.__update_value_jsonLd(templateJsonLd["rico:isOrWasInstantiationOf"],data["document"]["@id"])
        else:
            del templateJsonLd["rico:isOrWasInstantiationOf"]

        # Les sacs ont été produits par une ou plusieurs juridictions
        if data["hasOrganicProvenance"]:
            templateJsonLd["rico:hasOrganicProvenance"] = [self.__update_value_jsonLd(templateJsonLd["rico:hasOrganicProvenance"],identifier) for identifier in data["hasOrganicProvenance"]]
        else:
            del templateJsonLd["rico:hasOrganicProvenance"]
            
        # Les documents documentent une procédure
        templateJsonLd["rico:documents"] =  self.__update_value_jsonLd(templateJsonLd["rico:documents"],data["procedure"])
    
        return templateJsonLd
    
    def generate_sacs(self,data: dict):
        return self._set_sacs(data,self.template["sacs"].copy())

    # PROCEDURE         
    def __set_procedure(self,data:dict,templateProcedure: dict):
        
        
        templateProcedure["@id"] = self.__update_value_jsonLd(templateProcedure["@id"],data["id"])
        templateProcedure["rico:name"] = self.__update_value_jsonLd(templateProcedure["rico:name"],data["name"])
        
        if data["hasActivityType"]:
            templateProcedure["rico:hasActivityType"] = [self.__update_value_jsonLd(templateProcedure["rico:hasActivityType"],uri) for uri in data["hasActivityType"]]
        else:
            del templateProcedure["rico:hasActivityType"]
            
        
        if data["resultsOrResultedFrom"]:
            templateProcedure["rico:resultsOrResultedFrom"] = data["resultsOrResultedFrom"]
        else:
            del templateProcedure["rico:resultsOrResultedFrom"]
        
        # Instruction 
        if data["hasOrHadSubevent"]:
            templateProcedure["rico:hasOrHadSubevent"] = data["hasOrHadSubevent"]
        else:
            del templateProcedure["rico:hasOrHadSubevent"]

        # Une procédure peut être liée à une autre procédure ??
        if data["isAssociatedWithEvent"]:
            templateProcedure["rico:isAssociatedWithEvent"] = [self.__update_value_jsonLd(templateProcedure["rico:isAssociatedWithEvent"],id) for id in data["isAssociatedWithEvent"] if id != ""]            
        else:
            del templateProcedure["rico:isAssociatedWithEvent"]
        
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
            del templateProcedure["rico:hasEndDate"]
        
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
            templateJsonLd["rico:recordResourceExtent"] = self.__update_value_jsonLd(templateJsonLd["rico:recordResourceExtent"],data["nb_pieces"])
        else:
            del templateJsonLd["rico:recordResourceExtent"]
            
        if data["ark_identifier"] != "":
            templateJsonLd["rico:hasOrHadIdentifier"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadIdentifier"],data["ark_identifier"])
        else:
            del templateJsonLd["rico:hasOrHadIdentifier"]


        #
        if data["hasOrHadInstantiation"]:
            templateJsonLd["rico:hasOrHadInstantiation"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadInstantiation"], "https://data.archives.haute-garonne.fr/instanciation/{}".format(data["hasOrHadInstantiation"]))
        else:
            del templateJsonLd["rico:hasOrHadInstantiation"]

        #
        if data["procedure"]:
            templateJsonLd["rico:documents"] = self.__update_value_jsonLd(templateJsonLd["rico:documents"], "https://data.archives.haute-garonne.fr/evenement/{}".format(data["procedure"]))
        else:
            del templateJsonLd["rico:documents"]
        
        return templateJsonLd
        
    def get_document(self, data:dict):
        return self.__set_document(data,self.template["document"].copy())
    
    # Fait
    def __set_fait(self, data:dict,templateJsonLd: dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        #
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["name"])
        #
        if data["EventType"]:
            templateJsonLd["rico:hasEventType"] = [self.__update_value_jsonLd(templateJsonLd["rico:hasEventType"],id) for id in data["EventType"]]
        else:
            del templateJsonLd["rico:hasEventType"]
            
        #
        if data["hasOrHadLocation"]:
            templateJsonLd["rico:hasOrHadLocation"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadLocation"],data["hasOrHadLocation"])
        else:
            del templateJsonLd["rico:hasOrHadLocation"]
            
        return templateJsonLd
        
    def get_fait(self, data:dict):
        return self.__set_fait(data,self.template["fait"].copy())
    
    # Instruction
    def __set_instructions(self,data:dict,templateJsonLd: dict) -> dict:

        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],data["id"])
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["name"])
        
        if data["hasActivityType"]:
            templateJsonLd["rico:hasActivityType"] = [self.__update_value_jsonLd(templateJsonLd["rico:hasActivityType"],at) for at in data["hasActivityType"]]
        else:
            del templateJsonLd["rico:hasActivityType"]
        
        if data["isOrWasPerformedBy"]:
            templateJsonLd["rico:isOrWasPerformedBy"] = data["isOrWasPerformedBy"]
        else:
            del templateJsonLd["rico:isOrWasPerformedBy"]

        if data["precedesInTime"]:
            templateJsonLd["rico:precedesInTime"] = self.__update_value_jsonLd(templateJsonLd["rico:precedesInTime"],data["precedesInTime"])
        else:
            del templateJsonLd["rico:precedesInTime"]

        if data["followsInTime"]:
            templateJsonLd["rico:followsInTime"] = self.__update_value_jsonLd(templateJsonLd["rico:followsInTime"],data["followsInTime"])
        else:
            del templateJsonLd["rico:followsInTime"]

        return templateJsonLd

    def get_instruction(self,data):
        return self.__set_instructions(data,self.template["instruction"].copy())


    #########################################################
    #
    #   Créer json pour chaque colonne                       
    #
    #########################################################
    
    # Cote    
    def _set_cote(self, data:dict,templateJsonLd: dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],data["sac"])
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["cote"])
        
        return templateJsonLd
        
    def get_cote(self, data) -> dict:
        return self._set_cote(data,self.template["cote"].copy())
        
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
    def __set_date(self,procedureId,date,templateJsonLd:dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],procedureId)
        templateJsonLd["rico:expressedDate"] = self.__update_value_jsonLd(templateJsonLd["rico:expressedDate"],date)
        templateJsonLd["rico:normalizedDateValue"] = self.__update_value_jsonLd(templateJsonLd["rico:normalizedDateValue"],date)
        
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
            templateJsonLd["rico:agentHasOrHadLocation"] = self.__update_value_jsonLd(templateJsonLd["rico:agentHasOrHadLocation"],data["json_location"])
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
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["forme_autorisee"])
        
        # find type
        typeOfJuridiction = data["type_voc"]
        if isinstance(typeOfJuridiction,list):
            if typeOfJuridiction != "":
                templateJsonLd["rico:hasOrHadLegalStatus"] = [self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadLegalStatus"],idType) for idType in typeOfJuridiction]
            else:
                del templateJsonLd["rico:hasOrHadLegalStatus"]
        else:
            if typeOfJuridiction != "":
                templateJsonLd["rico:hasOrHadLegalStatus"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadLegalStatus"],typeOfJuridiction)
            else:
                del templateJsonLd["rico:hasOrHadLegalStatus"]
        
        # Had location
        location = data["json_location"]
        if location:
            templateJsonLd["rico:agentHasOrHadLocation"] = self.__update_value_jsonLd(templateJsonLd["rico:agentHasOrHadLocation"],location)
        else:
            del templateJsonLd["rico:agentHasOrHadLocation"]
        
        return templateJsonLd
    
    def get_Juridiction(self, data):
        return self.__set_Juridiction(data,self.template["Juridiction_1"].copy())
    
    # lieux_des_faits
    def __set_lieux_des_faits(self, data:dict,templateJsonLd: dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"], data["id"])
        # Name
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["vedette"])
        # Type Lieu
        if data["type"] is not None:
            templateJsonLd["rico:hasOrHadPlaceType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadPlaceType"],data["type"])
        else:
            del templateJsonLd["rico:hasOrHadPlaceType"]
        
        # find departement
        if data["json_isDirectlyContainedBy"]:
            templateJsonLd["rico:isDirectlyContainedBy"] = self.__update_value_jsonLd(templateJsonLd["rico:isDirectlyContainedBy"],data["json_isDirectlyContainedBy"]) 
        else:
            del templateJsonLd["rico:isDirectlyContainedBy"]

        if data["others"]:
            templateJsonLd["rico:directlyContains"] = self.__update_value_jsonLd(templateJsonLd["rico:directlyContains"],data["others"])
        else:
            del templateJsonLd["rico:directlyContains"]

        if data["code_INSEE"]:
            templateJsonLd["rico:hasOrHadIdentifier"] = self.get_insee(data)
        else:
            del templateJsonLd["rico:hasOrHadIdentifier"]

        if data["geocode"] != "":
            templateJsonLd["rico:geographicalCoordinates"] = self.__update_value_jsonLd(templateJsonLd["rico:geographicalCoordinates"],data["geocode"]) 
        else:
            del templateJsonLd["rico:geographicalCoordinates"]

        return templateJsonLd
        
    def get_lieux_des_faits(self,data:dict):
        return self.__set_lieux_des_faits(data,self.template["lieux_des_faits"].copy())

    # lieux_des_faits
    def __set_lieux_des_faits_directlyContains(self, data:dict,templateJsonLd: dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],data["json_isDirectlyContainedBy"])
        templateJsonLd["rico:directlyContains"] = self.__update_value_jsonLd(templateJsonLd["rico:directlyContains"],data["json"]["@id"])
        
        return templateJsonLd
        
    def get_lieux_des_faits_directlyContains(self,data:dict):
        return self.__set_lieux_des_faits_directlyContains(data,self.template["lieux_des_faits_directlyContains"].copy())
    

    def __set__geocode_lieux(self,data:dict, templateJsonLd: dict):
        
        templateJsonLd["@id"] =  self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        templateJsonLd["rico:geographicalCoordinates"] = self.__update_value_jsonLd(templateJsonLd["rico:geographicalCoordinates"],data)
        return templateJsonLd

    def get_geocode_lieux(self,data):
        return self.__set__geocode_lieux(data,self.template["geocode"].copy())

    # Lieux insee
    def __set_insee(self,data:dict, templateJsonLd: dict):
        
        templateJsonLd["@id"] =  self.__update_value_jsonLd(templateJsonLd["@id"],data["id"])
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["code_INSEE"])
        return templateJsonLd
        
    def get_insee(self,data):
        return self.__set_insee(data,self.template["insee"].copy())
    
    # Lieu département
    def __set_departement(self, data:dict, templateJsonLd: dict):
        
        templateJsonLd["@id"] =  self.__update_value_jsonLd(templateJsonLd["@id"],data["id"])
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["vedette"])

        if data["type"] != "":
            templateJsonLd["rico:hasOrHadPlaceType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadPlaceType"],data["type"])
        else:
            del templateJsonLd["rico:hasOrHadPlaceType"]
        
        if data["code_INSEE"]:
            templateJsonLd["rico:hasOrHadIdentifier"] = self.get_insee(data) #data["json_insee"]
        else:
            del templateJsonLd["rico:hasOrHadIdentifier"]
        
        return templateJsonLd
    
    def get_departement(self, data: dict):
        return self.__set_departement(data,self.template["departement"].copy())
        
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
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        templateJsonLd["rico:title"] = self.__update_value_jsonLd(templateJsonLd["rico:title"],data["title"])

        #
        if data["physicalCharacteristicsNote"] != "":
            templateJsonLd["rico:physicalCharacteristicsNote"] = self.__update_value_jsonLd(templateJsonLd["rico:physicalCharacteristicsNote"],data["physicalCharacteristicsNote"])
        else:
            del templateJsonLd["rico:physicalCharacteristicsNote"]
        
        if data["hadComponent"]:
            templateJsonLd["rico:hadComponent"] = [self.__update_value_jsonLd(templateJsonLd["rico:hadComponent"],str(sacId)) for sacId in data["hadComponent"]]
        else:
            del templateJsonLd["rico:hadComponent"]
            
        return templateJsonLd
    
    def get_liasse(self,data:dict):
        return self.__set_liasse(data,self.template["liasse"].copy())
    
    def __set_cotes_associees(self,data, templateJsonLd: dict):

        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        templateJsonLd["rico:isAssociatedWithEvent"] = self.__update_value_jsonLd(templateJsonLd["rico:isAssociatedWithEvent"],str(data["procedure_cote"]))

        return templateJsonLd

    def get_cotes_associees(self,data:dict):
        return self.__set_cotes_associees(data,self.template["cotes_associees"].copy())

    def __set_ark(self,data:dict,templateJsonLd: dict) -> dict:

        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],str(data["document"]))
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data["ark"])

        return templateJsonLd

    def get_ark(self,data:str):
        return self.__set_ark(data,self.template["ark"].copy())