from .tools import parseJsonTerm, generate_id, eval_type_data # Tools
import pandas as pd
import logging

class notices:
    
    def __init__(self,notices:dict, tempate_notice:dict, datamarts):
        self.data_notice = notices
        self.template = tempate_notice
        self.output = {}
        self.logger = logging.getLogger(__name__)
        # Datasets
        self.__df_personnes_physiques = datamarts.get_personnePhysique()
        self.__df_personnesmorales = datamarts.get_personnesmorales()
        self.__df_jurisdictions = datamarts.get_jurisdictions()
        self.__df_lieux = datamarts.get_lieux()
        
        # Vocabularies
        self.__voc_sexe = datamarts.get_voc_sexe()
        self.__voc_profession = datamarts.get_voc_profession()
        self.__voc_collectivite = datamarts.get_voc_collectivite()
        self.__voc_Juridiction = datamarts.get_voc_Juridiction()
        self.__voc_lieu = datamarts.get_voc_Lieux()
        self.__voc_qualification_faits = datamarts.get_voc_qualifFaits()
    
    # Datasets    
    def __find_personnes_physiques(self,coteId:str) -> pd.DataFrame:
        return self.__df_personnes_physiques[self.__df_personnes_physiques["cote"] == coteId]

    def __find_personnes_morales(self,personName:str) -> pd.DataFrame:
        return self.__df_personnesmorales[self.__df_personnesmorales["personnes_morales"] == personName.strip()]

    def __find_jurisdictions(self,forme) -> pd.DataFrame:
        return self.__df_jurisdictions[self.__df_jurisdictions["forme_autorisee"] == forme]

    def __find_lieux(self,lieuId:str) -> pd.DataFrame:
        # Filter for the place name
        return self.__df_lieux[self.__df_lieux["vedette"] == lieuId.strip()]
    
    # Vocabularies
    def __find_voc_sexe(self,sexeId):
        try:
            if self.__voc_sexe[sexeId]:
                return self.__voc_sexe[sexeId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the sexe {sexeId}")
            return None
    
    def __find_voc_profession(self,professionId):
        
        try:
            if self.__voc_profession[professionId]:
                return self.__voc_profession[professionId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the profession {professionId}")
            return None  
    
    def __find_voc_collectivite(self,typePersonneMorale):
        
        try:
            if self.__voc_collectivite[typePersonneMorale]:
                return self.__voc_collectivite[typePersonneMorale]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the collectivite {typePersonneMorale}")
            return None
    
    def __find_voc_juridiction(self,typeId):
        try:
            if self.__voc_Juridiction[typeId]:
                return self.__voc_Juridiction[typeId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the Juridiction {typeId}")
            return None
    
    def __find_voc_lieux(self,typeId):
        try:
            if self.__voc_lieu[typeId.strip()]:
                return self.__voc_lieu[typeId.strip()]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the Lieux {typeId}")
            return None
    
    def __find_voc_qualification_faits(self,typeId):
        
        try:
            if self.__voc_qualification_faits[typeId]:
                return self.__voc_qualification_faits[typeId]["Concept URI"]
        except Exception as e:
            self.logger.warning(f"Cannot find the qualification faits {typeId}")
            return None
    
    
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

    # Notices 
    def _set_cote(self, cote_name:str,cote_uri,templateJsonLd) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],cote_uri)
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],cote_name)
        
        return templateJsonLd
        
    def get_cote(self) -> dict:
        
        cote = self.data_notice["cote"]  # Description of cote
        cote_Id = self.data_notice["cote_id"] # Id unique
        templateJsonLd = self.template["cote"]
        
        return self._set_cote(cote,cote_Id,templateJsonLd)
        
    def _set_castan(self, valueNotice,notices,templateJsonLd: dict):
        
        idx = notices.index(valueNotice)+1
        # For castan notice set 2 values 
        templateJsonLd["@id"] = templateJsonLd["@id"].format(generate_id(),str(idx))
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],valueNotice.strip())
        
        return templateJsonLd
    
    def get_castan(self):
        
        __castan_data = self.data_notice["castan"]  # Description of cote
        templateJsonLd = self.template["castan"] # Template
        
        data = eval_type_data(__castan_data)        
        if isinstance(data,list):
            output_result = []
            for d in data:
                tmp_json_ld = templateJsonLd.copy()
                # Call function
                tmp = self._set_castan(d,data,tmp_json_ld)
                # Save in list
                output_result.append(tmp)
            return output_result

        if isinstance(data,str):
            return self._set_castan(data,data,templateJsonLd)

    # Notices Dates
    def __set_date(self,data,templateJsonLd:dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        templateJsonLd["rico:expressedDate"] = self.__update_value_jsonLd(templateJsonLd["rico:expressedDate"],data)
        templateJsonLd["rico:normalizedDateValue"] = self.__update_value_jsonLd(templateJsonLd["rico:normalizedDateValue"],data)
        
        return templateJsonLd
    
    def get_date_debut(self):
        
        data = self.data_notice["date_debut"]  # Get data of date_debut
        templateJsonLd = self.template["date_debut"]
        
        if data:
            return self.__set_date(data,templateJsonLd)
    
    def get_date_fin(self):
        data = self.data_notice["date_fin"]  # Get data of date_find
        templateJsonLd = self.template["date_fin"]
        
        if data:
            return self.__set_date(data,templateJsonLd)
    
    # Notice intitule
    def __set_intitule(self,data,templateJsonLd: dict) -> dict:
        
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],data)
        
        return templateJsonLd
        
    def get_intitule(self):
        
        data = self.data_notice["intitule"]  # Get data of date_find
        templateJsonLd = self.template["intitule"]
        
        if data:
            return self.__set_intitule(data,templateJsonLd)
        
    def __set_presentation_contenu(self): 
        return ""
    
    def get_presentation_contenu(self):	
        return self.__set_presentation_contenu()
    
    # Notices Personnes
    def __set_personnes_physiques(self,person: dict, templateJsonLd:dict):
        
        # @id
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        # rico:name
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],person["nom"])
        # Sexe
        sexe = person["sexe_supposé"].replace('F','Femme').replace('H','Homme').replace('Indéterminé','Indéterminé')
        sexePersonne =  self.__find_voc_sexe(sexe)
        if sexePersonne:
            templateJsonLd["rico:hasOrHadDemographicGroup"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadDemographicGroup"],sexePersonne)
        else:
            # remove the key dictionary
            del templateJsonLd["rico:hasOrHadDemographicGroup"]
            
        # Type of Profession
        typeOfProfession = self.__find_voc_profession(person["type_profession"])
        if typeOfProfession:
            templateJsonLd["rico:hasOrHadOccupationOfType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadOccupationOfType"],typeOfProfession)
        else:
            # remove the key dictionary
            del templateJsonLd["rico:hasOrHadOccupationOfType"]
        return templateJsonLd
        
    def get_personnes_physiques(self):
        
        output_pp = []
        #
        coteId = self.data_notice["cote"]
        templateJsonLd = self.template["personnes_physiques"]
        # find properties of the personn physique
        dfDataset = self.__find_personnes_physiques(coteId) 
        # Read Dataframe
        if dfDataset.size > 0:
            for index,row in dfDataset.iterrows():
                # copy template
                tmp_json_ld = templateJsonLd.copy()
                # Populate template
                output_pp.append(self.__set_personnes_physiques(row.to_dict(),tmp_json_ld))
            return output_pp
    
    def __set_personnes_morales(self, person:dict,templateJsonLd: dict ) -> dict:
        
        # @id
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        # rico:name
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],person["personnes_morales"])
        # Set type of personnes morales
        if self.__find_voc_collectivite(person["type_personne_morale"]) is not None:
            templateJsonLd["rico:hasOrHadCorporateBodyType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadCorporateBodyType"],self.__find_voc_collectivite(person["type_personne_morale"]))                                                            
        else:
            del templateJsonLd["rico:hasOrHadCorporateBodyType"]
        
        return templateJsonLd
        
    def get_personnes_morales(self):
        
        # Notice Value
        data = eval_type_data(self.data_notice["personnes_morales"])
        # Template
        templateJsonLd = self.template["personnes_morales"]
        # Save in list
        output = []
        if isinstance(data, list):
            for pm in data:                
                # find properties of the personnes morales                
                dfDataset = self.__find_personnes_morales(pm)
                # Read Dataframe
                if dfDataset.size > 0:
                    for idx, row in dfDataset.iterrows():
                        __tmp_json = templateJsonLd.copy()
                        output.append(self.__set_personnes_morales(row.to_dict(),__tmp_json))
            return output
        if isinstance(data, str):
            # find properties of the personnes morales
            dfDataset = self.__find_personnes_morales(data)
            # Read Dataframe
            if dfDataset.size > 0:
                for idx, row in dfDataset.iterrows():
                    output.append(self.__set_personnes_morales(row.to_dict(),templateJsonLd))
        return output
    
    # Notices Juridiction
    def __set_Juridiction_1(self, data:dict,Juridiction1:str,templateJsonLd: dict) -> dict:
        
        # @id
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        # rico:name
        templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],Juridiction1)
        
        # find type
        typeOfJuridiction = data["type_juridiction"]
        if "/" in typeOfJuridiction:
            listOftypes = str(typeOfJuridiction).split("/")
            for typeOfId in listOftypes:
                # find typeId in the vocabulary
                typeId = self.__find_voc_juridiction(typeOfId.strip().lower())
                if typeId is not  None:
                    templateJsonLd["rico:hasOrHadLegalStatus"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadLegalStatus"],typeId)
                else:
                    del templateJsonLd["rico:hasOrHadLegalStatus"]
        else:
            typeId = self.__find_voc_juridiction(typeOfJuridiction.strip().lower())
            if typeId is not  None:
                templateJsonLd["rico:hasOrHadLegalStatus"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadLegalStatus"],typeId)
            else:
                del templateJsonLd["rico:hasOrHadLegalStatus"]
            
        return templateJsonLd
    
    def get_Juridiction_1(self):
        
        Juridiction1 = self.data_notice["Juridiction_1"]
        # Notice Value
        data_juridiction = eval_type_data(Juridiction1)
        # Template
        templateJsonLd = self.template["Juridiction_1"]
        # Save in list
        output = []
        
        if isinstance(data_juridiction,list):
            for j in data_juridiction:
                # Filter with the value and get information
                dfJuridiction = self.__find_jurisdictions(j)
                if dfJuridiction.size > 0:
                    # Loop dataframe and bulding the graph
                    for idx, row in dfJuridiction.iterrows():
                        __tmp_json = templateJsonLd.copy()
                        output.append(self.__set_Juridiction_1(row.to_dict(),j,__tmp_json))
            return output  
        
        if isinstance(data_juridiction,str):
            # Filter with the value and get information
            dfJuridiction = self.__find_jurisdictions(data_juridiction)
            if dfJuridiction.size > 0:
                # Loop dataframe and bulding the graph
                for idx, row in dfJuridiction.iterrows():
                    output.append(self.__set_Juridiction_1(row.to_dict(),Juridiction1,templateJsonLd))

        return output
    
    def __set_lieux_des_faits(self, lieu:str,templateJsonLd: dict) -> dict:
        
        # Filter for the place name
        dfPivot = self.__find_lieux(lieu)
        if dfPivot.size > 0:
            for idx, row in dfPivot.iterrows():                
                templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
                # Name
                templateJsonLd["rico:name"] = self.__update_value_jsonLd(templateJsonLd["rico:name"],str(row["vedette"]))
                # Type Lieu
                typeLieux = self.__find_voc_lieux(row["type_lieu"])
                if typeLieux is not None:
                    templateJsonLd["rico:hasOrHadPlaceType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasOrHadPlaceType"],typeLieux)
                else:
                    del templateJsonLd["rico:hasOrHadPlaceType"]
            return templateJsonLd
        else:
            return None
        
    def get_lieux_des_faits(self):
        
        lieux_des_faits = self.data_notice["lieux_des_faits"]
        # Notice Value
        data = eval_type_data(lieux_des_faits)
        # Template
        templateJsonLd = self.template["lieux_des_faits"]
        #
        if isinstance(data,list):
            return [self.__set_lieux_des_faits(lieu,templateJsonLd.copy()) for lieu in data if self.__set_lieux_des_faits(lieu,templateJsonLd.copy()) is not None]
            
        if isinstance(data,str):
            if self.__set_lieux_des_faits(data,templateJsonLd) is not None:
                return self.__set_lieux_des_faits(data,templateJsonLd)
        
    # Notice qualification_faits
    def __value__qualification_faits_type(self,qf:str):
        
        # find in the vocabulary
        if isinstance(eval_type_data(qf),list):
            new_value = [e.strip() for e in eval_type_data(qf)]
            return [self.__find_voc_qualification_faits(typeQf.strip()) for typeQf in new_value if self.__find_voc_qualification_faits(typeQf) is not None]
        if isinstance(eval_type_data(qf),str):
            return self.__find_voc_qualification_faits(qf.strip())        
    
    def __set_qualification_faits(self,qf:str,templateJsonLd: dict) -> dict:
        
        # Generate URI
        templateJsonLd["@id"] = self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        # find in the vocabulary
        evalue_type = self.__value__qualification_faits_type(qf)
        if evalue_type:
            print(evalue_type)
            if isinstance(evalue_type,list):
                if len(evalue_type) > 0:
                    templateJsonLd["rico:hasEventType"] = [self.__update_value_jsonLd(templateJsonLd["rico:hasEventType"],qfType) for qfType in evalue_type]
            if isinstance(evalue_type,str):
                templateJsonLd["rico:hasEventType"] = self.__update_value_jsonLd(templateJsonLd["rico:hasEventType"],evalue_type)
                
        else:
            del templateJsonLd["rico:hasEventType"]
        return templateJsonLd
    
    def get_qualification_faits(self):
        
        qualification_faits = self.data_notice["qualification_faits"]
        templateJsonLd = self.template["qualification_faits"]
        #
        if qualification_faits is not None:
            return self.__set_qualification_faits(qualification_faits.strip(''),templateJsonLd)
    
    def __set_liasse(self,title:str,cotes,templateJsonLd: dict) -> dict:
        
        templateJsonLd["@id"]: self.__update_value_jsonLd(templateJsonLd["@id"],generate_id())
        templateJsonLd["rico:title"]: self.__update_value_jsonLd(templateJsonLd["@id"],title)
        
        #"rico:hadComponent": "{}",
        return templateJsonLd
    
    def get_liasse(self):
        # Title        
        reference_sacs_liasse = self.data_notice["reference_sacs_liasse"]
        # List of cotes
        intitule_liasse = self.data_notice["reference_sacs_liasse"]
        cotes = eval_type_data(intitule_liasse)
        # Template
        templateJsonLd = self.template["liasse"]
        
        if reference_sacs_liasse is not None or reference_sacs_liasse != "":
            return self.__set_liasse(reference_sacs_liasse,cotes,templateJsonLd)
            
        
    
    """    	
    def __set_Juridiction_2
    def get_Juridiction_2	
    def __set_Juridiction_3
    def get_Juridiction_3	
    
    	
    def __set_reference_sacs_liasse
    def get_reference_sacs_liasse	
    def __set_intitule_liasse
    def get_intitule_liasse
    def __set_nb_pieces
    def get_nb_pieces	
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