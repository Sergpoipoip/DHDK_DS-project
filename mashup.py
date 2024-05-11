import pandas as pd
import re
from datetime import datetime
from data_model import *
from handlers import MetadataQueryHandler, ProcessDataQueryHandler

class BasicMashup(object):
    def __init__(self) -> None:
        self.metadataQuery = []
        self.processQuery = []
    
    def cleanMetadataHandlers(self) -> bool:
        self.metadataQuery = []
        return True

    def cleanProcessHandlers(self) -> bool:
        self.processQuery = []
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        try:
            if not isinstance(handler, MetadataQueryHandler):
                raise TypeError("TypeError: handler must be an instance of MetadataQueryHandler class")
            
            self.metadataQuery.append(handler)
            return True
        except TypeError as e:
            print(e)
            return False

    def addProcessHandler(self, handler:ProcessDataQueryHandler) -> bool:
        try:
            if not isinstance(handler, ProcessDataQueryHandler):
                raise TypeError("TypeError: handler must be an instance of ProcessDataQueryHandler class")
            
            self.processQuery.append(handler)
            return True
        except TypeError as e:
            print(e)
            return False

    def getEntityById(self, id: str) -> IdentifiableEntity | None:
        df = pd.DataFrame()
        
        for metaData_qh in self.metadataQuery:
            meta_df_to_add = metaData_qh.getById(id)
            df = pd.concat([df, meta_df_to_add], ignore_index=True)
        
        if len(df) == 0:
            return None
        
        else:
            if ":" in id:
                result_person = Person(id, df.loc[0]["name"])
                return result_person
            else:
                dict_of_classes = {'NauticalChart': NauticalChart, 'ManuscriptPlate': ManuscriptPlate, 'ManuscriptVolume': ManuscriptVolume,
                                   'PrintedVolume': PrintedVolume, 'PrintedMaterial': PrintedMaterial, 'Herbarium': Herbarium,
                                   'Specimen': Specimen, 'Painting': Painting, 'Model': Model, 'Map': Map}
                list_of_authors = []
                authors = metaData_qh.getAuthorsOfCulturalHeritageObject(id)
                list_of_authors = [Person(row["id"], row["name"]) for _, row in authors.iterrows()]
                
                obj_title = df.loc[0]["title"]
                obj_date = str(df.loc[0]["date"]) if df.loc[0]["date"] != "" else None
                obj_authors = list_of_authors
                obj_owner = df.loc[0]["owner"]
                obj_place = df.loc[0]["place"]
                obj_type = df.loc[0]["type"]
                
                result_object = dict_of_classes[obj_type](id, obj_title,
                        obj_owner,
                        obj_place,
                        obj_date,
                        obj_authors)
                
                return result_object



    def getAllPeople(self) -> list[Person]:
        df = pd.DataFrame()
        
        for metaData_qh in self.metadataQuery:
            meta_df_to_add = metaData_qh.getAllPeople()
            df = pd.concat([df, meta_df_to_add], ignore_index=True).drop_duplicates()
        
        if len(df) == 0:
            return list()
        
        else:
            list_of_authors = [Person(row["id"], row["name"]) for _, row in df.iterrows()]
            
            return list_of_authors

    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]:
        df = pd.DataFrame()
        
        for metaData_qh in self.metadataQuery:
            meta_df_to_add = metaData_qh.getAllCulturalHeritageObjects()
            df = pd.concat([df, meta_df_to_add], ignore_index=True).drop_duplicates()
        
        if len(df) == 0:
            return list()
        
        else:
            dict_of_classes = {'NauticalChart': NauticalChart, 'ManuscriptPlate': ManuscriptPlate, 'ManuscriptVolume': ManuscriptVolume,
                                   'PrintedVolume': PrintedVolume, 'PrintedMaterial': PrintedMaterial, 'Herbarium': Herbarium,
                                   'Specimen': Specimen, 'Painting': Painting, 'Model': Model, 'Map': Map}
            df.drop_duplicates(subset='id', inplace=True, ignore_index=True) 
            list_of_objects = []
            for i, row in df.iterrows():
                authors_df = metaData_qh.getAuthorsOfCulturalHeritageObject(row['id'])
                list_of_authors = [Person(row1["id"], row1["name"]) for _, row1 in authors_df.iterrows()]

                obj_id = str(df.loc[i]["id"])
                obj_title = df.loc[i]["title"]
                obj_date = str(df.loc[i]["date"]) if df.loc[i]["date"] != "" else None
                obj_authors = list_of_authors
                obj_owner = df.loc[i]["owner"]
                obj_place = df.loc[i]["place"]
                obj_type = df.loc[i]["type"]
                
                result_object = dict_of_classes[obj_type](obj_id, obj_title,
                        obj_owner,
                        obj_place,
                        obj_date,
                        obj_authors)
                
                list_of_objects.append(result_object)
            
            return list_of_objects

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> list[Person]:
        df = pd.DataFrame()
        
        for metaData_qh in self.metadataQuery:
            meta_df_to_add = metaData_qh.getAuthorsOfCulturalHeritageObject(objectId)
            df = pd.concat([df, meta_df_to_add], ignore_index=True).drop_duplicates()
        
        if len(df) == 0:
            return list()
        
        else:
            list_of_authors = [Person(row["id"], row["name"]) for _, row in df.iterrows()]
            return list_of_authors


    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> list[CulturalHeritageObject]:
        df = pd.DataFrame()
        
        for metaData_qh in self.metadataQuery:
            meta_df_to_add = metaData_qh.getCulturalHeritageObjectsAuthoredBy(personId)
            df = pd.concat([df, meta_df_to_add], ignore_index=True).drop_duplicates()
        
        if len(df) == 0:
            return list()
        
        else:
            dict_of_classes = {'NauticalChart': NauticalChart, 'ManuscriptPlate': ManuscriptPlate, 'ManuscriptVolume': ManuscriptVolume,
                                   'PrintedVolume': PrintedVolume, 'PrintedMaterial': PrintedMaterial, 'Herbarium': Herbarium,
                                   'Specimen': Specimen, 'Painting': Painting, 'Model': Model, 'Map': Map}
            df.drop_duplicates(subset='id', inplace=True, ignore_index=True) 
            list_of_objects = []
            for i, row in df.iterrows():
                authors_df = metaData_qh.getAuthorsOfCulturalHeritageObject(row['id'])
                list_of_authors = [Person(row1["id"], row1["name"]) for _, row1 in authors_df.iterrows()]

                obj_id = str(df.loc[i]["id"])
                obj_title = df.loc[i]["title"]
                obj_date = str(df.loc[i]["date"]) if df.loc[i]["date"] != "" else None
                obj_authors = list_of_authors
                obj_owner = df.loc[i]["owner"]
                obj_place = df.loc[i]["place"]
                obj_type = df.loc[i]["type"]
                
                result_object = dict_of_classes[obj_type](obj_id, obj_title,
                        obj_owner,
                        obj_place,
                        obj_date,
                        obj_authors)
                
                list_of_objects.append(result_object)
            
            return list_of_objects

    def getAllActivities(self) -> list[Activity]:
        df = pd.DataFrame()
        
        for process_qh in self.processQuery:
            process_df_to_add = process_qh.getAllActivities()
            df = pd.concat([df, process_df_to_add], ignore_index=True).drop_duplicates()
            df.fillna('', inplace=True)
        
        if len(df) == 0:
            return list()
        
        else:
            dict_of_classes = {'acquisition': Acquisition, 'processing': Processing, 'modelling': Modelling,
                                   'optimising': Optimising, 'exporting': Exporting}
            list_of_activities = []
            for i, row in df.iterrows():

                act_refersTo = self.getEntityById(str(df.loc[i]["objectId"]))
                act_institute = df.loc[i]["responsible institute"]
                act_person = df.loc[i]["responsible person"]
                act_start = df.loc[i]["start date"]
                act_end = df.loc[i]["end date"]
                act_tool = df.loc[i]["tool"]
                match_type = re.search(r'^[^-]*', df.loc[i]["activityId"])
                act_type = match_type.group(0)
                
                if dict_of_classes[act_type] == Acquisition:
                    act_technique = df.loc[i]["technique"]
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_technique, act_person,
                            act_start,
                            act_end,
                            act_tool)
                else:
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_person,
                            act_start,
                            act_end,
                            act_tool)
                
                list_of_activities.append(result_activity)
            
            return list_of_activities

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]:
        df = pd.DataFrame()
        
        for process_qh in self.processQuery:
            process_df_to_add = process_qh.getActivitiesByResponsibleInstitution(partialName)
            df = pd.concat([df, process_df_to_add], ignore_index=True).drop_duplicates()
            df.fillna('', inplace=True)
        
        if len(df) == 0:
            return list()
        
        else:
            dict_of_classes = {'acquisition': Acquisition, 'processing': Processing, 'modelling': Modelling,
                                   'optimising': Optimising, 'exporting': Exporting}
            list_of_activities = []
            for i, row in df.iterrows():

                act_refersTo = self.getEntityById(str(df.loc[i]["objectId"]))
                act_institute = df.loc[i]["responsible institute"]
                act_person = df.loc[i]["responsible person"]
                act_start = df.loc[i]["start date"]
                act_end = df.loc[i]["end date"]
                act_tool = df.loc[i]["tool"]
                match_type = re.search(r'^[^-]*', df.loc[i]["activityId"])
                act_type = match_type.group(0)
                
                if dict_of_classes[act_type] == Acquisition:
                    act_technique = df.loc[i]["technique"]
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_technique, act_person,
                            act_start,
                            act_end,
                            act_tool)
                else:
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_person,
                            act_start,
                            act_end,
                            act_tool)
                
                list_of_activities.append(result_activity)
            
            return list_of_activities

    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[Activity]:
        df = pd.DataFrame()
        
        for process_qh in self.processQuery:
            process_df_to_add = process_qh.getActivitiesByResponsiblePerson(partialName)
            df = pd.concat([df, process_df_to_add], ignore_index=True).drop_duplicates()
            df.fillna('', inplace=True)
        
        if len(df) == 0:
            return list()
        
        else:
            dict_of_classes = {'acquisition': Acquisition, 'processing': Processing, 'modelling': Modelling,
                                   'optimising': Optimising, 'exporting': Exporting}
            list_of_activities = []
            for i, row in df.iterrows():

                act_refersTo = self.getEntityById(str(df.loc[i]["objectId"]))
                act_institute = df.loc[i]["responsible institute"]
                act_person = df.loc[i]["responsible person"]
                act_start = df.loc[i]["start date"]
                act_end = df.loc[i]["end date"]
                act_tool = df.loc[i]["tool"]
                match_type = re.search(r'^[^-]*', df.loc[i]["activityId"])
                act_type = match_type.group(0)
                
                if dict_of_classes[act_type] == Acquisition:
                    act_technique = df.loc[i]["technique"]
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_technique, act_person,
                            act_start,
                            act_end,
                            act_tool)
                else:
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_person,
                            act_start,
                            act_end,
                            act_tool)
                
                list_of_activities.append(result_activity)
            
            return list_of_activities

    def getActivitiesUsingTool(self, partialName: str) -> list[Activity]:
        df = pd.DataFrame()
        
        for process_qh in self.processQuery:
            process_df_to_add = process_qh.getActivitiesUsingTool(partialName)
            df = pd.concat([df, process_df_to_add], ignore_index=True).drop_duplicates()
            df.fillna('', inplace=True)
        
        if len(df) == 0:
            return list()
        
        else:
            dict_of_classes = {'acquisition': Acquisition, 'processing': Processing, 'modelling': Modelling,
                                   'optimising': Optimising, 'exporting': Exporting}
            list_of_activities = []
            for i, row in df.iterrows():

                act_refersTo = self.getEntityById(str(df.loc[i]["objectId"]))
                act_institute = df.loc[i]["responsible institute"]
                act_person = df.loc[i]["responsible person"]
                act_start = df.loc[i]["start date"]
                act_end = df.loc[i]["end date"]
                act_tool = df.loc[i]["tool"]
                match_type = re.search(r'^[^-]*', df.loc[i]["activityId"])
                act_type = match_type.group(0)
                
                if dict_of_classes[act_type] == Acquisition:
                    act_technique = df.loc[i]["technique"]
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_technique, act_person,
                            act_start,
                            act_end,
                            act_tool)
                else:
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_person,
                            act_start,
                            act_end,
                            act_tool)
                
                list_of_activities.append(result_activity)
            
            return list_of_activities

    def getActivitiesStartedAfter(self, date: str) -> list[Activity]:
        df = pd.DataFrame()
        
        for process_qh in self.processQuery:
            process_df_to_add = process_qh.getActivitiesStartedAfter(date)
            df = pd.concat([df, process_df_to_add], ignore_index=True).drop_duplicates()
            df.fillna('', inplace=True)
        
        if len(df) == 0:
            return list()
        
        else:
            dict_of_classes = {'acquisition': Acquisition, 'processing': Processing, 'modelling': Modelling,
                                   'optimising': Optimising, 'exporting': Exporting}
            list_of_activities = []
            for i, row in df.iterrows():

                act_refersTo = self.getEntityById(str(df.loc[i]["objectId"]))
                act_institute = df.loc[i]["responsible institute"]
                act_person = df.loc[i]["responsible person"]
                act_start = df.loc[i]["start date"]
                act_end = df.loc[i]["end date"]
                act_tool = df.loc[i]["tool"]
                match_type = re.search(r'^[^-]*', df.loc[i]["activityId"])
                act_type = match_type.group(0)
                
                if dict_of_classes[act_type] == Acquisition:
                    act_technique = df.loc[i]["technique"]
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_technique, act_person,
                            act_start,
                            act_end,
                            act_tool)
                else:
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_person,
                            act_start,
                            act_end,
                            act_tool)
                
                list_of_activities.append(result_activity)
            
            return list_of_activities

    def getActivitiesEndedBefore(self, date: str) -> list[Activity]:
        df = pd.DataFrame()
        
        for process_qh in self.processQuery:
            process_df_to_add = process_qh.getActivitiesEndedBefore(date)
            df = pd.concat([df, process_df_to_add], ignore_index=True).drop_duplicates()
            df.fillna('', inplace=True)
        
        if len(df) == 0:
            return list()
        
        else:
            dict_of_classes = {'acquisition': Acquisition, 'processing': Processing, 'modelling': Modelling,
                                   'optimising': Optimising, 'exporting': Exporting}
            list_of_activities = []
            for i, row in df.iterrows():

                act_refersTo = self.getEntityById(str(df.loc[i]["objectId"]))
                act_institute = df.loc[i]["responsible institute"]
                act_person = df.loc[i]["responsible person"]
                act_start = df.loc[i]["start date"]
                act_end = df.loc[i]["end date"]
                act_tool = df.loc[i]["tool"]
                match_type = re.search(r'^[^-]*', df.loc[i]["activityId"])
                act_type = match_type.group(0)
                
                if dict_of_classes[act_type] == Acquisition:
                    act_technique = df.loc[i]["technique"]
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_technique, act_person,
                            act_start,
                            act_end,
                            act_tool)
                else:
                    result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_person,
                            act_start,
                            act_end,
                            act_tool)
                
                list_of_activities.append(result_activity)
            
            return list_of_activities

    def getAcquisitionsByTechnique(self, partialName: str) -> list[Acquisition]:
        df = pd.DataFrame()
        
        for process_qh in self.processQuery:
            process_df_to_add = process_qh.getAcquisitionsByTechnique(partialName)
            df = pd.concat([df, process_df_to_add], ignore_index=True).drop_duplicates()
            df.fillna('', inplace=True)
        
        if len(df) == 0:
            return list()
        
        else:
            dict_of_classes = {'acquisition': Acquisition, 'processing': Processing, 'modelling': Modelling,
                                   'optimising': Optimising, 'exporting': Exporting}
            list_of_activities = []
            for i, row in df.iterrows():

                act_refersTo = self.getEntityById(str(df.loc[i]["objectId"]))
                act_institute = df.loc[i]["responsible institute"]
                act_person = df.loc[i]["responsible person"]
                act_start = df.loc[i]["start date"]
                act_end = df.loc[i]["end date"]
                act_tool = df.loc[i]["tool"]
                match_type = re.search(r'^[^-]*', df.loc[i]["acquisitionId"])
                act_type = match_type.group(0)
                
                act_technique = df.loc[i]["technique"]
                result_activity = dict_of_classes[act_type](act_refersTo, act_institute, act_technique, act_person,
                        act_start,
                        act_end,
                        act_tool)
                
                list_of_activities.append(result_activity)
            
            return list_of_activities

class AdvancedMashup(BasicMashup):
    def __init__(self):
        super().__init__()

    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]:
        
        list_of_objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        all_activities = self.getAllActivities()
    
        object_ids = {obj.getId() for obj in list_of_objects}
        activities = [act for act in all_activities if act.refersTo().id in object_ids]
        
        return activities


    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:
        
        activites = self.getActivitiesByResponsiblePerson(partialName)
        objects = []

        for act in activites:
            object = act.refersTo()
            if object.id not in [obj.id for obj in objects]:
                objects.append(object)

        return objects


    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[CulturalHeritageObject]:
        
        activites = self.getActivitiesByResponsibleInstitution(partialName)
        objects = []

        for act in activites:
            object = act.refersTo()
            if object.id not in [obj.id for obj in objects]:
                objects.append(object)

        return objects

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[Person]:
        
        activities_after = self.getActivitiesStartedAfter(start)
        filtered_activities_after = [activity for activity in activities_after if isinstance(activity, Acquisition)]

        ids_of_filtered_objects = set()
        for act in filtered_activities_after:
            date = datetime.strptime(act.end, '%Y-%m-%d')
            if date <= datetime.strptime(end, '%Y-%m-%d'):
                ids_of_filtered_objects.add(act.refersTo().id)
        
        result_list = []
        all_authors = []
        for id in ids_of_filtered_objects:
            authors = self.getAuthorsOfCulturalHeritageObject(id)
            if authors:
                all_authors = all_authors + authors

        unique_ids = set()
        # Iterate over the list in reverse order
        for i in range(len(all_authors) - 1, -1, -1):
            author = all_authors[i]
            if author.id in unique_ids:
                del all_authors[i]  # Remove duplicate author
            else:
                unique_ids.add(author.id)
                result_list.append(author)
        
        return result_list