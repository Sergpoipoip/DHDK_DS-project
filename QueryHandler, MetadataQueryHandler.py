import pandas as pd
import json

class QueryHandler:
    def __init__(self, json_file_path):
        with open(json_file_path, 'r') as file:
            self.data = pd.json_normalize(json.load(file))
    
    def getById(self, object_id):
        return self.data[self.data['object id'] == object_id]

class MetaDataQueryHandler:
    def __init__(self, csv_file_path):
        self.data = pd.read_csv(csv_file_path)
    
    def getAllPeople(self):
        return pd.DataFrame(self.data['Author'].dropna().unique(), columns=['Author'])

    def getAllCulturalHeritageObjects(self):
        return self.data

    def getAuthorsOfCulturalHeritageObject(self, object_id):
        return pd.DataFrame(self.data[self.data['Id'] == object_id]['Author'].dropna().unique(), columns=['Author'])

    def getCulturalHeritageObjectsAuthoredBy(self, author_name):
        return self.data[self.data['Author'] == author_name]

#example 
json_data_file_path = 'C:/Users/Эльвира/Downloads/process.json'
csv_data_file_path = 'C:/Users/Эльвира/Downloads/meta.csv'

query_handler = QueryHandler(json_data_file_path)
metadata_query_handler = MetaDataQueryHandler(csv_data_file_path)

#get an object by ID
object_details = query_handler.getById("1")
print(object_details)

#get all authors
authors = metadata_query_handler.getAllPeople()
print(authors)

#get metadata for a specific object
object_metadata = metadata_query_handler.getAuthorsOfCulturalHeritageObject(1)
print(object_metadata)

#get all objects authored by a specific person
authored_objects = metadata_query_handler.getCulturalHeritageObjectsAuthoredBy('Dioscorides Pedanius')
print(authored_objects)
