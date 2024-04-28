import pandas as pd
import urllib.parse as up
from pandas import read_sql
from sqlite3 import connect

class Handler(object):
    def __init__(self):
        self.dbPathOrUrl = ""
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl 
    def setDbPathOrUrl(self, newpath):
        if len(newpath.split('.')) and newpath.split('.')[-1] == "db":
            self.dbPathOrUrl = newpath
            return True
        elif len(up.urlparse(newpath).scheme) and len(up.urlparse(newpath).netloc):
            self.dbPathOrUrl = newpath
            return True
        return False
    
class QueryHandler(Handler): 
    def __init__(self): 
        super().__init__()
    def getById (self, ID:str): 
        result = self.data[self.data['ID'] == ID]
        if len(result) == 1:
            return result
        else: 
            return None 
        
class ProcessDataQueryHandler (QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllActivities (self):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query1 = "SELECT * FROM Activity"
                query1_table = pd.read_sql(query1, con)
                return query1_table
        except:
            pass #should we add part of code with errors?

    def getActivitiesByResponsibleInstitution (self, partialName: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query2 = f"SELECT * FROM activities WHERE responsible institute LIKE '{partialName}'"
                # how do we call columns in DB? Activities?
                query2_table = pd.read_sql(query2, con)
                return query2_table
        except:
            pass 
    
    def getActivitiesByResponsiblePerson (self, partialName: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query3 = f"SELECT * FROM activities WHERE responsible person LIKE '{partialName}'"
                query3_table = pd.read_sql(query3, con)
                return query3_table
        except:
            pass 

    def getActivitiesUsingTool (self, Name: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query4 = f"SELECT * FROM activities WHERE tool LIKE '{Name}'"
                query4_table = pd.read_sql(query4, con)
                return query4_table
        except:
            pass

    def getActivitiesStartedAfter (self, date: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query5 = f"SELECT * FROM activities WHERE start date LIKE '{date}'"
                query5_table = pd.read_sql(query5, con)
                return query5_table
        except:
            pass 

    def getActivitiesEndedBefore(self, date: str):
        try:
            with connect (self.getDbPathOrUrl()) as con:
                query6 = f"SELECT * FROM activities WHERE end date LIKE '{date}'"
                query6_table = pd.read_sql(query6, con)
                return query6_table
        except:
            pass

    def getAcquisitionsByTechnique (self, partialName: str):
        try:
            with connect (self.getDbPathOrUrl()) as con:
                query7 = f"SELECT * FROM acquisition WHERE technique LIKE '{partialName}'"
                query7_table = pd.read_sql(query7, con)
                return query7_table
        except:
            pass
    
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl("/Users/annapak/Desktop/datasc/example.db")


    




    
    