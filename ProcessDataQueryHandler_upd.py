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
    pass
        
class ProcessDataQueryHandler (QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllActivities (self): #add technique for each table, and ask sql to assign null value for this column
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query1 = """
                SELECT acquisitionId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                FROM acquisition
                UNION ALL  
                SELECT processingId, "responsible institute", "responsible person", tool, "start date", "end date", NULL AS technique
                FROM processing
                UNION ALL
                SELECT modellingId, "responsible institute", "responsible person", tool, "start date", "end date", NULL AS technique
                FROM modelling
                UNION ALL 
                SELECT optimisingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM optimising
                UNION ALL
                SELECT exportingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM exporting
                """
                query1_table = pd.read_sql(query1, con)
                return query1_table
        except Exception as e:
            print("An error occurred:", e)

    def getActivitiesByResponsibleInstitution (self, partialName: str):
        try:
            with connect(self.getDbPathOrUrl()) as con: #"responsible institute",
                query2 = """
                SELECT acquisitionId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                FROM acquisition
                WHERE "responsible institute" LIKE '%{partialName}%'
                UNION ALL  
                SELECT processingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM processing
                WHERE "responsible institute" LIKE '%{partialName}%'
                UNION ALL
                SELECT modellingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM modelling
                WHERE "responsible institute" LIKE '%{partialName}%'
                UNION ALL 
                SELECT optimisingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM optimising
                WHERE "responsible institute" LIKE '%{partialName}%'
                UNION ALL
                SELECT exportingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM exporting
                WHERE "responsible institute" LIKE '%{partialName}%'
                """
                query2_table = pd.read_sql(query2, con)
                return query2_table
        except Exception as e:
            print("An error occurred:", e) 
    
    def getActivitiesByResponsiblePerson (self, partialName: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query3 = """
                SELECT acquisitionId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                FROM acquisition
                WHERE "responsible person" LIKE '%{partialName}%'
                UNION ALL  
                SELECT processingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM processing
                WHERE "responsible person" LIKE '%{partialName}%'
                UNION ALL
                SELECT modellingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM modelling
                WHERE "responsible person" LIKE '%{partialName}%'
                UNION ALL 
                SELECT optimisingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM optimising
                WHERE "responsible person" LIKE '%{partialName}%'
                UNION ALL
                SELECT exportingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM exporting
                WHERE "responsible person" LIKE '%{partialName}%'
                """
                query3_table = pd.read_sql(query3, con)
                return query3_table
        except Exception as e:
            print("An error occurred:", e)

    def getActivitiesUsingTool (self, Name: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query4 = """
                SELECT acquisitionId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                FROM acquisition
                WHERE tool LIKE '%{Name}%'
                UNION ALL  
                SELECT processingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM processing
                WHERE tool LIKE '%{Name}%'
                UNION ALL
                SELECT modellingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM modelling
                WHERE tool LIKE '%{Name}%'
                UNION ALL 
                SELECT optimisingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM optimising
                WHERE tool LIKE '%{Name}%'
                UNION ALL
                SELECT exportingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM exporting
                WHERE tool LIKE '%{Name}%'
                """
                query4_table = pd.read_sql(query4, con)
                return query4_table
        except Exception as e:
            print("An error occurred:", e)

    def getActivitiesStartedAfter (self, date: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query5 = """
                SELECT acquisitionId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                FROM acquisition
                WHERE "start date" LIKE '%{date}%'
                UNION ALL  
                SELECT processingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM processing
                WHERE "start date" LIKE '%{date}%'
                UNION ALL
                SELECT modellingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM modelling
                WHERE "start date" LIKE '%{date}%'
                UNION ALL 
                SELECT optimisingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM optimising
                WHERE "start date" LIKE '%{date}%'
                UNION ALL
                SELECT exportingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM exporting
                WHERE "start date" LIKE '%{date}%'
                """
                query5_table = pd.read_sql(query5, con)
                return query5_table
        except Exception as e:
            print ("An error occured:", e) 

    def getActivitiesEndedBefore(self, date: str):
        try:
            with connect (self.getDbPathOrUrl()) as con:
                query6 = """
                SELECT acquisitionId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                FROM acquisition
                WHERE "end date" LIKE '%{date}%'
                UNION ALL  
                SELECT processingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM processing
                WHERE "end date" LIKE '%{date}%'
                UNION ALL
                SELECT modellingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM modelling
                WHERE "end date" LIKE '%{date}%'
                UNION ALL 
                SELECT optimisingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM optimising
                WHERE "end date" LIKE '%{date}%'
                UNION ALL
                SELECT exportingId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM exporting
                WHERE "end date" LIKE '%{date}%'
                """
                query6_table = pd.read_sql(query6, con)
                return query6_table
        except Exception as e:
            print ("An error occured:", e) 

    def getAcquisitionsByTechnique (self, partialName: str):
        try:
            with connect (self.getDbPathOrUrl()) as con:
                query7 = """
                SELECT acquisitionId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                FROM acquisition 
                WHERE technique LIKE '%{partialName}%'
                """
                query7_table = pd.read_sql(query7, con)
                return query7_table
        except Exception as e:
            print ("An error occured:", e) 
    
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl("/Users/annapak/Desktop/datasc/example.db")




    




    
    