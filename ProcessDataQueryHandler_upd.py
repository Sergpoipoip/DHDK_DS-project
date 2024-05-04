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

    def getAllActivities(self):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                tables = ['acquisition', 'processing', 'modelling', 'optimising', 'exporting']
                union_query_parts = []
                y = 'NULL AS technique'
                x = 'technique'
                for table in tables:
                    union_query_parts.append(f"""
                        SELECT {table}Id AS activityId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, {x if table == 'acquisition' else y} 
                        FROM {table}
                    """)
                query = '\nUNION ALL\n'.join(union_query_parts)
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print("An error occurred:", e)
    
    def getActivitiesByResponsibleInstitution(self, partialName: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                tables = ['processing', 'modelling', 'optimising', 'exporting']
                union_query_parts = []
                y = 'NULL AS technique'
                x = 'technique'
                for table in tables:
                    union_query_parts.append(f"""
                        SELECT {table}Id AS activityId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, {x if table == 'acquisition' else y}
                        FROM {table}
                        WHERE "responsible institute" LIKE '{partialName}'
                    """) 
                query = '\nUNION ALL\n'.join(union_query_parts) + 'ORDER BY objectId'
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print("An error occurred:", e)
    
    def getActivitiesByResponsiblePerson(self, partialName: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                tables = ['acquisition', 'processing', 'modelling', 'optimising', 'exporting']
                union_query_parts = []
                y = 'NULL AS technique'
                x = 'technique'
                for table in tables:
                    union_query_parts.append(f"""
                        SELECT {table}Id AS activityId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, {x if table == 'acquisition' else y} 
                        FROM {table}
                        WHERE "responsible person" LIKE '{partialName}'
                    """)
                query = '\nUNION ALL\n'.join(union_query_parts) + 'ORDER BY objectId'
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print("An error occurred:", e)

    def getActivitiesUsingTool(self, partialName: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                tables = ['acquisition', 'processing', 'modelling', 'optimising', 'exporting']
                union_query_parts = []
                y = 'NULL AS technique'
                x = 'technique'
                for table in tables:
                    union_query_parts.append(f"""
                        SELECT {table}Id AS activityId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, {x if table == 'acquisition' else y} 
                        FROM {table}
                        WHERE "tool" LIKE '{partialName}'
                    """)
                query = '\nUNION ALL\n'.join(union_query_parts) + 'ORDER BY objectId'
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print("An error occurred:", e)

    def getActivitiesStartedAfter(self, date: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                tables = ['acquisition', 'processing', 'modelling', 'optimising', 'exporting']
                union_query_parts = []
                y = 'NULL AS technique'
                x = 'technique'
                for table in tables:
                    union_query_parts.append(f"""
                        SELECT {table}Id AS activityId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, {x if table == 'acquisition' else y} 
                        FROM {table}
                        WHERE "start date" >= '{date}'
                    """)
                query = '\nUNION ALL\n'.join(union_query_parts) + 'ORDER BY objectId'
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print ("An error occured:", e) 

    def getActivitiesEndedBefore(self, date: str):
        try:
            with connect (self.getDbPathOrUrl()) as con:
                tables = ['acquisition', 'processing', 'modelling', 'optimising', 'exporting']
                union_query_parts = []
                y = 'NULL AS technique'
                x = 'technique'
                for table in tables:
                    union_query_parts.append(f"""
                        SELECT {table}Id AS activityId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, {x if table == 'acquisition' else y} 
                        FROM {table}
                        WHERE "end date" <= '{date}'
                    """)
                query = '\nUNION ALL\n'.join(union_query_parts) + 'ORDER BY objectId'
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print ("An error occured:", e) 

    def getAcquisitionsByTechnique(self, partialName: str):
        try:
            with connect (self.getDbPathOrUrl()) as con:
                query = f"""
                SELECT *
                FROM acquisition 
                WHERE technique LIKE '{partialName}'
                ORDER BY objectId
                """
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print ("An error occured:", e) 
    
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl("/Users/annapak/Desktop/datasc/example.db")




    




    
    