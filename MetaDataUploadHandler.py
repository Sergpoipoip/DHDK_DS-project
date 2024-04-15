import pandas as pd
import sqlite3 as sq
import urllib.parse as up
from neo4j import GraphDatabase, Node

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
    
class UploadHandler(Handler):
    def __init__(self):
        super().__init__()
    def pushDataToDb(self):
        pass

class ProcessDataUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path: str):
        try:
            length_activity = 0
            try:
                with sq.connect(self.getDbPathOrUrl()) as con:
                    q1="SELECT * FROM acquisition;" 
                    q1_table = pd.read_sql(q1, con)
                    length_activity = len(q1_table)
            except:
                pass

            # create one dataframe from csv file
            activities = pd.read_csv(path)

        except Exception as e:
            print(str(e))
            return False

class SQLiteUploader(Handler):
    def __init__(self):
        super().__init__()
    def pushDataToDb(self, path: str):
        try:
            length_activity = 0
            with sq.connect(self.getDbPathOrUrl()) as con:
                q1 = "SELECT * FROM acquisition;" 
                q1_table = pd.read_sql(q1, con)
                length_activity = len(q1_table)
        except sq.Error as e:
            print(f"SQLite error: {e}")
            return False

        activities = pd.read_csv(path)
        return True

class CSVToGraphUploader:
    def __init__(self, csv_file, graph_uri, auth):
        self.csv_file = csv_file
        self.graph = GraphDatabase.driver(graph_uri, auth=auth)

    def push_to_graph(self):
        try:
            df = pd.read_csv(self.csv_file)
            with self.graph.session() as session:
                for index, row in df.iterrows():
                    node_properties = row.to_dict()
                    node = Node("NodeLabel", **node_properties)
                    session.write_transaction(self.create_node, node)
            return True
        except Exception as e:
            print(f"Graph database error: {e}")
            return False

    @staticmethod
    def create_node(tx, node):
        tx.run("CREATE (n:NodeLabel) SET n = $props", props=node)

    
