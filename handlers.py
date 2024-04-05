import pandas as pd
import sqlite3 as sq
import urllib.parse as up

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

            # create one dataframe from json file
            activities = pd.read_json(path)
            
            # create 5 dataframes for each activity from activities dataframe and store them in one dictionary
            activities_column_names = activities.columns[1:].tolist()
            dict_of_dfs = {}
            for column_name in activities_column_names:
                column = activities[column_name]
                df = pd.DataFrame(column.tolist())
                df = df.applymap(lambda x: None if x == '' or x == [] else x) # replace empty strings and empty lists with None
                df = df.applymap(lambda x: ', '.join(x) if isinstance(x, list) else x) # replace lists with strings they contain
                internalId = []
                objectId = []
                for idx, row in df.iterrows():
                    internalId.append(f"{column_name}-" +str(idx+length_activity))
                    objectId.append(str(idx+1))
                df.insert(0, f"{column_name}Id", pd.Series(internalId, dtype = "object"))
                df.insert(len(df.columns), "objectId", pd.Series(objectId, dtype = "object"))
                dict_of_dfs[column_name] = df
            
            #upload 5 dataframes to database
            with sq.connect(self.getDbPathOrUrl()) as con:
                for key in dict_of_dfs.keys():
                    dict_of_dfs[key].to_sql(f"{key}", con, if_exists="append", index=False)       
            return True
            

        except Exception as e:
            print(str(e))
            return False
