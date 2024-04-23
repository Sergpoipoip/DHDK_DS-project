import pandas as pd
import urllib.parse as up
import sqlite3 as sq
import re
from rdflib import Graph, Namespace, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore


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

class MetaDataUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()
    
    def pushDataToDb(self, path: str):
        try:
            my_graph = Graph()
            
            # Define namespaces
            ns_dict = {
                "Classes": "https://github.com/Sergpoipoip/DHDK_DS-project/classes/",
                "Attributes": "https://github.com/Sergpoipoip/DHDK_DS-project/attributes/",
                "Relations": "https://github.com/Sergpoipoip/DHDK_DS-project/relations/",
                "Entities": "https://github.com/Sergpoipoip/DHDK_DS-project/entities/",
            }

            for prefix, uri in ns_dict.items():
                my_graph.bind(prefix, Namespace(uri))

            predicates = {key: URIRef(ns_dict["Attributes"] + key) for key in ['id', 'title', 'date', 'owner', 'place', 'name']}
            author = URIRef(ns_dict["Relations"] + "author")
            Person = URIRef(ns_dict["Classes"] + "Person")

            # Create a dataframe based on a provided csv file
            meta_df = pd.read_csv(path, keep_default_na=False)

            # Add all the persons to the graph
            object_id_author = {}
            ids_of_unique_authors = {}
            author_id = 0

            for _, row in meta_df.iterrows():
                if row['Author']:
                    pattern_id = r'\((.*?)\)' # regex pattern to match the substring within parentheses, i.e. the id part
                    pattern_name = r'^([^()]+)'
                    authors = [s.strip() for s in row['Author'].split(";")]

                    for author_str in authors:
                        match_id = re.search(pattern_id, author_str)
                        match_name = re.search(pattern_name, author_str)

                        if match_id and match_name:
                            person_id = match_id.group(1)
                            person_name = match_name.group(1).strip()
                            object_id = row['Id']

                            if person_id not in ids_of_unique_authors:
                                subject = URIRef(ns_dict["Entities"] + f'person-{author_id}')
                                author_id += 1

                                my_graph.add((subject, RDF.type, Person))
                                my_graph.add((subject, predicates['id'], Literal(person_id)))
                                my_graph.add((subject, predicates['name'], Literal(person_name)))
                                ids_of_unique_authors[person_id] = subject

                            subject = ids_of_unique_authors[person_id]
                            object_id_author.setdefault(object_id, []).append(subject)

            # Add all the cultural heritage objects to the graph
            author_id = 0

            for idx, row in meta_df.iterrows():
                subject = URIRef(ns_dict["Entities"] + f"culturalObject-{idx}")

                object_type = ''.join(word.capitalize() for word in row['Type'].lower().split())
                my_graph.add((subject, RDF.type, URIRef(ns_dict["Classes"] + object_type)))

                for column in meta_df.columns:
                    if column not in ['Type', 'Author'] and row[column]:
                        predicate = column.lower()
                        my_graph.add((subject, predicates[predicate], Literal(str(row[column]).strip())))

                if row['Author']:
                    author_id += 1
                    for person in object_id_author.get(row['Id'], []):
                        my_graph.add((subject, author, person))
        

            # Update the RDF database
            store = SPARQLUpdateStore()
            endpoint = self.getDbPathOrUrl()

            store.open((endpoint, endpoint))

            for triple in my_graph.triples((None, None, None)):
                store.add(triple)
            store.close()

            # Serialize the RDF graph in order to make human-readable its content
            with open('Graph_db.ttl', mode='a', encoding='utf-8') as f:
                f.write(my_graph.serialize(format='turtle'))

            return True

        except Exception as e:
            print(str(e))
            return False
