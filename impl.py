import pandas as pd
import urllib.parse as up
import sqlite3 as sq
import re
from datetime import datetime
from rdflib import Graph, Namespace, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

# DATA-MODEL

class IdentifiableEntity(object):
    def __init__(self, id:str):
        if not isinstance(id, str):
            raise ValueError("IdentifiableEntity.id must be a string")
        self.id = id

    def getId(self):
        return self.id

class Person(IdentifiableEntity):
    def __init__(self, id: str, name: str):
        super().__init__(id)
        if not isinstance(name, str):
            raise ValueError("Person.name must be a string")
        self.name = name

    def getName(self):
        return self.name


class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id: str, title: str, owner: str, place: str, date: str|None=None, authors: Person|list[Person]|None=None):
        super().__init__(id)
        if not isinstance(title, str):
            raise ValueError("CulturalHeritageObject.title must be a string")
        if not isinstance(owner, str):
            raise ValueError("CulturalHeritageObject.owner must be a string")
        if not isinstance(place, str):
            raise ValueError("CulturalHeritageObject.place must be a string")
        if (not isinstance(date, str)) and date is not None:
            raise ValueError("CulturalHeritageObject.date must be a string or None")
        if not isinstance(authors, Person) and not isinstance(authors, list) and authors is not None:
            raise ValueError('CulturalHeritageObject.author must be a list or a string or None')
        self.title = title
        self.owner = owner
        self.place = place
        self.date = date
        self.authors = list()

        if type(authors) == Person:
            self.authors.append(Person)
        elif type(authors) == list:
            self.authors = authors
        
    def getTitle(self):
        return self.title
    
    def getOwner(self):
        return self.owner
    
    def getPlace(self):
        return self.place
    
    def getDate(self):
        if self.date:
            return self.date
        return None
    
    def getAuthors(self):
        return self.authors
        
class NauticalChart(CulturalHeritageObject):
    pass

class ManuscriptPlate(CulturalHeritageObject):
    pass

class ManuscriptVolume(CulturalHeritageObject):
    pass

class PrintedVolume(CulturalHeritageObject):
    pass

class PrintedMaterial(CulturalHeritageObject):
    pass

class Herbarium(CulturalHeritageObject):
    pass

class Specimen(CulturalHeritageObject):
    pass

class Painting(CulturalHeritageObject):
    pass

class Model(CulturalHeritageObject):
    pass

class Map(CulturalHeritageObject):
    pass

class Activity(object):
    def __init__(self,
                 object: CulturalHeritageObject,
                 institute: str,
                 person: str|None=None,
                 start: str|None=None,
                 end: str|None=None,
                 tool: str|list[str]|None=None):
        if not isinstance(object, CulturalHeritageObject):
            raise ValueError("Activity.object must be a CulturalHeritageObject")
        if not isinstance(institute, str):
            raise ValueError("Activity.institute must be a string")
        if not isinstance(person, str) and person is not None:
            raise ValueError("Activity.person must be a string or None")
        if not isinstance(start, str) and start is not None:
            raise ValueError("Activity.start must be a string or None")
        if not isinstance(end, str) and end is not None:
            raise ValueError("Activity.end must be a string or None")
        
        self.tool = []

        if type(tool) == str:
            self.tool.append(tool)
        elif type(tool) == list:
            self.tool = tool
        
        self.object = object
        self.institute = institute
        self.person = person
        self.start = start
        self.end = end

    def getResponsibleInstitute(self):
        return self.institute
    
    def getResponsiblePerson(self):
        if self.person:
            return self.person
        return None
    
    def getStartDate(self):
        if self.start:
            return self.start
        return None
    
    def getEndDate(self):
        if self.end:
            return self.end
        return None
    
    def getTools(self):
        return self.tool
    
    def refersTo(self):
        return self.object

class Acquisition(Activity):
    def __init__(self,
                 object: CulturalHeritageObject,
                 institute: str,
                 technique: str,
                 person: str | None = None,
                 start: str | None = None,
                 end: str | None = None,
                 tool: str | list[str] | None = None):
        super().__init__(object, institute, person, start, end, tool)
        if not isinstance(technique, str):
            raise ValueError("Acquisition.technique must be a string")
        
        self.technique = technique
        
    def getTechnique(self):
        return self.technique

class Processing(Activity):
    pass

class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass
 
# HANDLERS

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
                    q1= "SELECT * FROM acquisition;" 
                    q2 = "SELECT objectId FROM acquisition;"
                    q1_table = pd.read_sql(q1, con)
                    q2_list = pd.read_sql(q2, con)['objectId'].tolist()
                    length_activity = len(q1_table)
            except:
                pass

            # Create one dataframe from json file
            activities = pd.read_json(path)
            
            # Create 5 dataframes for each activity from activities dataframe and store them in one dictionary
            activities_column_names = activities.columns[1:].tolist()
            objectId = activities['object id'].tolist()
            json_dataframes = {}
            for column_name in activities_column_names:
                column = activities[column_name]
                df = pd.DataFrame(column.tolist())
                df = df.applymap(lambda x: None if x == '' or x == [] else x) # replace empty strings and empty lists with None
                df = df.applymap(lambda x: ', '.join(x) if isinstance(x, list) else x) # replace lists with strings they contain
                internalId = []
                for idx, row in df.iterrows():
                    internalId.append(f"{column_name}-" +str(idx+length_activity))
                df.insert(0, f"{column_name}Id", pd.Series(internalId, dtype = "object"))
                df.insert(len(df.columns), "objectId", pd.Series(objectId, dtype = "object"))
                json_dataframes[column_name] = df
            
            # Solve data duplication problem. 5 dataframes created at this point are compared
            # with 5 dataframes (derived from 5 tables contained in DB). Comparison are made across all columns except the first
            # 1) Check if there are some data in DB 2) If yes, create 5 dataframes from 5 tables of DB 3) Compare 5 current dataframes
            # with 5 DB dataframes, leave 5 dfs with unique data
            if length_activity:
                result_dfs = {}
                for df_name, df_json in json_dataframes.items():

                    # Perform row-wise comparison and filter out rows from df_1 that are not in df_0
                    filtered_df_1 = df_json[~df_json['objectId'].isin(q2_list)]

                    # Add the filtered dataframe to the result_dfs dictionary
                    result_dfs[df_name] = filtered_df_1
            
            # Upload 5 dataframes to database
            with sq.connect(self.getDbPathOrUrl()) as con:
                if length_activity:
                    for key in result_dfs.keys():
                        result_dfs[key].to_sql(f"{key}", con, if_exists="append", index=False)
                else:
                    for key in json_dataframes.keys():
                        json_dataframes[key].to_sql(f"{key}", con, if_exists="append", index=False)       
            return True
            

        except Exception as e:
            print(str(e))
            return False

class MetadataUploadHandler(UploadHandler):
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

            # Here we solve a problem with populating RDF DB that already contains some data. In order to populate it correctly
            # with new data we need to create correct indexes of Entities:culturalObject- and Entities:person-. To do this we
            # need to find out the total number of culturalObjects and persons already contained in the RDF DB. So, we build
            # a SPARQL query returning a dataframe with 'personCount' and 'culturalObjectCount' columns.
            endpoint = self.getDbPathOrUrl()

            query_to_find_total_number_of_culturalObjects_and_persons = '''
                                        PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                                        PREFIX Entities: <https://github.com/Sergpoipoip/DHDK_DS-project/entities/>

                                        SELECT 
                                        (COUNT(DISTINCT ?person) AS ?personCount)
                                        (COUNT(DISTINCT ?culturalObject) AS ?culturalObjectCount)
                                        WHERE {
                                            {
                                                ?person a Classes:Person .
                                                FILTER (STRSTARTS(STR(?person), str(Entities:person)))
                                            }
                                            UNION
                                            {
                                                ?culturalObject a [] .
                                                FILTER (STRSTARTS(STR(?culturalObject), str(Entities:culturalObject)))
                                            }
                                        }
                                    '''
            df_res = get(endpoint, query_to_find_total_number_of_culturalObjects_and_persons, True)
            
            ids_of_meta_df = meta_df['Id'].astype(int).tolist()
            if all(num > df_res['culturalObjectCount'][0] for num in ids_of_meta_df):

                # Solve persons duplication problem. Here we get a dataframe with all the people from a DB. Then using ids
                # of people contained in a dataframe, we filter our list of people from meta_df so that to add to the graph
                # and then to DB only those people who are not already in it.

                query_to_get_all_people = """
                        PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
                        PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>

                        SELECT ?entity ?name ?id
                        WHERE {
                            ?entity a Classes:Person ;
                            Attributes:name ?name ;
                            Attributes:id ?id .
                        }
                        """
                df_with_all_people_from_db = get(endpoint, query_to_get_all_people, True)

                # Create a dictionary where keys are ids of authors contained in DB and values are Entities:person-
                index_dict = df_with_all_people_from_db.set_index('id')['entity'].to_dict()
                
                filter_list = sorted(list(index_dict.keys()))
                
                # Split the 'Author' column by ';' and expand it into separate rows
                authors_expanded = meta_df['Author'].str.split('; ', expand=True)

                rows = []

                # Iterate over the columns of the expanded DataFrame
                for col in authors_expanded.columns:
                    # Extract the author name and ID from each column and append to the 'rows' list
                    author_id_split = authors_expanded[col].str.split('(', expand=True)
                    author_id_split.columns = ['Author', 'Author_ID']
                    author_id_split['Author_ID'] = author_id_split['Author_ID'].str[:-1]
                    rows.extend(author_id_split.values.tolist())

                # Create a new DataFrame from the 'rows' list
                new_df = pd.DataFrame(rows, columns=['Author', 'Author_ID'])

                # Drop rows with NaN values
                new_df.dropna(inplace=True)
                new_df.drop_duplicates(ignore_index=True)
                
                # Create a DataFrame with all the persons from csv file that are not present in RDF DB
                filtered_df = new_df[~new_df['Author_ID'].isin(filter_list)]
            

                # Add all the persons to the graph
                object_id_author = {}
                ids_of_unique_authors = {}
                author_id = df_res['personCount'][0]

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

                                if person_id not in ids_of_unique_authors and person_id in filtered_df['Author_ID'].tolist():
                                    subject = URIRef(ns_dict["Entities"] + f'person-{author_id}')
                                    author_id += 1

                                    my_graph.add((subject, RDF.type, Person))
                                    my_graph.add((subject, predicates['id'], Literal(person_id)))
                                    my_graph.add((subject, predicates['name'], Literal(person_name)))
                                    ids_of_unique_authors[person_id] = subject

                                if person_id in list(ids_of_unique_authors.keys()):
                                    subject = ids_of_unique_authors[person_id]
                                else:
                                    subject = URIRef(index_dict[person_id])
                                object_id_author.setdefault(object_id, []).append(subject)
                
                # Add all the cultural heritage objects to the graph
                author_id = 0
                culturalObject_id = df_res['culturalObjectCount'][0]

                for idx, row in meta_df.iterrows():
                    subject = URIRef(ns_dict["Entities"] + f"culturalObject-{idx+culturalObject_id}")

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
            
            else:
                raise ValueError(f"Error: The provided CSV file contains incorrect value(s) in the 'Id' column. \nThe target graph database already contains object(s) with the specified identifier(s). \nPlease, change the 'Id' column in your CSV file so that all values in it are greater than {df_res['culturalObjectCount'][0]}.")
        except Exception as e:
            print(str(e))
            return False

class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def getById(self, Id: str):
        db_path = self.getDbPathOrUrl()

        if not len(up.urlparse(db_path).scheme) and not len(up.urlparse(db_path).netloc):
            return pd.DataFrame()
        
        else:
            endpoint = db_path
            
            if ":" in Id:
                query = """
                        PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>

                        SELECT ?entity ?name ?id
                        WHERE {
                            ?entity Attributes:id "%s" ;
                            Attributes:name ?name ;
                            Attributes:id ?id .
                        }
                        """ % Id
            else:
                query = """
                        PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                        PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
                        PREFIX Relations: <https://github.com/Sergpoipoip/DHDK_DS-project/relations/>
                        PREFIX Entities: <https://github.com/Sergpoipoip/DHDK_DS-project/entities/>

                        SELECT ?entity ?type ?date ?id ?owner ?place ?title ?author
                        WHERE {
                            ?entity Attributes:id  "%s";
                            a ?type ;
                            Attributes:id ?id ;
                            Attributes:owner ?owner ;
                            Attributes:place ?place ;
                            Attributes:title ?title .
                            OPTIONAL {
                                ?entity Relations:author ?author .
                            }
                            OPTIONAL {
                                ?entity Attributes:date ?date .
                            }
                        }
                        """ % Id

            try:
                df = get(endpoint, query, True)

                if len(df):
                    columns_to_process = ['entity'] if len(df.columns) == 3 else ['entity', 'type', 'author']

                    for column in columns_to_process:
                        if isinstance(df[column][0], str):
                            df[column] = df[column].apply(lambda x: x.rsplit('/', 1)[-1])
                

            except Exception as e:
                print(f"couldn't connect to blazegraph due to the following error: \n{e}")
                print(f"trying to reconnect via local connection at http://127.0.0.1:9999/blazegraph/sparql")
                try:
                    endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
                    df = get(endpoint, query, True)
                except Exception as e2:
                    print(f"couldn't connect to blazegraph due to the following error: {e2}")
                    return None
            
        return df.drop_duplicates()
    
class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
    def getAllActivities(self):
        try:
            with sq.connect(self.getDbPathOrUrl()) as con:
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
            with sq.connect(self.getDbPathOrUrl()) as con:
                tables = ['acquisition', 'processing', 'modelling', 'optimising', 'exporting']
                union_query_parts = []
                y = 'NULL AS technique'
                x = 'technique'
                for table in tables:
                    union_query_parts.append(f"""
                        SELECT {table}Id AS activityId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, {x if table == 'acquisition' else y}
                        FROM {table}
                        WHERE "responsible institute" LIKE '%{partialName}%'
                    """) 
                query = '\nUNION ALL\n'.join(union_query_parts) + 'ORDER BY objectId'
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print("An error occurred:", e)
    
    def getActivitiesByResponsiblePerson(self, partialName: str):
        try:
            with sq.connect(self.getDbPathOrUrl()) as con:
                tables = ['acquisition', 'processing', 'modelling', 'optimising', 'exporting']
                union_query_parts = []
                y = 'NULL AS technique'
                x = 'technique'
                for table in tables:
                    union_query_parts.append(f"""
                        SELECT {table}Id AS activityId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, {x if table == 'acquisition' else y} 
                        FROM {table}
                        WHERE "responsible person" LIKE '%{partialName}%'
                    """)
                query = '\nUNION ALL\n'.join(union_query_parts) + 'ORDER BY objectId'
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print("An error occurred:", e)

    def getActivitiesUsingTool(self, partialName: str):
        try:
            with sq.connect(self.getDbPathOrUrl()) as con:
                tables = ['acquisition', 'processing', 'modelling', 'optimising', 'exporting']
                union_query_parts = []
                y = 'NULL AS technique'
                x = 'technique'
                for table in tables:
                    union_query_parts.append(f"""
                        SELECT {table}Id AS activityId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, {x if table == 'acquisition' else y} 
                        FROM {table}
                        WHERE "tool" LIKE '%{partialName}%'
                    """)
                query = '\nUNION ALL\n'.join(union_query_parts) + 'ORDER BY objectId'
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print("An error occurred:", e)

    def getActivitiesStartedAfter(self, date: str):
        try:
            with sq.connect(self.getDbPathOrUrl()) as con:
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
            with sq.connect (self.getDbPathOrUrl()) as con:
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
            with sq.connect (self.getDbPathOrUrl()) as con:
                query = f"""
                SELECT *
                FROM acquisition 
                WHERE technique LIKE '%{partialName}%'
                ORDER BY objectId
                """
                query_table = pd.read_sql(query, con)
                return query_table
        except Exception as e:
            print ("An error occured:", e) 

class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
    def getAllPeople(self):
        endpoint = self.getDbPathOrUrl()
        query = """
                        PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
                        PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>

                        SELECT ?entity ?name ?id
                        WHERE {
                            ?entity a Classes:Person ;
                            Attributes:name ?name ;
                            Attributes:id ?id .
                        }
                        """
        df = get(endpoint, query, True)
        
        df['entity'] = df['entity'].apply(lambda x: x.rsplit('/', 1)[-1])
        df_sorted = df.loc[df['entity'].str.extract(r'(\d+)', expand=False).astype(int).sort_values().index]
        df_sorted.reset_index(drop=True, inplace=True)

        return df_sorted

    def getAllCulturalHeritageObjects(self):
        endpoint = self.getDbPathOrUrl()

        query = """
                        PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
                        PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                        PREFIX Relations: <https://github.com/Sergpoipoip/DHDK_DS-project/relations/>

                        SELECT ?entity ?id ?type ?title ?date ?author ?owner ?place
                        WHERE {
                            ?entity a ?type ;
                            Attributes:id ?id ;
                            Attributes:title ?title ;
                            Attributes:owner ?owner ;
                            Attributes:place ?place .
                            OPTIONAL {
                                ?entity Relations:author ?author .
                            }
                            OPTIONAL {
                                ?entity Attributes:date ?date .
                            } 
                        }
                        """
        df = get(endpoint, query, True)

        columns_to_process = ['entity', 'type', 'author']
        for column in columns_to_process:
            df[column] = df[column].apply(lambda x: x.rsplit('/', 1)[-1] if isinstance(x, str) else x)
        
        df_sorted = df.loc[df['entity'].str.extract(r'(\d+)', expand=False).astype(int).sort_values().index]
        df_sorted.reset_index(drop=True, inplace=True)
        
        return df_sorted

    def getAuthorsOfCulturalHeritageObject(self, objectId: str):
        endpoint = self.getDbPathOrUrl()

        query = """
                        PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                        PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
                        PREFIX Relations: <https://github.com/Sergpoipoip/DHDK_DS-project/relations/>
                        PREFIX Entities: <https://github.com/Sergpoipoip/DHDK_DS-project/entities/>

                        SELECT ?entity ?id ?author
                        WHERE {
                            ?entity Attributes:id  "%s";
                            a ?type ;
                            Attributes:id ?id ;
                            Attributes:owner ?owner ;
                            Attributes:place ?place ;
                            Attributes:title ?title .
                            OPTIONAL {
                                ?entity Relations:author ?author .
                            }
                            OPTIONAL {
                                ?entity Attributes:date ?date .
                            }
                        }
                        """ % objectId
        df = get(endpoint, query, True)
        columns_to_process = ['entity', 'author']
        for column in columns_to_process:
            df[column] = df[column].apply(lambda x: x.rsplit('/', 1)[-1] if isinstance(x, str) else x)

        authors = [a for a in df['author']]
        
        if authors and not pd.isna(authors[0]):
            all_dfs = []
            for author in authors:
                query_1 = """
                                PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
                                PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                                PREFIX Entities: <https://github.com/Sergpoipoip/DHDK_DS-project/entities/>

                                SELECT ?entity ?id ?name
                                WHERE {
                                    Entities:%s a Classes:Person ;
                                    Attributes:id ?id ;
                                    Attributes:name ?name ;
                                }
                                """ % author
                
                df_main = get(endpoint, query_1, True)
                id = df_main['id'][0]

                query_2 = """
                                PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
                                PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                                PREFIX Entities: <https://github.com/Sergpoipoip/DHDK_DS-project/entities/>

                                SELECT ?entity
                                WHERE {
                                    ?entity a Classes:Person ;
                                    Attributes:id "%s" ;
                                }
                                """ % id

            
                df_entity = get(endpoint, query_2, True)

                df_main['entity'] = df_main['entity'].fillna(df_entity['entity'][0])
                columns_to_process = ['entity', 'name']
                for column in columns_to_process:
                    df_main[column] = df_main[column].apply(lambda x: x.rsplit('/', 1)[-1] if isinstance(x, str) else x)
                all_dfs.append(df_main)

            resultant_df = pd.concat(all_dfs, ignore_index=True)

            return resultant_df
        else:
            return pd.DataFrame()
        
    def getCulturalHeritageObjectsAuthoredBy(self, personId: str):
        endpoint = self.getDbPathOrUrl()

        query = """
                                PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
                                PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                                PREFIX Entities: <https://github.com/Sergpoipoip/DHDK_DS-project/entities/>

                                SELECT ?entity ?id ?name
                                WHERE {
                                    ?entity a Classes:Person ;
                                    Attributes:id ?id ;
                                    Attributes:name ?name .
                                    FILTER (?id = "%s")
                                }
                                """ % personId
        
        df_authorData_by_Id = get(endpoint, query, True)
        if len(df_authorData_by_Id):
            person = df_authorData_by_Id['entity'][0]
        else:
            return df_authorData_by_Id

        query_1 = """
                        PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                        PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
                        PREFIX Relations: <https://github.com/Sergpoipoip/DHDK_DS-project/relations/>
                        PREFIX Entities: <https://github.com/Sergpoipoip/DHDK_DS-project/entities/>

                        SELECT ?entity ?id ?type ?title ?date ?author ?owner ?place
                        WHERE {
                            ?entity a ?type ;
                                Attributes:id ?id ;
                                Attributes:owner ?owner ;
                                Attributes:place ?place ;
                                Attributes:title ?title ;
                                Relations:author ?author .
                            OPTIONAL {
                                ?entity Attributes:date ?date .
                            }
                            FILTER (?author = <%s>)
                        }
                        ORDER BY ?id
                        """ % person
        resultant_df = get(endpoint, query_1, True)

        columns_to_process = ['entity', 'type', 'author']
        for column in columns_to_process:
            resultant_df[column] = resultant_df[column].apply(lambda x: x.rsplit('/', 1)[-1] if isinstance(x, str) else x)

        return resultant_df

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
                act_tool = df.loc[i]["tool"].split(", ")
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