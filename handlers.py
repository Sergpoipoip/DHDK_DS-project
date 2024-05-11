import pandas as pd
import urllib.parse as up
import sqlite3 as sq
import re
from rdflib import Graph, Namespace, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get 


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
    