import pandas as pd

class MetaDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllPeople(self) -> pd.DataFrame:
        try:
            endpoint = self.getDbPathOrUrl()
            query = """
                    PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                    PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>

                    SELECT ?entity ?name ?id
                    WHERE {
                        ?entity a Classes:Person ;
                        Attributes:name ?name ;
                        Attributes:id ?id .
                    }
                    """
            df = get(endpoint, query, True)

            # Process the 'entity' column to ensure it only contains identifiers
            df['entity'] = df['entity'].apply(lambda x: x.rsplit('/', 1)[-1])
            df_sorted = df.loc[df['entity'].str.extract(r'(\d+)', expand=False).astype(int).sort_values().index]
            df_sorted.reset_index(drop=True, inplace=True)

            return df_sorted
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()

    def getAllCulturalHeritageObjects(self) -> pd.DataFrame:
        try:
            endpoint = self.getDbPathOrUrl()
            query = """
                    PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                    PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
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
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()

    def getAuthorsOfCulturalHeritageObject(self, object_id: str) -> pd.DataFrame:
        try:
            endpoint = self.getDbPathOrUrl()
            query = """
                    PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                    PREFIX Relations: <https://github.com/Sergpoipoip/DHDK_DS-project/relations/>
                    PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>

                    SELECT ?author
                    WHERE {
                        ?entity Attributes:id "%s" ;
                        Relations:author ?author .
                    }
                    """ % object_id
            df = get(endpoint, query, True)
            if df.empty:
                return pd.DataFrame()

            df['author'] = df['author'].apply(lambda x: x.rsplit('/', 1)[-1])

            all_dfs = []
            for author in df['author']:
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
                if df_main.empty:
                    continue

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
                if not df_entity.empty:
                    df_main['entity'] = df_entity['entity'][0]

                df_main['entity'] = df_main['entity'].apply(lambda x: x.rsplit('/', 1)[-1])
                df_main['name'] = df_main['name'].apply(lambda x: x if isinstance(x, str) else x)
                all_dfs.append(df_main)

            if all_dfs:
                return pd.concat(all_dfs, ignore_index=True)
            else:
                return pd.DataFrame()
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()

    def getCulturalHeritageObjectsAuthoredBy(self, person_id: str) -> pd.DataFrame:
        try:
            endpoint = self.getDbPathOrUrl()
            query = """
                    PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                    PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>

                    SELECT ?entity ?name ?id
                    WHERE {
                        ?entity a Classes:Person ;
                        Attributes:id ?id ;
                        Attributes:name ?name .
                        FILTER (?id = "%s")
                    }
                    """ % person_id

            df_author_data_by_id = get(endpoint, query, True)
            if df_author_data_by_id.empty:
                return pd.DataFrame()

            person_entity = df_author_data_by_id['entity'][0]

            query_1 = """
                    PREFIX Classes: <https://github.com/Sergpoipoip/DHDK_DS-project/classes/>
                    PREFIX Attributes: <https://github.com/Sergpoipoip/DHDK_DS-project/attributes/>
                    PREFIX Relations: <https://github.com/Sergpoipoip/DHDK_DS-project/relations/>

                    SELECT ?entity ?type ?date ?id ?owner ?place ?title ?author
                    WHERE {
                        ?entity Relations:author <%s> ;
                        Attributes:type ?type ;
                        Attributes:date ?date ;
                        Attributes:id ?id ;
                        Attributes:owner ?owner ;
                        Attributes:place ?place ;
                        Attributes:title ?title .
                    }
                    ORDER BY ?id
                    """ % person_entity
            resultant_df = get(endpoint, query_1, True)

            columns_to_process = ['entity', 'type', 'author']
            for column in columns_to_process:
                resultant_df[column] = resultant_df[column].apply(lambda x: x.rsplit('/', 1)[-1] if isinstance(x, str) else x)

            return resultant_df
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()

