from os import sep
from handlers import MetadataUploadHandler, ProcessDataUploadHandler
from handlers import QueryHandler, MetadataQueryHandler, ProcessDataQueryHandler
from mashup import BasicMashup, AdvancedMashup

# Launch ProcessDataUploadHandler() Class
process = ProcessDataUploadHandler()
process.setDbPathOrUrl("." + sep + "relational.db")
process.pushDataToDb("data" + sep + "process.json")

# Launch MeatadataUploadHandler() Class
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
metadata = MetadataUploadHandler()
metadata.setDbPathOrUrl(grp_endpoint)
metadata.pushDataToDb("data" + sep + "meta.csv")

# Launch QueryHandler() Class
queryHandler = QueryHandler()
queryHandler.setDbPathOrUrl("http://127.0.0.1:9999/blazegraph/sparql")
queryHandler.getById("VIAF:265397758")

# Launch MetadataQueryHandler() Class
metaData_qh = MetadataQueryHandler()
metaData_qh.setDbPathOrUrl("http://127.0.0.1:9999/blazegraph/sparql")
metaData_qh.getAllPeople()

# Launch ProcessDataQueryHandler() Class
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl("." + sep + "relational.db")
process_qh.getAllActivities()

# Launch BasicMashup() Class
mashup = BasicMashup()
mashup.addMetadataHandler(metaData_qh)
mashup.addProcessHandler(process_qh)
mashup.getCulturalHeritageObjectsAuthoredBy("ULAN:500114874")
mashup.getActivitiesByResponsiblePerson("Ada Lovelace")

# Launch AdvancedMashup() Class
adv_mashup = AdvancedMashup()
adv_mashup.addMetadataHandler(metaData_qh)
adv_mashup.addProcessHandler(process_qh)
adv_mashup.getActivitiesOnObjectsAuthoredBy("VIAF:78822798")
adv_mashup.getObjectsHandledByResponsibleInstitution("HSE")
adv_mashup.getAuthorsOfObjectsAcquiredInTimeFrame('2015-03-04', '2018-05-10')
