# DHDK_DS-project

Final group project for the **Data Science** course in the **Digital Humanities and Digital Knowledge (DHDK)** Master's programme at the **University of Bologna**.

**Group members:** Sergei Slinkin, Elvira Kushlak, Anna Pak, Alisa Antonova

## Overview

This repository contains a Python-based data integration project focused on **cultural heritage data**. The goal of the assignment was to build a software solution capable of:

- processing data stored in **different formats**
- loading them into **two distinct databases**
- querying those databases both **individually** and **together** through predefined operations.

In this implementation:

- **JSON process data** are uploaded to a **SQLite relational database**
- **CSV metadata** are uploaded to a **Blazegraph RDF graph database**
- a mashup layer combines results from both sources and exposes them as Python objects.

## Academic context

The project was developed as the final assignment for the course *Data Science (1) (LM)* taught by **Silvio Peroni** in the academic year **2023/2024**. The course focused on data modelling, databases, query languages, data visualisation, and the integration of data-driven workflows into Python applications.

## Project goal

The objective of the project was to develop software that enables users to process heterogeneous data and upload them into two separate databases so they can be queried simultaneously according to predefined operations.

The provided exemplar data describe:

- the **acquisition and digitisation workflow** of cultural heritage objects, represented as JSON
- the **metadata of cultural heritage objects**, represented as CSV.

## Data sources

The repository includes two main sample datasets:

- `data/process.json` – process data describing five stages of work on each cultural heritage object:
  - acquisition
  - processing
  - modelling
  - optimising
  - exporting
- `data/meta.csv` – metadata about cultural heritage objects, including identifiers, object type, title, date, author, owner, and place.

These datasets are linked through the object identifier and then integrated through the mashup layer.

## What the project implements

### 1. Data model

The project defines Python classes for the main domain entities, including:

- `Person`
- `CulturalHeritageObject` and its subclasses:
  - `NauticalChart`
  - `ManuscriptPlate`
  - `ManuscriptVolume`
  - `PrintedVolume`
  - `PrintedMaterial`
  - `Herbarium`
  - `Specimen`
  - `Painting`
  - `Model`
  - `Map`
- `Activity` and its subclasses:
  - `Acquisition`
  - `Processing`
  - `Modelling`
  - `Optimising`
  - `Exporting`

### 2. Upload handlers

Two dedicated upload handlers manage ingestion into the two databases:

- `ProcessDataUploadHandler`
  - reads JSON process data
  - splits the five workflow stages into separate tabular structures
  - stores them in SQLite tables
  - includes logic to avoid duplicate uploads.

- `MetadataUploadHandler`
  - reads CSV metadata
  - transforms records into RDF triples
  - uploads them to Blazegraph through SPARQL
  - serialises a Turtle version of the graph into `Graph_db.ttl`.

### 3. Query handlers

The project also provides dedicated query handlers for each storage layer:

#### `MetadataQueryHandler`
Supports queries such as:

- `getById()`
- `getAllPeople()`
- `getAllCulturalHeritageObjects()`
- `getAuthorsOfCulturalHeritageObject()`
- `getCulturalHeritageObjectsAuthoredBy()`

#### `ProcessDataQueryHandler`
Supports queries such as:

- `getAllActivities()`
- `getActivitiesByResponsibleInstitution()`
- `getActivitiesByResponsiblePerson()`
- `getActivitiesUsingTool()`
- `getActivitiesStartedAfter()`
- `getActivitiesEndedBefore()`
- `getAcquisitionsByTechnique()`

### 4. Mashup layer

The integration logic is implemented through:

- `BasicMashup`
- `AdvancedMashup`

These classes combine data returned by the relational and graph query handlers and reconstruct higher-level Python objects.

`AdvancedMashup` also supports cross-database queries such as:

- `getActivitiesOnObjectsAuthoredBy()`
- `getObjectsHandledByResponsiblePerson()`
- `getObjectsHandledByResponsibleInstitution()`
- `getAuthorsOfObjectsAcquiredInTimeFrame()`

## Repository structure

```text
DHDK_DS-project/
├── data/
│   ├── meta.csv
│   └── process.json
├── impl.py
├── test.py
├── tests.py
├── README.md
├── blazegraph.jar
├── relational.db
├── Graph_db.ttl
└── .gitignore
```

### Notes on repository contents

- `impl.py` contains the full implementation.
- `test.py` contains the basic compliance tests used for the project.
- `tests.py` is the manual test script.
- `blazegraph.jar` is included to run a local Blazegraph instance.
- `relational.db` and `Graph_db.ttl` are generated database artefacts and can be recreated.

## Technologies used

- **Python**
- **pandas**
- **SQLite3**
- **RDFLib**
- **Blazegraph**
- **SPARQL**
- **sparql-dataframe**

## How to run the project

### 1. Clone the repository

```bash
git clone https://github.com/Sergpoipoip/DHDK_DS-project.git
cd DHDK_DS-project
```

### 2. Install dependencies

The dependencies should be installed manually.

```bash
pip install pandas rdflib sparql-dataframe
```

### 3. Start Blazegraph

Run the local Blazegraph instance from the project folder:

```bash
java -server -Xmx1g -jar blazegraph.jar
```

By default, the test file expects the SPARQL endpoint at:

```text
http://127.0.0.1:9999/blazegraph/sparql
```

### 4. Run the tests

```bash
python -m unittest test
```

## Example usage

```python
from impl import (
    ProcessDataUploadHandler,
    MetadataUploadHandler,
    ProcessDataQueryHandler,
    MetadataQueryHandler,
    AdvancedMashup,
)

# Upload process data to SQLite
process = ProcessDataUploadHandler()
process.setDbPathOrUrl("./relational.db")
process.pushDataToDb("data/process.json")

# Upload metadata to Blazegraph
metadata = MetadataUploadHandler()
metadata.setDbPathOrUrl("http://127.0.0.1:9999/blazegraph/sparql")
metadata.pushDataToDb("data/meta.csv")

# Create query handlers
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl("./relational.db")

metadata_qh = MetadataQueryHandler()
metadata_qh.setDbPathOrUrl("http://127.0.0.1:9999/blazegraph/sparql")

# Create mashup object
mashup = AdvancedMashup()
mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(metadata_qh)

# Example queries
all_activities = mashup.getAllActivities()
authors = mashup.getAuthorsOfCulturalHeritageObject("1")
people = mashup.getAllPeople()
```

## Learning outcomes demonstrated

This project reflects several core competencies developed in the course:

- modelling structured cultural heritage data in Python
- handling heterogeneous data formats
- building and querying both relational and graph databases
- using **SQL** and **SPARQL** in the same project
- integrating multiple data sources into a unified application workflow.

## Credits

This project was developed as a group assignment for the University of Bologna course **Data Science (1) (LM)**.

Official course and project resources:

- Course page: https://www.unibo.it/en/study/course-units-transferable-skills-moocs/course-unit-catalogue/course-unit/2023/467046
- Project brief: https://github.com/comp-data/2023-2024/tree/main/docs/project