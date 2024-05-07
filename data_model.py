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