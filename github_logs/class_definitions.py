def initiate_global_id(zero_value:int=0) -> None:
    """Initiates the global variable `global_id_value` (integer 
    used for creating id's) to 0."""
    global global_id_value
    global_id_value = zero_value
    return None

def next_global_id() -> int:
    """Increases the global variable `global_id_value` (integer 
    used for generating id's) by 1 before returning it."""
    global global_id_value
    global_id_value += 1
    return global_id_value
    

class ObjectType:
    """Representation of a record in the table `object_types` 
    with columns `id`, `description`."""
    def __init__(self,description:str):
        self.id = next_global_id()
        self.description = description

    def get_dict(self):
        return {'id':self.id,
                'description':self.description}
    
class ObjectAttribute:
    """Representation of a record in the table `object_attributes` 
    with columns `id`, `object_type_id`, `description`, `datatype`."""
    def __init__(self,object_type:ObjectType,description:str,datatype:str):
        self.id = next_global_id()
        self.object_type_id = object_type.id
        self.object_type_description = object_type.description
        self.description = description
        self.datatype = datatype

    def get_dict(self):
        return {'id':self.id,
                'object_type_id':self.object_type_id,
                'description':self.description,
                'datatype':self.datatype}
    
class Object:
    """Representation of a record in the table `objects` 
    with columns `id`, `object_type_id`, `description`."""
    def __init__(self,object_type:ObjectType,description:str):
        self.id = next_global_id()
        self.object_type_id = object_type.id
        self.object_type_description = object_type.description
        self.description = description

    def get_dict(self):
        return {'id':self.id,
                'object_type_id':self.object_type_id,
                'description':self.description}

class ObjectAttributeValue:
    """Representation of a record in the table `object_attribute_values` 
    with columns `id`, `object_id`, `object_attribute_id`, `timestamp`,
    `attribute_value`."""
    def __init__(self,object:Object,object_attribute:ObjectAttribute,
                 timestamp,attribute_value):
        self.id = next_global_id()
        self.object_id = object.id
        self.object_attribute_id = object_attribute.id
        self.timestamp = timestamp
        self.attribute_value = attribute_value

    def get_dict(self):
        return {'id':self.id,
                'object_id':self.object_id,
                'object_attribute_id':self.object_attribute_id,
                'timestamp':self.timestamp,
                'attribute_value':self.attribute_value}


class EventType:
    """Representation of a record in the table `event_types` 
    with columns `id`, `description`."""
    def __init__(self,description:str):
        self.id = next_global_id()
        self.description = description

    def get_dict(self):
        return {'id':self.id,
                'description':self.description}
    
class EventAttribute:
    """Representation of a record in the table `event_attributes` 
    with columns `id`, `event_type_id`, `description`, `datatype`."""
    def __init__(self,event_type:EventType,description:str,datatype:str):
        self.id = next_global_id()
        self.event_type_id = event_type.id
        self.event_type_description = event_type.description
        self.description = description
        self.datatype = datatype

    def get_dict(self):
        return {'id':self.id,
                'object_type_id':self.event_type_id,
                'description':self.description,
                'datatype':self.datatype}
    
class Event:
    """Representation of a record in the table `events` 
    with columns `id`, `event_type_id`, `timestamp`, `description`."""
    def __init__(self,event_type:EventType,timestamp,description:str):
        self.id = next_global_id()
        self.event_type_id = event_type.id
        self.event_type_description = event_type.description
        self.timestamp = timestamp
        self.description = description

    def get_dict(self):
        return {'id':self.id,
                'event_type_id':self.event_type_id,
                'timestamp':self.timestamp,
                'description':self.description}
    
class EventAttributeValue:
    """Representation of a record in the table `event_attribute_values` 
    with columns `id`, `event_id`, `event_attribute_id`, `attribute_value`."""
    def __init__(self,event:Event,event_attribute:EventAttribute,attribute_value):
        self.id = next_global_id()
        self.event_id = event.id
        self.event_attribute_id = event_attribute.id
        self.attribute_value = attribute_value

    def get_dict(self):
        return {'id':self.id,
                'event_id':self.event_id,
                'event_attribute_id':self.event_attribute_id,
                'attribute_value':self.attribute_value}


class RelationQualifier:
    """Representation of a record in the table `relation_qualifiers` 
    with columns `id`, `description`, `datatype`."""
    def __init__(self,description:str,datatype:str):
        self.id = next_global_id()
        self.description = description
        self.datatype = datatype

    def get_dict(self):
        return {'id':self.id,
                'description':self.description,
                'datatype':self.datatype}

class ObjectToObjectRelation:
    """Representation of a record in the table `object_to_object` 
    with columns `id`, `source_object_id`, `target_object_id`, `qualifier_id`,
    `timestamp`, `qualifier_value`."""
    def __init__(self,source_object:Object,target_object:Object,
                 qualifier:RelationQualifier,timestamp,qualifier_value):
        self.id = next_global_id()
        self.source_object_id = source_object.id
        self.target_object_id = target_object.id
        self.qualifier_id = qualifier.id
        self.timestamp = timestamp
        self.qualifier_value = qualifier_value

    def get_dict(self):
        return {'id':self.id,
                'source_object_id':self.source_object_id,
                'target_object_id':self.target_object_id,
                'qualifier_id':self.qualifier_id,
                'timestamp':self.timestamp,
                'qualifier_value':self.qualifier_value}

class EventToObjectRelation:
    """Representation of a record in the table `event_to_object` 
    with columns `id`, `event_id`, `object_id`, `qualifier_id`,
    `qualifier_value`."""
    def __init__(self,event:Event,object:Object,qualifier:RelationQualifier,qualifier_value):
        self.id = next_global_id()
        self.event_id = event.id
        self.object_id = object.id
        self.qualifier_id = qualifier.id
        self.qualifier_value = qualifier_value

    def get_dict(self):
        return {'id':self.id,
                'event_id':self.event_id,
                'object_id':self.object_id,
                'qualifier_id':self.qualifier_id,
                'qualifier_value':self.qualifier_value}
    
class EventToObjectAttributeValueRelation:
    """Representation of a record in the table `event_to_object_attribute_value` 
    with columns `id`, `event_id`, `object_attribute_value_id`, `qualifier_id`,
    `qualifier_value`."""
    def __init__(self,event:Event,object_attribute_value:ObjectAttributeValue,
                 qualifier:RelationQualifier,qualifier_value):
        self.id = next_global_id()
        self.event_id = event.id
        self.object_attribute_value_id = object_attribute_value.id
        self.qualifier_id = qualifier.id
        self.qualifier_value = qualifier_value

    def get_dict(self):
        return {'id':self.id,
                'event_id':self.event_id,
                'object_attribute_value_id':self.object_attribute_value_id,
                'qualifier_id':self.qualifier_id,
                'qualifier_value':self.qualifier_value}
