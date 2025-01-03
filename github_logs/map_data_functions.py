from class_definitions import *
from get_data_functions import *
import json

def new_object_issue(issue_data:dict,object_types:dict,objects:dict,object_attributes:dict,object_attribute_values:dict) -> Object:
    '''Returns a new object of type `issue` and adds it to the objects dictionary.
    Next, calls function `new_issue_attributes` to create and store its attributes.'''

    description = f"#{issue_data.get('number')}"
    object_type = object_types.get('issue')

    new_object = Object(object_type,description)
    objects[new_object.id] = new_object

    new_issue_attributes(new_object,issue_data,object_attributes,object_attribute_values)

    return new_object


def new_issue_attributes(issue_object:Object,issue_data:dict,object_attributes:dict,object_attribute_values:dict) -> None:
    '''Sets the attributes for a new object of type `issue` and adds them to the object_attribute_values dictionary.'''
    
    attributes = [['issue:number','number'],              # first string is key to get attribute, second string is key to use to extract from issue_data
                  ['issue:title','title'],
                  ['issue:timeline_url','timeline_url'],]
    
    timestamp = issue_data.get('created_at')
    
    for attribute_def in attributes:
        object_attribute = object_attributes.get(attribute_def[0])  # get object_attribute with attribute_name
        attribute_value = issue_data.get(attribute_def[1])          # extract attribute_value from issue_data with defined key

        new_object_attribute_value = ObjectAttributeValue(issue_object,object_attribute,timestamp,attribute_value)

        object_attribute_values[new_object_attribute_value.id] = new_object_attribute_value

    return None

def get_object_user(user_data:dict,users:dict,object_types:dict,objects:dict,object_attributes:dict,object_attribute_values:dict,github_access_token:str) -> Object:
    '''Returns a new or existing object of type 'User'. 
    In case new object is created, it is added to the objects and users dictionaries 
    and `new_user_attributes` function is called to create and store its attributes.'''

    user_id = user_data.get("id")

    if user_id is None:
        print("Couldn't extract user id.")
        return None
    
    else:
        existing_user = users.get(user_id)

        if existing_user is not None:
            return existing_user
        
        else:
            description = f"user:{user_id}"
            object_type = object_types.get('user')

            new_object = Object(object_type,description)
            objects[new_object.id] = new_object
            users[user_id] = new_object

            new_user_attributes(new_object,user_id,object_attributes,object_attribute_values,github_access_token)

            return new_object
        
def new_user_attributes(user_object:Object,user_id,object_attributes:dict,object_attribute_values:dict,github_access_token:str) -> None:
    '''Sets the attributes for a new object of type `user` and adds them to the object_attribute_values dictionary.'''

    attributes = [['user:id','id'],              # first string is key to get attribute, second string is key to use to extract from user_data
                  ['user:login','login'],
                  ['user:url','html_url'],
                  ['user:type','type'],]
    
    user_data = get_user_data(user_id,github_access_token)
    
    timestamp = user_data.get('created_at')
    
    for attribute_def in attributes:
        object_attribute = object_attributes.get(attribute_def[0])  # get object_attribute with attribute_name
        attribute_value = user_data.get(attribute_def[1])          # extract attribute_value from issue_data with defined key

        new_object_attribute_value = ObjectAttributeValue(user_object,object_attribute,timestamp,attribute_value)

        object_attribute_values[new_object_attribute_value.id] = new_object_attribute_value

    return None


def new_event_created(issue_data:dict,event_types:dict,events:dict) -> Event:
    '''Returns a new event of type `created` and adds it to the objects dictionary.'''

    description = f"create #{issue_data.get('number')}"
    timestamp = issue_data.get('created_at')
    event_type = get_or_create_event_type('created',event_types)

    new_event = Event(event_type,timestamp,description)
    events[new_event.id] = new_event

    return new_event


def new_timeline_event(issue_object,timeline_event_data:dict,event_types:dict,events:dict) -> Event:
    '''Returns new event. Event type is determined by `timeline_event_data`. 
    New types are added to `event_types`. New event is added to `events`.'''

    event_type_name = timeline_event_data.get('event')
    timestamp = timeline_event_data.get('created_at')

    if (event_type_name is None) or (timestamp is None): 
        # timeline_event_data does not follow the generic data mapping

        if event_type_name == 'committed':
            timestamp = (timeline_event_data.get('committer')).get('date')
        elif event_type_name == 'reviewed':
            timestamp = timeline_event_data.get('submitted_at')
        # elif event_type == 'line-commented':
            # multiple events in one, todo
        else:
            print(f"Couldn't map below event data for {event_type_name}")
            print(json.dumps(timeline_event_data))
            print('\n')
            return None
        
    # get the correct EventType object
    event_type = get_or_create_event_type(event_type_name,event_types)

    # create new event and add it to events
    new_event = Event(event_type,timestamp,f"{event_type_name} ({issue_object.description})")
    events[new_event.id] = new_event

    return new_event


def get_or_create_event_type(description:str,event_types:dict) -> EventType:
    """Use `description` as key to retrieve item from `event_types`.
     If item does not exist, create new EventType object and add it to `event_types`."""
    event_type = event_types.get(description)

    if event_type is None:
        event_type = EventType(description)
        event_types[description] = event_type

    return event_type


def link_event_to_object(event:Event,object:Object,qualifier_name,description,relation_qualifiers:dict,event_to_object:dict) -> EventToObjectRelation:
    '''Returns a new event-to-object link and adds it to the event_to_object dictionary.'''
    
    new_link = EventToObjectRelation(event,object,relation_qualifiers.get(qualifier_name),description)
    event_to_object[new_link.id] = new_link

    return  new_link

def link_object_to_object(from_object:Object,to_object:Object,timestamp,qualifier_name,description,relation_qualifiers:dict,object_to_object:dict) -> ObjectToObjectRelation:
    '''Returns a new object-to-object link and adds it to the event_to_object dictionary.'''
    
    new_link = ObjectToObjectRelation(from_object,to_object,relation_qualifiers.get(qualifier_name),timestamp,description)
    object_to_object[new_link.id] = new_link

    return  new_link
