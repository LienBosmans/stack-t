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


def get_object_user(user_data:dict,users:dict,object_types:dict,objects:dict,object_attributes:dict,object_attribute_values:dict,github_access_token:str,is_team:bool=False) -> Object:
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
            if is_team:
                description = f"team:{user_id}"
            else:
                description = f"user:{user_id}"
            
            if is_team:
                object_type = object_types.get('team')
            else:
                object_type = object_types.get('user')

            new_object = Object(object_type,description)
            objects[new_object.id] = new_object
            users[user_id] = new_object

            if is_team:
                new_team_attributes(new_object,user_data,object_attributes,object_attribute_values)
            else:
                new_user_attributes(new_object,user_id,user_data,object_attributes,object_attribute_values,github_access_token)

            return new_object
        
def new_user_attributes(user_object:Object,user_id,partial_user_data,object_attributes:dict,object_attribute_values:dict,github_access_token:str) -> None:
    '''Sets the attributes for a new object of type `user` and adds them to the object_attribute_values dictionary.'''

    attributes = [['user:id','id'],              # first string is key to get attribute, second string is key to use to extract from user_data
                  ['user:login','login'],
                  ['user:url','html_url'],
                  ['user:type','type'],]
    
    user_data = get_user_data(user_id,github_access_token)
    if user_data is None:
        user_data = partial_user_data
        timestamp = '1970-01-01T00:00:00Z' # created_at timestamp cannot be fetched
    else:
        timestamp = user_data.get('created_at')
    
    for attribute_def in attributes:
        attribute_value = user_data.get(attribute_def[1])          # extract attribute_value from issue_data with defined key

        if attribute_value is not None:
            object_attribute = object_attributes.get(attribute_def[0])  # get object_attribute with attribute_name
            new_object_attribute_value = ObjectAttributeValue(user_object,object_attribute,timestamp,attribute_value)
            object_attribute_values[new_object_attribute_value.id] = new_object_attribute_value

    return None


def new_team_attributes(team_object:Object,team_data:dict,object_attributes:dict,object_attribute_values:dict) -> None:
    '''Sets the attributes for a new object of type `user` and adds them to the object_attribute_values dictionary.'''

    attributes = [['team:id','id'],              # first string is key to get attribute, second string is key to use to extract from user_data
                  ['team:slug','slug'],
                  ['team:name','name'],
                  ['team:privacy','privacy'],
                  ['team:url','html_url'],]
    
    timestamp = '1970-01-01T00:00:00Z' # a lot of teams are private, therefore fetching the timestamp when the team is created is probably not possible anyway
    
    for attribute_def in attributes:
        attribute_value = team_data.get(attribute_def[1])          # extract attribute_value from issue_data with defined key

        if attribute_value is not None:
            object_attribute = object_attributes.get(attribute_def[0])  # get object_attribute with attribute_name
            new_object_attribute_value = ObjectAttributeValue(team_object,object_attribute,timestamp,attribute_value)
            object_attribute_values[new_object_attribute_value.id] = new_object_attribute_value

    return None


def new_object_commit(commit_event_data:dict,object_types:dict,objects:dict,object_attributes:dict,object_attribute_values:dict) -> Object:
    '''Returns a new object of type `commit` and adds it to the objects dictionary.
    Next, calls function `new_commit_attributes` to create and store its attributes.'''

    description = f"sha:{commit_event_data.get('sha')}"
    object_type = object_types.get('commit')

    new_object = Object(object_type,description)
    objects[new_object.id] = new_object

    new_commit_attributes(new_object,commit_event_data,object_attributes,object_attribute_values)

    return new_object


def new_commit_attributes(issue_object:Object,commit_event_data:dict,object_attributes:dict,object_attribute_values:dict) -> None:
    '''Sets the attributes for a new object of type `commit` and adds them to the object_attribute_values dictionary.'''
    
    attributes = [['commit:sha','sha'],              # first string is key to get attribute, second string is key to use to extract from issue_data
                  ['commit:commit_message','message'],
                  ['commit:url','html_url'],]
    
    timestamp = (commit_event_data.get('committer')).get('date')
    
    for attribute_def in attributes:
        object_attribute = object_attributes.get(attribute_def[0])  # get object_attribute with attribute_name
        attribute_value = commit_event_data.get(attribute_def[1])          # extract attribute_value from issue_data with defined key

        new_object_attribute_value = ObjectAttributeValue(issue_object,object_attribute,timestamp,attribute_value)

        object_attribute_values[new_object_attribute_value.id] = new_object_attribute_value

    return None


def new_event_created(issue_data:dict,event_types:dict,events:dict,event_attributes:dict,event_attribute_values:dict) -> Event:
    '''Returns a new event of type `created` and adds it to the objects dictionary.
    Next, calls function `new_issue_attributes` to create and store its attributes.'''

    description = f"create #{issue_data.get('number')}"
    timestamp = issue_data.get('created_at')
    event_type = get_or_create_event_type('created',event_types)

    new_event = Event(event_type,timestamp,description)
    events[new_event.id] = new_event

    new_event_attributes(new_event,issue_data,event_attributes,event_types,event_attribute_values)

    return new_event


def new_timeline_event(issue_object,timeline_event_data:dict,event_types:dict,events:dict,event_attributes:dict,event_attribute_values:dict,return_user_data:bool=False) -> Event:
    '''Returns new event. Event type is determined by `timeline_event_data`. 
    New types are added to `event_types`. New event is added to `events`.
    Next, calls function `new_issue_attributes` to create and store its attributes.
    In case `return_user_data` is set to `True`, the user data will be returned as a second argument.'''

    event_type_name = timeline_event_data.get('event')
    timestamp = timeline_event_data.get('created_at')
    user_data = {'actor':timeline_event_data.get('actor')}

    
    if event_type_name == 'committed':
        timestamp = (timeline_event_data.get('committer')).get('date')
        user_data = {'comitter':timeline_event_data.get('comitter')}

    elif event_type_name == 'reviewed':
        timestamp = timeline_event_data.get('submitted_at')
        user_data = {'actor':timeline_event_data.get('user')}

    elif event_type_name in ('review_requested','review_request_removed'):
        requested_reviewer = timeline_event_data.get('requested_reviewer')
        if requested_reviewer is not None:
            user_data['requested_reviewer'] = requested_reviewer
        
        requested_team = timeline_event_data.get('requested_team')
        if requested_team is not None:
            user_data['requested_team'] = requested_team

    elif event_type_name in ('assigned','unassigned'):
        user_data['assignee'] = timeline_event_data.get('assignee')

    # elif event_type == 'line-commented':
        # multiple events in one, todo
            
    if (event_type_name is None) or (timestamp is None) or (user_data is None): 
        # timeline_event_data does not follow the generic data mapping, or the exception handled above
        print(f"Couldn't map below event data for {event_type_name}")
        print(json.dumps(timeline_event_data))
        print('\n')
        return None
        
    # get the correct EventType object
    event_type = get_or_create_event_type(event_type_name,event_types)

    # create new event and add it to events
    new_event = Event(event_type,timestamp,f"{event_type_name} ({issue_object.description})")
    events[new_event.id] = new_event

    new_event_attributes(new_event,timeline_event_data,event_attributes,event_types,event_attribute_values)

    if return_user_data:
        return [new_event,user_data]
    else:
        return new_event


def new_event_attributes(new_event:Event,event_data:dict,event_attributes:dict,event_types:dict,event_attribute_values:dict) -> None:
    """Sets the attributes of a new event and add them to the event_attribute_values dictionary."""

    attributes = [['author_association','author_association'],]     # first string is key to get attribute, second string is key to use to extract from event_data

    for attribute_def in attributes:
        # get event_attribute with attribute_name
        event_attribute = get_or_create_event_attribute(attribute_def[0],new_event.event_type_description,event_types,event_attributes)

        # extract attribute_value from issue_data with defined key
        attribute_value = event_data.get(attribute_def[1])

        if attribute_value is not None:
            new_event_attribute_value = EventAttributeValue(new_event,event_attribute,attribute_value)
            event_attribute_values[new_event_attribute_value.id] = new_event_attribute_value

    return None


def get_or_create_event_type(description:str,event_types:dict) -> EventType:
    """Use `description` as key to retrieve item from `event_types`.
     If item does not exist, create new EventType object and add it to `event_types`."""
    event_type = event_types.get(description)

    if event_type is None:
        event_type = EventType(description)
        event_types[description] = event_type

    return event_type


def get_or_create_event_attribute(attribute_description:str,event_type_description:str,event_types:dict,event_attributes:dict) -> EventAttribute:
    """Use `attribute_description` and `event_type.description` as key to retrieve item from `event_attributes`.
     If item does not exist, create new EventAttribute object and add it to `event_attributes`."""
    
    key = f"{event_type_description}:{attribute_description}"
    event_attribute = event_attributes.get(key)

    if event_attribute is None:
        event_type = get_or_create_event_type(event_type_description,event_types)

        if attribute_description in ('author_association'):
            datatype = 'string'
        else: # catch-all just in case
            print(f"Unknown attribute_description '{attribute_description}' detected, defaulted to 'string' as datatype. Please add this attribute in the function `get_or_create_event_attribute()` for next time.")
            datatype = 'string'

        event_attribute = EventAttribute(event_type,attribute_description,datatype)
        event_attributes[key] = event_attribute

    return event_attribute


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
