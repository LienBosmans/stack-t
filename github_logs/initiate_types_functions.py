from class_definitions import *

def initiate_object_types() -> dict:
    """Initiates the object types `issue`, `user`, `team`, `commit`."""

    descriptions = ['issue',
                    'user',
                    'team',
                    'commit'
                   ]
    
    object_types = {}
    for description in descriptions:
        object_types[description] = ObjectType(description)

    return object_types

def initiate_object_attributes(object_types:dict) -> dict:
    """Initiates below object attributes, linking them to the correct object type.
    `issue`: `number`, `title`, `state`
    """

    descriptions = {'issue':[['number','integer'],
                             ['title','string'],
                             ['timeline_url','string']
                            ],
                    'user':[['id','integer'],
                            ['login','string'],
                            ['url','string'],
                            ['type','string'],
                           ],
                    'team':[['id','integer'],              
                            ['slug','string'],
                            ['name','string'],
                            ['privacy','string'],
                            ['url','string']
                           ],
                    'commit':[['sha','string'],              
                              ['commit_message','string'],
                              ['url','string'],
                           ],
                    }
    object_attributes = {}
    for object_type_description,object_attribute_descriptions in descriptions.items():
        object_type = object_types.get(object_type_description)

        for description in object_attribute_descriptions:
            object_attributes[f"{object_type_description}:{description[0]}"] = ObjectAttribute(object_type,description[0],description[1])

    return object_attributes


def initiate_relation_qualifiers() -> dict:
    """Initiates the relation qualifiers `created`, `timeline_event`."""

    descriptions = [['timeline_event','string'],
                    ['created','string'],
                    ['actor','string'],
                    ['requested_reviewer','string'],
                    ['requested_team','string'],
                    ['assignee','string'],
                    ['committer','string'],
                   ]
    
    relation_qualifiers = {}
    for description in descriptions:
        relation_qualifiers[description[0]] = RelationQualifier(description[0],description[1])

    return relation_qualifiers
