select
    md5(event_attribute) as id,
    md5(event_type) as event_type_id,
    event_attribute as description,
    datatype as datatype
from read_csv('models/transform/event_attributes.csv',delim=',',header=true,auto_detect=true)
