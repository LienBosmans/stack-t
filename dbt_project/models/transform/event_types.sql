select
    md5(event_type) as id,
    event_type as description
from read_csv('models/transform/event_types.csv',delim=',',header=true,auto_detect=true)
