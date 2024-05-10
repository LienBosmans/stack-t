select
    md5(object_type) as id,
    object_type as description
from read_csv('models/transform/object_types.csv',delim=',',header=true,auto_detect=true)
