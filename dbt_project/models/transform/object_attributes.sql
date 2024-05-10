select
    md5(object_attribute) as id,
    md5(object_type) as object_type_id,
    object_attribute as description,
    datatype as datatype
from read_csv('models/transform/object_attributes.csv',delim=',',header=true,auto_detect=true)
