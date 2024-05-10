select
    md5(qualifier) as id,
    qualifier as description,
    'string' as datatype
from read_csv('models/transform/qualifiers.csv',delim=',',header=true,auto_detect=true)
