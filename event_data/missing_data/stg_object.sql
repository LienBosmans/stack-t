select distinct * from {{ source('ocel2_logistics','object') }}
UNION ALL
select * from read_csv('../event_data/missing_data/ocel2_logistics_missing_object.csv',delim=',',header=true,auto_detect=true)
