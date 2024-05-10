{{ config(
    pre_hook = "SET GLOBAL sqlite_all_varchar = true;",
    post_hook = "SET GLOBAL sqlite_all_varchar = false;"
) }}

with source as (
    select * from {{ source('ocel2_logistics','object_Container') }}
),
fixed_text_null as (
    select
        ocel_id,
        ocel_time::datetime as ocel_time,
        AmountofHandlingUnits::numeric as AmountofHandlingUnits,
        Status,
        case
            when Weight = 'null' then null
            else Weight::numeric
        end as Weight, -- Mismatch Type Error: Invalid type in column "Weight": expected float or integer, found "null" of type "text" instead.
        ocel_changed_field
    from source
)

select * from fixed_text_null
