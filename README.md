# Stack't

Stack't is a small data stack (DuckDB + dbt) in a box (Docker container), that specializes in ingesting object-centric event logs in the OCEL2 format & transforming them into a data structure that is more general and therefore friendlier for data engineers. 

Stack't was inspired by
* 'Modern Data Stack in a Box with DuckDB', blogpost by Jacob Matson. (https://duckdb.org/2022/10/12/modern-data-stack-in-a-box.html)
* van der Aalst, Wil MP. "Object-Centric Process Mining: Unraveling the Fabric of Real Processes." Mathematics 11.12 (2023): 2691. (https://www.mdpi.com/2227-7390/11/12/2691)
* Berti, Alessandro, et al. "OCEL (Object-Centric Event Log) 2.0 Specification." (2023). (https://www.ocel-standard.org/2.0/ocel20_specification.pdf)

## Stack't relational schema

![a data engineer friendly relational schema for OCEL2 by Lien Bosmans version 31/10/2023](EFOCEL_data_structure_v20231031.png)

## Quick start

* Save your OCEL2 event log in SQLite format inside the folder `event_log_datasets`. You can export an example log from https://ocelot.pm/.
* Update the source and file names at the top of the python script `python_code\generate_dbt_models.py`.
* Run below commands to get started.

    ```
    docker build --progress plain -t stackt .
    docker run --rm -it -v my-path-to\stack-t\:/stackt stackt
    python3 ../python_code/generate_dbt_models.py 
    dbt build
    ```
* Use a database administrator (f.e. DBeaver) to explore the resulting DuckDB database `dev.duckdb`. The overview tables about your event log are located inside the `mart` schema.

## Slower start

You'll need a Docker environment (f.e. Docker desktop) to build and run the Python image that is defined in `Dockerfile`. Once Docker desktop is running, you execute a `docker build` command to build the image. This may take a while the first time.

```
docker build --progress plain -t stackt .
```

When the image is built succesfully, you can run a container of this image using a `docker run` command. You need to replace the 'my-path-to' with the full path of this project on your computer . This mounts the `stack-t` folder on your container and allows you to use the files inside it, make changes to them and create new ones.

```
docker run --rm -it -v my-path-to\stack-t\:/stackt stackt
```

Now you'll see something like `root@b7095ae55002:/stackt/dbt_project# `. This means you are now working inside the container. Inside the file `python_code\generate_dbt_models.py` you need to change the source name and file name, based on your ocel2 event log.

```
## Change source_name and sqlite_db_name below!

source_name = 'ocel2_source_name'
sqlite_db_name = 'ocel2_source_name.sqlite'
```

This python script is used to automatically generate the dbt models for staging and transformation. Run it inside the container using below command.

```
python3 ../python_code/generate_dbt_models.py 
```

Finally, you can run all dbt models.

```
dbt build
```

If all models run succesfully, you can use a database manager (f.e. DBeaver) to view the tables inside your DuckDB database `dev.duckdb`. The overview tables about your event log are located inside the `mart` schema.

You can use the instructions on the DuckDB website to download and install DBeaver: https://duckdb.org/docs/guides/sql_editors/dbeaver.html.

## Possible issues and work-arounds

### Mismatch Type Error

Since SQLite does not enforce column types, you might encounter a `Mismatch Type Error` when building your dbt models. You can bypass this by adding hooks to your model. An example is included below. More information can be found here: https://duckdb.org/docs/archive/0.7.1/extensions/sqlite.html#data-types & here: https://docs.getdbt.com/reference/resource-configs/pre-hook-post-hook.

The error message
```
Runtime Error in model object_Container (models/staging/stg_object_Container.sql)
Mismatch Type Error: Invalid type in column "Weight": expected float or integer, found "null" of type "text" instead.
```
can be fixed by manually rewriting your `stg_object_Container.sql` as follows:
```
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
```

Note that we included
* a pre-hook to activate the global setting `sqlite_all_varchar` before running the model,
* a post-hook to de-activate the global setting `sqlite_all_varchar` after running the model,
* a case statement to replace the 'null' string values by proper null values, and
* explicit type casting for every column that is not `varchar`.



## About me and this project

I'm Lien Bosmans, a date enthusiast located in Leuven (Belgium). This project is my personal adventure into process mining & building a MDS-in-a-box. Feel free to reach out on lienbosmans@live.com with your feedback and questions.
