# Stack't

Stack't is a small data stack (DuckDB + dbt) in a box (Docker container), that specializes in ingesting object-centric event logs in the OCEL2 format & transforming them into a data structure that is more general and therefore friendlier for data engineers. Looking at data pipelines is not that fun though, so now it can also generate graph visualizations of the ingested process data in Neo4j!

![A graph visualization example for Transport Documents, using the ocel2_logistics dataset available on https://doi.org/10.5281/zenodo.8289899](GraphVisualizationTransportDocuments.png)

Stack't was inspired by
* 'Modern Data Stack in a Box with DuckDB', blogpost by Jacob Matson. (https://duckdb.org/2022/10/12/modern-data-stack-in-a-box.html)
* van der Aalst, Wil MP. "Object-Centric Process Mining: Unravelling the Fabric of Real Processes." Mathematics 11.12 (2023): 2691. (https://www.mdpi.com/2227-7390/11/12/2691)
* Berti, Alessandro, et al. "OCEL (Object-Centric Event Log) 2.0 Specification." (2023). (https://www.ocel-standard.org/2.0/ocel20_specification.pdf)

## Stack't relational schema

![a data engineer friendly relational schema for OCEL2 by Lien Bosmans version 31/10/2023](EFOCEL_data_structure_v20231031.png)

## Object-centric process visualizations

Below gif shows how a classic directly-follow event graph is translated into the object-centric version used in this project.
The classic graph implicitly assumes that every event is linked to a single object (case). The visualization shows what happens if some events are linked to multiple objects and how this leads to multiple directly-follow edges. To differentiate between them, the objects are "pulled out" of the event nodes into their own separate object snapshot nodes. Finally, we add additional object snapshots for object attribute updates and include object-to-object relations.

![A gif showing how an classic directly-follow graph is translated into an object-centric version](process_visualizations.gif)

For the overview process visualizations we map all event nodes to event type nodes and all object snapshot nodes to object snapshot grouping nodes. Multiple groupings are possisble, but here the object snapshots are grouping based on object type, event type of the previous event and the set of updated attributes. Below figures show three examples of individual process visualizations and how these would be combined into an overview visualization.

![Examples of individual process visualizations](example_individual_process_visualizations.png)

![Example overview visualization](example_overview_process_visualization.png)


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

* In case you only want to run tests on the input SQLite, replace `dbt build` by `dbt build -s models/staging/*`.
* Use a database administrator (f.e. DBeaver) to explore the resulting DuckDB database `dev.duckdb`. The overview tables about your event log are located inside the `mart` schema.

To use the generated csv files inside `neo4j/import` for creating a graph database instance, run below commands. (Assuming you already have a neo4j image, otherwise use `docker pull neo4j` first.)

```
docker run --rm -it -v my-path-to\stack-t\neo4j\data:/data -v my-path-to\stack-t\neo4j\import:/import -p 7474:7474 -p 7687:7687 --env NEO4J_AUTH=none --entrypoint /bin/bash neo4j

bin/neo4j-admin database import full --nodes=/import/overview_event_type_nodes.csv --nodes=/import/overview_object_snapshot_grouping_nodes.csv --relationships=/import/overview_directly_follow_edges.csv --relationships=/import/overview_object_to_object_edges.csv  --nodes=/import/traces_event_nodes.csv --nodes=/import/traces_object_snapshot_nodes.csv --relationships=/import/traces_directly_follow_edges.csv --relationships=/import/traces_object_to_object_edges.csv  --overwrite-destination neo4j

exit
```

Next, start a new neo4j container without overwriting the entrypoint.

```
docker run --rm -it -v C:\github_projects\stack-t\neo4j\data:/data -v C:\github_projects\stack-t\neo4j\import:/import -p 7474:7474 -p 7687:7687 --env NEO4J_AUTH=none neo4j
```

A GUI to interact with the database can be found at `localhost:7474`.

## Slower start

You'll need a Docker environment (f.e. Docker desktop) to build and run the Python image that is defined in `Dockerfile`. Once Docker desktop is running, you execute a `docker build` command to build the image. This may take a while the first time.

```
docker build --progress plain -t stackt .
```

When the image is built successfully, you can run a container of this image using a `docker run` command. You need to replace the 'my-path-to' with the full path of this project on your computer . This mounts the `stack-t` folder on your container and allows you to use the files inside it, make changes to them and create new ones.

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

In case you only want to run tests on the input SQLite (this is a good idea if you are using a data source for the first time!), replace `dbt build` by `dbt build -s models/staging/*`.

If all models run successfully, you can use a database manager (f.e. DBeaver) to view the tables inside your DuckDB database `dev.duckdb`. The overview tables about your event log are located inside the `mart` schema.

You can use the instructions on the DuckDB website to download and install DBeaver: https://duckdb.org/docs/guides/sql_editors/dbeaver.html.


During the execution of `dbt build`, some csv files were created inside the folder `neo4j/import`. These file can be used to create a graph database with Neo4j. To run this database inside a Docker container, you need to have an image of Neo4j. You can download the official Neo4j Docker image by running `docker pull neo4j`. You only need to do this once.

Next, start a Neo4j container with below command. You can have multiple containers running at the same time, so there is no need to close your `stackt` container. Just open a new terminal to get started.

```
docker run --rm -it -v my-path-to\stack-t\neo4j\data:/data -v my-path-to\stack-t\neo4j\import:/import -p 7474:7474 -p 7687:7687 --env NEO4J_AUTH=none --entrypoint /bin/bash neo4j
```

Inside this container, run below code to import the csv files in a new database called `neo4j`.

```
bin/neo4j-admin database import full --nodes=/import/overview_event_type_nodes.csv --nodes=/import/overview_object_snapshot_grouping_nodes.csv --relationships=/import/overview_directly_follow_edges.csv --relationships=/import/overview_object_to_object_edges.csv  --nodes=/import/traces_event_nodes.csv --nodes=/import/traces_object_snapshot_nodes.csv --relationships=/import/traces_directly_follow_edges.csv --relationships=/import/traces_object_to_object_edges.csv  --overwrite-destination neo4j
```

After this is done, close this container using the command `exit`. Next, start a new neo4j container. This time we do not overwrite the entrypoint, so it starts up normally.

```
docker run --rm -it -v C:\github_projects\stack-t\neo4j\data:/data -v C:\github_projects\stack-t\neo4j\import:/import -p 7474:7474 -p 7687:7687 --env NEO4J_AUTH=none neo4j
```

As long as this container is running, you can play around with the database using a interactive graphic user interface (GUI) in the browser at `localhost:7474`.


## Possible issues and workarounds

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


### Failure in test relationships_stg_

```
  Failure in test relationships_stg_object_object_ocel_target_id__ocel_id__ref_stg_object_ (models/staging_models.yml)
    Got 2028 results, configured to fail if != 0
```

An error message like above means that an automatic tests on the dbt models failed. More specifically, there are foreign keys in the column `target_id` of the table  `object_object` that do not exist in the column `ocel_id` of the table `object`. Note that `2028 results` means that there are 2028 foreign keys missing. Since this can include duplicates, the number of missing rows in the `object` table is probably lower.

Ideally, you add these missing rows to the SQLite input file. If you cannot or do not want to modify the dataset directly, you can also add the rows in the dbt staging model. To get an overview of all missing keys, you can use a modified version of below query.

```
select distinct
    object_object.ocel_target_id 
from 
    main_staging.stg_object_object as object_object
    left join main_staging.stg_object as object
        on object_object.ocel_target_id = object.ocel_id 
where
    object.ocel_id is null
```

To add the missing rows during staging, you can add a csv file in the staging folder with the missing information. In this case, we used the `ocel_id` of the missing objects to guess their `ocel_type`. Please make sure your column headers match exactly with the column names of the table with missing rows. In this example, the csv file is called `missing_rows_object.csv`.

```
"ocel_id","ocel_type"
invoice receipt:0,invoice receipt
invoice receipt:1,invoice receipt
invoice receipt:5,invoice receipt
invoice receipt:7,invoice receipt
...
```

Finally, add a `UNION ALL` statement to your staging model (`stg_object`) to add the missing rows from the csv file.

```
select distinct * from {{ source('ocel2_procure_to_pay','object') }}
UNION ALL
select * from read_csv('models/staging/missing_rows_object.csv',delim=',',header=true,auto_detect=true)
```

If you run `dbt build` again, the missing rows will now be added in your dbt model without modifying the input file.

## About me and this project

I'm Lien Bosmans, a date enthusiast located in Leuven (Belgium). This project is my personal adventure into process mining & building a MDS-in-a-box. Feel free to reach out on lienbosmans@live.com with your feedback and questions.
