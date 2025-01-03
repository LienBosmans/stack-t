## User input

    # Create file `config.py` containing `GITHUB_ACCESS_TOKEN = <my_github_access_token>`
from config import GITHUB_ACCESS_TOKEN

    # Define repository
repo_owner = "dbt-labs"
repo_name = "dbt-core"
num_issues_to_get = 7


    # Define where to store output data
quack_database = '../event_data/github_logs/dbt-core.duckdb'


## Set-up

    # Load user-defined functions
from get_data_functions import *        # uses the PyGithub Python library to access the GitHub REST API to get the data
from class_definitions import *         # defines custom classes to store data (corresponds to final tables)
from initiate_types_functions import *  # contains pre-defined event/object/relation (attribute) types
from map_data_functions import *        # maps the data extracted via API to custom class objects
from output_data_functions import *     # converts custom class objects to dataframes (polars)

    # Load other libraries
import time                             # Used to provide user feedback on how long data extraction is taking.

    # Check if DuckDB database is available first. Don't connect to the database while the script is running!
print(f"Checking if DuckDB database file {quack_database} is available. New file will be created if it does not exist yet.")
con = duckdb.connect(quack_database)
con.close()
print(f"    IMPORTANT! Do not connect to DuckDB database file '{quack_database}' while this script is running!")
print(f"    (Or, if you like living on the edge, at least disconnect before all issues are extracted and the script tries writing to it.)")

    # Connect to repository
repo = connect_to_github_repo(GITHUB_ACCESS_TOKEN,repo_owner,repo_name)

    # Initiate global id used to generate unique integer id's
initiate_global_id()

    # Initiate dictionaries to store data
object_types = initiate_object_types()
object_attributes = initiate_object_attributes(object_types)
objects = {}
object_attribute_values = {}

existing_users = {} # aditional dictionary used to check if a user already exists as an object

event_types = {}
events = {}
event_attributes = {}
event_attribute_values = {}

relation_qualifiers = initiate_relation_qualifiers()
event_to_object = {}
object_to_object = {}
event_to_object_attribute_value = {}

## Data extraction & mapping        (can be slow because of REST API rate limits)
print(f"Starting extraction of object-centric event data from {num_issues_to_get} issues of repository {repo_owner}/{repo_name} using GitHub REST API via PyGitHub library.")
print("While you wait, you can read about GitHub API rate limits here: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api")

    # get list of issues
issues = get_issues(repo,num_issues=num_issues_to_get)

print_counter = 0
start_time = time.time()
for issue in issues:
    issue_data = issue.raw_data
    issue_number = issue_data.get('number')

    # new "issue" object with attributes
    issue_object = new_object_issue(issue_data,object_types,objects,object_attributes,object_attribute_values)

    # new "created" event
    created_event = new_event_created(issue_data,event_types,events)

    # link "issue" object to "created" event
    link_create_to_issue = link_event_to_object(created_event,issue_object,'created',"new issue created",relation_qualifiers,event_to_object)

    # get (or create) "user" object
    user_object = get_object_user(issue_data.get("user"),existing_users,object_types,objects,object_attributes,object_attribute_values,GITHUB_ACCESS_TOKEN)

    # link "user" object to "created" event
    link_create_to_user = link_event_to_object(created_event,user_object,'created',"new issue created by",relation_qualifiers,event_to_object)

    # link "user" object to "issue" object
    link_user_to_issue = link_object_to_object(issue_object,user_object,created_event.timestamp,'created',"created by",relation_qualifiers,object_to_object)

    # get list of timeline events
    timeline = get_events(issue)

    for timeline_event in timeline:
        timeline_event_data = timeline_event.raw_data

        # new event (type determined by timeline_event_data)
        new_event = new_timeline_event(issue_object,timeline_event_data,event_types,events)

        if new_event is not None:
            # link "issue" object to new event
            link_event_to_object(new_event,issue_object,'timeline_event',new_event.event_type_description,relation_qualifiers,event_to_object)

    # keep user informed about progress
    print_counter += 1
    perc_done = print_counter/num_issues_to_get
    seconds_done = time.time() - start_time
    print(f"    Extracting and mapping data for issue #{issue_number} done ...{round(100*perc_done,1)}% (about {round(seconds_done/perc_done - seconds_done,1)}s remaining)")

## Store the result
print(f"Saving object-centric event data extracted from {repo_owner}/{repo_name} to DuckDB database file {quack_database}.")
tables_to_store = [['object_types',object_types],
                   ['object_attributes',object_attributes],
                   ['objects',objects],
                   ['object_attribute_values',object_attribute_values],
                   ['event_types',event_types],
                   ['events',events],
                   ['event_attributes',event_attributes],
                   ['event_attribute_values',event_attribute_values],
                   ['relation_qualifiers',relation_qualifiers],
                   ['event_to_object',event_to_object],
                   ['object_to_object',object_to_object],
                   ['event_to_object_attribute_value',event_to_object_attribute_value],
                  ]

for tbl in tables_to_store:
    dataframe_to_persistent_duckdb(
        df_records=extract_dataframe(tbl[1]),
        table_name=tbl[0],
        duckdb_file_name=quack_database
        )
    print(f"    Table {tbl[0]} ({len(tbl[1])} records) done.")
    
print("All done!")
