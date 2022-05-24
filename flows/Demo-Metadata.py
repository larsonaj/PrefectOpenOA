import prefect

# Import python helpers
import json
import datetime
import sys
from azure.cosmos import exceptions, CosmosClient, PartitionKey


# Import tasks
from prefect.tasks import databricks
from prefect import Flow, task

# Import helpers
from prefect.client import Secret
from prefect.tasks.secrets import PrefectSecret
from prefect import config
from prefect.run_configs import UniversalRun
from prefect.storage import GitHub
from prefect.tasks import snowflake

## Parse metadata
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient

credential = AzureNamedKeyCredential("jlarrimoresadl", "gGhr1bwuV7JbsK4PN4kClSBoBkFrbR/Z3Q3d2ag84z65gi4ntNvJgdo7J9QKMczujaf9hGZwMjy+b3sePLVOTA==")

service = TableServiceClient(endpoint="https://jlarrimoresadl.table.core.windows.net", credential=credential)

tc = service.get_table_client('prefectmetadata')

git_options = tc.get_entity(partition_key="metadata", row_key="github_openoa")



# # storage
#with open('.\metadata\storage.json', 'r') as f:
git_parsed = json.loads(git_options['jsonvalue'])

# flow_storage = git_parsed['github_openoa']


# task info
#with open('.\metadata\adls_to_snowflake.json', 'r') as f:

sf_option = tc.get_entity(partition_key="metadata", row_key="snowflake_connection")

snowflake_parsed = json.loads(sf_option['jsonvalue'])

snowflake_conn = snowflake_parsed["snowflake_connection"]


# with open('.\metadata\databricks_notebook.json', 'r') as f:
#     dbx_parsed = json.loads(f.read())

# dbx_run_info = dbx_parsed["databricks_openoa"]

# print(git_parsed['repo'])


# Configure Context
storage = GitHub(
    repo=git_parsed['repo'],
    path=git_parsed['path'],
    ref=git_parsed['branch'],
    access_token_secret=git_parsed['secret_name']
)

run_config = UniversalRun(labels=['DESKTOP-ETPQA0T'])

## Build task specifications

# Snowflake account
account_prefix = snowflake_conn['account_prefix']
wh_name = snowflake_conn['warehouse_name']
db_name = snowflake_conn['database_name']
schema_name = snowflake_conn['schema_name']
user_name = snowflake_conn['user_name']
sn_pw_name = snowflake_conn['password_secret']

sn_password = PrefectSecret(sn_pw_name)


## Task specifications

snowflake_specs = snowflake_parsed['task_specs']

snowflake_task_specs = {'max_retries':snowflake_specs['max_retries'],
                        'retry_delay':datetime.timedelta(seconds=snowflake_specs['retry_delay'])}

query = snowflake_parsed['queries']

query_text = query['demo_query']


# Build tasks
@task(name='Printer Task')
def say_hello(printer):
    logger = prefect.context.get("logger")
    logger.info(f"{printer}")
#
@task(name='New Printer Task')
def new_printer(item):
    print(item)
    pass



## Build flow

with Flow("run-snowflake-demo-meta", run_config=run_config, storage=storage) as flow:
    password = PrefectSecret('SNOWFLAKE_PW')
    snowflake_run = snowflake.SnowflakeQuery(query=query_text, account=account_prefix, warehouse=wh_name,
                        database=db_name, schema=schema_name, user=user_name, **snowflake_task_specs)
    results = snowflake_run(password=password)
    logging = say_hello(results)
    print_results = new_printer('test')
    flow.add_edge(results, print_results)
