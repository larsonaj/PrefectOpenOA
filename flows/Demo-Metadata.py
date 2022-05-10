import prefect

# Import python helpers
import json
import datetime
import sys


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

# storage
with open(r'.\metadata\storage.json', 'r') as f:
    git_parsed = json.loads(f.read())

flow_storage = git_parsed['github_openoa']


# task info
with open(r'.\metadata\adls_to_snowflake.json', 'r') as f:
    snowflake_parsed = json.loads(f.read())

snowflake_conn = snowflake_parsed["snowflake_connection"]


with open(r'.\metadata\databricks_notebook.json', 'r') as f:
    dbx_parsed = json.loads(f.read())

dbx_run_info = dbx_parsed["databricks_openoa"]

print(flow_storage['secret_name'])


# Configure Context
storage = GitHub(
    repo=flow_storage['repo'],
    path=flow_storage['path'],
    ref=flow_storage['branch'],
    access_token_secret=flow_storage['secret_name']
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

# snowflake_specs = snowflake_parsed['task_specs']
snowflake_task_specs = {'max_retries':5,
                        'retry_delay':datetime.timedelta(seconds=5)}

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
