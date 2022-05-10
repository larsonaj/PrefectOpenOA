import prefect

# Import python helpers
import json
import datetime

# Import tasks
from prefect.tasks import databricks
from prefect import Flow, task
import actions

# Import helpers
from prefect.client import Secret
from prefect.tasks.secrets import PrefectSecret
from prefect import config
from prefect.run_configs import UniversalRun
from prefect.storage import GitHub
from prefect.tasks import snowflake

## Parse metadata

# storage
git_raw = open('.\metadata\storage.json')
git_parsed = json.loads(git_raw)



# task info
snowflake_raw = open('.\metadata\adls_to_snowflake.json')
snowflake_parsed = json.loads(snowflake_raw)
snowflake_conn = snowflake_parsed["snowflake_connection"]

dbx_raw = open('.\metadata\databricks_notebook.json')
dbx_parsed = json.loads(dbx_raw)
dbx_run_info = dbx_parsed["databricks_openoa"]



## Configure Context
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

query = snowflake_parsed['queries']

query_text = query['demo_query']

## Build tasks
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
                        database=db_name, schema=schema_name, user=user_name, **snowflake_specs)
    results = snowflake_run(password=password)
    logging = say_hello(results)
    print_results = new_printer('test')
    flow.add_edge(results, print_results)
