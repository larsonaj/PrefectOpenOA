import prefect

# Import python helpers
import json

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
    access_token_secret=git_prased['secret_name']
)

# storage = GitHub(
#     repo='larsonaj/PrefectOpenOA',
#     path=f"/flows/Open_OA.py",
#     ref="dev",
#     access_token_secret="GITHUB_API_KEY"
# )

run_config = UniversalRun(labels=['DESKTOP-ETPQA0T'])

<<<<<<< HEAD
## Run Variables
# DBX
sn_password = PrefectSecret('SNOWFLAKE_PW')
dbx_password = PrefectSecret('DBX_API_TOKEN')
json = {'existing_cluster_id': "0221-224854-qyhjvmno",
    'notebook_task': {
        'notebook_path': "/Users/alarson@captechventures.com/project_CapTech" 
        }
    }
    
# Snowflake
account_prefix = 'captech_partner.us-east-1'
wh_name = 'XS_WH'
db_name = 'TEST_DB'
schema_name = 'PUBLIC'
user_name = 'alarson'

query_text = """select top 10 * from OpenOA_Scada"""
=======

## Run Variables

# Dbx information
dbx_pw_name = dbx_run_info['secret_name']
dbx_password = PrefectSecret(dbx_pw_name)

# dbx_password = PrefectSecret('DBX_API_TOKEN')

dbx_payload = dbx_run_info['run_payload']

# json = {'existing_cluster_id': "0221-224854-qyhjvmno",
#     'notebook_task': {
#         'notebook_path': "/Users/alarson@captechventures.com/project_CapTech"
#         }
#     }

# Snowflake account
account_prefix = snowflake_conn['account_prefix']
wh_name = snowflake_conn['warehouse_name']
db_name = snowflake_conn['database_name']
schema_name = snowflake_conn['schema_name']
user_name = snowflake_conn['user_name']
sn_pw_name = snowflake_conn['password_secret']

sn_password = PrefectSecret(sn_pw_name)

# sn_password = PrefectSecret('SNOWFLAKE_PW')

# account_prefix = 'captech_partner.us-east-1'
# wh_name = 'XS_WH'
# db_name = 'TEST_DB'
# schema_name = 'PUBLIC'
# user_name = 'alarson'


## Task specifications
dbx_specs = {}
snowflake_specs = snowflake_parsed['task_specs']

# snowflake_specs = {'max_retries':5,
#                     'retry_delay':datetime.timedelta(seconds=5),
#                     'upstream_task': ['notebook_run']}

dbt_specs = {}
>>>>>>> fe2a3df6434916b4f458a68c87730f6b32298481



with Flow("Open-OA-etl", storage=storage, run_config=run_config) as flow:
    # Run DBX Notebook
    notebook_run = databricks.DatabricksSubmitRun(json=dbx_payload)
    notebook_run(databricks_conn_secret=dbx_password)

    # Ingest with Snowflake
    # SCADA
    scada_query = actions.ingest_scada_to_snowflake() ## grab from ADLS
    snowflake_task = snowflake.SnowflakeQuery(query=scada_query, account=account_prefix, warehouse=wh_name,
                            database=db_name, schema=schema_name, user=user_name, **snowflake_task_specs)
    snowflake_task(password=sn_password)

    # Reanalysis
    reanal_query = actions.ingest_reanalysis_to_snowflake()
    snowflake_task = snowflake.SnowflakeQuery(query=reanal_query, account=account_prefix, warehouse=wh_name,
                            database=db_name, schema=schema_name, user=user_name, **snowflake_task_specs)
    snowflake_task(password=sn_password)

    # Master Data
    mdm_query = actions.ingest_masterdata_to_snowflake()
    snowflake_task = snowflake.SnowflakeQuery(query=mdm_query, account=account_prefix, warehouse=wh_name,
                            database=db_name, schema=schema_name, user=user_name, **snowflake_task_specs)
    snowflake_task(password=sn_password)

    # Curtailment
    curt_query = actions.ingest_curtailment_to_snowflake()
    snowflake_task = snowflake.SnowflakeQuery(query=curt_query, account=account_prefix, warehouse=wh_name,
                            database=db_name, schema=schema_name, user=user_name, **snowflake_task_specs)
    snowflake_task(password=sn_password)

    # Run DBT Model
