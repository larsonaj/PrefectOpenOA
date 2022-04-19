import prefect


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

## Configure Context
config.cloud.use_local_secrets=False


storage = GitHub(
    repo='larsonaj/PrefectOpenOA',
    path=f"/flows/Open_OA.py",
    ref="dev",
    access_token_secret="GITHUB_API_KEY"
)

run_config = UniversalRun(labels=['DESKTOP-ETPQA0T'])

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



with Flow("Open-OA-etl", storage=storage, run_config=run_config) as flow:
    # Run DBX Notebook
    notebook_run = databricks.DatabricksSubmitRun(json=json)
    notebook_run(databricks_conn_secret=dbx_password)
    
    # Ingest with Snowflake

    snowflake_task = snowflake.SnowflakeQuery(query=query_text, account=account_prefix, warehouse=wh_name, 
                            database=db_name, schema=schema_name, user=user_name)
    results = snowflake_task(password=sn_password)

    # Run DBT Model