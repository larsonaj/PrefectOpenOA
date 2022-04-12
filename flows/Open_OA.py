import prefect


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



## Configure Context
storage = GitHub(
    repo='larsonaj/PrefectOpenOA',
    path=f"/flows/Open_OA.py",
    ref="dev",
    access_token_secret="GITHUB_API_KEY"
)

run_config = UniversalRun(labels=['DESKTOP-ETPQA0T'])

flow_type = 'ADLS_TO_SNOWFLAKE'




## Run Variables
if level == 'Prod':
    sn_password = PrefectSecret('SNOWFLAKE_PW')
else:
    sn_password = PrefectSecret('DEV_SNOWFLAKE_PW')

dbx_password = PrefectSecret('DBX_API_TOKEN')

json = {'existing_cluster_id': "0221-224854-qyhjvmno",
    'notebook_task': {
        'notebook_path': "/Users/alarson@captechventures.com/project_CapTech"
        }
    }

# Snowflake account
account_prefix = 'captech_partner.us-east-1'
wh_name = 'XS_WH'
db_name = 'TEST_DB'
schema_name = 'PUBLIC'
user_name = 'alarson'


{query: , warehouse: , database: ,...}

{json: {cluster_id:, notebook_task: {notebook_path}}}

## Task specifications
dbx_specs = {}
snowflake_specs = {'max_retries':5,
                    'retry_delay':datetime.timedelta(seconds=5),
                    'upstream_task': ['notebook_run']}
dbt_specs = {}



with Flow("Open-OA-etl", storage=storage, run_config=run_config) as flow:
    # task to grab JSON configs from ADLS based on "flow_type"

    # parse JSON into Snowflake part and ADLS part

    # Run DBX Notebook
    notebook_run = databricks.DatabricksSubmitRun(json=json)
    notebook_run(databricks_conn_secret=dbx_password)

    # Ingest with Snowflake
    # SCADA
    scada_query = actions.ingest_scada_to_snowflake() ## grab from ADLS
    snowflake_task = snowflake.SnowflakeQuery(query=scada_query, account=account_prefix, warehouse=wh_name,
                            database=db_name, schema=schema_name, user=user_name)
    snowflake_task(password=sn_password)

    # Reanalysis
    reanal_query = actions.ingest_reanalysis_to_snowflake()
    snowflake_task = snowflake.SnowflakeQuery(query=reanal_query, account=account_prefix, warehouse=wh_name,
                            database=db_name, schema=schema_name, user=user_name)
    snowflake_task(password=sn_password)

    # Master Data
    mdm_query = actions.ingest_masterdata_to_snowflake()
    snowflake_task = snowflake.SnowflakeQuery(query=mdm_query, account=account_prefix, warehouse=wh_name,
                            database=db_name, schema=schema_name, user=user_name)
    snowflake_task(password=sn_password)

    # Curtailment
    curt_query = actions.ingest_curtailment_to_snowflake()
    snowflake_task = snowflake.SnowflakeQuery(query=curt_query, account=account_prefix, warehouse=wh_name,
                            database=db_name, schema=schema_name, user=user_name)
    snowflake_task(password=sn_password)

    # Run DBT Model
