from cProfile import run
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


## Configure Context
config.cloud.use_local_secrets=False


storage = GitHub(
    repo='larsonaj/PrefectOpenOA',
    path=f"/flows/testint.py",
    ref="dev",
    access_token_secret="GITHUB_API_KEY"
)

run_config = UniversalRun(labels=['DESKTOP-ETPQA0T'])


dbx_token = PrefectSecret('DBX_API_TOKEN')

## Setup Cluster Info
conn = {"host":"adb-7101253137415266.6.azuredatabricks.net",
        "token":f"{dbx_token}"}

json = {'existing_cluster_id': "0221-224854-qyhjvmno",
    'notebook_task': {
        'notebook_path': "/Users/alarson@captechventures.com/project_CapTech" 
        }
    }

# json = {'existing_cluster_id': "0301-005003-1urzp405",
#     'notebook_task': {
#         'notebook_path': "/Users/alarson@captechventures.com/project_CapTech" 
#         }
#     }

## Setup Snowflake

## Build tasks
@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello, Cloud!")
    logger.info(f"{conn}")

## Build flow


with Flow("run-dbx-notebook", storage=storage, run_config=run_config) as flow:
    notebook_run = databricks.DatabricksSubmitRun(json=json)
    notebook_run(databricks_conn_secret=conn)