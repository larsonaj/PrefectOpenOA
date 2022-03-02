import prefect
from prefect import Flow, task
import sys
from prefect.tasks import databricks

sys.path.append('.')

import actions.dbx



# @task(name="Run Databricks Ingest")
# def run_dbx_notebook():
#     actions.dbx.run_dbx_notebook(nb_path="/Users/alarson@captechventures.com/project_CapTech",
#                                 cluster_id="0301-005003-1urzp405")


conn = {"host":"adb-7101253137415266.6.azuredatabricks.net",
"token":"dapibf1a2e725dbe5a79cd1dd14a9a55dcbd"}

json = {'existing_cluster_id': "0301-005003-1urzp405",
    'notebook_task': {
        'notebook_path': "/Users/alarson@captechventures.com/project_CapTech" 
        }
    }

with Flow("move-doc") as flow:
    notebook_run = databricks.DatabricksSubmitRun(json=json)
    notebook_run(databricks_conn_secret=conn)


flow.run()