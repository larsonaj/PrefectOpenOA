import prefect
from prefect.tasks import databricks

def run_dbx_notebook(nb_path, azure_conn, cluster_id):
    json = {'cluster_id': cluster_id,
            'notebook_task': {
                'notebook_path': nb_path 
                }
            }
    run = databricks.DatabricksSubmitRun(json=json)
    run(databricks_conn_secret=azure_conn)
