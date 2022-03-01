import prefect
from prefect.tasks import databricks

def run_dbx_notebook(nb_path, cluster_id):
    
    conn = {"host":"adb-7101253137415266.6.azuredatabricks.net",
           "token":"dapibf1a2e725dbe5a79cd1dd14a9a55dcbd"}

    json = {'cluster_id': cluster_id,
            'notebook_task': {
                'notebook_path': nb_path 
                }
            }
    
    run = databricks.DatabricksSubmitRun(json=json)
    
    run(databricks_conn_secret=conn)
