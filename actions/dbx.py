import prefect
from prefect.tasks import databricks


def dbx_connection(host="adb-7101253137415266.6.azuredatabricks.net", token="dapibf1a2e725dbe5a79cd1dd14a9a55dcbd"):
    
    conn = {"host":host,
            "token":token}

    return conn

def dbx_run_config(cluster_id=None, notebook=None, *args):
    if cluster_id is not None:
        json = {'cluster_id': cluster_id,
            'notebook_task': {
                'notebook_path': notebook 
                }
            }


def run_dbx_notebook(nb_path, cluster_id):

    json = {'cluster_id': cluster_id,
            'notebook_task': {
                'notebook_path': nb_path 
                }
            }
    
    run_name = databricks.DatabricksSubmitRun(json=json)

    print(run_name)

    return run_name