import prefect
from prefect import Flow, task
import sys

sys.path.append('.')

import actions



@task(name="Run Databricks Ingest")
def run_dbx_notebook():
    actions.dbx.run_dbx_notebook(nb_path="/Users/alarson@captechventures.com/project_CapTech",
                            cluster_id="0301-005003-1urzp405")


with Flow("move-doc") as flow:
    run_dbx_notebook()

flow.run()