import prefect
from prefect import Flow, task
import sys

sys.path.append('..')

import actions



@task(name="Run Databricks Ingest")
def run_dbx_notebook():
    actions.run_dbx_notebook(nb_path="", azure_conn="")


@task(name="Run Snowflake Ingest")
def snowflake_ingest():
    actions.snowflake_query(query_text="",)


@task(name="Run Snowflake Data Model")
def snowflake_model_run():
    actions.snowflake_query(query_text="",)


with Flow("scada-ingest") as flow:
    run_dbx_notebook()
    snowflake_ingest()
    snowflake_model_run()


flow.register(project_name='OpenOA')