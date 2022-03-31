from cProfile import run
import prefect
import os


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


storage = GitHub(
    repo='larsonaj/PrefectOpenOA',
    path=f"/flows/snowflake_test.py",
    ref="dev",
    access_token_secret="GITHUB_API_KEY"
)

run_config = UniversalRun(labels=['DESKTOP-ETPQA0T'])

## Setup Snowflake

## Build tasks
@task
def say_hello(printer):
    logger = prefect.context.get("logger")
    logger.info(f"{printer}")

## Build flow
password = PrefectSecret('SNOWFLAKE_PW')

with Flow("run-snowflake", storage=storage, run_config=run_config) as flow:
    account_prefix = 'captech_partner.us-east-1'
    wh_name = 'XS_WH'
    db_name = 'TEST_DB'
    schema_name = 'PUBLIC'
    user_name = 'alarson'
    
    query_text = """select top 10 * from OpenOA_Scada"""
    snowflake_task = snowflake.SnowflakeQuery(query=query_text, account=account_prefix, warehouse=wh_name, 
                            database=db_name, schema=schema_name, user=user_name)
    results = snowflake_task(password=password)
    say_hello(results)
    