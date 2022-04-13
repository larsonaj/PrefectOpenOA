from cProfile import run
import prefect
import os
import datetime


# Import tasks
from prefect.tasks import databricks
from prefect import Flow, task

# Import helpers
from prefect.client import Secret
from prefect.tasks.secrets import PrefectSecret
from prefect import config
from prefect.run_configs import UniversalRun
from prefect.tasks import snowflake
from prefect.schedules import IntervalSchedule


run_config = UniversalRun(labels=['mssl-alarson'])

## Setup Snowflake parameters
account_prefix = 'captech_partner.us-east-1'
wh_name = 'XS_WH'
db_name = 'TEST_DB'
schema_name = 'PUBLIC'
user_name = 'alarson'

query_text = """select top 10 * from OpenOA_Scada"""
password = PrefectSecret('SNOWFLAKE_PW')


## Build task specifications
snowflake_task_specs = {'max_retries':5,
                        'retry_delay':datetime.timedelta(seconds=5)}


## Build tasks
@task(name='Printer Task')
def say_hello(printer):
    logger = prefect.context.get("logger")
    logger.info(f"{printer}")

# @task(name='New Printer Task')
# def new_printer(item):
#     print(item)
#     pass

## Build flow

with Flow("run-snowflake-demo", run_config=run_config) as flow:
    snowflake_run = snowflake.SnowflakeQuery(query=query_text, account=account_prefix, warehouse=wh_name,
                        database=db_name, schema=schema_name, user=user_name, **snowflake_task_specs)
    results = snowflake_run(password=password)
    say_hello(results)
    # new_printer("test")
