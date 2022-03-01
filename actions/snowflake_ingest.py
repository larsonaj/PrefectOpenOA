import prefect
from prefect.tasks import snowflake


def ingest_scada_to_snowflake(account_prefix, wh_name, db_name, schema_name, user_name, username, password):
    query_text = """"""
    snowflake.SnowflakeQuery(query=query_text, account=account_prefix, warehouse=wh_name, 
                            database=db_name, schema=schema_name, user=user_name)


def ingest_reanalysis_to_snowflake(account_prefix, wh_name, db_name, schema_name, user_name, username, password):
    query_text = """"""
    snowflake.SnowflakeQuery(query=query_text, account=account_prefix, warehouse=wh_name, 
                            database=db_name, schema=schema_name, user=user_name)


def ingest_reanalysis_to_snowflake(account_prefix, wh_name, db_name, schema_name, user_name, username, password):
    query_text = """"""
    snowflake.SnowflakeQuery(query=query_text, account=account_prefix, warehouse=wh_name, 
                            database=db_name, schema=schema_name, user=user_name)