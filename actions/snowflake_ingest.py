##################################################
##
## Holds query text for snowflake bulk loads
##
##################################################

def ingest_scada_to_snowflake():

    query_text = """DROP TABLE IF EXISTS "TEST_DB"."PUBLIC"."OPENOA_SCADA";
                    CREATE TABLE IF NOT EXISTS "TEST_DB"."PUBLIC"."OPENOA_SCADA"
                        ("ID" STRING NOT NULL,
                        "ENERGY_KWH" STRING,
                        "TIME" TIMESTAMP_NTZ,
                        "TEMP_AVG" FLOAT,
                        "HORIZONTAL_WIND_DIRECTION" FLOAT,
                        "AVG_VANE_DIRECTION" FLOAT,
                        "WIND_SPEED" FLOAT,
                        "BLADE_PATH_ANGLE_VAL1" FLOAT,
                        "AVG_POWER" FLOAT,
                        "YAW_ANGLE" FLOAT);

                    copy into OPENOA_SCADA from (
                    select
                        replace(REGEXP_SUBSTR(
                        METADATA$FILENAME,
                        'R.....\/'), '\/', '') as id,
                        $1:energy_kwh as ENERGY_KWH,
                        to_timestamp_ntz($1:time::int,6) as TIME,
                        $1:wmet_EnvTmp_avg as TEMP_AVG,
                        $1:wmet_HorWdDir_avg as HORIZONTAL_WIND_DIRECTION,
                        $1:wmet_VaneDir_avg as AVG_VANE_DIRECTION,
                        $1:wmet_wdspd_avg as WIND_SPEED,
                        $1:wrot_BlPthAngVal1_avg as BLADE_PATH_ANGLE_VAL1,
                        $1:wtur_W_avg as AVG_POWER,
                        $1:wyaw_YwAng_avg as YAW_ANGLE
                    from @openOA_Azure_Blob_Stage/scada/)
                    pattern= '.*id=.*.parquet'
                    FILE_FORMAT='PARQUET_AUTOCOMPRESSION'
                    ON_ERROR = CONTINUE;"""

    return query_text

def ingest_reanalysis_to_snowflake():

    query_text = """DROP TABLE IF EXISTS "TEST_DB"."PUBLIC"."OPENOA_REANALYSIS" ;
                    CREATE TABLE IF NOT EXISTS "TEST_DB"."PUBLIC"."OPENOA_REANALYSIS"
                        ("TIME" timestamp_ntz NOT NULL,
                        "SITE" varchar,
                        "RHO_KGM-3" float,
                        "SURFACE_PRESSURE" float,
                        "TEMPERATURE_KELVIN" FLOAT,
                        "WINDSPEED_MS" FLOAT,
                        "U_MS" FLOAT,
                        "V_MS" FLOAT,
                        "WIND_DIRECTION_DEGREES" FLOAT);

                    copy into OPENOA_REANALYSIS from (
                    select
                        to_timestamp_ntz($1:time::int,6) as "TIME",
                        replace(replace(REGEXP_SUBSTR(
                        METADATA$FILENAME,
                        '_.*(\/)'), '\/', ''), '_', '') as SITE,
                        $1:rho_kgm-3 as "RHO_KGM-3",
                        $1:surf_pres as "SURFACE_PRESSURE",
                        $1:temperature_K as "TEMPERATURE_KELVIN",
                        $1:windspeed_ms as "WINDSPEED_MS",
                        $1:u_ms as "U_MS",
                        $1:v_ms as "V_MS",
                        $1:winddirection_deg as "WIND_DIRECTION_DEGREES"
                    from @openOA_Azure_Blob_Stage/)
                    pattern= '.*/reanalysis.*[.]parquet',
                    FILE_FORMAT='PARQUET_AUTOCOMPRESSION'
                    ON_ERROR = CONTINUE;"""
    
    return query_text

def ingest_masterdata_to_snowflake():
    query_text = """DROP TABLE IF EXISTS "TEST_DB"."PUBLIC"."openoa_assets";
                    CREATE TABLE IF NOT EXISTS "TEST_DB"."PUBLIC"."openoa_assets"
                        ("MANUFACTURER" varchar,
                        "MODEL" varchar,
                        "ELEVATION_M" int,
                        "HUB_HEIGHT_M" int,
                        "ID" varchar,
                        "LATITUDE" float,
                        "LONGITUDE" float,
                        "RATED_POWER_KW" int,
                        "ROTOR_DIAMETER_M" int,
                        "TYPE" varchar);
    
                    COPY INTO openoa_assets from
                    (SELECT
                        $1:Manufacturer,
                        $1:Model,
                        $1:elevation_m,
                        $1:hub_height_m,
                        $1:id,
                        $1:latitude,
                        $1:longitude,
                        $1:rated_power_kw,
                        $1:rotor_diameter_m,
                        $1:type
                    FROM @openOA_Azure_Blob_Stage/assets/Asset.parquet)
                FILE_FORMAT = 'PARQUET_AUTOCOMPRESSION'
                ON_ERROR = CONTINUE;"""

    return query_text

def ingest_curtailment_to_snowflake():
    query_text = """DROP TABLE IF EXISTS "TEST_DB"."PUBLIC"."OPENOA_CURTAILMENT";
                    CREATE TABLE IF NOT EXISTS "TEST_DB"."PUBLIC"."OPENOA_CURTAILMENT"
                        ("TIME" timestamp_ntz NOT NULL,
                        "AVAILABILITY_KWH" FLOAT, 
                        "CURTAILMENT_KWH" FLOAT);

                    copy into OPENOA_CURTAILMENT from (
                    select
                        to_timestamp_ntz($1:time::int,6) as TIME,
                        $1:availability_kwh as AVAILABILITY_KWH,
                        $1:curtailment_kwh as CURTAILMENT_KWH
                        from @openOA_Azure_Blob_Stage/curtailment/Curtailment.parquet)
                    FILE_FORMAT = 'PARQUET_AUTOCOMPRESSION'
                    ON_ERROR = CONTINUE;"""
    return query_text


def clear_main_tables():
    query_text = """"""
    return query_text