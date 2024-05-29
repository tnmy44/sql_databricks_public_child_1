from bkenganaf_ns4_zwtm_cgw_.utils import *

def DBT_0_1():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    dbt_deps_cmd = " deps"
    dbt_props_cmd = ""
    envs = {"DBT_DATABRICKS_INVOCATION_ENV" : "prophecy", "DBT_FAIL_FAST" : "true", "DBT_PRINT" : "false"}

    if "logs.txt":
        envs = {
            "DBT_DATABRICKS_INVOCATION_ENV": "prophecy", 
            "DBT_FAIL_FAST": "true", 
            "DBT_PRINT": "false", 
            "DBT_LOG_PATH": "logs.txt"
        }

    envs["DBT_FULL_REFRESH"] = "true"
    envs["DBT_PROFILE_SECRET"] = "61Yi8DU2q7fwUQEPT6O0S"
    envs["GIT_TOKEN_SECRET"] = "lfJ5vC2nkIrYJFHPCPJU9Q_"
    envs["GIT_ENTITY"] = "branch"
    envs["GIT_ENTITY_VALUE"] = "dev_staging"
    envs["GIT_SSH_URL"] = "https://github.com/abhisheks-prophecy/sql_databricks_public_child_1"
    envs["GIT_SUB_PATH"] = ""

    if "run_profile":
        dbt_props_cmd = " --profile run_profile"
        dbt_deps_cmd = " deps --profile run_profile"

    if "2":
        dbt_props_cmd = dbt_props_cmd + " --threads=" + "2"

    return BashOperator(
        task_id = "DBT_0_1",
        bash_command = f'''$PROPHECY_HOME/run_dbt.sh "{"; ".join(
          ["dbt" + dbt_deps_cmd, "dbt seed" + dbt_props_cmd, "dbt run" + dbt_props_cmd, "dbt test" + dbt_props_cmd]
        )}"''',
        env = envs,
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 0
    )
