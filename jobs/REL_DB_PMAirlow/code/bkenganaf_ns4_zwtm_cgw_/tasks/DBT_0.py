from bkenganaf_ns4_zwtm_cgw_.utils import *

def DBT_0():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    dbt_deps_cmd = " deps"
    dbt_props_cmd = ""

    if "run_profile":
        dbt_props_cmd = " --profile run_profile"
        dbt_deps_cmd = " deps --profile run_profile"

    if "2":
        dbt_props_cmd = dbt_props_cmd + " --threads=" + "2"

    if "env_uitesting_shared_mid_model_1":
        dbt_props_cmd = dbt_props_cmd + " -m " + "+env_uitesting_shared_mid_model_1+"

    return BashOperator(
        task_id = "DBT_0",
        bash_command = f'''$PROPHECY_HOME/run_dbt.sh "{"; ".join(["dbt" + dbt_deps_cmd, "dbt seed" + dbt_props_cmd, "dbt run" + dbt_props_cmd])}"''',
        env = {
          "DBT_FAIL_FAST": "true", 
          "DBT_PRINT": "false", 
          "DBT_FULL_REFRESH": "true", 
          "DBT_PROFILE_SECRET": "61Yi8DU2q7fwUQEPT6O0S", 
          "GIT_TOKEN_SECRET": "lfJ5vC2nkIrYJFHPCPJU9Q_", 
          "GIT_ENTITY": "branch", 
          "GIT_ENTITY_VALUE": "dev_staging", 
          "GIT_SSH_URL": "https://github.com/abhisheks-prophecy/sql_databricks_public_child_1", 
          "GIT_SUB_PATH": ""
        },
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 0
    )
