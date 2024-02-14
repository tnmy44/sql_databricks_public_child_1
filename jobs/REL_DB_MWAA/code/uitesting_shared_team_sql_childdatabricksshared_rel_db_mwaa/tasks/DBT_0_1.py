from uitesting_shared_team_sql_childdatabricksshared_rel_db_mwaa.utils import *

def DBT_0_1():
    from airflow.operators.bash import BashOperator
    envs = {}
    dbt_deps_cmd = " deps"
    dbt_props_cmd = ""

    if "/usr/local/airflow/dags":
        envs = {"DBT_PROFILES_DIR" : "/usr/local/airflow/dags"}

    envs["DBT_FULL_REFRESH"] = "true"

    if "run_profile":
        dbt_props_cmd = " --profile run_profile"
        dbt_deps_cmd = " deps --profile run_profile"

    if "2":
        dbt_props_cmd = dbt_props_cmd + " --threads=" + "2"

    return BashOperator(
        task_id = "DBT_0_1",
        bash_command = f'''{" && ".join(
          ["set -euxo pipefail && tmpDir=`mktemp -d` && git clone https://github.com/abhisheks-prophecy/sql_databricks_public_child_1 --branch dev_staging --single-branch $tmpDir && cd $tmpDir/",            "dbt" + dbt_deps_cmd,  "dbt seed" + dbt_props_cmd,  "dbt run" + dbt_props_cmd]
        )}''',
        env = envs,
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 1
    )
