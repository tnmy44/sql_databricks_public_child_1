from uitesting_shared_team_sql_databrickssharedbasic_rel_db_airflow.utils import *

def DBT_0():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    envs = {}
    dbt_deps_cmd = " deps"
    dbt_props_cmd = ""

    if "/home/airflow/gcs/data":
        envs = {"DBT_PROFILES_DIR" : "/home/airflow/gcs/data"}

    envs["DBT_PRINT"] = "false"
    envs["DBT_FULL_REFRESH"] = "true"

    if "run_profile":
        dbt_props_cmd = " --profile run_profile"
        dbt_deps_cmd = " deps --profile run_profile"

    if "1":
        dbt_props_cmd = dbt_props_cmd + " --threads=" + "1"

    return BashOperator(
        task_id = "DBT_0",
        bash_command = " && ".join(
          ["{} && cd $tmpDir/{}".format(
             (
               "set -euxo pipefail && tmpDir=`mktemp -d` && git clone "
               + "{} --branch {} --single-branch $tmpDir".format(
                 "https://github.com/abhisheks-prophecy/sql_databricks_public_child_1",
                 " {{ params.branch }}"
               )
             ),
             ""
           ),            "dbt" + dbt_deps_cmd,  "dbt seed" + dbt_props_cmd,  "dbt run" + dbt_props_cmd]
        ),
        env = envs,
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 0
    )
