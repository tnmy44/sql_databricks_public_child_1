from uitesting_shared_sql_childdatabricksshared_rel_db_mwaa.utils import *

def DBT_SharedChildModel():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    envs = {"DBT_DATABRICKS_INVOCATION_ENV" : "prophecy"}
    dbt_deps_cmd = " deps"
    dbt_props_cmd = ""

    if "/usr/local/airflow/dags":
        envs = {"DBT_DATABRICKS_INVOCATION_ENV" : "prophecy", "DBT_PROFILES_DIR" : "/usr/local/airflow/dags"}

    envs["DBT_FULL_REFRESH"] = "true"

    if "run_profile":
        dbt_props_cmd = " --profile run_profile"
        dbt_deps_cmd = " deps --profile run_profile"

    if "2":
        dbt_props_cmd = dbt_props_cmd + " --threads=" + "2"

    if "env_uitesting_shared_child_model_1":
        dbt_props_cmd = dbt_props_cmd + " -m " + "+env_uitesting_shared_child_model_1+"

    return BashOperator(
        task_id = "DBT_SharedChildModel",
        bash_command = " && ".join(
          ["{} && cd $tmpDir/{}".format(
             (
               "set -euxo pipefail && tmpDir=`mktemp -d` && git clone "
               + "{} --branch {} --single-branch $tmpDir".format(
                 "https://github.com/abhisheks-prophecy/sql_databricks_public_child_1",
                 "dev_staging"
               )
             ),
             ""
           ),            "dbt" + dbt_deps_cmd,  "dbt seed" + dbt_props_cmd,  "dbt run" + dbt_props_cmd]
        ),
        env = envs,
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 1
    )
