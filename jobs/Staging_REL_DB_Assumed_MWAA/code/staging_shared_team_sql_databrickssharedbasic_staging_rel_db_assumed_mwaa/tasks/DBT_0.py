from staging_shared_team_sql_databrickssharedbasic_staging_rel_db_assumed_mwaa.utils import *

def DBT_0():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    envs = {}
    dbt_props_cmd = ""

    if "/usr/local/airflow/dags":
        envs = {"DBT_PROFILES_DIR" : "/usr/local/airflow/dags"}

    envs["DBT_FULL_REFRESH"] = "true"

    if "run_profile":
        dbt_props_cmd = " --profile run_profile"

    if "env_uitesting_shared_child_model_1":
        dbt_props_cmd = dbt_props_cmd + " -m " + "+env_uitesting_shared_child_model_1"

    return BashOperator(
        task_id = "DBT_0",
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
           ),            "dbt run" + dbt_props_cmd,  "dbt test" + dbt_props_cmd]
        ),
        env = envs,
        append_env = True,
    )
