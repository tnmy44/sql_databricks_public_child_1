from uitesting_shared_team_sql_databrickssharedbasic_rel_db_airflow.utils import *

def DBT_0():
    from airflow.operators.bash import BashOperator
    from datetime import timedelta
    import os
    import zipfile
    import tempfile

    return BashOperator(
        task_id = "DBT_0",
        bash_command = " && ".join(
          ["{} && cd $tmpDir/{}".format(
             (
               "set -euxo pipefail && tmpDir=`mktemp -d` && git clone "
               + "--depth 1 {} --branch {} $tmpDir".format(
                 "https://github.com/abhisheks-prophecy/sql_databricks_public_child_1",
                 "__PROJECT_FULL_RELEASE_TAG_PLACEHOLDER__"
               )
             ),
             ""
           ),            "dbt deps --profile run_profile",  "dbt seed --profile run_profile --threads=1",            "dbt run --profile run_profile --threads=1"]
        ),
        env = {
          "DBT_DATABRICKS_INVOCATION_ENV": "prophecy", 
          "DBT_PROFILES_DIR": "/home/airflow/gcs/data", 
          "DBT_PRINT": "false", 
          "DBT_FULL_REFRESH": "true"
        },
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 0
    )
