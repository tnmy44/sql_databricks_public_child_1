from uitesting_shared_sql_childdatabricksshared_rel_db_mwaa.utils import *

def DBT_Useall():
    from airflow.operators.bash import BashOperator
    import os
    import zipfile
    import tempfile

    return BashOperator(
        task_id = "DBT_Useall",
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
           ),            "dbt deps --profile run_profile",            "dbt seed --profile run_profile --threads=2 -m env_uitesting_shared_useallmodel_1+",            "dbt run --profile run_profile --threads=2 -m env_uitesting_shared_useallmodel_1+"]
        ),
        env = {
          "DBT_DATABRICKS_INVOCATION_ENV": "prophecy", 
          "DBT_PROFILES_DIR": "/usr/local/airflow/dags", 
          "DBT_FULL_REFRESH": "true"
        },
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 0
    )
