from bkenganaf_ns4_zwtm_cgw_.utils import *

def DBT_0():
    from airflow.operators.bash import BashOperator
    from datetime import timedelta
    import os
    import zipfile
    import tempfile

    return BashOperator(
        task_id = "DBT_0",
        bash_command = f'''$PROPHECY_HOME/run_dbt.sh "{"; ".join(
          ["dbt deps --profile run_profile",            "dbt seed --profile run_profile --threads=2 -m +env_uitesting_shared_mid_model_1+",            "dbt run --profile run_profile --threads=2 -m +env_uitesting_shared_mid_model_1+"]
        )}"''',
        env = {
          "DBT_DATABRICKS_INVOCATION_ENV": "prophecy", 
          "DBT_FAIL_FAST": "true", 
          "DBT_PRINT": "false", 
          "DBT_FULL_REFRESH": "true", 
          "DBT_PROFILE_SECRET": "NBGW5z7eP5DyFVJyclt-j", 
          "GIT_TOKEN_SECRET": "lfJ5vC2nkIrYJFHPCPJU9Q_", 
          "GIT_ENTITY": "tag", 
          "GIT_ENTITY_VALUE": "__PROJECT_FULL_RELEASE_TAG_PLACEHOLDER__", 
          "GIT_SSH_URL": "https://github.com/abhisheks-prophecy/sql_databricks_public_child_1", 
          "GIT_SUB_PATH": "", 
          "IS_ADHOC_RUN_FROM_SAME_PROJECT": "false"
        },
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 0
    )
