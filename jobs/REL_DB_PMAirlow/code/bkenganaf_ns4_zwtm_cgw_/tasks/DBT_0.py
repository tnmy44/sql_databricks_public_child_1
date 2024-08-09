from bkenganaf_ns4_zwtm_cgw_.utils import *

def DBT_0():
    from airflow.operators.python import PythonOperator
    from datetime import timedelta
    import os
    import zipfile
    import tempfile

    return PythonOperator(
        task_id = "DBT_0",
        python_callable = invoke_dbt_runner,
        op_kwargs = {
          "is_adhoc_run_from_same_project": False,
          "is_prophecy_managed": True,
          "run_deps": True,
          "run_seeds": True,
          "run_parents": True,
          "run_children": True,
          "run_tests": False,
          "run_mode": "model",
          "entity_kind": "model",
          "entity_name": "env_uitesting_shared_mid_model_1",
          "project_id": "74",
          "git_entity": "tag",
          "git_entity_value": "__PROJECT_FULL_RELEASE_TAG_PLACEHOLDER__",
          "git_ssh_url": "https://github.com/abhisheks-prophecy/sql_databricks_public_child_1",
          "git_sub_path": "",
          "select": "",
          "exclude": "",
          "run_props": " --profile run_profile --threads 2",
          "envs": {
            "DBT_DATABRICKS_INVOCATION_ENV": "prophecy", 
            "DBT_FAIL_FAST": "true", 
            "DBT_PRINT": "false", 
            "DBT_FULL_REFRESH": "true"
          }, 
          "git_token_secret": "lfJ5vC2nkIrYJFHPCPJU9Q_", 
          "dbt_profile_secret": "NBGW5z7eP5DyFVJyclt-j"
        },
        retry_exponential_backoff = True, 
        retries = 0
    )
