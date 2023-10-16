def DBT_0():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(
        task_id = "DBT_0",
        bash_command = "$PROPHECY_HOME/run_dbt.sh \"dbt deps --profile run_profile; dbt seed --profile run_profile --threads=2; dbt run --profile run_profile --threads=2; \"",
        env = {
          "DBT_FAIL_FAST": "true", 
          "DBT_PRINT": "false", 
          "DBT_FULL_REFRESH": "true", 
          "DBT_PROFILE_SECRET": "HYeAnbJzaYSUe5b9576B0", 
          "GIT_TOKEN_SECRET": "a4eOXHTDccEGjW5Xfs35Mg_", 
          "GIT_ENTITY": "branch", 
          "GIT_ENTITY_VALUE": "dev", 
          "GIT_SSH_URL": "https://github.com/abhisheks-prophecy/sql_databricks_public_child_1", 
          "GIT_SUB_PATH": ""
        },
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 1
    )
