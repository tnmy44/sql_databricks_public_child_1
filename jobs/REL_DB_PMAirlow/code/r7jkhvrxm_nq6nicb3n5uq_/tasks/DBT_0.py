def DBT_0():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    envs = {}
    envs["DBT_FAIL_FAST"] = "true"
    envs["DBT_PRINT"] = "false"
    envs["DBT_FULL_REFRESH"] = "true"
    envs["DBT_PROFILE_SECRET"] = "kmn9NNG0rSJ_TyDnNDaE5"
    envs["GIT_TOKEN_SECRET"] = "a4eOXHTDccEGjW5Xfs35Mg_"
    envs["GIT_ENTITY"] = "branch"
    envs["GIT_ENTITY_VALUE"] = "dev"
    envs["GIT_SSH_URL"] = "https://github.com/abhisheks-prophecy/sql_databricks_public_child_1"
    envs["GIT_SUB_PATH"] = ""

    return BashOperator(
        task_id = "DBT_0",
        bash_command = f"$PROPHECY_HOME/run_dbt.sh \"dbt deps --profile run_profile; dbt seed --profile run_profile --threads=2; dbt run --profile run_profile --threads=2; \"",
        env = envs,
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 1
    )
