def DBT_0():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    envs = {}
    envs["DBT_PROFILES_DIR"] = "/home/airflow/gcs/data"
    envs["DBT_FAIL_FAST"] = "true"
    envs["DBT_FULL_REFRESH"] = "true"

    return BashOperator(
        task_id = "DBT_0",
        bash_command = "set -euxo pipefail; tmpDir=`mktemp -d`; git clone https://github.com/abhisheks-prophecy/sql_databricks_public_child_1 --branch main --single-branch $tmpDir; cd $tmpDir/; dbt deps --profile run_profile; dbt seed --profile run_profile; dbt run --profile run_profile; ",
        env = envs,
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 1
    )
