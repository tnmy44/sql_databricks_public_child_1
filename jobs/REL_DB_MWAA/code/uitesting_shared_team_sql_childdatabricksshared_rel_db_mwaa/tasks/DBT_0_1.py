def DBT_0_1():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(
        task_id = "DBT_0_1",
        bash_command = "set -euxo pipefail; tmpDir=`mktemp -d`; git clone https://github.com/abhisheks-prophecy/sql_databricks_public_child_1 --branch dev_staging --single-branch $tmpDir; cd $tmpDir/; dbt deps --profile run_profile; dbt seed --profile run_profile --threads=2; dbt run --profile run_profile --threads=2; ",
        env = {
          "DBT_PROFILES_DIR": "/usr/local/airflow/dags", 
          "DBT_FAIL_FAST": "true", 
          "DBT_PRINT": "false", 
          "DBT_FULL_REFRESH": "true"
        },
        append_env = True,
        retry_exponential_backoff = True, 
        retries = 1
    )
