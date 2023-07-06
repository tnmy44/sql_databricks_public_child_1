import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from r7jkhvrxm_nq6nicb3n5uq_.tasks import DBT_0, DBT_0_1, S3FileSensor_1, Slack_1
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "r7JkhVrXM_nQ6nIcb3N5UQ_", 
    schedule_interval = "0 0 17 * *", 
    default_args = {
      "owner": "Prophecy", 
      "retries": 1, 
      "retry_delay": timedelta(minutes = 1.0), 
      "ignore_first_depends_on_past": True, 
      "do_xcom_push": True, 
      "pool": "n7pJN9mh"
    }, 
    start_date = pendulum.today('UTC'), 
    end_date = pendulum.datetime(2023, 7, 11, tz = "UTC"), 
    catchup = True, 
    tags = []
) as dag:
    DBT_0_op = DBT_0()
    DBT_0_1_op = DBT_0_1()
    S3FileSensor_1_op = S3FileSensor_1()
    Slack_1_op = Slack_1()
    DBT_0_op >> [DBT_0_1_op, S3FileSensor_1_op, Slack_1_op]
