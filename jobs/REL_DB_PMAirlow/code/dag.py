import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from _5ktuqncylkcm0qqtcjahwa_.tasks import DBT_0, DBT_0_1, Email_2, HTTPSensor_1, S3FileSensor_1, Slack_1
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "5ktuqNCYLkcM0qqTCJaHWA_", 
    schedule_interval = "0 0 17 * *", 
    default_args = {
      "owner": "Prophecy", 
      "retries": 1, 
      "retry_delay": timedelta(minutes = 1.0), 
      "ignore_first_depends_on_past": True, 
      "do_xcom_push": True, 
      "pool": "zHk-xlzd"
    }, 
    start_date = pendulum.datetime(2023, 9, 5, tz = "UTC"), 
    end_date = pendulum.datetime(2024, 9, 4, tz = "UTC"), 
    catchup = True, 
    tags = []
) as dag:
    Email_2_op = Email_2()
    DBT_0_1_op = DBT_0_1()
    S3FileSensor_1_op = S3FileSensor_1()
    DBT_0_op = DBT_0()
    HTTPSensor_1_op = HTTPSensor_1()
    Slack_1_op = Slack_1()
    DBT_0_op >> [DBT_0_1_op, HTTPSensor_1_op, S3FileSensor_1_op, Slack_1_op]
    Slack_1_op >> Email_2_op
