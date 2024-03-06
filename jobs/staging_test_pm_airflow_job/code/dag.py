import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from gor1ydxslmdol1p7txwilw_.tasks import DBT_0
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "GOr1ydxSlMDol1P7TxWiLw_", 
    schedule_interval = None, 
    default_args = {"owner" : "Prophecy", "ignore_first_depends_on_past" : True, "do_xcom_push" : True, "pool" : "hhFvJ5E5"}, 
    start_date = pendulum.today('UTC'), 
    end_date = pendulum.datetime(2033, 9, 23, tz = "UTC"), 
    catchup = True
) as dag:
    DBT_0_op = DBT_0()
