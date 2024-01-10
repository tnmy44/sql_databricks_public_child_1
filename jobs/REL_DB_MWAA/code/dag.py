import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from uitesting_shared_team_sql_childdatabricksshared_rel_db_mwaa.tasks import DBT_0, DBT_0_1, Script_1, Script_2
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "uitesting_shared_team_SQL_ChildDatabricksShared_REL_DB_MWAA", 
    schedule_interval = "0 0 17 * *", 
    default_args = {
      "owner": "Prophecy", 
      "retries": 0, 
      "retry_delay": timedelta(minutes = 1.0), 
      "ignore_first_depends_on_past": True, 
      "do_xcom_push": True
    }, 
    start_date = pendulum.today('UTC'), 
    end_date = pendulum.datetime(2080, 1, 1, tz = "UTC"), 
    catchup = True, 
    tags = []
) as dag:
    DBT_0_op = DBT_0()
    Script_1_op = Script_1()
    Script_2_op = Script_2()
    DBT_0_1_op = DBT_0_1()
    DBT_0_op >> Script_1_op
    Script_1_op >> [DBT_0_1_op, Script_2_op]
