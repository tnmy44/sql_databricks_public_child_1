import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from staging_shared_team_sql_databrickssharedbasic_staging_rel_db_assumed_mwaa.tasks import DBT_0
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "staging_shared_team_SQL_DatabricksSharedBasic_Staging_REL_DB_Assumed_MWAA", 
    schedule_interval = "0 0 * * 1,4", 
    default_args = {"owner" : "Prophecy", "retries" : 0, "ignore_first_depends_on_past" : True, "do_xcom_push" : True}, 
    start_date = pendulum.today('UTC'), 
    catchup = True, 
    tags = []
) as dag:
    DBT_0_op = DBT_0()
