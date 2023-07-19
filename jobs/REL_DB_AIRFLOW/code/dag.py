import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from uitesting_shared_team_sql_databrickssharedbasic_rel_db_airflow.tasks import DBT_0, Script_1, Script_2, Slack_1
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "uitesting_shared_team_SQL_DatabricksSharedBasic_REL_DB_AIRFLOW", 
    schedule_interval = "0 0 17 * *", 
    default_args = {"owner" : "Prophecy", "ignore_first_depends_on_past" : True, "do_xcom_push" : True}, 
    params = {
      'branch': Param(
        """dev""", 
        type = "string", 
        maxLength = 10, 
        minLength = 1, 
        pattern = """""", 
        title = """branch"""
      ), 
      'project_name': Param("""SQL_DatabricksSharedBasic""", type = "string", title = """project_name"""), 
      'job_name': Param("""REL_DB_AIRFLOW""", type = "string", title = """job_name""")
    }, 
    start_date = pendulum.today('UTC'), 
    catchup = True
) as dag:
    DBT_0_op = DBT_0()
    Script_1_op = Script_1()
    Script_2_op = Script_2()
    Slack_1_op = Slack_1()
    DBT_0_op >> Script_1_op
    Script_1_op >> Script_2_op
