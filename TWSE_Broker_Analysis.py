from airflow.exceptions import AirflowException
from airflow import models
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.email import send_email
#from dateutil.relativedelta import relativedelta

from airflow.operators.bash_operator import BashOperator

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor

import pendulum
local_tz = pendulum.timezone("Asia/Taipei")


#import os

schedule_interval_dag = timedelta(days=1)

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    # set your start_date : airflow will run previous dags if dags since startdate has not run
    #'start_date': datetime(2020,11,1 ,tzinfo=local_tz),
    'start_date': datetime(2020, 10, 15 ,17, 10, tzinfo=local_tz),
    #'provide_context': True,
    #'depends_on_past': True,
    #'email_on_failure': True,
    #'email_on_retry': True,
    'project_id' : 'TWSE_Broker_Analysis',
    'retries': 1,
    #'on_failure_callback': notify_email,
    'retry_delay': timedelta(minutes=5),
}

with models.DAG(
    dag_id='TWSE_Broker_Analysis',
    # Continue to run DAG once per day
    schedule_interval = schedule_interval_dag,
    #schedule_interval = '10 17 * * *',
    #schedule_interval = None, 
    catchup = False,
    default_args=default_dag_args) as dag:

    check_TWSE_Transactions = ExternalTaskSensor(
        task_id='check_TWSE_Transactions',
        external_dag_id='TWSE_Transactions_MySQL',
        external_task_id= 'TWSE_ImportHDFS_task',
        execution_delta = None, 
        #execution_delta = timedelta(days=1),
        #check_existence = True,
        timeout = 300)

    check_TWSE_Broker = ExternalTaskSensor(
        task_id='check_TWSE_Broker',
        external_dag_id='TWSE_Broker_Transactions_MySQL',
        external_task_id= 'TWSE_Broker_ImportHDFS_task',
        execution_delta = None, 
        #execution_delta = timedelta(days=1),
        #check_existence = True,
        timeout = 300)

    #transform_table_1 = DummyOperator(task_id='end') 
        # code for transfromation of table 1

    TWSE_BrokerDR_OP = BashOperator(task_id='TWSE_Broker_DR_task',
            bash_command='/home/spark/PythonProjects/twstock_ETL_spark/broker_DR_daily.sh ')

    check_TWSE_Transactions
    check_TWSE_Broker
    TWSE_BrokerDR_OP.set_upstream([check_TWSE_Transactions,check_TWSE_Broker])
