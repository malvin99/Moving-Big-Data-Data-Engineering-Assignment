import airflow
import json
import boto3
import sqlalchemy as db
import pandas as pd
from airflow import DAG
from datetime import timedelta, datetime
from pathlib import Path
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator


HOME_DIR = "/opt/airflow"

#insert your mount folder

mount_path = "/opt/airflow/s3_mount"

# ==============================================================

# The default arguments for your Airflow, these have no reason to change for the purposes of this predict.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Defining the DAG

dag = DAG(
	"Malvin_Final_Dag",
	start_date=airflow.utils.dates.days_ago(0),
	default_args=default_args,
	template_searchpath="/opt/airflow/s3_mount/Scripts",
	on_success_callback=None,
	on_failure_callback=None
)


# The function that uploads data to the RDS database, it is called upon later.

def upload_to_postgres(**kwargs):

	engine = db.create_engine("postgresql://postgres:malvin2006@de-malvin-mbd-rds.cnyzy2rdilay.eu-west-2.rds.amazonaws.com/postgres")
	connection = engine.connect()
	
	file_path = "/opt/airflow/s3_mount/OutPut/historical_stock_data.csv"
	chunksize = 10000

	#Iterate over the CSV file in chunks and upload to PostgreSQL Database
	for chunk in pd.read_csv(file_path, chunksize=chunksize):
		chunk.to_sql("historical_stock_data", engine, if_exists="replace", index=False)

	connection.close()
	return "CSV Uploaded to postgres database"

#Declaring my SNS Topic ARN
sns_arn = "arn:aws:sns:eu-west-1:445492270995:Malvin-mbd-sns"

# Function will send success message when Pipeline works
def success_sns(context):
	topic_arn = sns_arn
	subject = "Pipeline ran successfully"

	task_instance = context["task_instance"]
	task_log_url = task_instance.log_url

	message = f"Task '{task_instance.task_id}' succeeded. Log URL: {task_log_url}"

	sns_op = SnsPublishOperator(
		task_id="send_success_sns_notification",
		target_arn=topic_arn,
		subject=subject,
		message=message,
		aws_conn_id="aws_default"
	)
	sns_op.execute(context=context)

def failure_sns(context):
	topic_arn = sns_arn
	subject = "Pipeline failed to run"

	task_instance = context["task_instance"]
	task_log_url = task_instance.log_url

	message = f"Task '{task_instance.task_id}' succeeded. Log URL: {task_log_url}"

	sns_op = SnsPublishOperator(
		task_id="send_failure_sns_notification",
		target_arn=topic_arn,
		subject=subject,
		message=message,
		aws_conn_id="aws_default"
	)
	sns_op.execute(context=context)


# The dag configuration ===========================================================================
t1 = BashOperator(
	task_id="run_python_script",
	bash_command='python "/opt/airflow/s3_mount/Scripts/malvin.py"',
	dag=dag
)

t2 = PythonOperator(
	task_id="loading_data_to_rds",
	python_callable=upload_to_postgres,
	on_success_callback=success_sns,
	on_failure_callback=failure_sns
)

# Define your Task flow below ===========================================================

t1 >> t2
