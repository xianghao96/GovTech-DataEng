import csv
from nameparser import HumanName

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

def parse_file(filepath):
    with open('filepath', mode="r") as csv_file:
        csv_reader = csv.reader(csv_file)
        all_lines = []
        single_line = []

    for row in csv_reader:
        # Remove rows with no name
        if len(row[0]) == 0:
            pass

        else:
            # Remove any other than first and last names
            name = HumanName(row[0])
            name.string_format = "{first} {last}"
            row[0] = str(name)
            
            # Remove prepended 0s
            row[1] = row[1].strip("0")
            
            # Add true if amount > 100
            if row[0] != "name":
                if float(row[1]) > 100:
                    row.append("true")
                else:
                    row.append("false")

            # Split the names to first and last names
            split_row = ' '.join(row).split()

            # Add them all into a nested list
            all_lines.append(split_row)

    # Create new columns for new csv
    all_lines[0] = ["first_name", "last_name", "price", "above_100"]    

    with open("dataeng_test/parsed_dataset.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(all_lines)


dag = DAG(
  dag_id='my_dag', 
  description='Section 1 DAG',
  default_args=default_args,
  schedule_interval=timedelta(days=1)) # Since we want this to run daily

t1 = PythonOperator(
  task_id='Python_Script', 
  depends_on_past=False,
  python_callable=parse_file, 
  op_kwargs={'filepath': 'dataeng_test/dataset.csv'},
  dag=dag)

# setting dependencies
t1