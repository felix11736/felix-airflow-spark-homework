import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "spark_airflow",
    default_args={
        "owner": "FELIX",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "0 1 * * *"
)

start = PythonOperator(
    task_id = 'start',
    python_callable = lambda: print("Jobs Started"),
    dag=dag
)

helloworld = SparkSubmitOperator(
    task_id = 'helloworld',
    conn_id = "spark-conn",
    application = "jobs/python/helloworld.py",
    dag=dag
)

wordcount = SparkSubmitOperator(
    task_id = 'wordcount',
    conn_id = "spark-conn",
    application = "jobs/python/wordcount.py",
    dag=dag
)

end = PythonOperator(
    task_id = 'end',
    python_callable = lambda: print("Jobs completed"),
    dag=dag
)

start >> [helloworld, wordcount] >> end