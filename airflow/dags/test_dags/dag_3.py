from datetime import datetime, timedelta
from airflow.decorators import dag, task
default_args = {
    'owner': 'xpham',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
@dag(
    dag_id='dag_taskflow',
    default_args=default_args,
    start_date=datetime(2025, 9, 21),
    schedule='@daily'
)
def hello_world():
    @task(multiple_outputs=True ) # multiple_outputs returns a dictionary
    def get_name():
        return {
            'f_name': 'John',
            'l_name': 'Doe'
        }
    @task()
    def get_age():
        return 18
    @task()
    def greet(fname, lname, age):
        print("Hello {} {}. You are {} years old.".format(fname, lname, age))
    name = get_name()
    age = get_age()
    greet(name['f_name'], name['l_name'], age)
    # The dependency is called implicitly by airflow
hello_world()

