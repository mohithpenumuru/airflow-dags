from datetime import datetime
from airflow.decorators import dag, task

@dag(start_date=datetime(2024, 1, 1), schedule=None, catchup=False)
def hello_k8s():
    @task
    def extract():
        return [1, 2, 3]

    @task
    def transform(nums):
        return [n * 10 for n in nums]

    @task
    def load(nums):
        print(f"Loaded: {nums}")

    load(transform(extract()))

hello_k8s()
