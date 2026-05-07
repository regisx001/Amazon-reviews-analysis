"""
my_custom_dag.py
================
Custom DAG template — add your task description here.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os


@dag(
    dag_id="my_custom_dag",                    # Unique DAG identifier
    description="My custom DAG description",   # What this DAG does
    schedule="0 3 * * *",                      # Cron: every day at 03:00 (set to None for manual)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["custom", "example"],                # Tags for organization
)
def my_custom_dag():
    """Main DAG function - defines task dependencies"""

    @task()
    def task_1():
        """First task - gets data"""
        print("✅ Task 1: Getting data...")
        # Add your logic here
        return {"data": "example"}

    @task()
    def task_2(result_from_task_1: dict):
        """Second task - processes data"""
        print("✅ Task 2: Processing data...")
        print(f"   Input: {result_from_task_1}")
        # Add your logic here
        return {"processed": "result"}

    @task()
    def task_3(result_from_task_2: dict):
        """Third task - saves results"""
        print("✅ Task 3: Saving results...")
        print(f"   Input: {result_from_task_2}")
        # Add your logic here

    # Define task dependencies (order of execution)
    task_1_result = task_1()
    task_2_result = task_2(task_1_result)
    task_3(task_2_result)


# Instantiate the DAG
dag = my_custom_dag()
