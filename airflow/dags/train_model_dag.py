"""
train_model_dag.py
==================
Runs the complete Logistic Regression model training pipeline:
1. Preprocessing (splits data, runs NLP pipeline, saves features & tf-idf models)
2. Training (trains Logistic Regression model on train_feat)
3. Evaluation (evaluates best model on test_feat and writes model_insights.json)
"""

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="train_logistic_regression_model",
    description="End-to-end model training pipeline (Preprocessing -> Training -> Evaluation)",
    schedule=None,  # Run manually or triggered externally
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ml", "training", "pipeline"],
)
def train_model_pipeline():

    # 1. Preprocessing Job
    run_preprocessing = SparkSubmitOperator(
        task_id="run_preprocessing",
        conn_id="spark_default",
        application="/opt/airflow/spark/preprocessing_job/spark_preprocessing_job.py",
        name="airflow-spark-preprocessing",
        verbose=True,
        
    )

    # 2. Training Job
    run_training = SparkSubmitOperator(
        task_id="run_training",
        conn_id="spark_default",
        application="/opt/airflow/spark/training_job/spark_training_job.py",
        name="airflow-spark-training",
        verbose=True,
    )

    # 3. Evaluation Job
    run_evaluation = SparkSubmitOperator(
        task_id="run_evaluation",
        conn_id="spark_default",
        application="/opt/airflow/spark/evaluation_job/spark_evaluation_job.py",
        name="airflow-spark-evaluation",
        verbose=True,
    )

    # Define the pipeline sequence
    run_preprocessing >> run_training >> run_evaluation

dag_instance = train_model_pipeline()
