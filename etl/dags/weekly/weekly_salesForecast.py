import os
import sys
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from airflow.operators.python import PythonOperator

MODELS_PATH = '/opt/airflow/models'
if MODELS_PATH not in sys.path:
    sys.path.append(MODELS_PATH)
    
try:
    from src.predict import forecastPredictor
    from src.train import forecastModel
    from src.extractData import trainingDataExtractor
except ImportError as e:
    print('Error while importing modules: {}'.format(e))
    
INPUT_DATA_DIR = '/opt/airflow/models/trainingData'
MODEL_OUTPUT_DIR = '/opt/airflow/models/model'
PREDICTION_OUTPUT_DIR = '/opt/airflow/models/predictions'

default_args = {
    'owner': 'neinAlkem',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}
def task_extract_data():
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_connection('postgres_default')
    
    db_params = {
        'host': 'postgres',      
        'database': 'fmgc',   
        'user': conn.login,
        'password': conn.password,
        'port': 5432
    }
    
    extractor = trainingDataExtractor(db_params=db_params, output_dir=INPUT_DATA_DIR)
    extractor.extract()
    
def task_train_model():
    
    trainer = forecastModel(input_dir=INPUT_DATA_DIR, output_dir=MODEL_OUTPUT_DIR)
    summary_df = trainer.modelPipeline()
    print(summary_df)

def task_predict_models():
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_connection('postgres_default')
    
    db_params = {
        'host': 'postgres',
        'database': 'fmgc', 
        'user': conn.login,
        'password': conn.password,
        'port': 5432
    }
    
    predictor = forecastPredictor(db_params=db_params)
    all_forecast = predictor.predict_all_products()
    
    if not all_forecast.empty:
        predictor._save_to_postgres(all_forecast)

with DAG(
    dag_id='weekly_products_sales_prediction',
    default_args=default_args,
    description='Extract -> Train -> Predict',
    schedule='0 0 * * 0', #Run weekly 
    start_date=datetime(2025,12,1),
    catchup=False,
    tags=['models, prediction, sales, products'],
) as dag:
    
    extract_data_task = PythonOperator(
        task_id="extract_data_task",
        python_callable=task_extract_data
    )
    
    train_model_task = PythonOperator(
        task_id="train_model_task",
        python_callable=task_train_model
    )
    
    predict_task = PythonOperator(
        task_id="predict_task",
        python_callable=task_predict_models
    )
    
extract_data_task >> train_model_task >> predict_task