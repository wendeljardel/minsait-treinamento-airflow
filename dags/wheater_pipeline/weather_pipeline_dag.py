from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Importando as tasks
from wheater_pipeline.tasks.extract import extract_weather_data
from wheater_pipeline.tasks.transform import transform_weather_data
from wheater_pipeline.tasks.load import load_to_postgres
from wheater_pipeline.tasks.generate_insights import generate_gemini_insights

default_args = {
    'owner': 'Wendel Vasconcelos',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='Pipeline para dados meteorológicos com insights do Gemini usando TaskFlow API',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 24),
    catchup=False,
    tags=['weather', 'gemini', 'etl', 'taskflow'],
)
def weather_pipeline():
    """
    DAG para processar dados meteorológicos de várias cidades e gerar insights usando a API Gemini.
    Este pipeline utiliza a TaskFlow API do Airflow 2.0+ para uma definição de fluxo mais organizada.
    """
    
    # Tarefa para criar tabelas caso não existam
    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_minsait',
        sql='sql/create_tables.sql',
    )
    
    # Chamada às funções com decorator @task
    extracted_data_path = extract_weather_data()
    transformed_data_path = transform_weather_data(extracted_data_path)
    db_loaded = load_to_postgres(transformed_data_path)
    insights = generate_gemini_insights(db_loaded)
    
    # Definindo a ordem de execução 
    chain(
        create_tables, 
        extracted_data_path, 
        transformed_data_path, 
        db_loaded, insights
        )

# Instanciando a DAG
weather_dag = weather_pipeline()
