import pandas as pd
from sqlalchemy import create_engine
from airflow.decorators import task
from wheater_pipeline.config.config import POSTGRES_CONN_STRING

@task
def load_to_postgres(input_file: str):
    """
    Carrega os dados transformados no banco de dados PostgreSQL.
    
    Args:
        input_file (str): Caminho para o arquivo com os dados transformados
        
    Returns:
        str: Mensagem de confirmação de carregamento
    """
    print("Iniciando carregamento de dados no PostgreSQL...")
    
    
    df = pd.read_csv(input_file)
    
    
    engine = create_engine(POSTGRES_CONN_STRING)
    
    try:
        
        df.to_sql('weather_data', engine, if_exists='append', index=False)
        
        
        table_name = f"weather_data_{pd.to_datetime('today').strftime('%Y%m%d')}"
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"Dados carregados com sucesso na tabela 'weather_data' e '{table_name}'")
        
        
        summary_df = df[['city_name', 'country', 'temperature', 'humidity', 
                         'weather_main', 'weather_description', 'temp_category']]
        
        
        summary_df.to_sql('weather_summary', engine, if_exists='replace', index=False)
        
        print("Resumo dos dados carregado na tabela 'weather_summary' para análise do Gemini")
        
        return "Dados carregados com sucesso no PostgreSQL"
        
    except Exception as e:
        print(f"Erro ao carregar dados no PostgreSQL: {str(e)}")
        raise
