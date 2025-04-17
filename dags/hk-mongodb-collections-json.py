"""
DAG para extrair dados de coleções do MongoDB e processá-los.
Esta DAG realiza as seguintes operações:
1. Extrai dados da coleção 'users'
2. Extrai dados da coleção 'payments'
3. Processa e resume os dados extraídos
"""

import os
import json
import pandas as pd
from dotenv import load_dotenv
from airflow.decorators import task, dag
from pymongo import MongoClient
from datetime import datetime, timedelta

# Carregar variáveis de ambiente
load_dotenv()

# Configurações do MongoDB
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb+srv://minsair-user:QLZcPUxeyrEf@minsait-airflow.tvllgfj.mongodb.net/?appName=minsait-airflow')
MONGODB_DB_NAME = os.getenv('MONGODB_DB_NAME', 'minsait')

# Verificar se as variáveis necessárias estão definidas
if not MONGODB_URI or not MONGODB_DB_NAME:
    raise ValueError("MONGODB_URI and MONGODB_DB_NAME must be set in environment variables")

# Argumentos padrão da DAG
default_args = {
    'owner': 'Wendel Vasconcelos',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': True
}

def convert_timestamps_to_strings(data):
    """
    Converte todos os timestamps em um dicionário para strings ISO format.
    
    Args:
        data: Dicionário, lista ou valor a ser convertido
        
    Returns:
        Dados com timestamps convertidos para strings
    """
    if isinstance(data, dict):
        return {k: convert_timestamps_to_strings(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_timestamps_to_strings(item) for item in data]
    elif isinstance(data, pd.Timestamp):
        return data.isoformat()
    else:
        return data

@dag(
    dag_id='hk-mongodb-collections-json',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 10, 20),
    catchup=False,
    tags=['hook', 'mongodb', 'extract'],
    description='DAG para extrair e processar dados do MongoDB'
)
def mongodb_data_extract_json():

    @task()
    def extract_users():
        """
        Extrai dados da coleção 'users' do MongoDB.
        
        Returns:
            Lista de dicionários contendo os dados dos usuários
        """
        try:
            client = MongoClient(MONGODB_URI)
            db = client[MONGODB_DB_NAME]

            try:
                users_cursor = db.users.find({})
                users_list = list(users_cursor)
                
                # Convertendo ObjectId para string para serialização JSON
                for user in users_list:
                    if '_id' in user:
                        user['_id'] = str(user['_id'])
                
                df_users = pd.DataFrame(users_list)

                print(f"Successfully extracted {len(df_users)} records from Users collection")
                if not df_users.empty:
                    print("Sample columns:", df_users.columns.tolist()[:5])
                    print("First record sample:", df_users.iloc[0].to_dict())
                else:
                    print("Warning: No records found in Users collection")

                # Converter timestamps para strings antes de retornar
                users_dict = df_users.to_dict('records')
                return convert_timestamps_to_strings(users_dict)

            except Exception as e:
                print(f"Error during Users collection processing: {str(e)}")
                raise
            finally:
                client.close()

        except Exception as e:
            print(f"Error connecting to MongoDB: {str(e)}")
            raise

    @task()
    def extract_payments():
        """
        Extrai dados da coleção 'payments' do MongoDB.
        
        Returns:
            Lista de dicionários contendo os dados de pagamentos
        """
        try:
            client = MongoClient(MONGODB_URI)
            db = client[MONGODB_DB_NAME]

            try:
                payments_cursor = db.payments.find({})
                payments_list = list(payments_cursor)
                
                # Convertendo ObjectId para string para serialização JSON
                for payment in payments_list:
                    if '_id' in payment:
                        payment['_id'] = str(payment['_id'])
                
                df_payments = pd.DataFrame(payments_list)

                print(f"Successfully extracted {len(df_payments)} records from Payments collection")
                if not df_payments.empty:
                    print("Sample columns:", df_payments.columns.tolist()[:5])
                    print("First record sample:", df_payments.iloc[0].to_dict())
                else:
                    print("Warning: No records found in Payments collection")

                # Converter timestamps para strings antes de retornar
                payments_dict = df_payments.to_dict('records')
                return convert_timestamps_to_strings(payments_dict)

            except Exception as e:
                print(f"Error during Payments collection processing: {str(e)}")
                raise
            finally:
                client.close()

        except Exception as e:
            print(f"Error connecting to MongoDB: {str(e)}")
            raise

    @task()
    def process_data(users_data, payments_data):
        """
        Processa e resume os dados extraídos das coleções.
        
        Args:
            users_data: Dados da coleção 'users'
            payments_data: Dados da coleção 'payments'
            
        Returns:
            Dicionário contendo o resumo dos dados processados
        """
        try:
            df_users = pd.DataFrame(users_data) if users_data else pd.DataFrame()
            df_payments = pd.DataFrame(payments_data) if payments_data else pd.DataFrame()

            summary = {
                "users_count": len(df_users),
                "payments_count": len(df_payments),
                "extraction_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "users_columns": df_users.columns.tolist() if not df_users.empty else [],
                "payments_columns": df_payments.columns.tolist() if not df_payments.empty else []
            }

            print("\nDetailed Data Summary:")
            print(f"Users Collection:")
            print(f"- Total records: {summary['users_count']}")
            print(f"- Columns: {', '.join(summary['users_columns']) if summary['users_columns'] else 'No columns'}")
            print(f"\nPayments Collection:")
            print(f"- Total records: {summary['payments_count']}")
            print(f"- Columns: {', '.join(summary['payments_columns']) if summary['payments_columns'] else 'No columns'}")
            print(f"\nExtraction Timestamp: {summary['extraction_timestamp']}")

            if df_users.empty and df_payments.empty:
                print("\nWarning: Both collections are empty!")

            return summary

        except Exception as e:
            print(f"Error processing extracted data: {str(e)}")
            raise

    # Definir o fluxo de execução
    users_data = extract_users()
    payments_data = extract_payments()
    process_data(users_data, payments_data)


# Criar a DAG
mongodb_dag = mongodb_data_extract_json()
