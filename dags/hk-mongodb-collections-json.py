import os
import json
import pandas as pd
from json import json_util
from dotenv import load_dotenv
from airflow.decorators import task, dag
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta

load_dotenv()

MONGODB_DB_NAME = os.getenv('MONGODB_DB_NAME', 'minsait')

default_args = {
    'owner': 'Wendel Vasconcelos',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='hk-mongodb-collections-json',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 10, 20),
    catchup=False,
    tags=['hook', 'mongodb', 'extract']
)
def mongodb_data_extract_json():

    @task()
    def extract_users():
        try:
            hook = MongoHook(conn_id='mongodb_default')
            client = hook.get_conn()
            db = client.minsait

            try:
                users_cursor = db.users.find({})
                users_list = list(users_cursor)
                users_json = json.loads(json_util.dumps(users_list))
                df_users = pd.DataFrame(users_json)

                print(f"Successfully extracted {len(df_users)} records from Users collection")
                if not df_users.empty:
                    print("Sample columns:", df_users.columns.tolist()[:5])
                    print("First record sample:", df_users.iloc[0].to_dict())
                else:
                    print("Warning: No records found in Users collection")

                return df_users.to_dict('records')

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
        try:
            hook = MongoHook(conn_id='mongodb_default')
            client = hook.get_conn()
            db = client.minsait

            try:
                payments_cursor = db.payments.find({})
                payments_list = list(payments_cursor)
                payments_json = json.loads(json_util.dumps(payments_list))
                df_payments = pd.DataFrame(payments_json)

                print(f"Successfully extracted {len(df_payments)} records from Payments collection")
                if not df_payments.empty:
                    print("Sample columns:", df_payments.columns.tolist()[:5])
                    print("First record sample:", df_payments.iloc[0].to_dict())
                else:
                    print("Warning: No records found in Payments collection")

                return df_payments.to_dict('records')

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

    users_data = extract_users()
    payments_data = extract_payments()
    process_data(users_data, payments_data)


mongodb_dag = mongodb_data_extract_json()
