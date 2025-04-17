"""
python cli.py gcs
python cli.py atlas
python cli.py postgres
"""

import typer
import os
from rich import print
from main import Storage
from dotenv import load_dotenv
from main import MongoDBStorage, PostgresStorage
from src.objects.users import Users
from src.objects.payments import Payments
from src.objects.drivers import Drivers
from src.objects.restaurants import Restaurants
from src.api import api_requests

load_dotenv()

bucket_name = os.getenv("GCS_BUCKET_NAME")
mongo_uri = os.getenv("MONGODB_URI")
db_name = os.getenv("MONGODB_DB_NAME")

gen_amount = 500
api = api_requests.Requests()


def handle_atlas_storage():
    """
    Handle MongoDB Atlas-specific storage logic for multiple datasets.
    """
    mongo_storage_instance = MongoDBStorage(mongo_uri, db_name)

    cpf_list = [api.gen_cpf() for _ in range(gen_amount)]

    dt_users = Users.get_multiple_rows(gen_dt_rows=gen_amount)
    users_json, _ = Storage.create_dataframe(dt_users, "users", is_cpf=True, gen_user_id=True, cpf_list=cpf_list)
    mongo_storage_instance.insert_users(users_json)

    dt_payments = Payments.get_multiple_rows(gen_dt_rows=gen_amount)
    payments_json, _ = Storage.create_dataframe(dt_payments, "payments", is_cpf=True, gen_user_id=True, cpf_list=cpf_list)
    mongo_storage_instance.insert_payments(payments_json)


def handle_postgres_storage():
    """
    Handle PostgreSQL-specific storage logic for drivers and restaurants data.
    """
    print(f"Attempting to connect to PostgreSQL:")
    print(f"Host: {os.getenv('POSTGRES_HOST')}")
    print(f"Port: {os.getenv('POSTGRES_PORT')}")
    print(f"Database: {os.getenv('POSTGRES_DB')}")
    print(f"User: {os.getenv('POSTGRES_USER')}")

    storage_instance = Storage(bucket_name)

    try:
        postgres_storage = PostgresStorage(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )

        if not postgres_storage.test_connection():
            print("Connection test failed. Exiting.")
            return

        print("Connection successful! Creating tables...")

        postgres_storage.create_tables()

        print("Generating drivers data...")
        dt_drivers = Drivers.get_multiple_rows(gen_dt_rows=gen_amount)
        drivers_json, _ = Storage.create_dataframe(
            dt_drivers,
            "postgres",
            is_cpf=True,
            gen_user_id=False
        )
        postgres_storage.insert_drivers(drivers_json)

        print("Generating restaurants data...")
        dt_restaurants = Restaurants.get_multiple_rows(gen_dt_rows=gen_amount)
        restaurants_json, _ = Storage.create_dataframe(
            dt_restaurants,
            "postgres",
            is_cnpj=True,
            gen_user_id=False
        )
        postgres_storage.insert_restaurants(restaurants_json)

    except Exception as e:
        print(f"Error during PostgreSQL operations: {str(e)}")
        raise
    finally:
        if 'postgres_storage' in locals():
            postgres_storage.close()


def handle_general_storage(dstype: str):
    """
    Handle the general storage logic for MSSQL, PostgreSQL, Salesforce, Kafka, and MongoDB.
    """
    storage_instance = Storage(bucket_name)

    if dstype == "gcs":
        print(storage_instance.write_file(ds_type="mssql"))
        print(storage_instance.write_file(ds_type="postgres"))
        print(storage_instance.write_file(ds_type="mongodb"))
        print(storage_instance.write_file(ds_type="salesforce"))
        print(storage_instance.write_file(ds_type="kafka"))


def main(dstype: str):
    """
    Perform actions based on the specified data source type.

    Allowed types are: mssql, postgres, mongodb, kafka, atlas, postgres_db, & all

    Args:
        dstype: The type of the data source.
    """
    if dstype == "atlas":
        handle_atlas_storage()
    elif dstype == "postgres":
        handle_postgres_storage()
    else:
        handle_general_storage(dstype)


if __name__ == "__main__":
    typer.run(main)
