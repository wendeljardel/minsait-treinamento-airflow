"""
Cryptocurrency Data Pipeline

This DAG fetches, processes, and analyzes cryptocurrency data from multiple sources.
"""

from airflow.decorators import dag, task_group
from datetime import datetime, timedelta
from typing import Dict

from crypto.tasks import (
    get_coingecko_prices,
    transform_prices,
    calculate_metrics,
    save_to_warehouse,
    check_anomalies
)

# TODO constants
DAG_ID = "crypto-prices-worflow"
OWNER = "luan moreno m. maciel"
RETRIES = 3
RETRY_DELAY = timedelta(minutes=5)
EXECUTION_TIMEOUT = timedelta(minutes=30)


@dag(
    dag_id=DAG_ID,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crypto', 'production'],
    default_args={
        'owner': OWNER,
        'retries': RETRIES,
        'retry_delay': RETRY_DELAY,
        'execution_timeout': EXECUTION_TIMEOUT,
    },
    doc_md=__doc__
)
def crypto_pipeline():
    """Main DAG for cryptocurrency data pipeline"""

    @task_group(group_id='extract_prices')
    def extract_price_data() -> Dict:
        return get_coingecko_prices()

    @task_group(group_id='transform_data')
    def process_price_data(raw_data: Dict) -> Dict:
        transformed_data = transform_prices(raw_data)
        metrics = calculate_metrics(transformed_data)
        return {'data': transformed_data, 'metrics': metrics}

    @task_group(group_id='load_data')
    def store_results(processed_data: Dict) -> None:
        save_to_warehouse(processed_data)
        check_anomalies(processed_data)

    # TODO define main workflow
    raw_prices = extract_price_data()
    processed_data = process_price_data(raw_prices)
    store_results(processed_data)


# TODO create DAG
dag = crypto_pipeline()
