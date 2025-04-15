"""
Crypto Price Pipeline with Advanced Decorators
Shows different TaskFlow API decorators and their functionalities.
"""

# TODO import libraries
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from typing import Dict, List
import requests


# TODO declare DAG
@dag(
    dag_id='tf-decorators-groups-crypto-pipeline',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crypto', 'pipeline'],
    default_args={
        'owner': 'Wendel Vasconcelos',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
)
def crypto_pipeline():
    """Crypto Price Pipeline with Advanced Decorators"""

    # TODO declare tasks
    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10)
    )
    def get_crypto_price() -> Dict:
        """Fetch crypto prices from CoinGecko API"""
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {
                "ids": "bitcoin,ethereum,cardano",
                "vs_currencies": "usd"
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(e)

    @task(multiple_outputs=True)
    def process_price(data: Dict) -> Dict:
        """Process the crypto prices and return multiple outputs"""
        return {
            'timestamp': datetime.now().isoformat(),
            'btc_price': data['bitcoin']['usd'],
            'eth_price': data['ethereum']['usd'],
            'ada_price': data['cardano']['usd']
        }

    # TODO add task group
    @task_group(group_id='analytics')
    def analyze_prices(btc_price: float, eth_price: float, ada_price: float) -> Dict:

        @task(task_id='calculate_metrics')
        def calculate_metrics(prices: List[float]) -> Dict:
            return {
                'total': sum(prices),
                'average': sum(prices) / len(prices),
                'max_price': max(prices),
                'min_price': min(prices)
            }

        @task(task_id='calculate_ratios')
        def calculate_ratios(btc: float, eth: float) -> Dict:
            return {
                'eth_btc_ratio': eth / btc
            }

        prices_list = [btc_price, eth_price, ada_price]
        metrics = calculate_metrics(prices_list)
        ratios = calculate_ratios(btc_price, eth_price)

        return {'metrics': metrics, 'ratios': ratios}

    # TODO add task with SLA
    # TODO create crypto pool
    @task(
        # sla=timedelta(seconds=30),
        pool='crypto_pool'
    )
    def load_price(
            timestamp: str,
            btc_price: float,
            eth_price: float,
            ada_price: float,
            analytics: Dict
    ) -> None:
        """Load the processed prices and analytics"""

        print(f"""
        Timestamp: {timestamp}

        Prices:
        -------
        BTC: ${btc_price:,.2f}
        ETH: ${eth_price:,.2f}
        ADA: ${ada_price:,.2f}

        Analytics:
        ----------
        Average Price: ${analytics['metrics']['average']:,.2f}
        ETH & BTC Ratio: {analytics['ratios']['eth_btc_ratio']:.4f}
        """)

    # TODO define workflow
    crypto_data = get_crypto_price()
    processed_data = process_price(crypto_data)

    analytics_data = analyze_prices(
        processed_data['btc_price'],
        processed_data['eth_price'],
        processed_data['ada_price']
    )

    load_price(
        processed_data['timestamp'],
        processed_data['btc_price'],
        processed_data['eth_price'],
        processed_data['ada_price'],
        analytics_data
    )


# TODO create DAG
dag = crypto_pipeline()
