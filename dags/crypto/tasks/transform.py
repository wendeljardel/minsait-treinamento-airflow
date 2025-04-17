import logging
from typing import Dict
from datetime import datetime
from airflow.decorators import task
from crypto.tasks.utils import DataValidationError

logger = logging.getLogger(__name__)


@task(multiple_outputs=True)
def transform_prices(data: Dict) -> Dict:
    """Transform raw price data into standardized format"""
    try:
        timestamp = datetime.now().isoformat()

        prices = {
            'BTC': data['bitcoin']['usd'],
            'ETH': data['ethereum']['usd'],
            'ADA': data['cardano']['usd']
        }

        return {
            'timestamp': timestamp,
            'prices': prices,
            'metadata': {
                'source': 'coingecko',
                'currency': 'usd'
            }
        }

    except KeyError as e:
        logger.error(f"Data structure error: {str(e)}")
        raise DataValidationError(f"Invalid data structure: {str(e)}")
    except Exception as e:
        logger.error(f"Transform error: {str(e)}")
        raise


@task
def calculate_metrics(prices: Dict) -> Dict:
    """Calculate various price metrics and statistics"""
    try:
        price_values = list(prices['prices'].values())

        return {
            'total_market_value': sum(price_values),
            'average_price': sum(price_values) / len(price_values),
            'price_ratios': {
                'eth_btc': prices['prices']['ETH'] / prices['prices']['BTC'],
                'ada_btc': prices['prices']['ADA'] / prices['prices']['BTC']
            }
        }
    except Exception as e:
        logger.error(f"Metrics calculation error: {str(e)}")
        raise
