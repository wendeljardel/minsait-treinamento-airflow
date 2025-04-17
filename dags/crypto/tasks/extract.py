import logging
import requests
from typing import Dict
from airflow.decorators import task
from airflow.models import Variable
from datetime import timedelta
from crypto.tasks.utils import CryptoAPIError, load_config, validate_prices

logger = logging.getLogger(__name__)


@task(
    retries=3,
    retry_delay=timedelta(minutes=2),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10)
)
def get_coingecko_prices() -> Dict:
    """Fetch cryptocurrency prices from CoinGecko API"""
    try:
        config = load_config()
        api_key = Variable.get("COINGECKO_API_KEY", default_var=None)

        url = config['apis']['coingecko']['url']
        params = {
            "ids": ",".join(config['apis']['coingecko']['coins']),
            "vs_currencies": "usd",
            "api_key": api_key
        }

        logger.info("Fetching prices from CoinGecko")
        response = requests.get(
            url,
            params=params,
            timeout=config['apis']['coingecko']['timeout']
        )
        response.raise_for_status()

        data = response.json()
        validate_prices(data, config)

        return data

    except requests.exceptions.Timeout:
        logger.error("CoinGecko API timeout")
        raise CryptoAPIError("API timeout - retry needed")
    except requests.exceptions.RequestException as e:
        logger.error(f"CoinGecko API error: {str(e)}")
        raise CryptoAPIError(f"API error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
