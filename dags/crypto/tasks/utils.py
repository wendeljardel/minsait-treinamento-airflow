import yaml
import os
from pathlib import Path
from typing import Dict
from airflow.exceptions import AirflowException


class DataValidationError(AirflowException):
    """Raised when data validation fails"""
    pass


class CryptoAPIError(AirflowException):
    """Raised when there's an error with the Crypto API"""
    pass


def load_config() -> Dict:
    """Load configuration from YAML file"""
    config_path = Path("/usr/local/airflow/dags/crypto/configs/crypto_config.yaml")
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
            environment = os.getenv('AIRFLOW_ENV', 'development')
            return config[environment]
    except Exception as e:
        raise AirflowException(f"Failed to load config: {str(e)}")


def validate_prices(data: Dict, config: Dict) -> None:
    """Validate price data structure and values"""
    required_coins = set(config['validation']['required_coins'])
    if not all(coin in data for coin in required_coins):
        raise DataValidationError(f"Missing required coins. Expected: {required_coins}")

    min_price = config['validation']['min_price']
    for coin, details in data.items():
        if 'usd' not in details:
            raise DataValidationError(f"Missing USD price for {coin}")
        if details['usd'] <= min_price:
            raise DataValidationError(f"Invalid price for {coin}: {details['usd']}")
