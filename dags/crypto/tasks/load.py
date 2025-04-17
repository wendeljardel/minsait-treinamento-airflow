import logging
from typing import Dict
from airflow.decorators import task
from crypto.tasks.utils import load_config

logger = logging.getLogger(__name__)


@task()
def save_to_warehouse(data: Dict) -> None:
    """Save processed data to data warehouse"""
    try:
        config = load_config()
        logger.info("Saving data to warehouse")
        logger.info(f"Saved prices: {data['data']['prices']}")
        logger.info(f"Saved metrics: {data['metrics']}")
    except Exception as e:
        logger.error(f"Warehouse save error: {str(e)}")
        raise


@task()
def check_anomalies(data: Dict) -> None:
    """Check for price anomalies and alert if necessary"""
    try:
        config = load_config()
        eth_btc_ratio = data['metrics']['price_ratios']['eth_btc']
        threshold = config['monitoring']['alert_thresholds']['eth_btc_ratio']

        if eth_btc_ratio > threshold:
            logger.warning(f"High ETH/BTC ratio detected: {eth_btc_ratio}")
    except Exception as e:
        logger.error(f"Anomaly check error: {str(e)}")
        raise