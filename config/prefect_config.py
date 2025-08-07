# config/prefect_config.py
"""
Configuration Prefect pour le pipeline ETL ABI
"""

from prefect import runtime
from prefect.logging import get_run_logger
from prefect.context import get_run_context
import logging

# Configuration du logging Prefect
def setup_prefect_logging():
    """Configure logging pour Prefect"""
    logger = get_run_logger()
    return logger

# Configuration des tags pour l'organisation des flows
ETL_TAGS = {
    'EXTRACTION': ['etl', 'extraction', 'data-ingestion'],
    'TRANSFORMATION': ['etl', 'transformation', 'data-processing'],
    'LOAD': ['etl', 'load', 'data-warehouse'],
    'VALIDATION': ['etl', 'validation', 'data-quality'],
    'FULL_PIPELINE': ['etl', 'full-pipeline', 'orchestration']
}

# Configuration des retry policies
RETRY_CONFIG = {
    'extraction': {'retries': 3, 'retry_delay_seconds': 30},
    'transformation': {'retries': 2, 'retry_delay_seconds': 60},
    'load': {'retries': 3, 'retry_delay_seconds': 45},
    'validation': {'retries': 2, 'retry_delay_seconds': 30}
}

# Configuration des timeouts
TIMEOUT_CONFIG = {
    'extraction': 3600,  # 1 hour
    'transformation': 7200,  # 2 hours
    'load': 1800,  # 30 minutes
    'validation': 900  # 15 minutes
}

# Configuration des notifications
NOTIFICATION_CONFIG = {
    'on_failure': True,
    'on_success': True,
    'on_retry': True,
    'webhook_url': None,  # À configurer si nécessaire
    'email_recipients': []  # À configurer si nécessaire
}
