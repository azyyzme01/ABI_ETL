# flows/prefect_etl_flows.py
"""
Flows Prefect pour le pipeline ETL ABI
Orchestration et monitoring avec Prefect 2.0
"""

import pandas as pd
import yaml
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, Any
import traceback
import sys
import os

# Ajout du path pour les imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from prefect import flow, task, get_run_logger
    from prefect.task_runners import ConcurrentTaskRunner
    from prefect.context import get_run_context
    from prefect.states import Failed, Completed
    from prefect.artifacts import create_markdown_artifact
    from prefect.blocks.system import Secret
    PREFECT_AVAILABLE = True
except ImportError:
    print("⚠️ Prefect not installed. Installing...")
    PREFECT_AVAILABLE = False

# Import des modules ETL existants
try:
    from scripts.extract import extract_data
    from scripts.transform import transform_data
    from scripts.load import load_data
    from validation.data_validator import validate_data
    from config.prefect_config import ETL_TAGS, RETRY_CONFIG, TIMEOUT_CONFIG
except ImportError as e:
    print(f"⚠️ Import error: {e}")
    # Fallback si les imports échouent
    ETL_TAGS = {'EXTRACTION': ['etl'], 'TRANSFORMATION': ['etl'], 'LOAD': ['etl'], 'VALIDATION': ['etl']}
    RETRY_CONFIG = {'extraction': {'retries': 2}, 'transformation': {'retries': 2}, 'load': {'retries': 2}}
    TIMEOUT_CONFIG = {'extraction': 3600, 'transformation': 7200, 'load': 1800}


@task(
    name="Extract Data",
    description="Extract data from various sources (TRL, PubMed, Companies)",
    retries=RETRY_CONFIG.get('extraction', {}).get('retries', 2),
    retry_delay_seconds=RETRY_CONFIG.get('extraction', {}).get('retry_delay_seconds', 30),
    timeout_seconds=TIMEOUT_CONFIG.get('extraction', 3600)
)
def prefect_extract_data(config_path: str = 'config/config.yaml') -> Dict[str, pd.DataFrame]:
    """
    Task Prefect pour l'extraction des données
    """
    logger = get_run_logger() if PREFECT_AVAILABLE else logging.getLogger(__name__)
    
    try:
        logger.info("🔄 Starting data extraction...")
        start_time = datetime.now()
        
        # Exécution de l'extraction
        extracted_data = extract_data(config_path)
        
        # Calcul des métriques
        duration = (datetime.now() - start_time).total_seconds()
        total_records = sum(len(df) for df in extracted_data.values())
        
        # Logging des résultats
        logger.info(f"✅ Data extraction completed successfully")
        logger.info(f"⏱️ Duration: {duration:.2f} seconds")
        logger.info(f"📊 Total records extracted: {total_records}")
        
        for source, df in extracted_data.items():
            logger.info(f"  - {source}: {len(df)} records")
        
        # Création d'un artifact de résumé
        if PREFECT_AVAILABLE:
            summary_md = f"""
# Data Extraction Summary

## Metrics
- **Duration**: {duration:.2f} seconds
- **Total Records**: {total_records:,}
- **Sources**: {len(extracted_data)}

## Sources Detail
{chr(10).join([f"- **{source}**: {len(df):,} records" for source, df in extracted_data.items()])}

## Status
✅ **Extraction completed successfully**
            """
            create_markdown_artifact(
                key="extraction-summary",
                markdown=summary_md,
                description="Data extraction summary and metrics"
            )
        
        return extracted_data
        
    except Exception as e:
        logger.error(f"❌ Data extraction failed: {str(e)}")
        logger.error(f"📋 Traceback: {traceback.format_exc()}")
        raise


@task(
    name="Transform Data", 
    description="Transform raw data into star schema format",
    retries=RETRY_CONFIG.get('transformation', {}).get('retries', 2),
    retry_delay_seconds=RETRY_CONFIG.get('transformation', {}).get('retry_delay_seconds', 60),
    timeout_seconds=TIMEOUT_CONFIG.get('transformation', 7200)
)
def prefect_transform_data(
    extracted_data: Dict[str, pd.DataFrame], 
    config_path: str = 'config/config.yaml'
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """
    Task Prefect pour la transformation des données
    """
    logger = get_run_logger() if PREFECT_AVAILABLE else logging.getLogger(__name__)
    
    try:
        logger.info("🔄 Starting data transformation...")
        start_time = datetime.now()
        
        # Exécution de la transformation
        dimensions, facts = transform_data(extracted_data, config_path)
        
        # Calcul des métriques
        duration = (datetime.now() - start_time).total_seconds()
        total_dim_records = sum(len(df) for df in dimensions.values())
        total_fact_records = sum(len(df) for df in facts.values())
        
        # Logging des résultats
        logger.info(f"✅ Data transformation completed successfully")
        logger.info(f"⏱️ Duration: {duration:.2f} seconds")
        logger.info(f"📊 Dimensions: {total_dim_records} records")
        logger.info(f"📊 Facts: {total_fact_records} records")
        
        # Création d'un artifact de résumé
        if PREFECT_AVAILABLE:
            summary_md = f"""
# Data Transformation Summary

## Metrics
- **Duration**: {duration:.2f} seconds
- **Total Dimension Records**: {total_dim_records:,}
- **Total Fact Records**: {total_fact_records:,}

## Dimensions Created
{chr(10).join([f"- **{name}**: {len(df):,} records" for name, df in dimensions.items()])}

## Facts Created  
{chr(10).join([f"- **{name}**: {len(df):,} records" for name, df in facts.items()])}

## Status
✅ **Transformation completed successfully**
            """
            create_markdown_artifact(
                key="transformation-summary",
                markdown=summary_md,
                description="Data transformation summary and metrics"
            )
        
        return dimensions, facts
        
    except Exception as e:
        logger.error(f"❌ Data transformation failed: {str(e)}")
        logger.error(f"📋 Traceback: {traceback.format_exc()}")
        raise


@task(
    name="Load Data",
    description="Load transformed data to warehouse",
    retries=RETRY_CONFIG.get('load', {}).get('retries', 3),
    retry_delay_seconds=RETRY_CONFIG.get('load', {}).get('retry_delay_seconds', 45),
    timeout_seconds=TIMEOUT_CONFIG.get('load', 1800)
)
def prefect_load_data(
    dimensions: Dict[str, pd.DataFrame], 
    facts: Dict[str, pd.DataFrame],
    config_path: str = 'config/config.yaml'
) -> Dict[str, Any]:
    """
    Task Prefect pour le chargement des données
    """
    logger = get_run_logger() if PREFECT_AVAILABLE else logging.getLogger(__name__)
    
    try:
        logger.info("🔄 Starting data loading...")
        start_time = datetime.now()
        
        # Exécution du chargement
        load_results = load_data(dimensions, facts, config_path)
        
        # Calcul des métriques
        duration = (datetime.now() - start_time).total_seconds()
        
        # Logging des résultats
        logger.info(f"✅ Data loading completed successfully")
        logger.info(f"⏱️ Duration: {duration:.2f} seconds")
        logger.info(f"📊 Load results: {load_results}")
        
        # Création d'un artifact de résumé
        if PREFECT_AVAILABLE:
            summary_md = f"""
# Data Loading Summary

## Metrics
- **Duration**: {duration:.2f} seconds
- **Tables Loaded**: {len(dimensions) + len(facts)}

## Results
```json
{load_results}
```

## Status
✅ **Loading completed successfully**
            """
            create_markdown_artifact(
                key="loading-summary", 
                markdown=summary_md,
                description="Data loading summary and results"
            )
        
        return load_results
        
    except Exception as e:
        logger.error(f"❌ Data loading failed: {str(e)}")
        logger.error(f"📋 Traceback: {traceback.format_exc()}")
        raise


@task(
    name="Validate Data",
    description="Validate data quality and completeness",
    retries=RETRY_CONFIG.get('validation', {}).get('retries', 2),
    retry_delay_seconds=RETRY_CONFIG.get('validation', {}).get('retry_delay_seconds', 30),
    timeout_seconds=TIMEOUT_CONFIG.get('validation', 900)
)
def prefect_validate_data(
    config_path: str = 'config/config.yaml'
) -> Dict[str, Any]:
    """
    Task Prefect pour la validation des données
    """
    logger = get_run_logger() if PREFECT_AVAILABLE else logging.getLogger(__name__)
    
    try:
        logger.info("🔄 Starting data validation...")
        start_time = datetime.now()
        
        # Exécution de la validation
        validation_results = validate_data(config_path)
        
        # Calcul des métriques
        duration = (datetime.now() - start_time).total_seconds()
        
        # Logging des résultats
        logger.info(f"✅ Data validation completed")
        logger.info(f"⏱️ Duration: {duration:.2f} seconds")
        
        # Vérification du statut global
        overall_status = validation_results.get('overall_status', 'unknown')
        if overall_status == 'passed':
            logger.info("✅ All validations passed")
        else:
            logger.warning(f"⚠️ Validation status: {overall_status}")
        
        # Création d'un artifact de résumé
        if PREFECT_AVAILABLE:
            status_emoji = "✅" if overall_status == 'passed' else "⚠️"
            summary_md = f"""
# Data Validation Summary

## Metrics
- **Duration**: {duration:.2f} seconds
- **Overall Status**: {status_emoji} {overall_status}

## Validation Results
```json
{validation_results}
```

## Status
{status_emoji} **Validation completed**
            """
            create_markdown_artifact(
                key="validation-summary",
                markdown=summary_md, 
                description="Data validation summary and results"
            )
        
        return validation_results
        
    except Exception as e:
        logger.error(f"❌ Data validation failed: {str(e)}")
        logger.error(f"📋 Traceback: {traceback.format_exc()}")
        raise


@flow(
    name="ABI ETL Pipeline",
    description="Complete ETL pipeline for ABI business intelligence data",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
    retries=1,
    retry_delay_seconds=300  # 5 minutes
)
def abi_etl_pipeline_flow(
    config_path: str = 'config/config.yaml',
    skip_validation: bool = False
) -> Dict[str, Any]:
    """
    Flow principal orchestrant tout le pipeline ETL ABI
    
    Args:
        config_path: Chemin vers le fichier de configuration
        skip_validation: Si True, ignore l'étape de validation
    
    Returns:
        Dictionnaire avec les résultats de chaque étape
    """
    logger = get_run_logger() if PREFECT_AVAILABLE else logging.getLogger(__name__)
    
    try:
        logger.info("🚀 Starting ABI ETL Pipeline")
        pipeline_start = datetime.now()
        
        # Étape 1: Extraction
        logger.info("📥 Step 1: Data Extraction")
        extracted_data = prefect_extract_data(config_path)
        
        # Étape 2: Transformation  
        logger.info("🔄 Step 2: Data Transformation")
        dimensions, facts = prefect_transform_data(extracted_data, config_path)
        
        # Étape 3: Chargement
        logger.info("📤 Step 3: Data Loading")
        load_results = prefect_load_data(dimensions, facts, config_path)
        
        # Étape 4: Validation (optionnelle)
        validation_results = None
        if not skip_validation:
            logger.info("✅ Step 4: Data Validation")
            validation_results = prefect_validate_data(config_path)
        else:
            logger.info("⏭️ Step 4: Data Validation (skipped)")
        
        # Calcul des métriques globales
        pipeline_duration = (datetime.now() - pipeline_start).total_seconds()
        total_records_processed = sum(len(df) for df in extracted_data.values())
        
        # Résultats finaux
        pipeline_results = {
            'status': 'success',
            'duration_seconds': pipeline_duration,
            'total_records_processed': total_records_processed,
            'extraction_records': {name: len(df) for name, df in extracted_data.items()},
            'dimension_records': {name: len(df) for name, df in dimensions.items()},
            'fact_records': {name: len(df) for name, df in facts.items()},
            'load_results': load_results,
            'validation_results': validation_results,
            'completed_at': datetime.now().isoformat()
        }
        
        logger.info(f"🎉 ABI ETL Pipeline completed successfully!")
        logger.info(f"⏱️ Total duration: {pipeline_duration:.2f} seconds")
        logger.info(f"📊 Total records processed: {total_records_processed:,}")
        
        # Artifact final de résumé
        if PREFECT_AVAILABLE:
            summary_md = f"""
# 🎉 ABI ETL Pipeline Execution Summary

## 📊 Overall Metrics
- **Status**: ✅ Success
- **Duration**: {pipeline_duration:.2f} seconds ({pipeline_duration/60:.1f} minutes)
- **Total Records Processed**: {total_records_processed:,}
- **Completed At**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📥 Extraction Results
{chr(10).join([f"- **{name}**: {count:,} records" for name, count in pipeline_results['extraction_records'].items()])}

## 🔄 Transformation Results
### Dimensions
{chr(10).join([f"- **{name}**: {count:,} records" for name, count in pipeline_results['dimension_records'].items()])}

### Facts
{chr(10).join([f"- **{name}**: {count:,} records" for name, count in pipeline_results['fact_records'].items()])}

## 📤 Load Results
```json
{load_results}
```

## ✅ Validation Results
{"```json" if validation_results else "⏭️ Skipped"}
{validation_results if validation_results else ""}
{"```" if validation_results else ""}

---
*Pipeline executed with Prefect orchestration*
            """
            create_markdown_artifact(
                key="pipeline-execution-summary",
                markdown=summary_md,
                description=f"Complete ABI ETL Pipeline execution summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        
        return pipeline_results
        
    except Exception as e:
        logger.error(f"❌ ABI ETL Pipeline failed: {str(e)}")
        logger.error(f"📋 Traceback: {traceback.format_exc()}")
        
        # Artifact d'erreur
        if PREFECT_AVAILABLE:
            error_md = f"""
# ❌ ABI ETL Pipeline Execution Failed

## Error Details
- **Error**: {str(e)}
- **Failed At**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Traceback
```
{traceback.format_exc()}
```

---
*Pipeline failed during Prefect orchestration*
            """
            create_markdown_artifact(
                key="pipeline-execution-error",
                markdown=error_md,
                description=f"ABI ETL Pipeline execution error - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        
        raise


# Flow pour extraction seule
@flow(
    name="ABI ETL - Extract Only",
    description="Extract data only from various sources"
)
def extract_only_flow(config_path: str = 'config/config.yaml') -> Dict[str, pd.DataFrame]:
    """Flow pour exécuter seulement l'extraction"""
    return prefect_extract_data(config_path)


# Flow pour transformation seule
@flow(
    name="ABI ETL - Transform Only",
    description="Transform extracted data only"
)
def transform_only_flow(
    extracted_data: Dict[str, pd.DataFrame],
    config_path: str = 'config/config.yaml'
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """Flow pour exécuter seulement la transformation"""
    return prefect_transform_data(extracted_data, config_path)


if __name__ == "__main__":
    # Test local du flow
    print("🧪 Testing ABI ETL Pipeline Flow locally...")
    try:
        result = abi_etl_pipeline_flow()
        print(f"✅ Pipeline completed successfully: {result['status']}")
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
