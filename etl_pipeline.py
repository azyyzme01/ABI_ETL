"""
etl_pipeline.py - Main ETL pipeline orchestrator
Coordinates the complete ETL process from extraction to validation
"""

import argparse
import logging
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
import yaml
import pandas as pd
from typing import Dict, Optional
import os

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'validation'))

# Import ETL modules
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data
from validation.data_validator import validate_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize pipeline with configuration"""
        self.config = self._load_config(config_path)
        self.config_path = config_path
        self.start_time = None
        self.metrics = {
            'extraction': {'status': 'pending', 'duration': 0, 'records': 0},
            'transformation': {'status': 'pending', 'duration': 0, 'records': 0},
            'loading': {'status': 'pending', 'duration': 0, 'records': 0},
            'validation': {'status': 'pending', 'duration': 0, 'checks': 0}
        }
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def run(self, mode: str = 'full', skip_validation: bool = False, 
            initialize_db: bool = False, drop_existing: bool = False) -> bool:
        """
        Run the ETL pipeline
        
        Args:
            mode: 'full', 'incremental', or 'validate-only'
            skip_validation: Skip data validation step
            initialize_db: Initialize database schema before loading
            drop_existing: Drop existing tables before creating (use with caution!)
            
        Returns:
            Success status
        """
        self.start_time = time.time()
        logger.info(f"Starting ETL pipeline in {mode} mode...")
        logger.info(f"Configuration: {self.config['project']['name']} v{self.config['project']['version']}")
        
        success = True
        
        try:
            if mode == 'validate-only':
                # Only run validation
                self._run_validation()
            else:
                # Run full or incremental pipeline
                extracted_data = self._run_extraction()
                dimensions, facts = self._run_transformation(extracted_data)
                self._run_loading(dimensions, facts, initialize_db, drop_existing)
                
                if not skip_validation:
                    self._run_validation()
            
            # Log completion
            self._log_completion(success=True)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            logger.error(traceback.format_exc())
            self._log_completion(success=False, error=str(e))
            success = False
        
        return success
    
    def _run_extraction(self) -> Dict[str, pd.DataFrame]:
        """Run extraction phase"""
        logger.info("\n" + "="*50)
        logger.info("PHASE 1: DATA EXTRACTION")
        logger.info("="*50)
        
        phase_start = time.time()
        
        try:
            # Extract data
            extracted_data = extract_data(self.config_path)
            
            # Update metrics
            total_records = sum(len(df) for df in extracted_data.values())
            self.metrics['extraction'].update({
                'status': 'success',
                'duration': time.time() - phase_start,
                'records': total_records,
                'sources': list(extracted_data.keys())
            })
            
            logger.info(f"✅ Extraction completed: {total_records} total records")
            return extracted_data
            
        except Exception as e:
            self.metrics['extraction']['status'] = 'failed'
            self.metrics['extraction']['error'] = str(e)
            raise
    
    def _run_transformation(self, extracted_data: Dict[str, pd.DataFrame]) -> tuple:
        """Run transformation phase"""
        logger.info("\n" + "="*50)
        logger.info("PHASE 2: DATA TRANSFORMATION")
        logger.info("="*50)
        
        phase_start = time.time()
        
        try:
            # Transform data
            dimensions, facts = transform_data(extracted_data, self.config_path)
            
            # Update metrics
            dim_records = sum(len(df) for df in dimensions.values())
            fact_records = sum(len(df) for df in facts.values())
            
            self.metrics['transformation'].update({
                'status': 'success',
                'duration': time.time() - phase_start,
                'records': dim_records + fact_records,
                'dimensions': {name: len(df) for name, df in dimensions.items()},
                'facts': {name: len(df) for name, df in facts.items()}
            })
            
            logger.info(f"✅ Transformation completed: {dim_records} dimension records, {fact_records} fact records")
            return dimensions, facts
            
        except Exception as e:
            self.metrics['transformation']['status'] = 'failed'
            self.metrics['transformation']['error'] = str(e)
            raise
    
    def _run_loading(self, dimensions: Dict[str, pd.DataFrame], 
                    facts: Dict[str, pd.DataFrame], 
                    initialize_db: bool = False,
                    drop_existing: bool = False) -> None:
        """Run loading phase"""
        logger.info("\n" + "="*50)
        logger.info("PHASE 3: DATA LOADING")
        logger.info("="*50)
        
        phase_start = time.time()
        
        try:
            # Load data
            load_data(dimensions, facts, self.config_path, initialize_db, drop_existing)
            
            # Update metrics
            total_loaded = sum(len(df) for df in dimensions.values())
            total_loaded += sum(len(df) for df in facts.values())
            
            self.metrics['loading'].update({
                'status': 'success',
                'duration': time.time() - phase_start,
                'records': total_loaded,
                'initialize_db': initialize_db,
                'drop_existing': drop_existing
            })
            
            logger.info(f"✅ Loading completed: {total_loaded} records loaded")
            
        except Exception as e:
            self.metrics['loading']['status'] = 'failed'
            self.metrics['loading']['error'] = str(e)
            raise
    
    def _run_validation(self) -> None:
        """Run validation phase"""
        logger.info("\n" + "="*50)
        logger.info("PHASE 4: DATA VALIDATION")
        logger.info("="*50)
        
        phase_start = time.time()
        
        try:
            # Run validation
            report = validate_data(self.config_path)
            
            # Update metrics
            self.metrics['validation'].update({
                'status': 'success',
                'duration': time.time() - phase_start,
                'checks': report['summary']['total_checks'],
                'passed': report['summary']['passed'],
                'warnings': report['summary']['warnings'],
                'failed': report['summary']['failed']
            })
            
            # Check for critical failures
            if report['summary']['failed'] > 0:
                raise ValueError(f"Validation failed with {report['summary']['failed']} critical issues")
            
            logger.info(f"✅ Validation completed: {report['summary']['success_rate']:.1f}% success rate")
            
        except Exception as e:
            self.metrics['validation']['status'] = 'failed'
            self.metrics['validation']['error'] = str(e)
            raise
    
    def _log_completion(self, success: bool, error: Optional[str] = None) -> None:
        """Log pipeline completion and save metrics"""
        total_duration = time.time() - self.start_time
        
        # Prepare log entry
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'duration_seconds': total_duration,
            'success': success,
            'error': error,
            'metrics': self.metrics,
            'config_version': self.config['project']['version']
        }
        
        # Log to CSV
        log_file = Path('logs/etl_log.csv')
        log_exists = log_file.exists()
        
        # Create simplified log for CSV
        csv_entry = {
            'timestamp': log_entry['timestamp'],
            'duration_seconds': round(total_duration, 2),
            'success': success,
            'error': error or '',
            'extraction_status': self.metrics['extraction']['status'],
            'extraction_records': self.metrics['extraction'].get('records', 0),
            'transformation_status': self.metrics['transformation']['status'],
            'transformation_records': self.metrics['transformation'].get('records', 0),
            'loading_status': self.metrics['loading']['status'],
            'loading_records': self.metrics['loading'].get('records', 0),
            'validation_status': self.metrics['validation']['status'],
            'validation_checks': self.metrics['validation'].get('checks', 0),
            'validation_passed': self.metrics['validation'].get('passed', 0),
            'validation_failed': self.metrics['validation'].get('failed', 0)
        }
        
        # Append to CSV log
        df_log = pd.DataFrame([csv_entry])
        df_log.to_csv(log_file, mode='a', header=not log_exists, index=False)
        
        # Save detailed metrics
        metrics_file = Path('logs') / f'etl_metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.yaml'
        with open(metrics_file, 'w') as f:
            yaml.dump(log_entry, f, default_flow_style=False)
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("ETL PIPELINE SUMMARY")
        logger.info("="*60)
        logger.info(f"Status: {'✅ SUCCESS' if success else '❌ FAILED'}")
        logger.info(f"Duration: {total_duration:.2f} seconds")
        
        if success:
            logger.info("\nPhase Summary:")
            for phase, metrics in self.metrics.items():
                status_icon = '✅' if metrics['status'] == 'success' else '❌'
                logger.info(f"  {status_icon} {phase.capitalize()}: "
                          f"{metrics.get('duration', 0):.2f}s, "
                          f"{metrics.get('records', metrics.get('checks', 0))} items")
        else:
            logger.error(f"\nError: {error}")
        
        logger.info(f"\nDetailed metrics saved to: {metrics_file}")
        logger.info("="*60)


def main():
    """Main entry point with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description='ABI ETL Pipeline - Transform biotech data into analytics-ready warehouse'
    )
    
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental', 'validate-only'],
        default='full',
        help='Pipeline execution mode (default: full)'
    )
    
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip data validation step'
    )
    
    parser.add_argument(
        '--initialize-db',
        action='store_true',
        help='Initialize database schema before loading'
    )
    
    parser.add_argument(
        '--drop-existing',
        action='store_true',
        help='Drop existing tables before creating new ones (WARNING: destroys data!)'
    )
    
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Path to configuration file (default: config/config.yaml)'
    )
    
    args = parser.parse_args()
    
    # Print banner
    print("\n" + "="*60)
    print("🚀 ABI ETL PIPELINE")
    print("="*60)
    print(f"Mode: {args.mode}")
    print(f"Config: {args.config}")
    print(f"Initialize DB: {args.initialize_db}")
    print(f"Drop Existing: {args.drop_existing}")
    print(f"Skip Validation: {args.skip_validation}")
    print("="*60 + "\n")
    
    # Warn about dropping tables
    if args.drop_existing:
        print("⚠️  WARNING: --drop-existing will DELETE ALL EXISTING DATA!")
        response = input("Are you sure? (yes/no): ")
        if response.lower() != 'yes':
            print("Operation cancelled.")
            sys.exit(0)
    
    # Create and run pipeline
    pipeline = ETLPipeline(args.config)
    success = pipeline.run(
        mode=args.mode,
        skip_validation=args.skip_validation,
        initialize_db=args.initialize_db,
        drop_existing=args.drop_existing
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()