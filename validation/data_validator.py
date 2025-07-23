"""
validation/data_validator.py - Data quality validation
Performs comprehensive data quality checks throughout the ETL pipeline
"""

import pandas as pd
import numpy as np
import yaml
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataValidator:
    """Comprehensive data quality validation for the ETL pipeline"""
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize validator with configuration"""
        self.config = self._load_config(config_path)
        self.validation_results = []
        self.engine = self._create_engine()
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _create_engine(self):
        """Create SQLAlchemy engine for database validation"""
        db_config = {
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'abi_warehouse')
        }
        
        connection_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        return create_engine(connection_string)
    
    def validate_pipeline(self) -> Dict[str, any]:
        """Run complete validation suite"""
        logger.info("Starting comprehensive data validation...")
        
        # Validate source files
        self._validate_source_files()
        
        # Validate extracted data
        self._validate_extracted_data()
        
        # Validate transformed data
        self._validate_transformed_data()
        
        # Validate loaded data in database
        self._validate_database_data()
        
        # Generate validation report
        report = self._generate_validation_report()
        
        return report
    
    def _validate_source_files(self) -> None:
        """Validate that source files exist and are readable"""
        logger.info("Validating source files...")
        
        raw_path = Path(self.config['paths']['raw_data'])
        
        for file_key, filename in self.config['files'].items():
            file_path = raw_path / filename
            
            validation = {
                'stage': 'Source Files',
                'check': f'File exists: {filename}',
                'status': 'PASS' if file_path.exists() else 'FAIL',
                'details': ''
            }
            
            if file_path.exists():
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                validation['details'] = f"Size: {file_size_mb:.2f} MB"
            else:
                validation['details'] = "File not found"
            
            self.validation_results.append(validation)
    
    def _validate_extracted_data(self) -> None:
        """Validate extracted data quality"""
        logger.info("Validating extracted data...")
        
        extracted_path = Path(self.config['paths']['processed_data']) / 'extracted'
        
        if not extracted_path.exists():
            self.validation_results.append({
                'stage': 'Extracted Data',
                'check': 'Extracted data exists',
                'status': 'SKIP',
                'details': 'No extracted data found'
            })
            return
        
        for file in extracted_path.glob('*_extracted.parquet'):
            df = pd.read_parquet(file)
            source_name = file.stem.replace('_extracted', '')
            
            # Check for empty DataFrames
            self.validation_results.append({
                'stage': 'Extracted Data',
                'check': f'{source_name}: Not empty',
                'status': 'PASS' if not df.empty else 'FAIL',
                'details': f'{len(df)} rows, {len(df.columns)} columns'
            })
            
            # Check null percentage
            null_percentage = (df.isnull().sum().sum() / (df.shape[0] * df.shape[1]) * 100)
            threshold = self.config['validation']['null_threshold'] * 100
            
            self.validation_results.append({
                'stage': 'Extracted Data',
                'check': f'{source_name}: Null percentage < {threshold}%',
                'status': 'PASS' if null_percentage < threshold else 'WARN',
                'details': f'{null_percentage:.2f}% nulls'
            })
    
    def _validate_transformed_data(self) -> None:
        """Validate transformed data quality"""
        logger.info("Validating transformed data...")
        
        processed_path = Path(self.config['paths']['processed_data'])
        
        # Validate dimensions
        dimension_files = list(processed_path.glob('dim_*.parquet'))
        fact_files = list(processed_path.glob('fact_*.parquet'))
        
        # Check dimension integrity
        for dim_file in dimension_files:
            df = pd.read_parquet(dim_file)
            table_name = dim_file.stem
            
            # Check for surrogate keys
            sk_column = f"{table_name.split('_')[1]}_sk"
            
            if sk_column in df.columns:
                # Check uniqueness
                is_unique = df[sk_column].is_unique
                self.validation_results.append({
                    'stage': 'Transformed Data',
                    'check': f'{table_name}: Surrogate keys unique',
                    'status': 'PASS' if is_unique else 'FAIL',
                    'details': f'{len(df[sk_column].unique())} unique keys'
                })
                
                # Check sequential
                if not df.empty:
                    is_sequential = (df[sk_column].max() - df[sk_column].min() + 1) == len(df)
                    self.validation_results.append({
                        'stage': 'Transformed Data',
                        'check': f'{table_name}: Surrogate keys sequential',
                        'status': 'PASS' if is_sequential else 'WARN',
                        'details': f'Range: {df[sk_column].min()} to {df[sk_column].max()}'
                    })
        
        # Validate fact grain
        for fact_file in fact_files:
            df = pd.read_parquet(fact_file)
            table_name = fact_file.stem
            
            # Define expected grain columns
            grain_columns = {
                'fact_technology': ['technology_sk', 'publication_sk'],
                'fact_financial': ['company_sk', 'fiscal_year', 'fiscal_period'],
                'fact_scoring': ['company_sk', 'date_sk']
            }
            
            if table_name in grain_columns:
                cols = grain_columns[table_name]
                if all(col in df.columns for col in cols):
                    duplicates = df.duplicated(subset=cols).sum()
                    self.validation_results.append({
                        'stage': 'Transformed Data',
                        'check': f'{table_name}: Grain integrity (no duplicates)',
                        'status': 'PASS' if duplicates == 0 else 'FAIL',
                        'details': f'{duplicates} duplicate records found'
                    })
    
    def _validate_database_data(self) -> None:
        """Validate data loaded in the database"""
        logger.info("Validating database data...")
        
        # Check table existence
        with self.engine.connect() as conn:
            # Get all tables
            result = conn.execute(
                text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """)
            )
            tables = [row[0] for row in result]
            
            expected_tables = [
                'dim_company', 'dim_date', 'dim_publication', 'dim_technology',
                'fact_financial', 'fact_scoring', 'fact_technology'
            ]
            
            for table in expected_tables:
                self.validation_results.append({
                    'stage': 'Database',
                    'check': f'Table exists: {table}',
                    'status': 'PASS' if table in tables else 'FAIL',
                    'details': ''
                })
            
            # Check row counts
            for table in tables:
                if table in expected_tables:
                    count = conn.execute(
                        text(f"SELECT COUNT(*) FROM {table}")
                    ).scalar()
                    
                    self.validation_results.append({
                        'stage': 'Database',
                        'check': f'{table}: Has data',
                        'status': 'PASS' if count > 0 else 'WARN',
                        'details': f'{count} rows'
                    })
            
            # Validate referential integrity
            self._validate_referential_integrity(conn)
            
            # Validate data quality rules
            self._validate_business_rules(conn)
    
    def _validate_referential_integrity(self, conn) -> None:
        """Validate foreign key relationships"""
        
        # Check fact_technology foreign keys
        orphaned_tech = conn.execute(
            text("""
            SELECT COUNT(*) FROM fact_technology f
            LEFT JOIN dim_technology d ON f.technology_sk = d.technology_sk
            WHERE d.technology_sk IS NULL
            """)
        ).scalar()
        
        self.validation_results.append({
            'stage': 'Referential Integrity',
            'check': 'fact_technology → dim_technology',
            'status': 'PASS' if orphaned_tech == 0 else 'FAIL',
            'details': f'{orphaned_tech} orphaned records'
        })
        
        # Check fact_financial foreign keys
        orphaned_fin = conn.execute(
            text("""
            SELECT COUNT(*) FROM fact_financial f
            LEFT JOIN dim_company d ON f.company_sk = d.company_sk
            WHERE d.company_sk IS NULL
            """)
        ).scalar()
        
        self.validation_results.append({
            'stage': 'Referential Integrity',
            'check': 'fact_financial → dim_company',
            'status': 'PASS' if orphaned_fin == 0 else 'FAIL',
            'details': f'{orphaned_fin} orphaned records'
        })
    
    def _validate_business_rules(self, conn) -> None:
        """Validate business logic and data quality rules"""
        
        # Rule: TRL values should be between 1 and 9
        invalid_trl = conn.execute(
            text("""
            SELECT COUNT(*) FROM dim_technology
            WHERE current_trl NOT BETWEEN 1 AND 9
            OR predicted_trl NOT BETWEEN 1 AND 9
            """)
        ).scalar()
        
        self.validation_results.append({
            'stage': 'Business Rules',
            'check': 'TRL values between 1-9',
            'status': 'PASS' if invalid_trl == 0 else 'FAIL',
            'details': f'{invalid_trl} invalid TRL values'
        })
        
        # Rule: Financial metrics should be reasonable
        suspicious_financials = conn.execute(
            text("""
            SELECT COUNT(*) FROM fact_financial
            WHERE revenues < 0 
            OR assets < 0
            OR (assets > 0 AND liabilities > assets * 10)
            """)
        ).scalar()
        
        self.validation_results.append({
            'stage': 'Business Rules',
            'check': 'Financial metrics reasonable',
            'status': 'PASS' if suspicious_financials == 0 else 'WARN',
            'details': f'{suspicious_financials} suspicious records'
        })
        
        # Rule: Scores should be between 0 and 100
        invalid_scores = conn.execute(
            text("""
            SELECT COUNT(*) FROM fact_scoring
            WHERE rd_score NOT BETWEEN 0 AND 100
            OR cash_score NOT BETWEEN 0 AND 100
            OR market_score NOT BETWEEN 0 AND 100
            OR total_score NOT BETWEEN 0 AND 100
            """)
        ).scalar()
        
        self.validation_results.append({
            'stage': 'Business Rules',
            'check': 'Scores between 0-100',
            'status': 'PASS' if invalid_scores == 0 else 'FAIL',
            'details': f'{invalid_scores} invalid scores'
        })
        
        # Rule: Dates should be within expected range
        date_min = self.config['validation']['date_range']['min']
        date_max = self.config['validation']['date_range']['max']
        
        invalid_dates = conn.execute(
            text(f"""
            SELECT COUNT(*) FROM dim_publication
            WHERE publication_date NOT BETWEEN '{date_min}' AND '{date_max}'
            """)
        ).scalar()
        
        self.validation_results.append({
            'stage': 'Business Rules',
            'check': f'Publication dates in range',
            'status': 'PASS' if invalid_dates == 0 else 'WARN',
            'details': f'{invalid_dates} dates outside range'
        })
    
    def _generate_validation_report(self) -> Dict[str, any]:
        """Generate comprehensive validation report"""
        
        # Summary statistics
        total_checks = len(self.validation_results)
        passed = sum(1 for r in self.validation_results if r['status'] == 'PASS')
        warnings = sum(1 for r in self.validation_results if r['status'] == 'WARN')
        failed = sum(1 for r in self.validation_results if r['status'] == 'FAIL')
        
        # Generate report
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_checks': total_checks,
                'passed': passed,
                'warnings': warnings,
                'failed': failed,
                'success_rate': (passed / total_checks * 100) if total_checks > 0 else 0
            },
            'details': self.validation_results
        }
        
        # Print summary
        print("\n" + "="*60)
        print("DATA VALIDATION REPORT")
        print("="*60)
        print(f"Timestamp: {report['timestamp']}")
        print(f"\nSummary:")
        print(f"  Total Checks: {total_checks}")
        print(f"  ✅ Passed: {passed}")
        print(f"  ⚠️  Warnings: {warnings}")
        print(f"  ❌ Failed: {failed}")
        print(f"  Success Rate: {report['summary']['success_rate']:.1f}%")
        
        # Print detailed results
        print("\nDetailed Results:")
        
        # Group by stage
        stages = {}
        for result in self.validation_results:
            stage = result['stage']
            if stage not in stages:
                stages[stage] = []
            stages[stage].append(result)
        
        for stage, results in stages.items():
            print(f"\n{stage}:")
            table_data = []
            for r in results:
                status_symbol = {
                    'PASS': '✅',
                    'WARN': '⚠️ ',
                    'FAIL': '❌',
                    'SKIP': '⏭️ '
                }.get(r['status'], '❓')
                
                table_data.append([
                    f"{status_symbol} {r['status']}",
                    r['check'],
                    r['details']
                ])
            
            print(tabulate(table_data, headers=['Status', 'Check', 'Details'], 
                          tablefmt='grid'))
        
        # Save report to file
        report_path = Path('logs') / f'validation_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.yaml'
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, 'w') as f:
            yaml.dump(report, f, default_flow_style=False)
        
        print(f"\n📄 Full report saved to: {report_path}")
        
        # Overall status
        if failed > 0:
            print("\n❌ VALIDATION FAILED - Critical issues found!")
        elif warnings > 0:
            print("\n⚠️  VALIDATION PASSED WITH WARNINGS")
        else:
            print("\n✅ ALL VALIDATIONS PASSED!")
        
        return report


# Standalone validation function
def validate_data(config_path: str = 'config/config.yaml') -> Dict[str, any]:
    """
    Main validation function to be called from ETL pipeline or independently
    
    Returns:
        Validation report dictionary
    """
    validator = DataValidator(config_path)
    report = validator.validate_pipeline()
    
    # Return success status
    return report


if __name__ == "__main__":
    # Run validation
    print("🔍 Running data quality validation...")
    
    try:
        report = validate_data()
        
        # Exit with appropriate code
        if report['summary']['failed'] > 0:
            exit(1)
        else:
            exit(0)
            
    except Exception as e:
        print(f"\n❌ Validation error: {str(e)}")
        exit(1)