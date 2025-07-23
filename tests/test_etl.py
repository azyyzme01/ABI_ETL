"""
tests/test_etl.py - Unit tests for ETL pipeline components
Tests extraction, transformation, loading, and validation modules
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path
import tempfile
import yaml
import os
import sys

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts'))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'validation'))

from scripts.extract import DataExtractor
from scripts.transform import DataTransformer
from scripts.load import DataLoader
from validation.data_validator import DataValidator
from models.models import Base, DimTechnology, DimPublication, DimCompany, DimDate


# ===========================
# FIXTURES
# ===========================

@pytest.fixture
def sample_config():
    """Create a sample configuration for testing"""
    config = {
        'project': {
            'name': 'Test ETL Pipeline',
            'version': '1.0.0'
        },
        'paths': {
            'raw_data': 'test_data/raw',
            'processed_data': 'test_data/processed',
            'archive': 'test_data/archive',
            'exports': 'test_data/exports',
            'logs': 'test_logs'
        },
        'files': {
            'trl_predictions': 'test_trl.xlsx',
            'pubmed_articles': 'test_pubmed.xlsx',
            'biotech_companies': 'test_companies.csv'
        },
        'database': {
            'dialect': 'sqlite',
            'database': ':memory:'
        },
        'validation': {
            'null_threshold': 0.05,
            'duplicate_threshold': 0.01,
            'date_range': {
                'min': '2020-01-01',
                'max': '2024-12-31'
            }
        }
    }
    return config


@pytest.fixture
def sample_trl_data():
    """Create sample TRL prediction data"""
    return pd.DataFrame({
        'Technology': ['Gene Therapy', 'CRISPR', 'mRNA Vaccines', 'CAR-T'],
        'Current TRL': [6, 7, 9, 8],
        'Predicted TRL': [8, 9, 9, 9],
        'TRL Change': [2, 2, 0, 1],
        'Predicted Category': ['High', 'High', 'Deployed', 'High'],
        'Confidence %': [85.5, 92.0, 98.0, 88.5]
    })


@pytest.fixture
def sample_pubmed_data():
    """Create sample PubMed articles data"""
    return pd.DataFrame({
        'title': [
            'Gene Therapy Advances in 2023',
            'CRISPR Applications in Medicine',
            'mRNA Vaccine Development',
            'CAR-T Cell Therapy Progress'
        ],
        'authors': [
            'Smith J, Doe A',
            'Johnson B, Lee C',
            'Williams D, Brown E',
            'Davis F, Miller G'
        ],
        'summary': [
            'Recent advances in gene therapy...',
            'CRISPR technology has shown...',
            'mRNA vaccines have revolutionized...',
            'CAR-T therapy continues to evolve...'
        ],
        'link': [
            'https://pubmed.com/1',
            'https://pubmed.com/2',
            'https://pubmed.com/3',
            'https://pubmed.com/4'
        ],
        'country': ['USA', 'UK', 'Germany', 'USA'],
        'trl': [6, 7, 9, 8],
        'publication_date': pd.to_datetime([
            '2023-01-15', '2023-03-20', '2023-06-10', '2023-09-05'
        ])
    })


@pytest.fixture
def sample_companies_data():
    """Create sample biotech companies data"""
    return pd.DataFrame({
        'cik': [1001, 1002, 1003, 1004],
        'entityName': ['BioTech Corp', 'Gene Solutions Inc', 'RNA Therapeutics', 'Cell Therapy Ltd'],
        'ticker': ['BTC', 'GSI', 'RNAT', 'CTL'],
        'stateOfIncorporation': ['DE', 'CA', 'MA', 'NY'],
        'sic': [2834, 2834, 2834, 2834],
        'sicDescription': ['Pharmaceutical Preparations'] * 4,
        'industryCategory': ['Biotechnology'] * 4,
        'fiscalYear': [2023] * 4,
        'period': ['Q4'] * 4,
        'Revenues': [150000000, 80000000, 200000000, 120000000],
        'NetIncomeLoss': [20000000, -5000000, 50000000, 15000000],
        'ResearchAndDevelopmentExpense': [45000000, 30000000, 60000000, 40000000],
        'Assets': [500000000, 300000000, 800000000, 400000000],
        'Liabilities': [200000000, 150000000, 300000000, 180000000],
        'CashAndCashEquivalentsAtCarryingValue': [100000000, 50000000, 200000000, 80000000]
    })


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create temporary directory structure for testing"""
    # Create directory structure
    dirs = ['raw', 'processed', 'processed/extracted', 'archive', 'exports', 'logs']
    for dir_name in dirs:
        (tmp_path / dir_name).mkdir(parents=True, exist_ok=True)
    
    return tmp_path


# ===========================
# EXTRACTION TESTS
# ===========================

class TestDataExtractor:
    """Test data extraction functionality"""
    
    def test_extractor_initialization(self, sample_config, temp_data_dir):
        """Test that extractor initializes correctly"""
        # Save config to temp directory
        config_path = temp_data_dir / 'config.yaml'
        with open(config_path, 'w') as f:
            yaml.dump(sample_config, f)
        
        extractor = DataExtractor(str(config_path))
        assert extractor.config == sample_config
        assert extractor.raw_path.exists()
    
    def test_file_hash_calculation(self, temp_data_dir):
        """Test file hash calculation"""
        # Create a test file
        test_file = temp_data_dir / 'test.txt'
        test_file.write_text('Test content')
        
        # Create minimal extractor
        extractor = DataExtractor.__new__(DataExtractor)
        hash_value = extractor._calculate_file_hash(test_file)
        
        assert len(hash_value) == 32  # MD5 hash length
        assert isinstance(hash_value, str)
    
    def test_validate_extraction(self, sample_trl_data):
        """Test extraction validation"""
        extractor = DataExtractor.__new__(DataExtractor)
        extractor.config = {'validation': {}}
        
        # Should not raise exception
        extractor._validate_extraction(sample_trl_data, 'Test Data', ['Technology'])
        
        # Should raise exception for missing columns
        with pytest.raises(ValueError):
            extractor._validate_extraction(sample_trl_data, 'Test Data', ['NonExistentColumn'])


# ===========================
# TRANSFORMATION TESTS
# ===========================

class TestDataTransformer:
    """Test data transformation functionality"""
    
    def test_transformer_initialization(self, sample_config, temp_data_dir):
        """Test that transformer initializes correctly"""
        config_path = temp_data_dir / 'config.yaml'
        with open(config_path, 'w') as f:
            yaml.dump(sample_config, f)
        
        transformer = DataTransformer(str(config_path))
        assert transformer.config == sample_config
        assert len(transformer.dimensions) == 0
        assert len(transformer.facts) == 0
    
    def test_create_date_dimension(self, sample_config):
        """Test date dimension creation"""
        transformer = DataTransformer.__new__(DataTransformer)
        transformer.config = sample_config
        
        dim_date = transformer._create_date_dimension()
        
        # Check structure
        assert 'date_sk' in dim_date.columns
        assert 'date' in dim_date.columns
        assert 'year' in dim_date.columns
        assert 'month' in dim_date.columns
        assert 'quarter' in dim_date.columns
        
        # Check date range
        assert dim_date['date'].min() >= pd.to_datetime('2020-01-01')
        assert dim_date['date'].max() <= pd.to_datetime('2024-12-31')
        
        # Check surrogate keys are unique
        assert dim_date['date_sk'].is_unique
    
    def test_transform_technology_dimension(self, sample_trl_data):
        """Test technology dimension transformation"""
        transformer = DataTransformer.__new__(DataTransformer)
        
        dim_tech = transformer._transform_technology_dimension(sample_trl_data)
        
        # Check structure
        assert 'technology_sk' in dim_tech.columns
        assert 'technology' in dim_tech.columns
        assert 'current_trl' in dim_tech.columns
        
        # Check data quality
        assert len(dim_tech) == len(sample_trl_data)
        assert dim_tech['technology_sk'].is_unique
        assert dim_tech['technology'].notna().all()
    
    def test_transform_company_dimension(self, sample_companies_data):
        """Test company dimension transformation"""
        transformer = DataTransformer.__new__(DataTransformer)
        
        dim_company = transformer._transform_company_dimension(sample_companies_data)
        
        # Check structure
        assert 'company_sk' in dim_company.columns
        assert 'entity_name' in dim_company.columns
        assert 'ticker' in dim_company.columns
        
        # Check transformations
        assert dim_company['entity_name'].str.isupper().all()
        assert dim_company['company_sk'].is_unique
    
    def test_get_trl_stage(self):
        """Test TRL stage mapping"""
        transformer = DataTransformer.__new__(DataTransformer)
        
        assert transformer._get_trl_stage(1) == 'Research'
        assert transformer._get_trl_stage(5) == 'Development'
        assert transformer._get_trl_stage(7) == 'Demonstration'
        assert transformer._get_trl_stage(9) == 'Deployment'
        assert transformer._get_trl_stage(np.nan) == 'Unknown'
    
    def test_calculate_scores(self, sample_companies_data):
        """Test score calculation methods"""
        transformer = DataTransformer.__new__(DataTransformer)
        
        # Create sample financial data
        financials = pd.DataFrame({
            'rd_expenses': [45000000],
            'revenues': [150000000],
            'cash_and_equivalents': [100000000],
            'liabilities': [200000000],
            'fiscal_year': [2023]
        })
        
        # Test R&D score
        rd_score = transformer._calculate_rd_score(financials)
        assert 0 <= rd_score <= 100
        
        # Test cash score
        cash_score = transformer._calculate_cash_score(financials)
        assert 0 <= cash_score <= 100
        
        # Test growth score
        growth_score = transformer._calculate_growth_score(financials)
        assert 0 <= growth_score <= 100
    
    def test_get_recommendation(self):
        """Test investment recommendation logic"""
        transformer = DataTransformer.__new__(DataTransformer)
        
        assert transformer._get_recommendation(85) == 'BUY'
        assert transformer._get_recommendation(70) == 'HOLD'
        assert transformer._get_recommendation(50) == 'WATCH'
        assert transformer._get_recommendation(30) == 'SELL'


# ===========================
# LOADING TESTS
# ===========================

class TestDataLoader:
    """Test data loading functionality"""
    
    @pytest.fixture
    def sqlite_engine(self):
        """Create in-memory SQLite engine for testing"""
        from sqlalchemy import create_engine
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        return engine
    
    def test_loader_initialization(self, sample_config, temp_data_dir):
        """Test that loader initializes correctly"""
        # Modify config for SQLite
        sample_config['database'] = {
            'dialect': 'sqlite',
            'database': ':memory:'
        }
        
        config_path = temp_data_dir / 'config.yaml'
        with open(config_path, 'w') as f:
            yaml.dump(sample_config, f)
        
        # Mock environment variables
        os.environ['DB_USER'] = ''
        os.environ['DB_PASSWORD'] = ''
        
        loader = DataLoader(str(config_path))
        assert loader.config == sample_config
        assert loader.engine is not None
    
    def test_prepare_dataframe(self, sqlite_engine):
        """Test DataFrame preparation for loading"""
        loader = DataLoader.__new__(DataLoader)
        loader.engine = sqlite_engine
        
        # Create test DataFrame with various data types
        df = pd.DataFrame({
            'technology_sk': [1, 2, 3],
            'technology': ['Tech1', 'Tech2', 'Tech3'],
            'current_trl': [5, 6, 7],
            'confidence_percent': [85.5, np.nan, 92.0],
            'created_at': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        })
        
        prepared_df = loader._prepare_dataframe(df, DimTechnology)
        
        # Check that NaN is replaced with None
        assert prepared_df['confidence_percent'].iloc[1] is None
        
        # Check that datetime is timezone-naive
        assert prepared_df['created_at'].dt.tz is None


# ===========================
# VALIDATION TESTS
# ===========================

class TestDataValidator:
    """Test data validation functionality"""
    
    def test_validator_initialization(self, sample_config, temp_data_dir):
        """Test that validator initializes correctly"""
        config_path = temp_data_dir / 'config.yaml'
        with open(config_path, 'w') as f:
            yaml.dump(sample_config, f)
        
        validator = DataValidator(str(config_path))
        assert validator.config == sample_config
        assert len(validator.validation_results) == 0
    
    def test_validate_source_files(self, sample_config, temp_data_dir):
        """Test source file validation"""
        # Setup
        config_path = temp_data_dir / 'config.yaml'
        sample_config['paths']['raw_data'] = str(temp_data_dir / 'raw')
        with open(config_path, 'w') as f:
            yaml.dump(sample_config, f)
        
        # Create test files
        raw_dir = temp_data_dir / 'raw'
        (raw_dir / 'test_trl.xlsx').write_text('test')
        (raw_dir / 'test_pubmed.xlsx').write_text('test')
        
        validator = DataValidator(str(config_path))
        validator._validate_source_files()
        
        # Check results
        assert len(validator.validation_results) >= 2
        
        # Check for expected validations
        trl_check = next(
            (r for r in validator.validation_results 
             if 'test_trl.xlsx' in r['check']), 
            None
        )
        assert trl_check is not None
        assert trl_check['status'] == 'PASS'


# ===========================
# INTEGRATION TESTS
# ===========================

class TestETLIntegration:
    """Test complete ETL pipeline integration"""
    
    def test_extract_transform_integration(self, sample_trl_data, sample_pubmed_data, 
                                         sample_companies_data, temp_data_dir):
        """Test extraction and transformation together"""
        # Create transformer
        transformer = DataTransformer.__new__(DataTransformer)
        transformer.config = {
            'validation': {
                'date_range': {
                    'min': '2020-01-01',
                    'max': '2024-12-31'
                }
            }
        }
        transformer.dimensions = {}
        transformer.facts = {}
        
        # Simulate extracted data
        extracted_data = {
            'trl': sample_trl_data,
            'pubmed': sample_pubmed_data,
            'companies': sample_companies_data
        }
        
        # Transform all data
        dimensions, facts = transformer.transform_all(extracted_data)
        
        # Verify dimensions
        assert 'dim_technology' in dimensions
        assert 'dim_publication' in dimensions
        assert 'dim_company' in dimensions
        assert 'dim_date' in dimensions
        
        # Verify facts
        assert 'fact_technology' in facts
        assert 'fact_financial' in facts
        assert 'fact_scoring' in facts
        
        # Check relationships
        assert len(dimensions['dim_technology']) == len(sample_trl_data)
        assert len(dimensions['dim_company']) == len(sample_companies_data['entityName'].unique())


# ===========================
# UTILITY TESTS
# ===========================

def test_imports():
    """Test that all modules can be imported"""
    try:
        from scripts.extract import DataExtractor, extract_data
        from scripts.transform import DataTransformer, transform_data
        from scripts.load import DataLoader, load_data
        from validation.data_validator import DataValidator, validate_data
        from scripts.bi_connector import BIConnector, export_to_bi
        assert True
    except ImportError as e:
        pytest.fail(f"Import failed: {e}")


def test_model_relationships():
    """Test SQLAlchemy model relationships"""
    # Test that foreign key relationships are properly defined
    from models.models import FactTechnology, FactFinancial, FactScoring
    
    # Check FactTechnology relationships
    assert hasattr(FactTechnology, 'technology')
    assert hasattr(FactTechnology, 'publication')
    assert hasattr(FactTechnology, 'date')
    
    # Check FactFinancial relationships
    assert hasattr(FactFinancial, 'company')
    assert hasattr(FactFinancial, 'date')
    
    # Check FactScoring relationships
    assert hasattr(FactScoring, 'company')
    assert hasattr(FactScoring, 'date')


# ===========================
# PERFORMANCE TESTS
# ===========================

@pytest.mark.slow
class TestPerformance:
    """Performance tests for large datasets"""
    
    def test_large_dataset_transformation(self):
        """Test transformation with large dataset"""
        # Create large dataset
        n_records = 10000
        large_df = pd.DataFrame({
            'Technology': [f'Tech_{i}' for i in range(n_records)],
            'Current TRL': np.random.randint(1, 10, n_records),
            'Predicted TRL': np.random.randint(1, 10, n_records)
        })
        
        transformer = DataTransformer.__new__(DataTransformer)
        
        # Time the transformation
        import time
        start_time = time.time()
        dim_tech = transformer._transform_technology_dimension(large_df)
        duration = time.time() - start_time
        
        # Should complete in reasonable time
        assert duration < 5.0  # 5 seconds max
        assert len(dim_tech) == n_records


if __name__ == "__main__":
    pytest.main([__file__, "-v"])