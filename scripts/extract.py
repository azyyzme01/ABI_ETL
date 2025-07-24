"""
scripts/extract.py - Extract data from source files
Reads raw data files and performs initial validation
"""

import pandas as pd
import numpy as np
import yaml
import logging
from pathlib import Path
from datetime import datetime
import hashlib
import shutil
from typing import Dict, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles extraction of data from various source files"""
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize extractor with configuration"""
        self.config = self._load_config(config_path)
        self.raw_path = Path(self.config['paths']['raw_data'])
        self.archive_path = Path(self.config['paths']['archive'])
        self.extracted_data = {}
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """Extract all configured data sources"""
        logger.info("Starting data extraction process...")
        
        # Extract TRL predictions
        self.extracted_data['trl'] = self._extract_trl_predictions()
        
        # Extract PubMed articles
        self.extracted_data['pubmed'] = self._extract_pubmed_articles()
        
        # Extract biotech companies data
        self.extracted_data['companies'] = self._extract_biotech_companies()
        
        # Archive raw files
        self._archive_raw_files()
        
        logger.info("Data extraction completed successfully")
        return self.extracted_data
    
    def _extract_trl_predictions(self) -> pd.DataFrame:
        """Extract TRL predictions from Excel file"""
        file_path = self.raw_path / self.config['files']['trl_predictions']
        logger.info(f"Extracting TRL predictions from {file_path}")
        
        try:
            # Read Excel file
            df = pd.read_excel(file_path, engine='openpyxl')
            
            # Log basic info
            logger.info(f"Loaded {len(df)} TRL records with columns: {list(df.columns)}")
            
            # Basic validation
            self._validate_extraction(df, 'TRL Predictions', required_cols=['Technology'])
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract TRL predictions: {str(e)}")
            raise
    
    def _extract_pubmed_articles(self) -> pd.DataFrame:
   
         file_path = self.raw_path / self.config['files']['pubmed_articles']
         logger.info(f"Extracting PubMed articles from {file_path}")
         try:
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            if len(excel_file.sheet_names) > 1:
              logger.info(f"Found {len(excel_file.sheet_names)} sheets in PubMed file")
              dfs = []
              for sheet in excel_file.sheet_names:
                df_sheet = pd.read_excel(excel_file, sheet_name=sheet)
                dfs.append(df_sheet)
                df = pd.concat(dfs, ignore_index=True)
            else:
                df = pd.read_excel(file_path, engine='openpyxl')
        
            logger.info(f"Loaded {len(df)} PubMed articles with columns: {list(df.columns)}")
        
        
            for col in ['Year', 'Month', 'TRL']:
              if col in df.columns:
                invalid_rows = df[df[col].apply(lambda x: pd.isna(x) or isinstance(x, str) or (isinstance(x, float) and not x.is_integer()))]
                if not invalid_rows.empty:
                    logger.warning(f"Found {len(invalid_rows)} rows with invalid {col} values:")
                    logger.warning(invalid_rows[[col]].head().to_string())
        
            self._validate_extraction(df, 'PubMed Articles')
            return df
    
         except Exception as e:
           logger.error(f"Failed to extract PubMed articles: {str(e)}")
           raise
    
    def _extract_biotech_companies(self) -> pd.DataFrame:
        """Extract biotech companies financial data from CSV with proper encoding detection"""
        file_path = self.raw_path / self.config['files']['biotech_companies']
        logger.info(f"Extracting biotech companies from {file_path}")
        
        try:
            # Try multiple encodings in order of preference
            encodings_to_try = [
                'utf-8-sig',        # UTF-8 with BOM
                'utf-8',            # Standard UTF-8
                'latin1',           # ISO-8859-1 (handles most Western European chars)
                'cp1252',           # Windows-1252 (Windows default)
                'iso-8859-1',       # Latin-1
                'utf-16',           # UTF-16 with BOM detection
            ]
            
            df = None
            encoding_used = None
            
            for encoding in encodings_to_try:
                try:
                    logger.info(f"Trying encoding: {encoding}")
                    df = pd.read_csv(file_path, encoding=encoding)
                    encoding_used = encoding
                    logger.info(f"✅ Successfully loaded with encoding: {encoding}")
                    break
                except UnicodeDecodeError as e:
                    logger.warning(f"❌ Failed with {encoding}: {str(e)}")
                    continue
                except Exception as e:
                    logger.warning(f"❌ Unexpected error with {encoding}: {str(e)}")
                    continue
            
            if df is None:
                # Last resort: try with error handling
                logger.warning("All encodings failed, trying with error handling...")
                try:
                    df = pd.read_csv(file_path, encoding='utf-8', errors='replace')
                    encoding_used = 'utf-8 (with error replacement)'
                    logger.info("✅ Loaded with error replacement")
                except Exception as e:
                    logger.error(f"Complete failure to read file: {str(e)}")
                    raise
            
            logger.info(f"File loaded successfully using encoding: {encoding_used}")
            logger.info(f"Loaded {len(df)} company records")
            logger.info(f"Actual columns in dataset: {list(df.columns)}")
            
            # Clean up any encoding artifacts if we used error replacement
            if 'error replacement' in str(encoding_used):
                # Replace common replacement characters
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str).str.replace('�', '', regex=False)
            
            # Log sample of first few rows to understand the data
            logger.info("Sample data (first 3 rows):")
            try:
                logger.info(df.head(3).to_string())
            except Exception as e:
                logger.warning(f"Could not display sample data: {str(e)}")
            
            # Check for data availability in key columns
            expected_columns = [
                'cik', 'entityName', 'ticker', 'fiscalYear', 'period',
                'Revenues', 'NetIncomeLoss', 'OperatingIncomeLoss', 'Assets', 
                'Liabilities', 'StockholdersEquity', 'CashAndCashEquivalentsAtCarryingValue',
                'ResearchAndDevelopmentExpense', 'OperatingCashFlow', 'Goodwill',
                'IntangibleAssetsNetExcludingGoodwill', 'PatentsIssued',
                'dataCompleteness', 'dataFreshness'
            ]
            
            logger.info("Column availability check:")
            available_columns = []
            missing_columns = []
            
            for col in expected_columns:
                if col in df.columns:
                    available_columns.append(col)
                    non_null_count = df[col].notna().sum()
                    non_zero_count = (df[col].fillna(0) != 0).sum() if pd.api.types.is_numeric_dtype(df[col]) else non_null_count
                    logger.info(f"  ✅ {col}: {non_null_count}/{len(df)} non-null, {non_zero_count} non-zero values")
                else:
                    missing_columns.append(col)
                    logger.warning(f"  ❌ {col}: COLUMN NOT FOUND!")
            
            logger.info(f"Available columns: {len(available_columns)}/{len(expected_columns)}")
            
            if missing_columns:
                logger.warning(f"Missing expected columns: {missing_columns}")
                logger.info(f"Actual columns in file: {list(df.columns)}")
            
            # Clean PatentsIssued column properly if it exists
            if 'PatentsIssued' in df.columns:
                logger.info("Cleaning PatentsIssued column...")
                original_dtype = df['PatentsIssued'].dtype
                df['PatentsIssued'] = pd.to_numeric(df['PatentsIssued'], errors='coerce').fillna(0).astype(int)
                logger.info(f"PatentsIssued: converted from {original_dtype} to int64")
            
            # Clean fiscal year if it exists
            if 'fiscalYear' in df.columns:
                logger.info("Cleaning fiscalYear column...")
                df['fiscalYear'] = pd.to_numeric(df['fiscalYear'], errors='coerce')
                invalid_years = df['fiscalYear'].isna().sum()
                if invalid_years > 0:
                    logger.warning(f"Found {invalid_years} invalid fiscal years")
            
            # Basic validation
            self._validate_extraction(df, 'Biotech Companies', 
                                required_cols=['entityName'])  # Minimum required
        
            return df
        
        except Exception as e:
            logger.error(f"Failed to extract biotech companies: {str(e)}")
            raise
    
    def _validate_extraction(self, df: pd.DataFrame, source_name: str, 
                           required_cols: list = None) -> None:
        """Perform basic validation on extracted data"""
        
        # Check if DataFrame is empty
        if df.empty:
            raise ValueError(f"{source_name}: Extracted DataFrame is empty")
        
        # Check for required columns
        if required_cols:
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(f"{source_name}: Missing required columns: {missing_cols}")
        
        # Log statistics
        logger.info(f"{source_name} Statistics:")
        logger.info(f"  - Rows: {len(df)}")
        logger.info(f"  - Columns: {len(df.columns)}")
        logger.info(f"  - Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        logger.info(f"  - Null values: {df.isnull().sum().sum()}")
        
        # Check for complete empty columns
        empty_cols = df.columns[df.isnull().all()].tolist()
        if empty_cols:
            logger.warning(f"{source_name}: Found completely empty columns: {empty_cols}")
    
    def _archive_raw_files(self) -> None:
        """Archive raw files with timestamp"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_dir = self.archive_path / timestamp
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Archiving raw files to {archive_dir}")
        
        # Copy each configured file
        for file_key, filename in self.config['files'].items():
            source = self.raw_path / filename
            if source.exists():
                destination = archive_dir / filename
                shutil.copy2(source, destination)
                
                # Calculate and store file hash for integrity
                file_hash = self._calculate_file_hash(source)
                hash_file = archive_dir / f"{filename}.md5"
                hash_file.write_text(file_hash)
                
                logger.info(f"Archived {filename} (MD5: {file_hash})")
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of a file"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def get_extraction_summary(self) -> Dict[str, dict]:
        """Get summary statistics of extracted data"""
        summary = {}
        
        for key, df in self.extracted_data.items():
            summary[key] = {
                'rows': len(df),
                'columns': len(df.columns),
                'memory_mb': df.memory_usage(deep=True).sum() / 1024**2,
                'null_count': df.isnull().sum().sum(),
                'null_percentage': (df.isnull().sum().sum() / (df.shape[0] * df.shape[1]) * 100)
            }
        
        return summary
    
    def save_extracted_data(self, output_path: str = 'data/processed/extracted') -> None:
        """Save extracted data to parquet files for efficient processing"""
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for key, df in self.extracted_data.items():
            file_path = output_dir / f"{key}_extracted.parquet"
            df.to_parquet(file_path, engine='pyarrow', compression='snappy')
            logger.info(f"Saved {key} data to {file_path}")


# Standalone extraction function
def extract_data(config_path: str = 'config/config.yaml') -> Dict[str, pd.DataFrame]:
    """
    Main extraction function to be called from ETL pipeline
    
    Returns:
        Dictionary containing extracted DataFrames
    """
    extractor = DataExtractor(config_path)
    data = extractor.extract_all()
    
    # Log summary
    summary = extractor.get_extraction_summary()
    logger.info("Extraction Summary:")
    for source, stats in summary.items():
        logger.info(f"  {source}: {stats['rows']} rows, "
                   f"{stats['columns']} columns, "
                   f"{stats['null_percentage']:.1f}% nulls")
    
    # Save for next stage
    extractor.save_extracted_data()
    
    return data


if __name__ == "__main__":
    # Test extraction
    print("🔍 Testing data extraction...")
    
    try:
        data = extract_data()
        print("\n✅ Extraction successful!")
        
        # Show sample data
        for source, df in data.items():
            print(f"\n{source.upper()} - First 3 rows:")
            print(df.head(3))
            print(f"Shape: {df.shape}")
            
    except Exception as e:
        print(f"\n❌ Extraction failed: {str(e)}")
        raise