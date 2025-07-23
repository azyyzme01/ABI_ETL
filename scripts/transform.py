# scripts/transform.py
"""
Data transformation module for ABI ETL Pipeline
FIXED VERSION: Proper deduplication, no surrogate key generation, correct business logic
"""

import pandas as pd
import numpy as np
import yaml
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles transformation of raw data into star schema format"""
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize transformer with configuration"""
        self.config = self._load_config(config_path)
        self.processed_path = Path(self.config['paths']['processed_data'])
        self.dimensions = {}
        self.facts = {}
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def transform_all(self, extracted_data: Dict[str, pd.DataFrame]) -> Tuple[dict, dict]:
        """Transform all data into star schema format"""
        logger.info("Starting data transformation process...")
        
        # Create date dimension first (special case - includes date_sk)
        self.dimensions['dim_date'] = self._create_date_dimension()
        
        # Transform dimensions - NO SURROGATE KEYS GENERATED HERE
        self.dimensions['dim_technology'] = self._transform_technology_dimension(
            extracted_data['trl']
        )
        self.dimensions['dim_publication'] = self._transform_publication_dimension(
            extracted_data['pubmed']
        )
        self.dimensions['dim_company'] = self._transform_company_dimension(
            extracted_data['companies']
        )
        
        # Transform facts - using natural keys, not surrogate keys
        self.facts['fact_technology'] = self._transform_technology_facts(
            extracted_data['trl'], 
            extracted_data['pubmed']
        )
        self.facts['fact_financial'] = self._transform_financial_facts(
            extracted_data['companies']
        )
        self.facts['fact_scoring'] = self._transform_scoring_facts(
            extracted_data['companies']
        )
        
        logger.info("Data transformation completed successfully")
        return self.dimensions, self.facts
    
    def _create_date_dimension(self) -> pd.DataFrame:
        """Create a comprehensive date dimension table"""
        logger.info("Creating date dimension...")
        
        # Generate date range based on config
        start_date = pd.to_datetime(self.config['validation']['date_range']['min'])
        end_date = pd.to_datetime(self.config['validation']['date_range']['max'])
        
        # Create date range
        dates = pd.date_range(start=start_date, end=end_date, freq='D', inclusive='both')
        
        # Build dimension
        dim_date = pd.DataFrame({
            'date': dates,
            'date_sk': dates.strftime('%Y%m%d').astype(int),  # Special case - we generate this
            'year': dates.year,
            'month': dates.month,
            'day': dates.day,
            'quarter': dates.quarter,
            'month_name': dates.strftime('%B'),
            'day_name': dates.strftime('%A'),
            'day_of_week': dates.dayofweek,
            'week_of_year': dates.isocalendar().week,
            'is_weekend': dates.dayofweek.isin([5, 6]),
            'fiscal_year': dates.year,
            'fiscal_quarter': dates.quarter,
            'is_holiday': False
        })
        
        logger.info(f"Created date dimension with {len(dim_date)} dates")
        logger.info(f"Date range: {dim_date['date'].min()} to {dim_date['date'].max()}")
        
        return dim_date
    
    def _transform_technology_dimension(self, trl_df: pd.DataFrame) -> pd.DataFrame:
        """Transform technology data into dimension table"""
        logger.info("Transforming technology dimension...")
        
        dim_tech = trl_df.copy()
        
        # Standardize column names
        column_mapping = {
            'Technology': 'technology',
            'Current_TRL': 'current_trl',
            'Predicted_TRL': 'predicted_trl',
            'Current_Category': 'current_category',
            'Predicted_Category': 'predicted_category',
            'Confidence_%': 'confidence_percent',
            'Change_Category': 'change_category',
            'Annual_Change_Rate': 'annual_change_rate',
            'Model_Used': 'model_used'
        }
        dim_tech = dim_tech.rename(columns=column_mapping)
        
        # Clean technology names (natural key)
        dim_tech['technology'] = dim_tech['technology'].astype(str).str.strip().str.lower()
        
        # CRITICAL: Deduplicate by natural key (technology name)
        # Keep the first occurrence of each technology
        dim_tech = dim_tech.drop_duplicates(subset=['technology'], keep='first')
        
        # Fix data types
        dim_tech['current_trl'] = pd.to_numeric(dim_tech['current_trl'], errors='coerce').fillna(0).astype(int)
        dim_tech['predicted_trl'] = pd.to_numeric(dim_tech['predicted_trl'], errors='coerce').fillna(0)
        
        # FIXED: Calculate TRL change correctly
        dim_tech['trl_change'] = dim_tech['predicted_trl'] - dim_tech['current_trl']
        
        # Clean confidence percentage
        if 'confidence_percent' in dim_tech.columns:
            dim_tech['confidence_percent'] = pd.to_numeric(
                dim_tech['confidence_percent'].astype(str).str.replace('%', ''), 
                errors='coerce'
            )
        
        # Handle annual change rate
        if 'annual_change_rate' in dim_tech.columns:
            dim_tech['annual_change_rate'] = pd.to_numeric(dim_tech['annual_change_rate'], errors='coerce')
        
        # Add metadata
        dim_tech['created_at'] = datetime.utcnow()
        dim_tech['updated_at'] = datetime.utcnow()
        
        # Select final columns - NO SURROGATE KEY
        columns = ['technology', 'current_trl', 'predicted_trl', 
                  'trl_change', 'current_category', 'predicted_category', 
                  'change_category', 'confidence_percent', 'annual_change_rate',
                  'model_used', 'created_at', 'updated_at']
        
        dim_tech = dim_tech[[col for col in columns if col in dim_tech.columns]]
        
        logger.info(f"Created technology dimension with {len(dim_tech)} unique technologies")
        return dim_tech
    
    def _transform_publication_dimension(self, pubmed_df: pd.DataFrame) -> pd.DataFrame:
        """Transform publication data into dimension table"""
        logger.info("Transforming publication dimension...")
        
        dim_pub = pubmed_df.copy()
        
        # Standardize column names
        column_mapping = {
            'Titre': 'title',
            'Résumé': 'summary',
            'Auteurs': 'authors',
            'Lien': 'link',  # Natural key
            'Mots-clés': 'keywords',
            'Year': 'year',
            'Month': 'month',
            'Technologies': 'technologies',
            'Technology Category': 'tech_category',
            'TRL': 'trl',
            'TRL_Category': 'trl_category',
            'TRL_Stage': 'trl_stage',
            'Maturité': 'maturity',
            'Article Type': 'article_type',
            'Country': 'country',
            'Research Domain': 'research_domain'
        }
        dim_pub = dim_pub.rename(columns=column_mapping)
        
        # CRITICAL: Deduplicate by natural key (link)
        # Clean link field first
        dim_pub['link'] = dim_pub['link'].astype(str).str.strip()
        # Remove any rows with invalid links
        dim_pub = dim_pub[dim_pub['link'].notna() & (dim_pub['link'] != '') & (dim_pub['link'] != 'nan')]
        # Keep first occurrence of each link
        dim_pub = dim_pub.drop_duplicates(subset=['link'], keep='first')
        
        # Create publication date from Year and Month
        if 'year' in dim_pub.columns and 'month' in dim_pub.columns:
            # Handle missing months
            dim_pub['month'] = pd.to_numeric(dim_pub['month'], errors='coerce').fillna(1)
            dim_pub['year'] = pd.to_numeric(dim_pub['year'], errors='coerce')
            
            # Create date safely
            def safe_date_creation(row):
                try:
                    year = int(row['year']) if pd.notna(row['year']) else 2020
                    month = int(row['month']) if pd.notna(row['month']) else 1
                    month = max(1, min(12, month))  # Ensure valid month
                    return pd.Timestamp(year=year, month=month, day=1)
                except:
                    return pd.Timestamp('2020-01-01')
            
            dim_pub['publication_date'] = dim_pub.apply(safe_date_creation, axis=1)
        
        # Extract TRL information
        if 'trl' in dim_pub.columns:
            dim_pub['trl'] = pd.to_numeric(dim_pub['trl'], errors='coerce')
        
        # Handle other fields - keep original data as requested
        # Add placeholders for missing metrics (these would normally come from external sources)
        dim_pub['impact_factor'] = np.nan
        dim_pub['citation_count'] = np.nan
        dim_pub['peer_reviewed'] = True
        
        # Add metadata
        dim_pub['created_at'] = datetime.utcnow()
        dim_pub['updated_at'] = datetime.utcnow()
        
        # Select final columns - NO SURROGATE KEY
        columns = ['link', 'title', 'authors', 'summary', 'keywords', 'country', 
                  'trl', 'trl_stage', 'trl_category', 'maturity', 'year', 'month',
                  'technologies', 'tech_category', 'article_type',
                  'research_domain', 'impact_factor', 'citation_count', 'peer_reviewed', 
                  'publication_date', 'created_at', 'updated_at']
        
        dim_pub = dim_pub[[col for col in columns if col in dim_pub.columns]]
        
        logger.info(f"Created publication dimension with {len(dim_pub)} unique publications")
        return dim_pub
    
    def _transform_company_dimension(self, companies_df: pd.DataFrame) -> pd.DataFrame:
        """Transform company data into dimension table"""
        logger.info("Transforming company dimension...")
        
        dim_company = companies_df.copy()
        
        # Standardize column names
        column_mapping = {
            'entityName': 'entity_name',
            'ticker': 'ticker',
            'stateOfIncorporation': 'state_of_incorporation',
            'sic': 'sic_code',
            'sicDescription': 'sic_description',
            'industryCategory': 'industry_category',
            'cik': 'cik'
        }
        dim_company = dim_company.rename(columns=column_mapping)
        
        # Clean company names (natural key)
        dim_company['entity_name'] = dim_company['entity_name'].str.strip().str.upper()
        
        # CRITICAL: Deduplicate by natural key (entity_name)
        # Keep the first occurrence of each company
        dim_company = dim_company.drop_duplicates(subset=['entity_name'], keep='first')
        
        # Clean other fields
        if 'ticker' in dim_company.columns:
            dim_company['ticker'] = dim_company['ticker'].astype(str).str.strip().str.upper()
            dim_company['ticker'] = dim_company['ticker'].replace(['NAN', 'NONE', ''], np.nan)
        
        # Ensure numeric fields
        if 'sic_code' in dim_company.columns:
            dim_company['sic_code'] = pd.to_numeric(dim_company['sic_code'], errors='coerce')
        
        # Add metadata
        dim_company['created_at'] = datetime.utcnow()
        dim_company['updated_at'] = datetime.utcnow()
        
        # Select final columns - NO SURROGATE KEY
        columns = ['entity_name', 'cik', 'ticker', 'state_of_incorporation', 
                  'sic_code', 'sic_description', 'industry_category', 
                  'created_at', 'updated_at']
        
        dim_company = dim_company[[col for col in columns if col in dim_company.columns]]
        
        logger.info(f"Created company dimension with {len(dim_company)} unique companies")
        return dim_company
    
    def _transform_technology_facts(self, trl_df: pd.DataFrame, 
                                  pubmed_df: pd.DataFrame) -> pd.DataFrame:
        """Create technology fact table"""
        logger.info("Creating technology fact table...")
        
        # Prepare data
        trl_clean = trl_df.copy()
        pubmed_clean = pubmed_df.copy()
        
        # Standardize technology names in TRL data
        trl_clean['Technology'] = trl_clean['Technology'].astype(str).str.strip().str.lower()
        
        # Extract technologies from PubMed data
        pubmed_clean['Technologies'] = pubmed_clean['Technologies'].astype(str).str.strip().str.lower()
        pubmed_clean['Lien'] = pubmed_clean['Lien'].astype(str).str.strip()
        
        fact_tech_list = []
        
        # Create fact records by matching technologies in publications
        for _, pub_row in pubmed_clean.iterrows():
            if pd.isna(pub_row['Technologies']) or pub_row['Technologies'] == 'nan':
                continue
                
            # Get publication date for date_sk
            pub_year = pub_row.get('Year', 2020)
            pub_month = pub_row.get('Month', 1)
            if pd.isna(pub_year):
                pub_year = 2020
            if pd.isna(pub_month):
                pub_month = 1
                
            # Create date_sk
            try:
                date_sk = int(f"{int(pub_year)}{int(pub_month):02d}01")
            except:
                date_sk = 20200101
            
            # Check if this technology exists in our technology dimension
            tech_name = pub_row['Technologies']
            if tech_name in trl_clean['Technology'].values:
                # Get TRL from the publication
                trl_value = pub_row.get('TRL', np.nan)
                if pd.notna(trl_value):
                    trl_value = int(trl_value)
                else:
                    trl_value = None
                
                fact_record = {
                    'technology_natural_key': tech_name,
                    'publication_natural_key': pub_row['Lien'],
                    'date_sk': date_sk,
                    'trl': trl_value,
                    # FIXED: Set metrics to NULL instead of random values
                    'maturity_score': None,
                    'innovation_score': None,
                    'market_score': None,
                    'risk_assessment': None,
                    'created_at': datetime.utcnow()
                }
                
                fact_tech_list.append(fact_record)
        
        fact_tech = pd.DataFrame(fact_tech_list)
        
        # CRITICAL: Respect granularity - remove duplicates
        if not fact_tech.empty:
            fact_tech = fact_tech.drop_duplicates(
                subset=['technology_natural_key', 'publication_natural_key'],
                keep='first'
            )
        
        logger.info(f"Created technology fact table with {len(fact_tech)} records")
        return fact_tech
    
    def _transform_financial_facts(self, companies_df: pd.DataFrame) -> pd.DataFrame:
        """Create financial fact table"""
        logger.info("Creating financial fact table...")
        
        fact_fin = companies_df.copy()
        
        # Clean company names
        fact_fin['entityName'] = fact_fin['entityName'].str.strip().str.upper()
        
        # Create date_sk from fiscal year and period
        quarter_to_month = {'Q1': '01', 'Q2': '04', 'Q3': '07', 'Q4': '10', 'FY': '12'}
        
        def create_date_sk(row):
            try:
                year = int(row['fiscalYear'])
                period = str(row['period']) if pd.notna(row['period']) else 'FY'
                month = quarter_to_month.get(period, '12')
                return int(f"{year}{month}01")
            except:
                return 20200101
        
        fact_fin['date_sk'] = fact_fin.apply(create_date_sk, axis=1)
        
        # Rename columns
        column_mapping = {
            'entityName': 'company_natural_key',
            'Revenues': 'revenues',
            'NetIncomeLoss': 'net_income',
            'OperatingIncomeLoss': 'operating_income',
            'ResearchAndDevelopmentExpense': 'rd_expenses',
            'Assets': 'assets',
            'Liabilities': 'liabilities',
            'StockholdersEquity': 'stockholders_equity',
            'CashAndCashEquivalentsAtCarryingValue': 'cash_and_equivalents',
            'OperatingCashFlow': 'operating_cash_flow',
            'Goodwill': 'goodwill',
            'IntangibleAssetsNetExcludingGoodwill': 'intangible_assets',
            'PatentsIssued': 'patents_issued',
            'dataCompleteness': 'data_completeness',
            'dataFreshness': 'data_freshness',
            'fiscalYear': 'fiscal_year',
            'period': 'fiscal_period'
        }
        fact_fin = fact_fin.rename(columns=column_mapping)
        
        # Convert numeric columns
        numeric_columns = ['revenues', 'net_income', 'operating_income', 'rd_expenses',
                          'assets', 'liabilities', 'stockholders_equity', 
                          'cash_and_equivalents', 'operating_cash_flow', 
                          'goodwill', 'intangible_assets', 'patents_issued']
        
        for col in numeric_columns:
            if col in fact_fin.columns:
                fact_fin[col] = pd.to_numeric(fact_fin[col], errors='coerce')
        
        # Add metadata
        fact_fin['created_at'] = datetime.utcnow()
        fact_fin['updated_at'] = datetime.utcnow()
        
        # Select columns
        columns = ['company_natural_key', 'date_sk', 'revenues', 'net_income', 
                  'operating_income', 'rd_expenses', 'assets', 'liabilities',
                  'stockholders_equity', 'cash_and_equivalents', 
                  'operating_cash_flow', 'goodwill', 'intangible_assets',
                  'patents_issued', 'data_completeness', 'data_freshness',
                  'fiscal_year', 'fiscal_period', 'created_at', 'updated_at']
        
        fact_fin = fact_fin[[col for col in columns if col in fact_fin.columns]]
        
        # CRITICAL: Respect granularity - remove duplicates
        fact_fin = fact_fin.drop_duplicates(
            subset=['company_natural_key', 'fiscal_year', 'fiscal_period'],
            keep='last'  # Keep most recent data
        )
        
        logger.info(f"Created financial fact table with {len(fact_fin)} records")
        return fact_fin
    
    def _transform_scoring_facts(self, companies_df: pd.DataFrame) -> pd.DataFrame:
        """Create scoring fact table"""
        logger.info("Creating scoring fact table...")
        
        # Get unique companies
        unique_companies = companies_df[['entityName', 'fiscalYear']].copy()
        unique_companies['entityName'] = unique_companies['entityName'].str.strip().str.upper()
        
        # Group by company to get latest fiscal year
        latest_year_by_company = unique_companies.groupby('entityName')['fiscalYear'].max().reset_index()
        
        fact_scoring_list = []
        
        # Create scoring records for each company's latest year
        for _, company in latest_year_by_company.iterrows():
            company_name = company['entityName']
            latest_year = company['fiscalYear']
            
            # Get company's financial data
            company_financials = companies_df[
                (companies_df['entityName'].str.strip().str.upper() == company_name) &
                (companies_df['fiscalYear'] == latest_year)
            ]
            
            if not company_financials.empty:
                # Calculate scores based on financial metrics
                rd_score = self._calculate_rd_score(company_financials)
                cash_score = self._calculate_cash_score(company_financials)
                growth_score = self._calculate_growth_score(company_financials)
                patent_score = self._calculate_patent_score(company_financials)
                
                # Use end of fiscal year as date
                date_sk = int(f"{int(latest_year)}1231")
                
                scoring_record = {
                    'company_natural_key': company_name,
                    'date_sk': date_sk,
                    'rd_score': rd_score if rd_score is not None else 50.0,  # Default to 50 if NULL
                    'cash_score': cash_score if cash_score is not None else 50.0,
                    'market_score': 50.0,  # Default market score
                    'patent_score': patent_score if patent_score is not None else 0.0,  # Default to 0 if no patents
                    'growth_score': growth_score if growth_score is not None else 50.0,
                    'total_score': None,  # Will calculate below
                    'scoring_version': '1.0',
                    'scoring_date': datetime.utcnow(),
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                }
                
                # Calculate total score from all scores (now none should be None)
                score_columns = ['rd_score', 'cash_score', 'market_score', 'patent_score', 'growth_score']
                scores = [scoring_record[col] for col in score_columns]
                
                # Calculate average of all scores
                avg_score = np.mean(scores)
                # Ensure total score is between 0 and 100
                scoring_record['total_score'] = max(0, min(100, avg_score))
                scoring_record['investment_recommendation'] = self._get_recommendation(scoring_record['total_score'])
                scoring_record['confidence_level'] = 1.0  # All scores available
                
                fact_scoring_list.append(scoring_record)
        
        fact_scoring = pd.DataFrame(fact_scoring_list)
        
        # CRITICAL: Respect granularity - one score per company per date
        if not fact_scoring.empty:
            fact_scoring = fact_scoring.drop_duplicates(
                subset=['company_natural_key', 'date_sk'],
                keep='last'
            )
            
            # Final validation: ensure all scores are within 0-100 or NULL
            score_columns = ['rd_score', 'cash_score', 'market_score', 'patent_score', 'growth_score', 'total_score']
            for col in score_columns:
                if col in fact_scoring.columns:
                    # Replace any out-of-range values with NULL
                    mask = fact_scoring[col].notna()
                    out_of_range = mask & ((fact_scoring[col] < 0) | (fact_scoring[col] > 100))
                    if out_of_range.any():
                        logger.warning(f"Found {out_of_range.sum()} out-of-range values in {col}, setting to NULL")
                        fact_scoring.loc[out_of_range, col] = None
        
        logger.info(f"Created scoring fact table with {len(fact_scoring)} records")
        return fact_scoring
    
    def _calculate_rd_score(self, financials: pd.DataFrame) -> float:
        """Calculate R&D score based on R&D intensity"""
        rd_expense = financials['ResearchAndDevelopmentExpense'].sum()
        revenue = financials['Revenues'].sum()
        
        if pd.notna(rd_expense) and pd.notna(revenue) and revenue > 0:
            rd_intensity = rd_expense / revenue
            # Score: 0-100 based on R&D intensity (capped at 20% = 100 score)
            # Ensure score is between 0 and 100
            score = min(rd_intensity * 500, 100)
            return max(0, score)  # Ensure non-negative
        return None
    
    def _calculate_cash_score(self, financials: pd.DataFrame) -> float:
        """Calculate cash score based on cash ratio"""
        # Get the latest values safely
        if financials.empty:
            return None
            
        cash = financials['CashAndCashEquivalentsAtCarryingValue'].iloc[-1]
        liabilities = financials['Liabilities'].iloc[-1]
        
        if pd.notna(cash) and pd.notna(liabilities) and liabilities > 0:
            cash_ratio = cash / liabilities
            # Score: 0-100 based on cash ratio (1.0 = 100 score)
            # Ensure score is between 0 and 100
            score = min(cash_ratio * 100, 100)
            return max(0, score)  # Ensure non-negative
        return None
    
    def _calculate_patent_score(self, financials: pd.DataFrame) -> float:
        """Calculate patent score"""
        if financials.empty:
            return None
            
        patents = financials['PatentsIssued'].iloc[-1]
        
        if pd.notna(patents) and patents > 0:
            # Simple scoring: more patents = higher score (capped at 50 patents = 100 score)
            # Ensure score is between 0 and 100
            score = min(patents * 2, 100)
            return max(0, score)  # Ensure non-negative
        return None
    
    def _calculate_growth_score(self, financials: pd.DataFrame) -> float:
        """Calculate growth score based on revenue trend"""
        if len(financials) < 2:
            return None
            
        revenues = financials.sort_values('fiscalYear')['Revenues'].values
        
        # Remove NaN values
        revenues = [r for r in revenues if pd.notna(r) and r >= 0]
        
        if len(revenues) >= 2 and revenues[0] > 0:
            growth_rate = (revenues[-1] - revenues[0]) / revenues[0]
            # Score: 50 base + growth rate (20% growth = 70 score)
            # Cap negative growth at -100% to keep score >= 0
            # Cap positive growth to keep score <= 100
            score = 50 + (growth_rate * 100)
            # Ensure score is strictly between 0 and 100
            return max(0, min(100, score))
        return None
    
    def _get_recommendation(self, total_score: float) -> str:
        """Get investment recommendation based on total score"""
        if total_score >= 80:
            return 'BUY'
        elif total_score >= 60:
            return 'HOLD'
        elif total_score >= 40:
            return 'WATCH'
        else:
            return 'SELL'
    
    def save_transformed_data(self) -> None:
        """Save all transformed data to parquet files"""
        self.processed_path.mkdir(parents=True, exist_ok=True)
        
        # Save dimensions
        for name, df in self.dimensions.items():
            file_path = self.processed_path / f"{name}.parquet"
            df.to_parquet(file_path, engine='pyarrow', compression='snappy')
            logger.info(f"Saved {name} to {file_path}")
        
        # Save facts
        for name, df in self.facts.items():
            file_path = self.processed_path / f"{name}.parquet"
            df.to_parquet(file_path, engine='pyarrow', compression='snappy')
            logger.info(f"Saved {name} to {file_path}")


# Standalone transformation function
def transform_data(extracted_data: Dict[str, pd.DataFrame], 
                  config_path: str = 'config/config.yaml') -> Tuple[dict, dict]:
    """
    Main transformation function to be called from ETL pipeline
    
    Returns:
        Tuple of (dimensions, facts) dictionaries
    """
    transformer = DataTransformer(config_path)
    dimensions, facts = transformer.transform_all(extracted_data)
    
    # Log summary
    logger.info("Transformation Summary:")
    logger.info("Dimensions:")
    for name, df in dimensions.items():
        logger.info(f"  {name}: {len(df)} rows")
    
    logger.info("Facts:")
    for name, df in facts.items():
        logger.info(f"  {name}: {len(df)} rows")
    
    # Save transformed data
    transformer.save_transformed_data()
    
    return dimensions, facts