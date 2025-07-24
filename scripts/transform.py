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
    
        # FIXED: Clean numeric fields to avoid integer overflow
        # Extract TRL information
        if 'trl' in dim_pub.columns:
            dim_pub['trl'] = pd.to_numeric(dim_pub['trl'], errors='coerce')
            # Ensure TRL is within valid range (1-9)
            dim_pub['trl'] = dim_pub['trl'].where(
                (dim_pub['trl'] >= 1) & (dim_pub['trl'] <= 9), 
                None
            )
        
        # FIXED: Clean maturity field (was causing integer overflow)
        if 'maturity' in dim_pub.columns:
            dim_pub['maturity'] = pd.to_numeric(dim_pub['maturity'], errors='coerce')
            # Ensure maturity is within reasonable range
            dim_pub['maturity'] = dim_pub['maturity'].where(
                (dim_pub['maturity'] >= 0) & (dim_pub['maturity'] <= 100), 
                None
            )
        
        # FIXED: Clean year and month to ensure they're valid integers
        if 'year' in dim_pub.columns:
            dim_pub['year'] = pd.to_numeric(dim_pub['year'], errors='coerce')
            # Ensure year is within reasonable range
            dim_pub['year'] = dim_pub['year'].where(
                (dim_pub['year'] >= 1900) & (dim_pub['year'] <= 2030), 
                None
            ).astype('Int64')  # Use nullable integer type
        
        if 'month' in dim_pub.columns:
            dim_pub['month'] = pd.to_numeric(dim_pub['month'], errors='coerce')
            # Ensure month is within valid range
            dim_pub['month'] = dim_pub['month'].where(
                (dim_pub['month'] >= 1) & (dim_pub['month'] <= 12), 
                None
            ).astype('Int64')  # Use nullable integer type
        
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
                
                # FIXED: Calculate actual metrics instead of NULL
                maturity_score = self._calculate_maturity_score(pub_row, trl_value)
                innovation_score = self._calculate_innovation_score(pub_row)
                market_score = self._calculate_market_score(pub_row)
                risk_assessment = self._calculate_risk_assessment(trl_value, pub_row)
                
                fact_record = {
                    'technology_natural_key': tech_name,
                    'publication_natural_key': pub_row['Lien'],
                    'date_sk': date_sk,
                    'trl': trl_value,
                    'maturity_score': maturity_score,
                    'innovation_score': innovation_score,
                    'market_score': market_score,
                    'risk_assessment': risk_assessment,
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

    def _calculate_maturity_score(self, pub_row: pd.Series, trl_value: Optional[int]) -> float:
        """Calculate maturity score based on TRL and publication data"""
        score = 0.0
        
        # Base score from TRL (0-9 scale to 0-40 points)
        if trl_value is not None and 1 <= trl_value <= 9:
            score += (trl_value / 9) * 40
        
        # Add points for publication maturity indicators
        maturity = pub_row.get('Maturité', 0)
        if pd.notna(maturity) and isinstance(maturity, (int, float)):
            score += min(maturity * 10, 30)  # Up to 30 points
        
        # Add points for research domain maturity
        research_domain = str(pub_row.get('Research Domain', '')).lower()
        if 'clinical' in research_domain or 'trial' in research_domain:
            score += 20  # Clinical stage = more mature
        elif 'preclinical' in research_domain:
            score += 10  # Preclinical = moderate maturity
        
        # Add points for article type
        article_type = str(pub_row.get('Article Type', '')).lower()
        if 'review' in article_type:
            score += 10  # Review articles indicate established field
        
        return min(score, 100.0)  # Cap at 100

    def _calculate_innovation_score(self, pub_row: pd.Series) -> float:
        """Calculate innovation score based on publication characteristics"""
        score = 50.0  # Base score
        
        # Innovation keywords in title/summary
        title = str(pub_row.get('Titre', '')).lower()
        summary = str(pub_row.get('Résumé', '')).lower()
        keywords = str(pub_row.get('Mots-clés', '')).lower()
        
        innovation_terms = [
            'novel', 'innovative', 'breakthrough', 'cutting-edge', 'advanced',
            'revolutionary', 'pioneering', 'first-time', 'unprecedented',
            'nanoparticle', 'crispr', 'gene therapy', 'artificial intelligence',
            'machine learning', 'biomarker', 'personalized', 'precision'
        ]
        
        innovation_count = 0
        all_text = f"{title} {summary} {keywords}"
        for term in innovation_terms:
            if term in all_text:
                innovation_count += 1
        
        # Add points for innovation indicators
        score += min(innovation_count * 5, 30)  # Up to 30 bonus points
        
        # Bonus for recent publication (more likely to be innovative)
        pub_year = pub_row.get('Year', 2020)
        if pd.notna(pub_year) and pub_year >= 2020:
            score += 10
        elif pd.notna(pub_year) and pub_year >= 2018:
            score += 5
        
        return min(score, 100.0)

    def _calculate_market_score(self, pub_row: pd.Series) -> float:
        """Calculate market score based on publication market indicators"""
        score = 30.0  # Base score
        
        # Market-related keywords
        title = str(pub_row.get('Titre', '')).lower()
        summary = str(pub_row.get('Résumé', '')).lower()
        keywords = str(pub_row.get('Mots-clés', '')).lower()
        
        market_terms = [
            'market', 'commercial', 'therapeutic', 'treatment', 'therapy',
            'clinical', 'patient', 'drug', 'pharmaceutical', 'fda approved',
            'regulatory', 'efficacy', 'safety', 'trial', 'study'
        ]
        
        market_count = 0
        all_text = f"{title} {summary} {keywords}"
        for term in market_terms:
            if term in all_text:
                market_count += 1
        
        # Add points for market readiness indicators
        score += min(market_count * 3, 25)  # Up to 25 bonus points
        
        # Bonus for high TRL (market readiness)
        trl = pub_row.get('TRL', 0)
        if pd.notna(trl) and trl >= 7:
            score += 25  # High TRL = market ready
        elif pd.notna(trl) and trl >= 5:
            score += 15  # Medium TRL = approaching market
        elif pd.notna(trl) and trl >= 3:
            score += 10  # Low-medium TRL = early market potential
        
        # Bonus for clinical research domain
        research_domain = str(pub_row.get('Research Domain', '')).lower()
        if 'clinical' in research_domain:
            score += 20
        
        return min(score, 100.0)

    def _calculate_risk_assessment(self, trl_value: Optional[int], pub_row: pd.Series) -> float:
        """Calculate risk assessment (lower score = higher risk)"""
        score = 50.0  # Base medium risk
        
        # Lower TRL = higher risk (inverse relationship)
        if trl_value is not None:
            if trl_value >= 8:
                score += 30  # Low risk
            elif trl_value >= 6:
                score += 20  # Medium-low risk
            elif trl_value >= 4:
                score += 10  # Medium risk
            elif trl_value >= 2:
                score -= 10  # Medium-high risk
            else:
                score -= 20  # High risk
        
        # Research stage risk assessment
        research_domain = str(pub_row.get('Research Domain', '')).lower()
        if 'clinical' in research_domain:
            score += 15  # Clinical = lower risk
        elif 'preclinical' in research_domain:
            score += 5   # Preclinical = medium risk
        else:
            score -= 5   # Basic research = higher risk
        
        # Recent publications = lower risk (more active research)
        pub_year = pub_row.get('Year', 2020)
        if pd.notna(pub_year) and pub_year >= 2022:
            score += 10
        elif pd.notna(pub_year) and pub_year >= 2020:
            score += 5
        elif pd.notna(pub_year) and pub_year < 2018:
            score -= 10  # Older research = higher risk
        
        return min(max(score, 0.0), 100.0)  # Keep between 0-100
    
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
                
                # FIXED: Calculate actual market score instead of hardcoded 50.0
                market_score = self._calculate_company_market_score(company_financials)
                
                # Use end of fiscal year as date
                date_sk = int(f"{int(latest_year)}1231")
                
                scoring_record = {
                    'company_natural_key': company_name,
                    'date_sk': date_sk,
                    'rd_score': rd_score if rd_score is not None else 50.0,
                    'cash_score': cash_score if cash_score is not None else 50.0,
                    'market_score': market_score if market_score is not None else 50.0,  # FIXED
                    'patent_score': patent_score if patent_score is not None else 0.0,
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

    def _transform_financial_facts(self, companies_df: pd.DataFrame) -> pd.DataFrame:
        """Create financial fact table using ONLY actual dataset columns"""
        logger.info("Creating financial fact table...")
    
        # LOG available columns for debugging
        logger.info(f"Available columns in companies data: {list(companies_df.columns)}")
    
        # Prepare data
        companies_clean = companies_df.copy()
        companies_clean['entityName'] = companies_clean['entityName'].str.strip().str.upper()
    
        fact_financial_list = []
    
        # Create financial records for each company-year combination
        for _, row in companies_clean.iterrows():
            if pd.isna(row['fiscalYear']) or pd.isna(row['entityName']):
                continue
            
            # Create date_sk from fiscal year (use end of year)
            fiscal_year = int(row['fiscalYear'])
            date_sk = int(f"{fiscal_year}1231")
            
            # FIXED: Use ONLY the actual columns from your dataset
            fact_record = {
                'company_natural_key': row['entityName'],
                'date_sk': date_sk,
                'fiscal_year': fiscal_year,
                'fiscal_period': 'Annual',  # Annual data
                
                # Map to ACTUAL column names from your dataset
                'revenues': self._safe_numeric(row.get('Revenues')),
                'net_income': self._safe_numeric(row.get('NetIncomeLoss')),
                'operating_income': self._safe_numeric(row.get('OperatingIncomeLoss')),
                
                # Assets and Liabilities - use actual column names
                'total_assets': self._safe_numeric(row.get('Assets')),
                'total_liabilities': self._safe_numeric(row.get('Liabilities')),
                'stockholders_equity': self._safe_numeric(row.get('StockholdersEquity')),
                
                # Cash - use actual column name
                'cash_and_equivalents': self._safe_numeric(row.get('CashAndCashEquivalentsAtCarryingValue')),
                
                # R&D - use actual column name
                'rd_expenses': self._safe_numeric(row.get('ResearchAndDevelopmentExpense')),
                
                # Cash flow - use actual column name
                'operating_cash_flow': self._safe_numeric(row.get('OperatingCashFlow')),
                
                # Intangible assets - use actual column names
                'goodwill': self._safe_numeric(row.get('Goodwill')),
                'intangible_assets': self._safe_numeric(row.get('IntangibleAssetsNetExcludingGoodwill')),
                
                # Patents - use actual column name
                'patents_issued': self._safe_numeric(row.get('PatentsIssued'), is_integer=True),
                
                # Data quality from dataset
                'data_completeness': self._safe_numeric(row.get('dataCompleteness', 1.0)),
                'data_freshness': row.get('dataFreshness', 'Current'),
                
                'created_at': datetime.utcnow()
            }
            
            # Calculate derived metrics ONLY if we have the base data
            if fact_record['revenues'] and fact_record['revenues'] > 0:
                if fact_record['rd_expenses']:
                    fact_record['rd_intensity'] = fact_record['rd_expenses'] / fact_record['revenues']
                else:
                    fact_record['rd_intensity'] = None
                
                fact_record['net_margin'] = (fact_record['net_income'] or 0) / fact_record['revenues']
            else:
                fact_record['rd_intensity'] = None
                fact_record['net_margin'] = None
            
            if fact_record['total_assets'] and fact_record['total_assets'] > 0:
                fact_record['roa'] = (fact_record['net_income'] or 0) / fact_record['total_assets']
            else:
                fact_record['roa'] = None
            
            if fact_record['stockholders_equity'] and fact_record['stockholders_equity'] > 0:
                fact_record['roe'] = (fact_record['net_income'] or 0) / fact_record['stockholders_equity']
            else:
                fact_record['roe'] = None
        
            fact_financial_list.append(fact_record)
    
        fact_financial = pd.DataFrame(fact_financial_list)
    
        # CRITICAL: Respect granularity - one record per company per fiscal year
        if not fact_financial.empty:
            fact_financial = fact_financial.drop_duplicates(
                subset=['company_natural_key', 'date_sk'],
                keep='last'
            )
    
        logger.info(f"Created financial fact table with {len(fact_financial)} records")
    
        # Log what we actually created
        logger.info("Financial fact columns with data:")
        for col in fact_financial.columns:
            non_null_count = fact_financial[col].notna().sum()
            if non_null_count > 0:
                logger.info(f"  - {col}: {non_null_count}/{len(fact_financial)} non-null values")
    
        return fact_financial

    def _safe_numeric(self, value, is_integer: bool = False):
        """Safely convert value to numeric, handling various edge cases"""
        if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'none', 'null']:
            return None
        
        try:
            numeric_value = pd.to_numeric(value, errors='coerce')
            if pd.isna(numeric_value):
                return None
            
            if is_integer:
                return int(numeric_value)
            else:
                return float(numeric_value)
        except:
            return None

    def _calculate_rd_score(self, financials: pd.DataFrame) -> float:
        """Calculate R&D score based on R&D investment"""
        if financials.empty:
            return 50.0
        
        latest = financials.iloc[-1]
        rd_expense = latest.get('ResearchAndDevelopmentExpense', 0)
        revenues = latest.get('Revenues', 0)
        
        if pd.isna(rd_expense) or pd.isna(revenues) or revenues <= 0:
            return 50.0
        
        rd_intensity = rd_expense / revenues
        
        # Score based on R&D intensity
        if rd_intensity > 0.20:  # > 20%
            return 90.0
        elif rd_intensity > 0.15:  # > 15%
            return 80.0
        elif rd_intensity > 0.10:  # > 10%
            return 70.0
        elif rd_intensity > 0.05:  # > 5%
            return 60.0
        elif rd_intensity > 0.02:  # > 2%
            return 50.0
        else:
            return 30.0

    def _calculate_cash_score(self, financials: pd.DataFrame) -> float:
        """Calculate cash score based on cash position"""
        if financials.empty:
            return 50.0
        
        latest = financials.iloc[-1]
        cash = latest.get('CashAndCashEquivalents', 0)
        total_assets = latest.get('TotalAssets', 0)
        
        if pd.isna(cash) or pd.isna(total_assets) or total_assets <= 0:
            return 50.0
        
        cash_ratio = cash / total_assets
        
        # Score based on cash ratio
        if cash_ratio > 0.30:  # > 30%
            return 90.0
        elif cash_ratio > 0.20:  # > 20%
            return 80.0
        elif cash_ratio > 0.15:  # > 15%
            return 70.0
        elif cash_ratio > 0.10:  # > 10%
            return 60.0
        elif cash_ratio > 0.05:  # > 5%
            return 50.0
        else:
            return 30.0

    def _calculate_growth_score(self, financials: pd.DataFrame) -> float:
        """Calculate growth score based on revenue growth"""
        if len(financials) < 2:
            return 50.0
        
        # Sort by fiscal year
        sorted_financials = financials.sort_values('fiscalYear')
        
        # Get latest two years
        latest = sorted_financials.iloc[-1]
        previous = sorted_financials.iloc[-2]
        
        latest_revenue = latest.get('Revenues', 0)
        previous_revenue = previous.get('Revenues', 0)
        
        if pd.isna(latest_revenue) or pd.isna(previous_revenue) or previous_revenue <= 0:
            return 50.0
        
        growth_rate = (latest_revenue - previous_revenue) / previous_revenue
        
        # Score based on growth rate
        if growth_rate > 0.50:  # > 50%
            return 95.0
        elif growth_rate > 0.30:  # > 30%
            return 85.0
        elif growth_rate > 0.20:  # > 20%
            return 75.0
        elif growth_rate > 0.10:  # > 10%
            return 65.0
        elif growth_rate > 0.05:  # > 5%
            return 55.0
        elif growth_rate > 0:      # Positive growth
            return 50.0
        elif growth_rate > -0.10:  # Small decline
            return 40.0
        else:                      # Significant decline
            return 20.0

    def _calculate_patent_score(self, financials: pd.DataFrame) -> float:
        """Calculate patent score based on patent portfolio"""
        if financials.empty:
            return 0.0
        
        latest = financials.iloc[-1]
        patents = latest.get('PatentsIssued', 0)
        
        if pd.isna(patents) or patents <= 0:
            return 0.0
        
        # Score based on patent count
        if patents > 1000:
            return 100.0
        elif patents > 500:
            return 90.0
        elif patents > 200:
            return 80.0
        elif patents > 100:
            return 70.0
        elif patents > 50:
            return 60.0
        elif patents > 20:
            return 50.0
        elif patents > 10:
            return 40.0
        elif patents > 5:
            return 30.0
        else:
            return 20.0

    def _calculate_company_market_score(self, financials: pd.DataFrame) -> float:
        """Calculate market score based on company financials and market position"""
        if financials.empty:
            return 50.0  # Default medium score
        
        score = 30.0  # Base score
        
        # Get latest financial data
        latest = financials.iloc[-1]
        
        # Revenue size indicates market presence
        revenue = latest.get('Revenues', 0)
        if pd.notna(revenue) and revenue > 0:
            if revenue > 10_000_000_000:  # > $10B
                score += 25
            elif revenue > 1_000_000_000:  # > $1B
                score += 20
            elif revenue > 100_000_000:   # > $100M
                score += 15
            elif revenue > 10_000_000:    # > $10M
                score += 10
            else:
                score += 5
        
        # Market cap proxy (stockholders equity)
        equity = latest.get('StockholdersEquity', 0)
        if pd.notna(equity) and equity > 0:
            if equity > 5_000_000_000:    # > $5B equity
                score += 20
            elif equity > 1_000_000_000:  # > $1B equity
                score += 15
            elif equity > 100_000_000:    # > $100M equity
                score += 10
            else:
                score += 5
        
        # Patents indicate market competitive advantage
        patents = latest.get('PatentsIssued', 0)
        if pd.notna(patents) and patents > 0:
            if patents > 100:
                score += 15
            elif patents > 50:
                score += 10
            elif patents > 10:
                score += 5
            else:
                score += 2
        
        # R&D investment indicates market innovation
        rd_expense = latest.get('ResearchAndDevelopmentExpense', 0)
        if pd.notna(rd_expense) and pd.notna(revenue) and revenue > 0:
            rd_intensity = rd_expense / revenue
            if rd_intensity > 0.15:  # High R&D intensity
                score += 10
            elif rd_intensity > 0.05:
                score += 5
        
        return min(score, 100.0)

    def _get_recommendation(self, total_score: float) -> str:
        """Get investment recommendation based on total score"""
        if total_score >= 80:
            return 'Strong Buy'
        elif total_score >= 70:
            return 'Buy'
        elif total_score >= 60:
            return 'Hold'
        elif total_score >= 40:
            return 'Weak Hold'
        else:
            return 'Sell'

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