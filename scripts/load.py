"""
scripts/load.py - Load transformed data into PostgreSQL
FIXED VERSION: Implements proper idempotent loading with UPSERT
"""

import pandas as pd
import numpy as np
import yaml
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import sys

# Add parent directory to path to import models
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models.models import (
    Base, DimTechnology, DimPublication, DimCompany, DimDate,
    FactTechnology, FactFinancial, FactScoring
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLoader:
    """Handles idempotent loading of data into PostgreSQL warehouse"""
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize loader with configuration and database connection"""
        self.config = self._load_config(config_path)
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.processed_path = Path(self.config['paths']['processed_data'])
        
        # Natural key to surrogate key mappings (populated during dimension loading)
        self.technology_map = {}  # technology_name -> technology_sk
        self.publication_map = {}  # link -> publication_sk
        self.company_map = {}  # entity_name -> company_sk
        self.date_map = {}  # date_sk remains the same (special case)
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _create_engine(self):
        """Create SQLAlchemy engine with connection from .env"""
        db_config = {
            'user': os.getenv('DB_USER', self.config['database']['database']),
            'password': os.getenv('DB_PASSWORD', ''),
            'host': os.getenv('DB_HOST', self.config['database']['host']),
            'port': os.getenv('DB_PORT', self.config['database']['port']),
            'database': os.getenv('DB_NAME', self.config['database']['database'])
        }
        
        connection_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        logger.info(f"Connecting to database at {db_config['host']}:{db_config['port']}")
        
        engine = create_engine(
            connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False
        )
        
        return engine
    
    def initialize_database(self, drop_existing: bool = False):
        """Initialize database schema"""
        logger.info("Initializing database schema...")
        
        if drop_existing:
            logger.warning("Dropping existing tables...")
            Base.metadata.drop_all(bind=self.engine)
        
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database schema created successfully")
        
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        logger.info(f"Created tables: {', '.join(sorted(tables))}")
    
    def load_all(self, dimensions: Dict[str, pd.DataFrame], 
                 facts: Dict[str, pd.DataFrame]) -> None:
        """Load all dimensions and facts into the database idempotently"""
        logger.info("Starting idempotent data loading process...")
        
        # Step 1: Load dimensions and build surrogate key mappings
        logger.info("Loading dimensions and building key mappings...")
        
        # Load date dimension (special case - date_sk is preserved)
        if 'dim_date' in dimensions:
            self._load_date_dimension(dimensions['dim_date'])
        
        # Load other dimensions
        if 'dim_technology' in dimensions:
            self.technology_map = self._load_technology_dimension(dimensions['dim_technology'])
            
        if 'dim_publication' in dimensions:
            self.publication_map = self._load_publication_dimension(dimensions['dim_publication'])
            
        if 'dim_company' in dimensions:
            self.company_map = self._load_company_dimension(dimensions['dim_company'])
        
        # Step 2: Load facts using the surrogate key mappings
        logger.info("Loading facts with resolved surrogate keys...")
        
        if 'fact_technology' in facts:
            self._load_technology_facts(facts['fact_technology'])
            
        if 'fact_financial' in facts:
            self._load_financial_facts(facts['fact_financial'])
            
        if 'fact_scoring' in facts:
            self._load_scoring_facts(facts['fact_scoring'])
        
        logger.info("Data loading completed successfully")
        
        # Log final statistics
        self._log_load_statistics()
    
    def _load_date_dimension(self, df: pd.DataFrame) -> None:
        """Load date dimension - special handling to preserve date_sk"""
        logger.info(f"Loading date dimension with {len(df)} dates...")
        
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                stmt = insert(DimDate).values(
                    date_sk=row['date_sk'],
                    date=row['date'],
                    year=row['year'],
                    month=row['month'],
                    day=row['day'],
                    quarter=row['quarter'],
                    fiscal_year=row['fiscal_year'],
                    fiscal_quarter=row['fiscal_quarter'],
                    month_name=row['month_name'],
                    day_name=row['day_name'],
                    day_of_week=row['day_of_week'],
                    week_of_year=row['week_of_year'],
                    is_weekend=row['is_weekend'],
                    is_holiday=row['is_holiday']
                )
                
                # On conflict, do nothing (date already exists)
                stmt = stmt.on_conflict_do_nothing(index_elements=['date'])
                conn.execute(stmt)
        
        # Build date mapping (date_sk remains the same)
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT date_sk FROM dim_date"))
            for row in result:
                self.date_map[row[0]] = row[0]
        
        logger.info(f"Loaded {len(self.date_map)} dates")
    
    def _load_technology_dimension(self, df: pd.DataFrame) -> Dict[str, int]:
        """Load technology dimension and return natural key to surrogate key mapping"""
        logger.info(f"Loading technology dimension with {len(df)} technologies...")
        
        mapping = {}
        
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                # Prepare the record
                technology_name = row['technology']
                
                # First, try to get existing record
                result = conn.execute(
                    text("SELECT technology_sk FROM dim_technology WHERE technology = :tech"),
                    {'tech': technology_name}
                ).fetchone()
                
                if result:
                    # Technology exists, get its surrogate key
                    mapping[technology_name] = result[0]
                else:
                    # Insert new technology
                    stmt = insert(DimTechnology).values(
                        technology=technology_name,
                        current_trl=row.get('current_trl'),
                        predicted_trl=row.get('predicted_trl'),
                        trl_change=row.get('trl_change'),
                        current_category=row.get('current_category'),
                        predicted_category=row.get('predicted_category'),
                        change_category=row.get('change_category'),
                        confidence_percent=row.get('confidence_percent'),
                        annual_change_rate=row.get('annual_change_rate'),
                        model_used=row.get('model_used'),
                        created_at=row.get('created_at', datetime.utcnow()),
                        updated_at=row.get('updated_at', datetime.utcnow())
                    )
                    
                    # On conflict, update non-key fields
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['technology'],
                        set_={
                            'current_trl': stmt.excluded.current_trl,
                            'predicted_trl': stmt.excluded.predicted_trl,
                            'trl_change': stmt.excluded.trl_change,
                            'current_category': stmt.excluded.current_category,
                            'predicted_category': stmt.excluded.predicted_category,
                            'change_category': stmt.excluded.change_category,
                            'confidence_percent': stmt.excluded.confidence_percent,
                            'annual_change_rate': stmt.excluded.annual_change_rate,
                            'model_used': stmt.excluded.model_used,
                            'updated_at': datetime.utcnow()
                        }
                    ).returning(DimTechnology.technology_sk)
                    
                    result = conn.execute(stmt).fetchone()
                    if result:
                        mapping[technology_name] = result[0]
        
        logger.info(f"Loaded {len(mapping)} technologies")
        return mapping
    
    def _load_publication_dimension(self, df: pd.DataFrame) -> Dict[str, int]:
        """Load publication dimension and return natural key to surrogate key mapping"""
        logger.info(f"Loading publication dimension with {len(df)} publications...")
        
        mapping = {}
        
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                link = row['link']
                
                # First, try to get existing record
                result = conn.execute(
                    text("SELECT publication_sk FROM dim_publication WHERE link = :link"),
                    {'link': link}
                ).fetchone()
                
                if result:
                    # Publication exists, get its surrogate key
                    mapping[link] = result[0]
                else:
                    # Insert new publication
                    stmt = insert(DimPublication).values(
                        link=link,
                        title=row.get('title'),
                        authors=row.get('authors'),
                        summary=row.get('summary'),
                        keywords=row.get('keywords'),
                        country=row.get('country'),
                        trl=row.get('trl'),
                        trl_stage=row.get('trl_stage'),
                        trl_category=row.get('trl_category'),
                        maturity=row.get('maturity'),
                        year=row.get('year'),
                        month=row.get('month'),
                        technologies=row.get('technologies'),
                        tech_category=row.get('tech_category'),
                        article_type=row.get('article_type'),
                        research_domain=row.get('research_domain'),
                        impact_factor=row.get('impact_factor'),
                        citation_count=row.get('citation_count'),
                        peer_reviewed=row.get('peer_reviewed', True),
                        publication_date=row.get('publication_date'),
                        created_at=row.get('created_at', datetime.utcnow()),
                        updated_at=row.get('updated_at', datetime.utcnow())
                    )
                    
                    # On conflict, update non-key fields
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['link'],
                        set_={
                            'title': stmt.excluded.title,
                            'authors': stmt.excluded.authors,
                            'summary': stmt.excluded.summary,
                            'keywords': stmt.excluded.keywords,
                            'country': stmt.excluded.country,
                            'trl': stmt.excluded.trl,
                            'trl_stage': stmt.excluded.trl_stage,
                            'trl_category': stmt.excluded.trl_category,
                            'maturity': stmt.excluded.maturity,
                            'year': stmt.excluded.year,
                            'month': stmt.excluded.month,
                            'technologies': stmt.excluded.technologies,
                            'tech_category': stmt.excluded.tech_category,
                            'article_type': stmt.excluded.article_type,
                            'research_domain': stmt.excluded.research_domain,
                            'publication_date': stmt.excluded.publication_date,
                            'updated_at': datetime.utcnow()
                        }
                    ).returning(DimPublication.publication_sk)
                    
                    result = conn.execute(stmt).fetchone()
                    if result:
                        mapping[link] = result[0]
        
        logger.info(f"Loaded {len(mapping)} publications")
        return mapping
    
    def _load_company_dimension(self, df: pd.DataFrame) -> Dict[str, int]:
        """Load company dimension and return natural key to surrogate key mapping"""
        logger.info(f"Loading company dimension with {len(df)} companies...")
        
        mapping = {}
        
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                entity_name = row['entity_name']
                
                # First, try to get existing record
                result = conn.execute(
                    text("SELECT company_sk FROM dim_company WHERE entity_name = :name"),
                    {'name': entity_name}
                ).fetchone()
                
                if result:
                    # Company exists, get its surrogate key
                    mapping[entity_name] = result[0]
                else:
                    # Insert new company
                    stmt = insert(DimCompany).values(
                        entity_name=entity_name,
                        cik=row.get('cik'),
                        ticker=row.get('ticker'),
                        state_of_incorporation=row.get('state_of_incorporation'),
                        sic_code=row.get('sic_code'),
                        sic_description=row.get('sic_description'),
                        industry_category=row.get('industry_category'),
                        created_at=row.get('created_at', datetime.utcnow()),
                        updated_at=row.get('updated_at', datetime.utcnow())
                    )
                    
                    # On conflict, update non-key fields
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['entity_name'],
                        set_={
                            'ticker': stmt.excluded.ticker,
                            'state_of_incorporation': stmt.excluded.state_of_incorporation,
                            'sic_code': stmt.excluded.sic_code,
                            'sic_description': stmt.excluded.sic_description,
                            'industry_category': stmt.excluded.industry_category,
                            'updated_at': datetime.utcnow()
                        }
                    ).returning(DimCompany.company_sk)
                    
                    result = conn.execute(stmt).fetchone()
                    if result:
                        mapping[entity_name] = result[0]
        
        logger.info(f"Loaded {len(mapping)} companies")
        return mapping
    
    def _load_technology_facts(self, df: pd.DataFrame) -> None:
        """Load technology facts using surrogate key mappings"""
        logger.info(f"Loading technology facts with {len(df)} records...")
        
        loaded_count = 0
        skipped_count = 0
        
        # Convert NaN values to None for proper NULL handling
        def clean_value(val):
            if pd.isna(val):
                return None
            return val
        
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                # Resolve natural keys to surrogate keys
                tech_name = row['technology_natural_key']
                pub_link = row['publication_natural_key']
                
                tech_sk = self.technology_map.get(tech_name)
                pub_sk = self.publication_map.get(pub_link)
                date_sk = row['date_sk']
                
                if not tech_sk or not pub_sk:
                    skipped_count += 1
                    continue
                
                # Insert fact record
                stmt = insert(FactTechnology).values(
                    technology_sk=tech_sk,
                    publication_sk=pub_sk,
                    date_sk=date_sk,
                    trl=clean_value(row.get('trl')),
                    maturity_score=clean_value(row.get('maturity_score')),
                    innovation_score=clean_value(row.get('innovation_score')),
                    market_score=clean_value(row.get('market_score')),
                    risk_assessment=clean_value(row.get('risk_assessment')),
                    created_at=row.get('created_at', datetime.utcnow())
                )
                
                # On conflict (duplicate grain), update metrics
                stmt = stmt.on_conflict_do_update(
                    index_elements=['technology_sk', 'publication_sk'],
                    set_={
                        'trl': stmt.excluded.trl,
                        'maturity_score': stmt.excluded.maturity_score,
                        'innovation_score': stmt.excluded.innovation_score,
                        'market_score': stmt.excluded.market_score,
                        'risk_assessment': stmt.excluded.risk_assessment,
                        'updated_at': datetime.utcnow()
                    }
                )
                
                conn.execute(stmt)
                loaded_count += 1
        
        logger.info(f"Loaded {loaded_count} technology facts ({skipped_count} skipped due to missing dimensions)")
    
    def _load_financial_facts(self, df: pd.DataFrame) -> None:
        """Load financial facts using surrogate key mappings"""
        logger.info(f"Loading financial facts with {len(df)} records...")
        
        loaded_count = 0
        skipped_count = 0
        
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                # Resolve natural key to surrogate key
                company_name = row['company_natural_key']
                company_sk = self.company_map.get(company_name)
                date_sk = row['date_sk']
                
                if not company_sk:
                    skipped_count += 1
                    continue
                
                # Convert NaN values to None for proper NULL handling
                def clean_value(val):
                    if pd.isna(val):
                        return None
                    return val
                
                def clean_int_value(val):
                    if pd.isna(val):
                        return None
                    return int(val)
                
                # Insert fact record
                stmt = insert(FactFinancial).values(
                    company_sk=company_sk,
                    date_sk=date_sk,
                    revenues=clean_value(row.get('revenues')),
                    net_income=clean_value(row.get('net_income')),
                    operating_income=clean_value(row.get('operating_income')),
                    rd_expenses=clean_value(row.get('rd_expenses')),
                    assets=clean_value(row.get('assets')),
                    liabilities=clean_value(row.get('liabilities')),
                    stockholders_equity=clean_value(row.get('stockholders_equity')),
                    cash_and_equivalents=clean_value(row.get('cash_and_equivalents')),
                    operating_cash_flow=clean_value(row.get('operating_cash_flow')),
                    goodwill=clean_value(row.get('goodwill')),
                    intangible_assets=clean_value(row.get('intangible_assets')),
                    patents_issued=clean_int_value(row.get('patents_issued')),
                    data_completeness=clean_value(row.get('data_completeness')),
                    data_freshness=row.get('data_freshness'),
                    fiscal_year=row['fiscal_year'],
                    fiscal_period=row['fiscal_period'],
                    created_at=row.get('created_at', datetime.utcnow())
                )
                
                # On conflict (duplicate grain), update metrics
                stmt = stmt.on_conflict_do_update(
                    index_elements=['company_sk', 'fiscal_year', 'fiscal_period'],
                    set_={
                        'revenues': stmt.excluded.revenues,
                        'net_income': stmt.excluded.net_income,
                        'operating_income': stmt.excluded.operating_income,
                        'rd_expenses': stmt.excluded.rd_expenses,
                        'assets': stmt.excluded.assets,
                        'liabilities': stmt.excluded.liabilities,
                        'stockholders_equity': stmt.excluded.stockholders_equity,
                        'cash_and_equivalents': stmt.excluded.cash_and_equivalents,
                        'operating_cash_flow': stmt.excluded.operating_cash_flow,
                        'goodwill': stmt.excluded.goodwill,
                        'intangible_assets': stmt.excluded.intangible_assets,
                        'patents_issued': stmt.excluded.patents_issued,
                        'data_completeness': stmt.excluded.data_completeness,
                        'data_freshness': stmt.excluded.data_freshness,
                        'updated_at': datetime.utcnow()
                    }
                )
                
                conn.execute(stmt)
                loaded_count += 1
        
        logger.info(f"Loaded {loaded_count} financial facts ({skipped_count} skipped due to missing dimensions)")
    
    def _load_scoring_facts(self, df: pd.DataFrame) -> None:
        """Load scoring facts using surrogate key mappings"""
        logger.info(f"Loading scoring facts with {len(df)} records...")
        
        loaded_count = 0
        skipped_count = 0
        
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                # Resolve natural key to surrogate key
                company_name = row['company_natural_key']
                company_sk = self.company_map.get(company_name)
                date_sk = row['date_sk']
                
                if not company_sk:
                    skipped_count += 1
                    continue
                
                # Insert fact record
                stmt = insert(FactScoring).values(
                    company_sk=company_sk,
                    date_sk=date_sk,
                    rd_score=row.get('rd_score'),
                    cash_score=row.get('cash_score'),
                    market_score=row.get('market_score'),
                    patent_score=row.get('patent_score'),
                    growth_score=row.get('growth_score'),
                    total_score=row.get('total_score'),
                    investment_recommendation=row.get('investment_recommendation'),
                    confidence_level=row.get('confidence_level'),
                    scoring_version=row.get('scoring_version', '1.0'),
                    scoring_date=row.get('scoring_date', datetime.utcnow()),
                    created_at=row.get('created_at', datetime.utcnow())
                )
                
                # On conflict (duplicate grain), update scores
                stmt = stmt.on_conflict_do_update(
                    index_elements=['company_sk', 'date_sk'],
                    set_={
                        'rd_score': stmt.excluded.rd_score,
                        'cash_score': stmt.excluded.cash_score,
                        'market_score': stmt.excluded.market_score,
                        'patent_score': stmt.excluded.patent_score,
                        'growth_score': stmt.excluded.growth_score,
                        'total_score': stmt.excluded.total_score,
                        'investment_recommendation': stmt.excluded.investment_recommendation,
                        'confidence_level': stmt.excluded.confidence_level,
                        'scoring_version': stmt.excluded.scoring_version,
                        'scoring_date': stmt.excluded.scoring_date,
                        'updated_at': datetime.utcnow()
                    }
                )
                
                conn.execute(stmt)
                loaded_count += 1
        
        logger.info(f"Loaded {loaded_count} scoring facts ({skipped_count} skipped due to missing dimensions)")
    
    def _log_load_statistics(self) -> None:
        """Log statistics about loaded data"""
        logger.info("\nFinal Load Statistics:")
        
        with self.engine.connect() as conn:
            # Dimension counts
            tables = ['dim_technology', 'dim_publication', 'dim_company', 'dim_date',
                     'fact_technology', 'fact_financial', 'fact_scoring']
            
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                logger.info(f"  {table}: {result} records")
            
            # Check referential integrity
            logger.info("\nReferential Integrity Check:")
            
            # Check for orphaned fact records
            orphan_checks = [
                ("fact_technology without valid technology", 
                 """SELECT COUNT(*) FROM fact_technology f 
                    WHERE NOT EXISTS (SELECT 1 FROM dim_technology d WHERE d.technology_sk = f.technology_sk)"""),
                ("fact_technology without valid publication", 
                 """SELECT COUNT(*) FROM fact_technology f 
                    WHERE NOT EXISTS (SELECT 1 FROM dim_publication d WHERE d.publication_sk = f.publication_sk)"""),
                ("fact_financial without valid company", 
                 """SELECT COUNT(*) FROM fact_financial f 
                    WHERE NOT EXISTS (SELECT 1 FROM dim_company d WHERE d.company_sk = f.company_sk)"""),
                ("fact_scoring without valid company", 
                 """SELECT COUNT(*) FROM fact_scoring f 
                    WHERE NOT EXISTS (SELECT 1 FROM dim_company d WHERE d.company_sk = f.company_sk)""")
            ]
            
            all_valid = True
            for check_name, query in orphan_checks:
                orphaned_count = conn.execute(text(query)).scalar()
                if orphaned_count > 0:
                    logger.error(f"  ❌ {check_name}: {orphaned_count} orphaned records")
                    all_valid = False
                else:
                    logger.info(f"  ✓ {check_name}: No orphaned records")
            
            if all_valid:
                logger.info("\n✅ All referential integrity checks passed!")
            else:
                logger.error("\n❌ Referential integrity issues detected!")
    
    def create_indexes(self) -> None:
        """Create additional indexes for query performance"""
        logger.info("Creating performance indexes...")
        
        indexes = [
            # Dimension lookups
            "CREATE INDEX IF NOT EXISTS idx_dim_tech_name_lower ON dim_technology(LOWER(technology))",
            "CREATE INDEX IF NOT EXISTS idx_dim_company_name_upper ON dim_company(UPPER(entity_name))",
            
            # Fact table performance
            "CREATE INDEX IF NOT EXISTS idx_fact_tech_all_fks ON fact_technology(technology_sk, publication_sk, date_sk)",
            "CREATE INDEX IF NOT EXISTS idx_fact_fin_company_year ON fact_financial(company_sk, fiscal_year)",
            "CREATE INDEX IF NOT EXISTS idx_fact_score_company_date ON fact_scoring(company_sk, date_sk)"
        ]
        
        with self.engine.begin() as conn:
            for index_sql in indexes:
                try:
                    conn.execute(text(index_sql))
                    logger.info(f"Created index: {index_sql.split('ON')[0].split('EXISTS')[-1].strip()}")
                except Exception as e:
                    logger.warning(f"Could not create index: {str(e)}")


# Standalone load function
def load_data(dimensions: Optional[Dict[str, pd.DataFrame]] = None,
              facts: Optional[Dict[str, pd.DataFrame]] = None,
              config_path: str = 'config/config.yaml',
              initialize_db: bool = False,
              drop_existing: bool = False) -> None:
    """
    Main load function to be called from ETL pipeline
    
    Args:
        dimensions: Dictionary of dimension DataFrames
        facts: Dictionary of fact DataFrames
        config_path: Path to configuration file
        initialize_db: Whether to initialize database schema
        drop_existing: Whether to drop existing tables before creating new ones
    """
    
    # If no data provided, load from processed files
    if dimensions is None or facts is None:
        logger.info("Loading transformed data from files...")
        processed_path = Path('data/processed')
        
        dimensions = {}
        facts = {}
        
        for file in processed_path.glob('dim_*.parquet'):
            table_name = file.stem
            dimensions[table_name] = pd.read_parquet(file)
        
        for file in processed_path.glob('fact_*.parquet'):
            table_name = file.stem
            facts[table_name] = pd.read_parquet(file)
    
    # Create loader and load data
    loader = DataLoader(config_path)
    
    if initialize_db:
        loader.initialize_database(drop_existing=drop_existing)
    
    loader.load_all(dimensions, facts)
    loader.create_indexes()


if __name__ == "__main__":
    # Test loading
    print("📥 Testing idempotent data loading...")
    
    try:
        # Initialize database and load data
        load_data(initialize_db=True, drop_existing=True)
        print("\n✅ Data loading successful!")
        
        # Test idempotency by running again
        print("\n🔄 Testing idempotency by reloading...")
        load_data(initialize_db=False, drop_existing=False)
        print("\n✅ Idempotent reload successful!")
        
    except Exception as e:
        print(f"\n❌ Loading failed: {str(e)}")
        raise