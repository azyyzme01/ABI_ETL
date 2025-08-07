"""
models/models.py - SQLAlchemy ORM models for the ABI data warehouse
FIXED VERSION: Implements proper constraints and relationships
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, Boolean,
    ForeignKey, DateTime, Text, UniqueConstraint, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime

Base = declarative_base()

# ============================================
# DIMENSION TABLES (with proper constraints)
# ============================================

class DimTechnology(Base):
    """Technology dimension table with temporal predictions support"""
    __tablename__ = 'dim_technology'
    
    # Surrogate key - autoincrement handled by database
    technology_sk = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural key and attributes
    technology = Column(String(500), nullable=False)
    prediction_year = Column(Integer, nullable=False)  # ADD: Support temporal predictions
    current_trl = Column(Integer)
    predicted_trl = Column(Float)  # Can be decimal in predictions
    trl_change = Column(Float)
    current_category = Column(String(100))
    predicted_category = Column(String(100))
    change_category = Column(String(50))  # Stable, Progressive, etc.
    confidence_percent = Column(Float)
    annual_change_rate = Column(Float)
    model_used = Column(String(100))  # ML model used for prediction
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # FIXED: Unique constraint on technology + prediction_year (supports temporal predictions)
    __table_args__ = (
        UniqueConstraint('technology', 'prediction_year', name='uq_technology_year'),
        Index('idx_technology_name', 'technology'),
        Index('idx_technology_year', 'technology', 'prediction_year'),
    )


class DimPublication(Base):
    """Publication dimension table with unique constraint on natural key"""
    __tablename__ = 'dim_publication'
    
    # Surrogate key - autoincrement handled by database
    publication_sk = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural key (using link as unique identifier)
    link = Column(String(1000), nullable=False)
    
    # Business attributes
    title = Column(Text)
    authors = Column(Text)
    summary = Column(Text)
    keywords = Column(Text)
    country = Column(String(100))
    trl = Column(Integer)
    trl_stage = Column(String(50))
    trl_category = Column(String(100))  # From source data
    maturity = Column(Float)  # Changed from Integer to Float
    impact_factor = Column(Float)
    citation_count = Column(Float)  # Changed from Integer to Float to handle large values
    peer_reviewed = Column(Boolean, default=True)
    publication_date = Column(Date)
    year = Column(Integer)  # From source data
    month = Column(Integer)  # From source data
    
    # Additional attributes from source data
    technologies = Column(Text)  # From source data
    tech_category = Column(String(200))  # Technology Category from source
    article_type = Column(String(100))  # Article Type from source
    research_domain = Column(String(200))  # Research Domain from source
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # CRITICAL: Unique constraint on natural key (link)
    __table_args__ = (
        UniqueConstraint('link', name='uq_publication_link'),
        Index('idx_publication_date', 'publication_date'),
        Index('idx_publication_trl', 'trl'),
        Index('idx_publication_link', 'link'),
    )


class DimCompany(Base):
    """Company dimension table with unique constraint on natural key"""
    __tablename__ = 'dim_company'
    
    # Surrogate key - autoincrement handled by database
    company_sk = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural key
    entity_name = Column(String(500), nullable=False)
    
    # Business attributes
    cik = Column(Integer)
    ticker = Column(String(10))
    state_of_incorporation = Column(String(100))
    sic_code = Column(Integer)
    sic_description = Column(String(500))
    industry_category = Column(String(200))
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # CRITICAL: Unique constraint on natural key
    __table_args__ = (
        UniqueConstraint('entity_name', name='uq_company_name'),
        Index('idx_company_name', 'entity_name'),
        Index('idx_company_ticker', 'ticker'),
        Index('idx_company_industry', 'industry_category'),
    )


class DimDate(Base):
    """Date dimension table - pre-populated with date attributes"""
    __tablename__ = 'dim_date'
    
    # Date surrogate key (format: YYYYMMDD) - NOT autoincrement
    date_sk = Column(Integer, primary_key=True, autoincrement=False)
    
    # Natural key
    date = Column(Date, nullable=False, unique=True)
    
    # Date attributes
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    fiscal_year = Column(Integer)
    fiscal_quarter = Column(Integer)
    month_name = Column(String(20))
    day_of_week = Column(Integer)
    day_name = Column(String(20))
    is_weekend = Column(Boolean, default=False)
    is_holiday = Column(Boolean, default=False)
    week_of_year = Column(Integer)
    
    # Indexes for common date queries
    __table_args__ = (
        UniqueConstraint('date', name='uq_date_value'),
        Index('idx_date_sk', 'date_sk'),
        Index('idx_date_year', 'year'),
        Index('idx_date_quarter', 'year', 'quarter'),
        Index('idx_date_month', 'year', 'month'),
    )


# ============================================
# FACT TABLES (with proper grain constraints)
# ============================================

class FactTechnology(Base):
    """
    Technology fact table
    Grain: One row per technology per publication
    """
    __tablename__ = 'fact_technology'
    
    # Primary key
    fact_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign keys (defining the grain)
    technology_sk = Column(Integer, ForeignKey('dim_technology.technology_sk'), nullable=False)
    publication_sk = Column(Integer, ForeignKey('dim_publication.publication_sk'), nullable=False)
    date_sk = Column(Integer, ForeignKey('dim_date.date_sk'), nullable=False)
    
    # Metrics
    trl = Column(Integer)
    maturity_score = Column(Float)
    innovation_score = Column(Float)
    market_score = Column(Float)
    risk_assessment = Column(Float)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    technology = relationship("DimTechnology", backref="technology_facts")
    publication = relationship("DimPublication", backref="technology_facts")
    date = relationship("DimDate", backref="technology_facts")
    
    # CRITICAL: Ensure grain integrity with unique constraint
    __table_args__ = (
        UniqueConstraint('technology_sk', 'publication_sk', name='uq_fact_tech_grain'),
        Index('idx_fact_tech_date', 'date_sk'),
        Index('idx_fact_tech_fks', 'technology_sk', 'publication_sk'),
    )


class FactFinancial(Base):
    """
    Financial fact table - matching actual dataset columns
    Grain: One row per company per fiscal period
    """
    __tablename__ = 'fact_financial'
    
    # Primary key
    fact_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign keys (defining the grain)
    company_sk = Column(Integer, ForeignKey('dim_company.company_sk'), nullable=False)
    date_sk = Column(Integer, ForeignKey('dim_date.date_sk'), nullable=False)
    
    # Core Financial Metrics - ONLY from actual dataset
    revenues = Column(Float)                    # Revenues
    net_income = Column(Float)                  # NetIncomeLoss
    operating_income = Column(Float)            # OperatingIncomeLoss
    total_assets = Column(Float)                # Assets
    total_liabilities = Column(Float)           # Liabilities
    stockholders_equity = Column(Float)         # StockholdersEquity
    cash_and_equivalents = Column(Float)        # CashAndCashEquivalentsAtCarryingValue
    rd_expenses = Column(Float)                 # ResearchAndDevelopmentExpense
    operating_cash_flow = Column(Float)         # OperatingCashFlow
    goodwill = Column(Float)                    # Goodwill
    intangible_assets = Column(Float)           # IntangibleAssetsNetExcludingGoodwill
    patents_issued = Column(Integer)            # PatentsIssued
    
    # Calculated Financial Ratios
    rd_intensity = Column(Float)                # R&D / Revenue
    net_margin = Column(Float)                  # Net Income / Revenue
    roa = Column(Float)                         # Return on Assets
    roe = Column(Float)                         # Return on Equity
    
    # Data quality from dataset
    data_completeness = Column(Float)           # dataCompleteness
    data_freshness = Column(String(50))         # dataFreshness
    
    # Period identifiers
    fiscal_year = Column(Integer, nullable=False)
    fiscal_period = Column(String(10), nullable=False, default='Annual')
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    company = relationship("DimCompany", backref="financial_facts")
    date = relationship("DimDate", backref="financial_facts")
    
    # CRITICAL: Ensure grain integrity with unique constraint
    __table_args__ = (
        UniqueConstraint('company_sk', 'fiscal_year', 'fiscal_period', name='uq_fact_fin_grain'),
        Index('idx_fact_fin_date', 'date_sk'),
        Index('idx_fact_fin_company', 'company_sk'),
        Index('idx_fact_fin_period', 'fiscal_year', 'fiscal_period'),
    )


class FactScoring(Base):
    """
    Company scoring fact table
    Grain: One row per company per scoring date
    """
    __tablename__ = 'fact_scoring'
    
    # Primary key
    fact_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign keys (defining the grain)
    company_sk = Column(Integer, ForeignKey('dim_company.company_sk'), nullable=False)
    date_sk = Column(Integer, ForeignKey('dim_date.date_sk'), nullable=False)
    
    # Scoring metrics (0-100 scale)
    rd_score = Column(Float)
    cash_score = Column(Float)
    market_score = Column(Float)
    patent_score = Column(Float)
    growth_score = Column(Float)
    total_score = Column(Float)
    
    # Investment recommendation
    investment_recommendation = Column(String(50))
    confidence_level = Column(Float)
    
    # Scoring metadata
    scoring_version = Column(String(20), default='1.0')
    scoring_date = Column(DateTime, default=datetime.utcnow)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    company = relationship("DimCompany", backref="scoring_facts")
    date = relationship("DimDate", backref="scoring_facts")
    
    # CRITICAL: Ensure grain integrity with unique constraint
    __table_args__ = (
        UniqueConstraint('company_sk', 'date_sk', name='uq_fact_score_grain'),
        Index('idx_fact_score_date', 'date_sk'),
        Index('idx_fact_score_company', 'company_sk'),
        Index('idx_fact_score_total', 'total_score'),
    )


# ============================================
# UTILITY FUNCTIONS
# ============================================

def create_database_engine(connection_string):
    """Create SQLAlchemy engine with connection pooling"""
    return create_engine(
        connection_string,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        echo=False
    )


def create_all_tables(engine):
    """Create all tables in the database"""
    Base.metadata.create_all(bind=engine)
    print("✓ All tables created successfully")


def drop_all_tables(engine):
    """Drop all tables (use with caution!)"""
    Base.metadata.drop_all(bind=engine)
    print("⚠️  All tables dropped")


def get_session(engine):
    """Get a new database session"""
    Session = sessionmaker(bind=engine)
    return Session()