"""
fix_schema.py - Script to fix database schema mismatches
Adds missing columns to existing tables without losing data
"""

import os
import sys
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_connection_string():
    """Build PostgreSQL connection string from environment variables"""
    db_config = {
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'abi_warehouse')
    }
    
    return (
        f"postgresql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )

def fix_dim_publication_schema(engine):
    """Add missing columns to dim_publication table"""
    logger.info("Checking dim_publication schema...")
    
    # Get current columns
    inspector = inspect(engine)
    existing_columns = [col['name'] for col in inspector.get_columns('dim_publication')]
    
    # Define required columns that might be missing
    missing_columns = []
    
    # Check each required column
    required_columns = {
        'technologies': 'TEXT',
        'tech_category': 'VARCHAR(200)',
        'article_type': 'VARCHAR(100)',
        'research_domain': 'VARCHAR(200)',
        'trl_category': 'VARCHAR(100)',
        'maturity': 'FLOAT',
        'year': 'INTEGER',
        'month': 'INTEGER'
    }
    
    for col_name, col_type in required_columns.items():
        if col_name not in existing_columns:
            missing_columns.append((col_name, col_type))
    
    if not missing_columns:
        logger.info("✓ dim_publication schema is up to date")
        return
    
    # Add missing columns
    with engine.begin() as conn:
        for col_name, col_type in missing_columns:
            try:
                logger.info(f"Adding column {col_name} ({col_type}) to dim_publication...")
                conn.execute(text(f"ALTER TABLE dim_publication ADD COLUMN {col_name} {col_type}"))
                logger.info(f"✓ Added column {col_name}")
            except Exception as e:
                logger.warning(f"Could not add column {col_name}: {e}")

def fix_dim_technology_schema(engine):
    """Add missing columns to dim_technology table"""
    logger.info("Checking dim_technology schema...")
    
    # Get current columns
    inspector = inspect(engine)
    existing_columns = [col['name'] for col in inspector.get_columns('dim_technology')]
    
    # Define required columns that might be missing
    missing_columns = []
    
    # Check each required column
    required_columns = {
        'current_category': 'VARCHAR(100)',
        'change_category': 'VARCHAR(50)',
        'annual_change_rate': 'FLOAT',
        'model_used': 'VARCHAR(100)',
        'predicted_trl': 'FLOAT'  # Change from INTEGER to FLOAT if needed
    }
    
    for col_name, col_type in required_columns.items():
        if col_name not in existing_columns:
            missing_columns.append((col_name, col_type))
    
    if not missing_columns:
        logger.info("✓ dim_technology schema is up to date")
        return
    
    # Add missing columns
    with engine.begin() as conn:
        for col_name, col_type in missing_columns:
            try:
                logger.info(f"Adding column {col_name} ({col_type}) to dim_technology...")
                conn.execute(text(f"ALTER TABLE dim_technology ADD COLUMN {col_name} {col_type}"))
                logger.info(f"✓ Added column {col_name}")
            except Exception as e:
                logger.warning(f"Could not add column {col_name}: {e}")
        
        # Special case: if predicted_trl needs to be changed from INTEGER to FLOAT
        if 'predicted_trl' in existing_columns:
            try:
                logger.info("Altering predicted_trl column type to FLOAT...")
                conn.execute(text("ALTER TABLE dim_technology ALTER COLUMN predicted_trl TYPE FLOAT"))
                logger.info("✓ Updated predicted_trl column type")
            except Exception as e:
                logger.warning(f"Could not alter predicted_trl type: {e}")

def check_and_fix_all_tables(engine):
    """Check and fix schema for all tables"""
    logger.info("Starting schema validation and fixes...")
    
    # Check if tables exist
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    if 'dim_publication' in tables:
        fix_dim_publication_schema(engine)
    else:
        logger.warning("dim_publication table does not exist. Run ETL with --initialize-db first.")
    
    if 'dim_technology' in tables:
        fix_dim_technology_schema(engine)
    else:
        logger.warning("dim_technology table does not exist. Run ETL with --initialize-db first.")
    
    logger.info("Schema fixes completed!")

def main():
    """Main function"""
    logger.info("ABI ETL Schema Fix Utility")
    logger.info("=" * 50)
    
    # Create engine
    connection_string = get_connection_string()
    engine = create_engine(connection_string)
    
    # Test connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✓ Database connection successful")
    except Exception as e:
        logger.error(f"❌ Could not connect to database: {e}")
        sys.exit(1)
    
    # Fix schemas
    check_and_fix_all_tables(engine)
    
    logger.info("\n✅ Schema fixes completed successfully!")
    logger.info("You can now run the ETL pipeline again.")

if __name__ == "__main__":
    main()