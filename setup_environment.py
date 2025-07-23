#!/usr/bin/env python3
"""
setup_environment.py - First-time project setup
Creates folder structure and generates initial configuration files
"""

import os
import yaml
from pathlib import Path

def create_project_structure():
    """Create the required directory structure for the ETL pipeline"""
    
    # Define the directory structure
    directories = [
        'config',
        'data/raw',
        'data/processed',
        'data/archive',
        'data/exports',
        'scripts',
        'models',
        'validation',
        'logs',
        'tests'
    ]
    
    # Create directories
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✓ Created directory: {directory}")
    
    # Create initial configuration file
    create_config_yaml()
    
    # Create .env template
    create_env_template()
    
    # Create requirements.txt
    create_requirements()
    
    # Create run script
    create_run_script()
    
    # Create README
    create_readme()
    
    print("\n✓ Project structure created successfully!")

def create_config_yaml():
    """Create the initial configuration YAML file"""
    
    config = {
        'project': {
            'name': 'ABI ETL Pipeline',
            'version': '1.0.0',
            'description': 'ETL pipeline for ABI biotech intelligence platform'
        },
        'paths': {
            'raw_data': 'data/raw',
            'processed_data': 'data/processed',
            'archive': 'data/archive',
            'exports': 'data/exports',
            'logs': 'logs'
        },
        'files': {
            'trl_predictions': 'realistic_trl_predictions.xlsx',
            'pubmed_articles': 'pubmed_articles_2015_2025_mergeds.xlsx',
            'biotech_companies': 'abi_targeted_biotech_dataset_2019_2025.csv'
        },
        'database': {
            'dialect': 'postgresql',
            'driver': 'psycopg2',
            'host': 'localhost',
            'port': 5432,
            'database': 'abi_warehouse',
            'schema': 'public'
        },
        'validation': {
            'null_threshold': 0.05,  # Max 5% nulls allowed
            'duplicate_threshold': 0.01,  # Max 1% duplicates allowed
            'date_range': {
                'min': '2015-01-01',
                'max': '2025-12-31'
            }
        },
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }
    }
    
    with open('config/config.yaml', 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    print("✓ Created config/config.yaml")

def create_env_template():
    """Create .env template file"""
    
    env_content = """# Database credentials
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=abi_warehouse

# Optional: BI tool credentials
POWERBI_WORKSPACE_ID=
POWERBI_API_KEY=
SUPERSET_URL=
SUPERSET_USER=
SUPERSET_PASSWORD=
"""
    
    with open('.env', 'w') as f:
        f.write(env_content)
    
    print("✓ Created .env template")

def create_requirements():
    """Create requirements.txt file"""
    
    requirements = """# Core dependencies
pandas==2.1.4
numpy==1.24.3
openpyxl==3.1.2
xlrd==2.0.1

# Database
sqlalchemy==2.0.23
psycopg2-binary==2.9.9
alembic==1.13.0

# Configuration and environment
python-dotenv==1.0.0
pyyaml==6.0.1

# Data validation
great-expectations==0.18.7
pandera==0.17.2

# Logging and monitoring
loguru==0.7.2

# Testing
pytest==7.4.3
pytest-cov==4.1.0
pytest-mock==3.12.0

# CLI and utilities
click==8.1.7
tqdm==4.66.1
tabulate==0.9.0

# Data quality
pandas-profiling==3.6.6

# Optional: BI connectors
# pypowerbi==1.1.0
# apache-superset==3.0.1
"""
    
    with open('requirements.txt', 'w') as f:
        f.write(requirements)
    
    print("✓ Created requirements.txt")

def create_run_script():
    """Create shell script to run the ETL pipeline"""
    
    script_content = """#!/bin/bash

# ABI ETL Pipeline Runner

echo "🚀 Starting ABI ETL Pipeline..."
echo "================================"

# Activate virtual environment if exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Check if PostgreSQL is running
pg_isready -h localhost -p 5432
if [ $? -ne 0 ]; then
    echo "❌ PostgreSQL is not running. Please start the database."
    exit 1
fi

# Run the ETL pipeline
python etl_pipeline.py --mode full

# Check exit status
if [ $? -eq 0 ]; then
    echo "✅ ETL Pipeline completed successfully!"
else
    echo "❌ ETL Pipeline failed. Check logs for details."
    exit 1
fi

# Optional: Run data quality checks
echo "Running data quality validation..."
python validation/data_validator.py

echo "================================"
echo "📊 Pipeline execution complete!"
"""
    
    with open('run_etl.sh', 'w', encoding='utf-8') as f:
     f.write(script_content)
    
    os.chmod('run_etl.sh', 0o755)
    print("✓ Created run_etl.sh")

def create_readme():
    """Create initial README.md"""
    
    readme_content = """# ABI ETL Pipeline

## 🎯 Overview
This ETL pipeline transforms biotech intelligence data into a clean star-schema data warehouse optimized for BI analytics.

## 🏗️ Architecture

### Why Star Schema?
We chose a **Star Schema** design for several key reasons:

1. **Query Performance**: Star schemas are optimized for read-heavy analytical workloads
2. **Simplicity**: Easy to understand and query - fact tables at the center, dimensions around
3. **BI Tool Compatibility**: Most BI tools are optimized for star schemas
4. **Scalability**: Can handle large volumes of data efficiently
5. **Flexibility**: Easy to add new dimensions or facts without major restructuring

### Schema Design
- **Fact Tables**: Store measurable events (technology assessments, financial metrics, company scores)
- **Dimension Tables**: Store descriptive attributes (technologies, publications, companies, dates)
- **Surrogate Keys**: Each dimension has a synthetic key for performance and consistency

## 📂 Project Structure
```
bi-etl-pipeline/
├── config/           # Configuration files
├── data/            # Data directories
├── scripts/         # ETL scripts
├── models/          # SQLAlchemy ORM models
├── validation/      # Data quality checks
├── logs/            # ETL execution logs
└── tests/           # Unit tests
```

## 🚀 Quick Start

1. **Setup Environment**
   ```bash
   python setup_environment.py
   pip install -r requirements.txt
   ```

2. **Configure Database**
   - Copy `.env` template and add your credentials
   - Create PostgreSQL database: `abi_warehouse`

3. **Run ETL Pipeline**
   ```bash
   ./run_etl.sh
   ```

## 📊 Data Sources
- **TRL Predictions**: Technology readiness evolution data
- **PubMed Articles**: Scientific publications metadata
- **Biotech Companies**: Financial and market information

## 🎯 BI Integration Goals
This warehouse supports:
- TRL progression heatmaps
- Publication trend analysis
- Company strategic scoring
- Financial performance tracking
- Investment recommendations

## 🔄 Idempotency
The pipeline is fully idempotent - running it multiple times with the same input produces identical results without duplicates.

## 📈 Analytics Use Cases
1. **Technology Maturity Tracking**: Monitor TRL progression over time
2. **Research Trends**: Analyze publication patterns by domain
3. **Company Assessment**: Strategic scoring based on multiple factors
4. **Financial Analysis**: Quarter-over-quarter performance metrics
5. **Investment Intelligence**: Data-driven recommendations

## 🛠️ Maintenance
- Logs are stored in `logs/etl_log.csv`
- Run tests: `pytest tests/`
- Validate data quality: `python validation/data_validator.py`
"""
    
    with open('README.md', 'w', encoding='utf-8') as f:
     f.write(readme_content)

    
    print("✓ Created README.md")

if __name__ == "__main__":
    print("🏗️  Setting up ABI ETL Pipeline project structure...")
    print("=" * 50)
    create_project_structure()
    print("\n✅ Setup complete! Next steps:")
    print("1. Place your data files in data/raw/")
    print("2. Update .env with your database credentials")
    print("3. Run: pip install -r requirements.txt")
    print("4. Run: ./run_etl.sh to start the pipeline")