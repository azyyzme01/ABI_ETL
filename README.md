# ABI ETL Pipeline

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
