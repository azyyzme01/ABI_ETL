#!/bin/bash

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
