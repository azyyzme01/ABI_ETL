"""
scripts/bi_connector.py - BI tool integration
Exports data warehouse to formats compatible with Power BI, Superset, and other BI tools
"""

import pandas as pd
import numpy as np
import yaml
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BIConnector:
    """Handles exporting data warehouse to BI tools"""
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize BI connector with configuration"""
        self.config = self._load_config(config_path)
        self.engine = self._create_engine()
        self.export_path = Path(self.config['paths']['exports'])
        self.export_path.mkdir(parents=True, exist_ok=True)
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _create_engine(self):
        """Create SQLAlchemy engine"""
        db_config = {
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'abi_warehouse')
        }
        
        connection_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        return create_engine(connection_string)
    
    def export_all(self, format: str = 'csv') -> Dict[str, str]:
        """
        Export all tables to specified format
        
        Args:
            format: Export format ('csv', 'excel', 'parquet', 'json')
            
        Returns:
            Dictionary of table names to export file paths
        """
        logger.info(f"Starting BI export in {format} format...")
        
        # Get all tables
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        
        export_files = {}
        
        # Export each table
        for table in sorted(tables):
            if table.startswith(('dim_', 'fact_')):
                file_path = self._export_table(table, format)
                export_files[table] = str(file_path)
        
        # Create metadata file
        self._create_metadata_file(export_files, format)
        
        # Create Power BI specific files if requested
        if format in ['csv', 'excel']:
            self._create_powerbi_files()
        
        logger.info(f"Export completed: {len(export_files)} tables exported")
        return export_files
    
    def _export_table(self, table_name: str, format: str) -> Path:
        """Export a single table to file"""
        logger.info(f"Exporting {table_name}...")
        
        # Read table data
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, self.engine)
        
        # Define file path
        timestamp = datetime.now().strftime('%Y%m%d')
        
        if format == 'csv':
            file_path = self.export_path / f"{table_name}_{timestamp}.csv"
            df.to_csv(file_path, index=False)
        
        elif format == 'excel':
            file_path = self.export_path / f"abi_warehouse_{timestamp}.xlsx"
            # Append to Excel file with sheet per table
            with pd.ExcelWriter(file_path, mode='a' if file_path.exists() else 'w') as writer:
                df.to_excel(writer, sheet_name=table_name, index=False)
        
        elif format == 'parquet':
            file_path = self.export_path / f"{table_name}_{timestamp}.parquet"
            df.to_parquet(file_path, engine='pyarrow', compression='snappy')
        
        elif format == 'json':
            file_path = self.export_path / f"{table_name}_{timestamp}.json"
            df.to_json(file_path, orient='records', lines=True, date_format='iso')
        
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"  → Exported {len(df)} rows to {file_path.name}")
        return file_path
    
    def _create_metadata_file(self, export_files: Dict[str, str], format: str) -> None:
        """Create metadata file for BI tools"""
        metadata = {
            'export_timestamp': datetime.now().isoformat(),
            'format': format,
            'database': self.config['database']['database'],
            'tables': {}
        }
        
        # Get schema information for each table
        inspector = inspect(self.engine)
        
        for table_name, file_path in export_files.items():
            columns = inspector.get_columns(table_name)
            pk_constraint = inspector.get_pk_constraint(table_name)
            fk_constraints = inspector.get_foreign_keys(table_name)
            
            # Get row count
            with self.engine.connect() as conn:
                row_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).scalar()
            
            metadata['tables'][table_name] = {
                'file_path': file_path,
                'row_count': row_count,
                'columns': [
                    {
                        'name': col['name'],
                        'type': str(col['type']),
                        'nullable': col['nullable']
                    }
                    for col in columns
                ],
                'primary_key': pk_constraint['constrained_columns'],
                'foreign_keys': [
                    {
                        'columns': fk['constrained_columns'],
                        'referenced_table': fk['referred_table'],
                        'referenced_columns': fk['referred_columns']
                    }
                    for fk in fk_constraints
                ]
            }
        
        # Save metadata
        metadata_path = self.export_path / f"metadata_{datetime.now().strftime('%Y%m%d')}.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Created metadata file: {metadata_path.name}")
    
    def _create_powerbi_files(self) -> None:
        """Create Power BI specific configuration files"""
        
        # Create relationships file for Power BI
        relationships = []
        inspector = inspect(self.engine)
        
        for table in inspector.get_table_names():
            if table.startswith('fact_'):
                fks = inspector.get_foreign_keys(table)
                for fk in fks:
                    relationships.append({
                        'from_table': table,
                        'from_column': fk['constrained_columns'][0],
                        'to_table': fk['referred_table'],
                        'to_column': fk['referred_columns'][0],
                        'cardinality': 'many-to-one'
                    })
        
        # Save relationships
        rel_path = self.export_path / 'powerbi_relationships.json'
        with open(rel_path, 'w') as f:
            json.dump(relationships, f, indent=2)
        
        # Create measures suggestions
        measures = self._generate_powerbi_measures()
        measures_path = self.export_path / 'powerbi_measures.txt'
        with open(measures_path, 'w') as f:
            f.write(measures)
        
        logger.info("Created Power BI configuration files")
    
    def _generate_powerbi_measures(self) -> str:
        """Generate suggested DAX measures for Power BI"""
        measures = """
# ABI Data Warehouse - Suggested Power BI Measures

## Technology Metrics
Average TRL = AVERAGE(fact_technology[trl])
TRL Progress = AVERAGE(dim_technology[trl_change])
High Maturity Count = CALCULATE(COUNTROWS(fact_technology), fact_technology[maturity_score] > 70)
Innovation Index = AVERAGE(fact_technology[innovation_score])
Risk Level = AVERAGE(fact_technology[risk_assessment])

## Financial Metrics
Total Revenue = SUM(fact_financial[revenues])
Revenue YoY Growth = 
    VAR CurrentYear = MAX(fact_financial[fiscal_year])
    VAR PreviousYear = CurrentYear - 1
    VAR CurrentRevenue = CALCULATE(SUM(fact_financial[revenues]), fact_financial[fiscal_year] = CurrentYear)
    VAR PreviousRevenue = CALCULATE(SUM(fact_financial[revenues]), fact_financial[fiscal_year] = PreviousYear)
    RETURN DIVIDE(CurrentRevenue - PreviousRevenue, PreviousRevenue, 0)

R&D Intensity = DIVIDE(SUM(fact_financial[rd_expenses]), SUM(fact_financial[revenues]), 0)
Net Margin = DIVIDE(SUM(fact_financial[net_income]), SUM(fact_financial[revenues]), 0)
Cash Ratio = DIVIDE(SUM(fact_financial[cash_and_equivalents]), SUM(fact_financial[liabilities]), 0)

## Company Scoring
Average Total Score = AVERAGE(fact_scoring[total_score])
Top Performers = CALCULATE(COUNTROWS(fact_scoring), fact_scoring[total_score] > 80)
Investment Grade = CALCULATE(COUNTROWS(fact_scoring), fact_scoring[investment_recommendation] IN {"BUY", "HOLD"})

## Time Intelligence
YTD Revenue = TOTALYTD(SUM(fact_financial[revenues]), dim_date[date])
QTD Revenue = TOTALQTD(SUM(fact_financial[revenues]), dim_date[date])
MTD Revenue = TOTALMTD(SUM(fact_financial[revenues]), dim_date[date])

## Publication Metrics
Total Publications = COUNTROWS(dim_publication)
Avg Impact Factor = AVERAGE(dim_publication[impact_factor])
High Impact Publications = CALCULATE(COUNTROWS(dim_publication), dim_publication[impact_factor] > 5)
Publications by TRL = COUNTROWS(FILTER(dim_publication, dim_publication[trl] = SELECTEDVALUE(dim_technology[current_trl])))
"""
        return measures
    
    def create_superset_config(self) -> None:
        """Create Apache Superset configuration"""
        config = {
            'database': {
                'name': 'ABI Data Warehouse',
                'sqlalchemy_uri': os.getenv('DATABASE_URL', ''),
                'tables': []
            },
            'datasets': [],
            'charts': [],
            'dashboards': []
        }
        
        # Define datasets
        inspector = inspect(self.engine)
        for table in inspector.get_table_names():
            if table.startswith(('dim_', 'fact_')):
                config['database']['tables'].append(table)
                
                # Create dataset configuration
                dataset = {
                    'table_name': table,
                    'main_dttm_col': 'created_at' if 'fact' in table else None,
                    'metrics': [],
                    'columns': []
                }
                
                # Add metrics for fact tables
                if table.startswith('fact_'):
                    if table == 'fact_financial':
                        dataset['metrics'] = [
                            {'metric_name': 'sum__revenues', 'expression': 'SUM(revenues)'},
                            {'metric_name': 'sum__rd_expenses', 'expression': 'SUM(rd_expenses)'},
                            {'metric_name': 'avg__net_margin', 'expression': 'AVG(net_income/NULLIF(revenues,0))'}
                        ]
                    elif table == 'fact_technology':
                        dataset['metrics'] = [
                            {'metric_name': 'avg__maturity_score', 'expression': 'AVG(maturity_score)'},
                            {'metric_name': 'avg__innovation_score', 'expression': 'AVG(innovation_score)'},
                            {'metric_name': 'count', 'expression': 'COUNT(*)'}
                        ]
                
                config['datasets'].append(dataset)
        
        # Define suggested charts
        config['charts'] = [
            {
                'name': 'TRL Evolution Heatmap',
                'dataset': 'fact_technology',
                'viz_type': 'heatmap',
                'metrics': ['count'],
                'groupby': ['technology_sk', 'trl']
            },
            {
                'name': 'Revenue Trend',
                'dataset': 'fact_financial',
                'viz_type': 'line',
                'metrics': ['sum__revenues'],
                'groupby': ['fiscal_year', 'fiscal_period']
            },
            {
                'name': 'Company Scores Distribution',
                'dataset': 'fact_scoring',
                'viz_type': 'histogram',
                'metrics': ['count'],
                'groupby': ['total_score']
            },
            {
                'name': 'R&D Investment Analysis',
                'dataset': 'fact_financial',
                'viz_type': 'scatter',
                'metrics': ['sum__rd_expenses', 'sum__revenues'],
                'groupby': ['company_sk']
            }
        ]
        
        # Save configuration
        config_path = self.export_path / 'superset_config.json'
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        logger.info("Created Superset configuration file")
    
    def export_for_tableau(self) -> None:
        """Export data optimized for Tableau"""
        logger.info("Creating Tableau-optimized exports...")
        
        # Create denormalized views for easier Tableau usage
        views = {
            'technology_analysis': """
                SELECT 
                    t.technology,
                    t.current_trl,
                    t.predicted_trl,
                    t.trl_change,
                    t.confidence_percent,
                    p.title as publication_title,
                    p.publication_date,
                    p.country,
                    p.impact_factor,
                    f.maturity_score,
                    f.innovation_score,
                    f.market_score,
                    f.risk_assessment,
                    d.year,
                    d.quarter,
                    d.month_name
                FROM fact_technology f
                JOIN dim_technology t ON f.technology_sk = t.technology_sk
                JOIN dim_publication p ON f.publication_sk = p.publication_sk
                JOIN dim_date d ON f.date_sk = d.date_sk
            """,
            
            'financial_performance': """
                SELECT 
                    c.entity_name,
                    c.ticker,
                    c.industry_category,
                    c.state_of_incorporation,
                    f.fiscal_year,
                    f.fiscal_period,
                    f.revenues,
                    f.net_income,
                    f.rd_expenses,
                    f.assets,
                    f.liabilities,
                    f.cash_and_equivalents,
                    CASE WHEN f.revenues > 0 
                         THEN f.net_income / f.revenues 
                         ELSE NULL END as net_margin,
                    CASE WHEN f.revenues > 0 
                         THEN f.rd_expenses / f.revenues 
                         ELSE NULL END as rd_intensity,
                    d.year,
                    d.quarter
                FROM fact_financial f
                JOIN dim_company c ON f.company_sk = c.company_sk
                JOIN dim_date d ON f.date_sk = d.date_sk
            """,
            
            'company_scoring': """
                SELECT 
                    c.entity_name,
                    c.ticker,
                    c.industry_category,
                    s.rd_score,
                    s.cash_score,
                    s.market_score,
                    s.patent_score,
                    s.growth_score,
                    s.total_score,
                    s.investment_recommendation,
                    s.confidence_level,
                    d.date
                FROM fact_scoring s
                JOIN dim_company c ON s.company_sk = c.company_sk
                JOIN dim_date d ON s.date_sk = d.date_sk
            """
        }
        
        # Export each view
        for view_name, query in views.items():
            df = pd.read_sql(query, self.engine)
            
            # Export to Tableau-friendly format
            file_path = self.export_path / f"tableau_{view_name}_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(file_path, index=False)
            
            logger.info(f"Exported Tableau view: {view_name} ({len(df)} rows)")
        
        # Create Tableau workbook template
        self._create_tableau_template()
    
    def _create_tableau_template(self) -> None:
        """Create Tableau workbook template XML"""
        template = """<?xml version='1.0' encoding='utf-8' ?>
<workbook version='10.0' xmlns:user='http://www.tableausoftware.com/xml/user'>
  <datasources>
    <datasource caption='ABI Technology Analysis' inline='true' name='technology_analysis' version='10.0'>
      <connection class='textfile' filename='tableau_technology_analysis_*.csv'>
        <relation name='technology_analysis' table='[tableau_technology_analysis_*.csv]' type='table' />
      </connection>
    </datasource>
    <datasource caption='ABI Financial Performance' inline='true' name='financial_performance' version='10.0'>
      <connection class='textfile' filename='tableau_financial_performance_*.csv'>
        <relation name='financial_performance' table='[tableau_financial_performance_*.csv]' type='table' />
      </connection>
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='TRL Evolution'>
      <note>Suggested visualization: Heatmap showing TRL progression by technology over time</note>
    </worksheet>
    <worksheet name='Financial Trends'>
      <note>Suggested visualization: Line chart showing revenue and R&D trends by quarter</note>
    </worksheet>
    <worksheet name='Company Scorecard'>
      <note>Suggested visualization: Scatter plot of companies by total score and market category</note>
    </worksheet>
    <worksheet name='Investment Dashboard'>
      <note>Suggested visualization: Dashboard combining scoring, financials, and recommendations</note>
    </worksheet>
  </worksheets>
</workbook>"""
        
        template_path = self.export_path / 'tableau_template.twb'
        with open(template_path, 'w') as f:
            f.write(template)
        
        logger.info("Created Tableau workbook template")


# Standalone export functions
def export_to_bi(format: str = 'csv', config_path: str = 'config/config.yaml') -> Dict[str, str]:
    """
    Export data warehouse to BI-friendly format
    
    Args:
        format: Export format ('csv', 'excel', 'parquet', 'json')
        config_path: Path to configuration file
        
    Returns:
        Dictionary of exported file paths
    """
    connector = BIConnector(config_path)
    
    # Export in requested format
    export_files = connector.export_all(format)
    
    # Create BI-specific configurations
    connector.create_superset_config()
    connector.export_for_tableau()
    
    return export_files


def create_bi_views(config_path: str = 'config/config.yaml') -> None:
    """Create database views optimized for BI tools"""
    connector = BIConnector(config_path)
    
    views = {
        'v_technology_summary': """
            CREATE OR REPLACE VIEW v_technology_summary AS
            SELECT 
                t.technology,
                t.current_trl,
                t.predicted_trl,
                COUNT(DISTINCT f.publication_sk) as publication_count,
                AVG(f.maturity_score) as avg_maturity_score,
                AVG(f.innovation_score) as avg_innovation_score,
                AVG(f.market_score) as avg_market_score
            FROM dim_technology t
            LEFT JOIN fact_technology f ON t.technology_sk = f.technology_sk
            GROUP BY t.technology_sk, t.technology, t.current_trl, t.predicted_trl
        """,
        
        'v_company_performance': """
            CREATE OR REPLACE VIEW v_company_performance AS
            SELECT 
                c.entity_name,
                c.ticker,
                c.industry_category,
                f.fiscal_year,
                SUM(f.revenues) as total_revenue,
                SUM(f.rd_expenses) as total_rd,
                AVG(s.total_score) as avg_score,
                MAX(s.investment_recommendation) as latest_recommendation
            FROM dim_company c
            LEFT JOIN fact_financial f ON c.company_sk = f.company_sk
            LEFT JOIN fact_scoring s ON c.company_sk = s.company_sk
            GROUP BY c.company_sk, c.entity_name, c.ticker, c.industry_category, f.fiscal_year
        """
    }
    
    # Create views
    with connector.engine.begin() as conn:
        for view_name, view_sql in views.items():
            try:
                conn.execute(text(view_sql))
                logger.info(f"Created view: {view_name}")
            except Exception as e:
                logger.warning(f"Could not create view {view_name}: {str(e)}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Export ABI data warehouse to BI tools')
    parser.add_argument(
        '--format',
        choices=['csv', 'excel', 'parquet', 'json'],
        default='csv',
        help='Export format (default: csv)'
    )
    parser.add_argument(
        '--create-views',
        action='store_true',
        help='Create database views for BI tools'
    )
    
    args = parser.parse_args()
    
    print(f"📊 Exporting data warehouse to {args.format} format...")
    
    try:
        if args.create_views:
            create_bi_views()
        
        export_files = export_to_bi(args.format)
        
        print("\n✅ Export completed successfully!")
        print("\nExported files:")
        for table, path in export_files.items():
            print(f"  - {table}: {Path(path).name}")
        
    except Exception as e:
        print(f"\n❌ Export failed: {str(e)}")
        raise