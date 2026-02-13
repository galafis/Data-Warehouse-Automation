#!/usr/bin/env python3
"""
Data Warehouse Automation
Automated ETL pipeline with data quality checks, scheduling, and monitoring.
Built with Python, SQLite, and modern data engineering practices.
"""

import sqlite3
import pandas as pd
import schedule
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataWarehouseAutomation:
    def __init__(self, db_path: str = 'data_warehouse.db'):
        self.db_path = db_path
        self.init_database()
        self.job_history = []
        
    def init_database(self):
        """Initialize data warehouse schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create dimension tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_customers (
                customer_id INTEGER PRIMARY KEY,
                customer_name TEXT NOT NULL,
                email TEXT,
                city TEXT,
                state TEXT,
                country TEXT,
                created_date DATE,
                updated_date DATE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_products (
                product_id INTEGER PRIMARY KEY,
                product_name TEXT NOT NULL,
                category TEXT,
                subcategory TEXT,
                price DECIMAL(10,2),
                cost DECIMAL(10,2),
                created_date DATE,
                updated_date DATE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_time (
                date_key INTEGER PRIMARY KEY,
                full_date DATE,
                year INTEGER,
                quarter INTEGER,
                month INTEGER,
                week INTEGER,
                day_of_week INTEGER,
                day_name TEXT,
                month_name TEXT,
                is_weekend BOOLEAN
            )
        ''')
        
        # Create fact table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fact_sales (
                sale_id TEXT PRIMARY KEY,
                customer_id INTEGER,
                product_id INTEGER,
                date_key INTEGER,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                total_amount DECIMAL(10,2),
                discount DECIMAL(10,2),
                profit DECIMAL(10,2),
                created_date TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
                FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
                FOREIGN KEY (date_key) REFERENCES dim_time(date_key)
            )
        ''')
        
        # Create staging tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS staging_sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_name TEXT,
                customer_email TEXT,
                product_name TEXT,
                category TEXT,
                sale_date DATE,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                total_amount DECIMAL(10,2),
                processed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create job monitoring table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS etl_jobs (
                job_id TEXT PRIMARY KEY,
                job_name TEXT,
                status TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                records_processed INTEGER,
                errors_count INTEGER,
                error_details TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    
    def populate_time_dimension(self, start_date: str = '2023-01-01', end_date: str = '2024-12-31'):
        """Populate time dimension table"""
        conn = sqlite3.connect(self.db_path)
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        date_range = pd.date_range(start=start, end=end, freq='D')
        
        time_data = []
        for date in date_range:
            time_data.append({
                'date_key': int(date.strftime('%Y%m%d')),
                'full_date': date.strftime('%Y-%m-%d'),
                'year': date.year,
                'quarter': date.quarter,
                'month': date.month,
                'week': date.isocalendar()[1],
                'day_of_week': date.weekday() + 1,
                'day_name': date.strftime('%A'),
                'month_name': date.strftime('%B'),
                'is_weekend': date.weekday() >= 5
            })
        
        df = pd.DataFrame(time_data)
        df.to_sql('dim_time', conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Time dimension populated with {len(time_data)} records")
    
    def load_sample_data(self):
        """Load sample data into staging table"""
        conn = sqlite3.connect(self.db_path)
        
        sample_data = [
            ('John Smith', 'john@email.com', 'Laptop Pro', 'Electronics', '2024-01-15', 1, 1299.99, 1299.99),
            ('Jane Doe', 'jane@email.com', 'Wireless Mouse', 'Electronics', '2024-01-16', 2, 29.99, 59.98),
            ('Bob Johnson', 'bob@email.com', 'Office Chair', 'Furniture', '2024-01-17', 1, 199.99, 199.99),
            ('Alice Brown', 'alice@email.com', 'Smartphone', 'Electronics', '2024-01-18', 1, 799.99, 799.99),
            ('Charlie Wilson', 'charlie@email.com', 'Desk Lamp', 'Furniture', '2024-01-19', 3, 39.99, 119.97)
        ]
        
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT INTO staging_sales 
            (customer_name, customer_email, product_name, category, sale_date, quantity, unit_price, total_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', sample_data)
        
        conn.commit()
        conn.close()
        logger.info(f"Sample data loaded: {len(sample_data)} records")
    
    # Allowlist of valid table and column names for SQL injection prevention
    VALID_TABLES = {'staging_sales', 'dim_customers', 'dim_products', 'dim_time', 'fact_sales', 'etl_jobs'}

    def _validate_identifier(self, name: str, kind: str = 'table') -> str:
        """Validate a SQL identifier against known schema to prevent SQL injection."""
        import re
        if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
            raise ValueError(f"Invalid {kind} name: {name!r}")
        if kind == 'table' and name not in self.VALID_TABLES:
            raise ValueError(f"Unknown table: {name!r}. Valid tables: {self.VALID_TABLES}")
        return name

    def data_quality_check(self, table_name: str) -> Dict[str, Any]:
        """Perform data quality checks"""
        # Validate table name against allowlist to prevent SQL injection
        self._validate_identifier(table_name, 'table')

        conn = sqlite3.connect(self.db_path)
        
        quality_report = {
            'table_name': table_name,
            'total_records': 0,
            'null_checks': {},
            'duplicate_checks': {},
            'data_types': {},
            'issues': []
        }
        
        try:
            # Get total records
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            quality_report['total_records'] = cursor.fetchone()[0]
            
            # Get table info — used to discover column names from schema
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            for column in columns:
                col_name = column[1]
                # Validate column name from PRAGMA result
                self._validate_identifier(col_name, 'column')
                
                # Check for nulls
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL")
                null_count = cursor.fetchone()[0]
                quality_report['null_checks'][col_name] = null_count
                
                if null_count > 0:
                    quality_report['issues'].append(f"Column {col_name} has {null_count} null values")
            
            # Check for duplicates in staging table
            if table_name == 'staging_sales':
                cursor.execute('''
                    SELECT customer_email, product_name, sale_date, COUNT(*) as cnt
                    FROM staging_sales 
                    GROUP BY customer_email, product_name, sale_date 
                    HAVING COUNT(*) > 1
                ''')
                duplicates = cursor.fetchall()
                quality_report['duplicate_checks']['potential_duplicates'] = len(duplicates)
                
                if duplicates:
                    quality_report['issues'].append(f"Found {len(duplicates)} potential duplicate records")
        
        except Exception as e:
            quality_report['issues'].append(f"Error during quality check: {str(e)}")
        
        finally:
            conn.close()
        
        return quality_report
    
    def etl_process(self) -> str:
        """Execute ETL process"""
        job_id = str(uuid.uuid4())
        start_time = datetime.now()
        conn = None
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Log job start
            cursor.execute('''
                INSERT INTO etl_jobs (job_id, job_name, status, start_time, records_processed, errors_count)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (job_id, 'ETL Process', 'RUNNING', start_time, 0, 0))
            conn.commit()
            
            records_processed = 0
            errors_count = 0
            
            # Extract unprocessed data from staging
            staging_df = pd.read_sql_query('''
                SELECT * FROM staging_sales WHERE processed = FALSE
            ''', conn)
            
            if staging_df.empty:
                logger.info("No new data to process")
                return job_id
            
            # Transform and load customers
            customers_df = staging_df[['customer_name', 'customer_email']].drop_duplicates()
            for _, customer in customers_df.iterrows():
                cursor.execute('''
                    INSERT OR IGNORE INTO dim_customers 
                    (customer_name, email, created_date, updated_date)
                    VALUES (?, ?, ?, ?)
                ''', (customer['customer_name'], customer['customer_email'], 
                     datetime.now().date(), datetime.now().date()))
            
            # Transform and load products
            products_df = staging_df[['product_name', 'category']].drop_duplicates()
            for _, product in products_df.iterrows():
                cursor.execute('''
                    INSERT OR IGNORE INTO dim_products 
                    (product_name, category, created_date, updated_date)
                    VALUES (?, ?, ?, ?)
                ''', (product['product_name'], product['category'], 
                     datetime.now().date(), datetime.now().date()))
            
            # Load fact table
            for _, row in staging_df.iterrows():
                try:
                    # Get customer_id
                    cursor.execute('SELECT customer_id FROM dim_customers WHERE email = ?', 
                                 (row['customer_email'],))
                    customer_id = cursor.fetchone()[0]
                    
                    # Get product_id
                    cursor.execute('SELECT product_id FROM dim_products WHERE product_name = ?', 
                                 (row['product_name'],))
                    product_id = cursor.fetchone()[0]
                    
                    # Get date_key
                    sale_date = pd.to_datetime(row['sale_date'])
                    date_key = int(sale_date.strftime('%Y%m%d'))
                    
                    # Insert into fact table
                    cursor.execute('''
                        INSERT INTO fact_sales 
                        (sale_id, customer_id, product_id, date_key, quantity, unit_price, 
                         total_amount, discount, profit, created_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (str(uuid.uuid4()), customer_id, product_id, date_key,
                         row['quantity'], row['unit_price'], row['total_amount'],
                         0, row['total_amount'] * 0.2, datetime.now()))
                    
                    # Mark as processed
                    cursor.execute('UPDATE staging_sales SET processed = TRUE WHERE id = ?', 
                                 (row['id'],))
                    
                    records_processed += 1
                    
                except Exception as e:
                    errors_count += 1
                    logger.error(f"Error processing record {row['id']}: {str(e)}")
            
            # Update job status
            end_time = datetime.now()
            cursor.execute('''
                UPDATE etl_jobs 
                SET status = ?, end_time = ?, records_processed = ?, errors_count = ?
                WHERE job_id = ?
            ''', ('COMPLETED', end_time, records_processed, errors_count, job_id))
            
            conn.commit()
            
            logger.info(f"ETL job {job_id} completed. Processed: {records_processed}, Errors: {errors_count}")
            
        except Exception as e:
            # Update job status to failed
            try:
                fail_conn = sqlite3.connect(self.db_path)
                fail_cursor = fail_conn.cursor()
                fail_cursor.execute('''
                    UPDATE etl_jobs 
                    SET status = ?, end_time = ?, error_details = ?
                    WHERE job_id = ?
                ''', ('FAILED', datetime.now(), str(e), job_id))
                fail_conn.commit()
                fail_conn.close()
            except Exception:
                pass
            
            logger.error(f"ETL job {job_id} failed: {str(e)}")
        
        finally:
            if conn is not None:
                conn.close()
        
        return job_id
    
    def get_analytics_report(self) -> Dict[str, Any]:
        """Generate analytics report from data warehouse"""
        conn = sqlite3.connect(self.db_path)
        
        report = {}
        
        try:
            # Sales summary
            sales_summary = pd.read_sql_query('''
                SELECT 
                    COUNT(*) as total_sales,
                    SUM(total_amount) as total_revenue,
                    AVG(total_amount) as avg_order_value,
                    SUM(quantity) as total_items_sold
                FROM fact_sales
            ''', conn)
            report['sales_summary'] = sales_summary.to_dict('records')[0]
            
            # Top products
            top_products = pd.read_sql_query('''
                SELECT 
                    p.product_name,
                    p.category,
                    COUNT(*) as sales_count,
                    SUM(f.total_amount) as revenue
                FROM fact_sales f
                JOIN dim_products p ON f.product_id = p.product_id
                GROUP BY p.product_id, p.product_name, p.category
                ORDER BY revenue DESC
                LIMIT 5
            ''', conn)
            report['top_products'] = top_products.to_dict('records')
            
            # Sales by month
            monthly_sales = pd.read_sql_query('''
                SELECT 
                    t.year,
                    t.month,
                    t.month_name,
                    COUNT(*) as sales_count,
                    SUM(f.total_amount) as revenue
                FROM fact_sales f
                JOIN dim_time t ON f.date_key = t.date_key
                GROUP BY t.year, t.month, t.month_name
                ORDER BY t.year, t.month
            ''', conn)
            report['monthly_sales'] = monthly_sales.to_dict('records')
            
            # Customer analysis
            customer_analysis = pd.read_sql_query('''
                SELECT 
                    c.customer_name,
                    c.email,
                    COUNT(*) as order_count,
                    SUM(f.total_amount) as total_spent,
                    AVG(f.total_amount) as avg_order_value
                FROM fact_sales f
                JOIN dim_customers c ON f.customer_id = c.customer_id
                GROUP BY c.customer_id, c.customer_name, c.email
                ORDER BY total_spent DESC
                LIMIT 10
            ''', conn)
            report['top_customers'] = customer_analysis.to_dict('records')
            
        except Exception as e:
            report['error'] = str(e)
        
        finally:
            conn.close()
        
        return report
    
    def schedule_etl_jobs(self):
        """Schedule ETL jobs"""
        # Schedule ETL to run every hour
        schedule.every().hour.do(self.etl_process)
        
        # Schedule data quality checks daily
        schedule.every().day.at("02:00").do(self.run_quality_checks)
        
        logger.info("ETL jobs scheduled successfully")
    
    def run_quality_checks(self):
        """Run data quality checks on all tables"""
        tables = ['staging_sales', 'dim_customers', 'dim_products', 'fact_sales']
        
        for table in tables:
            quality_report = self.data_quality_check(table)
            logger.info(f"Quality check for {table}: {len(quality_report['issues'])} issues found")
            
            if quality_report['issues']:
                for issue in quality_report['issues']:
                    logger.warning(f"Data quality issue in {table}: {issue}")

def demo_data_warehouse():
    """Demonstrate data warehouse automation"""
    print("=== Data Warehouse Automation Demo ===\\n")
    
    # Initialize data warehouse
    dw = DataWarehouseAutomation()
    
    # Populate time dimension
    dw.populate_time_dimension()
    
    # Load sample data
    dw.load_sample_data()
    
    # Run data quality checks
    print("Running data quality checks...")
    quality_report = dw.data_quality_check('staging_sales')
    print(f"Quality check results: {len(quality_report['issues'])} issues found")
    
    # Run ETL process
    print("\\nRunning ETL process...")
    job_id = dw.etl_process()
    print(f"ETL job completed: {job_id}")
    
    # Generate analytics report
    print("\\nGenerating analytics report...")
    report = dw.get_analytics_report()
    
    print("\\n=== Sales Summary ===")
    summary = report['sales_summary']
    print(f"Total Sales: {summary['total_sales']}")
    print(f"Total Revenue: ${summary['total_revenue']:,.2f}")
    print(f"Average Order Value: ${summary['avg_order_value']:,.2f}")
    
    print("\\n=== Top Products ===")
    for product in report['top_products']:
        print(f"{product['product_name']} ({product['category']}): ${product['revenue']:,.2f}")
    
    print("\\n=== Top Customers ===")
    for customer in report['top_customers']:
        print(f"{customer['customer_name']}: ${customer['total_spent']:,.2f} ({customer['order_count']} orders)")

if __name__ == '__main__':
    demo_data_warehouse()

