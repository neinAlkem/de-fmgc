import psycopg2
import logging

logging.basicConfig(format='%(asctime)s %(message)s',
                    level=logging.INFO)
log = logging.getLogger()

conn = None
params = {
    'host': 'localhost',
    'database': 'fmgc',
    'user': 'airflow',
    'password': 'airflow',
    'port': '5435'
}

try:
    """ 
        This block of code is responsible for connecting to a PostgreSQL database using the `psycopg2`, library in Python, creating project schemas and a table if they do not already exist, and then closing the database connection.
    """
    
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    
    log.info("Creating Schema if not exists...")
    stagingSchema = """
        CREATE SCHEMA IF NOT EXISTS dev_stg; 
        CREATE SCHEMA IF NOT EXISTS dev_dbo;
        CREATE SCHEMA IF NOT EXISTS dev_marts
        """
    cursor.execute(stagingSchema)
    
    log.info("Creating Table if not exists...")
    rawPos_table = """
        CREATE TABLE IF NOT EXISTS dev_stg.pos 
        ( 
            transaction_id VARCHAR PRIMARY KEY, 
            date_purchased DATE,
            area_code VARCHAR,
            store_id VARCHAR,
            product_id VARCHAR,
            quantity VARCHAR,
            unit_price VARCHAR,
            total_price VARCHAR,
            cogs VARCHAR,
            inventory_latest VARCHAR,
            inventory_after VARCHAR
            )"""
    
    cursor.execute(rawPos_table)
    
    conn.commit()
    log.info('Successfully creating stg.pos table...')

except (Exception, psycopg2.Error) as e:
    log.error(f'Error while connecting to DB: {e}')
    
finally:
    if conn:
        cursor.close()
        conn.close()
        log.info('DB Connection closed...')

