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
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    
    log.info("Creating Schema if not exists...")
    staging_schema = 'CREATE SCHEMA IF NOT EXISTS staging'
    cursor.execute(staging_schema)
    
    log.info("Creating Table if not exists...")
    rawPos_table = """CREATE TABLE IF NOT EXISTS staging.pos 
        ( 
            transaction_id VARCHAR PRIMARY KEY, 
            date_purchased DATE,
            area_code CHAR(3),
            store_id CHAR(8),
            product_id CHAR(2),
            quantity INT,
            unit_price FLOAT,
            total_price FLOAT,
            cogs FLOAT,
            inventory_latest INT,
            inventory_after INT
            )"""
    
    cursor.execute(rawPos_table)
    
    conn.commit()
    log.info('Successfully creating table and schema...')

except (Exception, psycopg2.Error) as e:
    log.error(f'Error while connecting to DB: {e}')
    
finally:
    if conn:
        cursor.close()
        conn.close()
        log.info('DB Connection closed...')

