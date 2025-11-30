import pandas as pd
import psycopg2
import os
import logging
logging.basicConfig(format='%(asctime)s %(message)s',
                    level=logging.INFO)
log = logging.getLogger()

product_id = [1,2,3,4,5,6,7,8,9,10,11,12]

conn = None
params = {
    'host': 'localhost',
    'database': 'fmgc',
    'user': 'airflow',
    'password': 'airflow',
    'port': '5435'
}

output_dir = "models/trainingData"
os.makedirs(output_dir, exist_ok=True)

for id in product_id:
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        
        getData = f"""
            COPY (
                SELECT 
                    date AS ds,
                    total_sales AS y
                FROM 
                    dev_marts.marts_products_daily_sales
                WHERE product_id = {id} 
                ORDER BY date 
                )
                TO STDOUT WITH CSV HEADER DELIMITER ';'
            """
            
        with open(f"{output_dir}/{id}.csv", 'w') as f:
            cursor.copy_expert(getData, f)

        cursor.close()
        conn.close()
        
        log.info(f"File saved to {output_dir}/{id}.csv")
    except Exception as e:
        log.error(f'Error fetching data: {e}')
