import psycopg2
import os
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
log = logging.getLogger()

class trainingDataExtractor:
    def __init__(self, db_params, output_dir):
        self.db_params = db_params
        self.output_dir = output_dir
        self.product_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

    def extract(self):
        conn = None
        try:
            # Connect to DB
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            for pid in self.product_ids:
                try:
                    copy_sql = f"""
                        COPY (
                            SELECT 
                                date AS ds,
                                total_sales AS y
                            FROM 
                                dev_marts.marts_products_daily_sales
                            WHERE product_id = {pid} 
                            ORDER BY date 
                        )
                        TO STDOUT WITH CSV HEADER DELIMITER ';'
                    """
                    
                    file_path = os.path.join(self.output_dir, f"{pid}.csv")
                    
                    with open(file_path, 'w') as f:
                        cursor.copy_expert(copy_sql, f)
                    
                    log.info(f"Successfully extracted Product {pid} to {file_path}")
                    
                except Exception as e:
                    log.error(f"Error extracting product {pid}: {e}")

            cursor.close()
            conn.close()
            log.info("Data Extraction Complete.")

        except Exception as e:
            log.error(f"Database connection failed: {e}")
            if conn:
                conn.close()