from joblib import load
import os
import logging
import pandas as pd
import psycopg2

logging.basicConfig(format='%(asctime)s %(message)s',
                    level=logging.INFO)
log = logging.getLogger()

OUTPUT_DIR = "models/predictions"
os.makedirs(OUTPUT_DIR, exist_ok=True)

class forecastPredictor():
    def __init__(self, db_params):
         self.db_params = db_params
    
    def _connect_db(self):
        return psycopg2.connect(**self.db_params)
    
    def _save_to_postgres(self,df):
        conn = self._connect_db()
        curr = conn.cursor()
        curr.execute("CREATE SCHEMA IF NOT EXISTS dev_marts;")
        
        create_table = """
            CREATE TABLE IF NOT EXISTS dev_marts.product_forecast(
                ds DATE,
                product INT,
                yhat DOUBLE PRECISION, 
                yhat_lower DOUBLE PRECISION,
                yhat_upper DOUBLE PRECISION
            );
            """
        curr.execute(create_table)
        
        delete_previous_prediction = """
            DELETE FROM dev_marts.product_forecast
        """
        curr.execute(delete_previous_prediction)
        
        insert_query = """
            INSERT INTO dev_marts.product_forecast (ds, product, yhat, yhat_lower, yhat_upper)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        for _, row in df.iterrows():
            curr.execute(insert_query, (
                row["ds"],
                row["product"],
                row["yhat"],
                row["yhat_lower"],
                row["yhat_upper"]
            ))

        conn.commit()
        curr.close()
        conn.close()
        log.info("Data successfully inserted into SQL.")

    def predict_next_7_days(self,model):
        future = model.make_future_dataframe(periods=7, freq='D')
        if "is_weekend" not in future.columns:
            future["is_weekend"] = future["ds"].dt.weekday >= 5
        forecast = model.predict(future)
        next_7 = forecast[['ds','yhat','yhat_lower','yhat_upper']].tail(7)
        
        return next_7

    def predict_all_products(self):
        predictions = []
        
        for product in range(1,13):
            model_path = f'models/model/model_{product}.joblib' 
            if not os.path.exists(model_path):
                    log.error(f"Model for product {product} not found. Skipping...")
                    continue
                    
            model = load(model_path)
            forecast_7 = self.predict_next_7_days(model)
            
            forecast_7["product"] = product
            predictions.append(forecast_7)
            
            output_path = os.path.join(OUTPUT_DIR, f"product_{product}.csv")
            forecast_7.to_csv(output_path, index=False)
            log.info(f"Saved CSV for product {product}: {output_path}")

        if len(predictions) == 0:
            return pd.DataFrame()

        return pd.concat(predictions, ignore_index=True)
            
if __name__ == '__main__':
    dbParams = {
    'host': 'localhost',
    'database': 'fmgc',
    'user': 'airflow',
    'password': 'airflow',
    'port': '5435'
}
    
    predictor = forecastPredictor(db_params=dbParams)
    all_forecast = predictor.predict_all_products()
    if not all_forecast.empty:
        predictor._save_to_postgres(all_forecast)
    print(all_forecast)