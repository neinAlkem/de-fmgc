import os
import logging
import pandas as pd
from prophet import Prophet
from joblib import dump
from sklearn.metrics import mean_absolute_error as mae

logging.basicConfig(format='%(asctime)s %(message)s',
                    level=logging.INFO)
log = logging.getLogger()

output_dir = "models/model"
input_dir = "models/trainingData"

class forecastModel:
    def __init__(self, input_dir, output_dir):
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.results_summary = []
        
        os.makedirs(output_dir, exist_ok=True)
    
    def _loadData(self, path):
        if not os.path.exists(path) :
            raise FileNotFoundError(f"File {path} not found..")
        df = pd.read_csv(path, sep=';', parse_dates=['ds'])
        df = df.sort_values('ds').dropna(subset=(['ds','y']))
        df['y'] = df['y'].astype(float)
        return df
    
    def _trainModel(self, df, extra_regressors=None):
        model = Prophet(daily_seasonality=False, weekly_seasonality=True, yearly_seasonality=False)
        if extra_regressors:
            for r in extra_regressors:
                model.add_regressor(r)
        model.fit(df)
        return model
    
    def _evaluate(self,model, df):
        future = model.predict(df[["ds"]])
        y_true = df["y"].values
        y_pred = future["yhat"].values
        return mae(y_true, y_pred)
    
    def modelPipeline(self, extra_regressors=None):
        product = 1
        while product <= 12 :
            fileName = f'{product}.csv'
            data_path = os.path.join(f'{self.input_dir}/{fileName}')
            
            try:    
                log.info(f'Loading data from {data_path}')
                df = self._loadData(data_path)

                model = self._trainModel(df, extra_regressors)

                score_mae = self._evaluate(model, df)
                log.info(f"Objek {data_path} selesai. MAE: {score_mae:.2f}")

                safe_filename = str(product).replace(" ", "_").replace("/", "-")
                model_path = os.path.join(self.output_dir, f"model_{safe_filename}.joblib")
                dump(model, model_path)

                self.results_summary.append({
                    'product_code': product,
                    'mae': score_mae,
                    'model_path': model_path
                })
            except Exception as e:
                    log.error(f"Fail to process product {product}. Error: {e}")
            finally:
                    product+=1

        summary_df = pd.DataFrame(self.results_summary)
        log.info("Pipeline Selesai.")
        return summary_df

if __name__ == "__main__":
    # Inisialisasi object
    pipeline = forecastModel(input_dir=input_dir, output_dir=output_dir)
    
    # Jalankan pipeline
    summary = pipeline.modelPipeline()
    print(summary)
            
             
        
    