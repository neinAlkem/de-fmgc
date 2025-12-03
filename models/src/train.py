import os
import logging
import pandas as pd
import numpy as np
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
    
    # ----------------------------------------------
    # LOAD + CLEANING (MISSING DATE + OUTLIER)
    # ----------------------------------------------
    def _loadData(self, path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"File {path} not found.")

        df = pd.read_csv(path, sep=';', parse_dates=['ds'])
        df = df.sort_values('ds').dropna(subset=['ds','y'])
        df['y'] = df['y'].astype(float)

        # --- Make daily frequency to avoid Prophet issues ---
        df = df.set_index('ds').asfreq('D')

        # --- Fill with interpolation for missing days ---
        df['y'] = df['y'].interpolate()

        # --- Outlier Smoothing (IQR) ---
        Q1 = df['y'].quantile(0.25)
        Q3 = df['y'].quantile(0.75)
        IQR = Q3 - Q1
        lower, upper = Q1 - 1.5*IQR, Q3 + 1.5*IQR
        df['y'] = np.clip(df['y'], lower, upper)

        df = df.reset_index()  
        return df
    
    # ----------------------------------------------
    # TRAIN MODEL
    # ----------------------------------------------
    def _trainModel(self, df, cp_scale, sp_scale, extra_regressors=None):
        model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=True,
            yearly_seasonality=False,
            changepoint_prior_scale=cp_scale,
            seasonality_prior_scale=sp_scale,
        )
        
        if extra_regressors:
            for r in extra_regressors:
                model.add_regressor(r)

        model.fit(df)
        return model
    
    # ----------------------------------------------
    # EVALUATE MODEL (MAE)
    # ----------------------------------------------
    def _evaluate(self, model, df):
        preds = model.predict(df[['ds']])
        return mae(df['y'], preds['yhat'])
    
    # ----------------------------------------------
    # HYPERPARAMETER TUNING
    # ----------------------------------------------
    def _tune(self, df):
        param_grid = [
            (0.01, 5),
            (0.1, 10),
            (0.3, 10),
            (0.5, 15)
        ]

        best_mae = float('inf')
        best_params = None
        best_model = None

        for cp, sp in param_grid:
            try:
                model = self._trainModel(df, cp, sp)
                score = self._evaluate(model, df)

                if score < best_mae:
                    best_mae = score
                    best_params = (cp, sp)
                    best_model = model

            except Exception as e:
                log.error(f"Tuning error: {e}")
                continue

        return best_model, best_params, best_mae
    
    # ----------------------------------------------
    # FULL PIPELINE
    # ----------------------------------------------
    def modelPipeline(self, extra_regressors=None):
        for product in range(1, 13):
            filePath = os.path.join(self.input_dir, f"{product}.csv")
            
            try:
                log.info(f"Loading data from {filePath}")
                df = self._loadData(filePath)

                model, best_params, best_score = self._tune(df)
                cp, sp = best_params

                log.info(
                    f"Product {product} | MAE: {best_score:.2f} | "
                    f"cp={cp}, sp={sp}"
                )

                model_path = os.path.join(self.output_dir, f"model_{product}.joblib")
                dump(model, model_path)

                self.results_summary.append({
                    'product_code': product,
                    'mae': best_score,
                    'changepoint_prior_scale': cp,
                    'seasonality_prior_scale': sp,
                    'model_path': model_path
                })

            except Exception as e:
                log.error(f"Fail to process product {product}. Error: {e}")

        summary_df = pd.DataFrame(self.results_summary)
        log.info("Pipeline Selesai.")
        return summary_df

if __name__ == "__main__":
    pipeline = forecastModel(input_dir=input_dir, output_dir=output_dir)
    summary = pipeline.modelPipeline()
    print(summary)
