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

    def _loadData(self, path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"File {path} not found.")

        df = pd.read_csv(path, sep=';', parse_dates=['ds'])
        df = df.sort_values('ds').dropna(subset=['ds','y'])
        df['y'] = df['y'].astype(float)

        # Daily frequency
        df = df.set_index('ds').asfreq('D')

        # Interpolate missing
        df['y'] = df['y'].interpolate()

        # Smooth outlier with rolling median
        df['y'] = df['y'].rolling(7, min_periods=1, center=True).median()

        # Add regressor
        df['is_weekend'] = df.index.dayofweek >= 5

        return df.reset_index()

    def _trainModel(self, df, cp_scale, sp_scale, extra_regressors=None):
        model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=True,
            yearly_seasonality=True,
            changepoint_prior_scale=cp_scale,
            seasonality_prior_scale=sp_scale,
        )
        
        if extra_regressors:
            for r in extra_regressors:
                model.add_regressor(r)

        model.fit(df)
        return model
    
    def _evaluate(self, model, df):
        split = int(len(df) * 0.8)
        train = df.iloc[:split]
        test = df.iloc[split:]

        # FIX: include regressors
        future = test[['ds', 'is_weekend']]
        preds = model.predict(future)

        return mae(test['y'], preds['yhat'])

    def _prepare_future(self, df, periods=7):
        last_date = df['ds'].max()
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=periods)
        
        future = pd.DataFrame({"ds": future_dates})
        future["is_weekend"] = future["ds"].dt.dayofweek >= 5
        return future

    def _tune(self, df):
        param_grid = [
            (0.5, 5),
            (1.0, 10),
            (2.0, 10),
            (5.0, 15),
            (10.0, 20)
        ]

        best_mae = float('inf')
        best_params = None
        best_model = None

        for cp, sp in param_grid:
            try:
                model = self._trainModel(df, cp, sp, extra_regressors=['is_weekend'])
                score = self._evaluate(model, df)

                if score < best_mae:
                    best_mae = score
                    best_params = (cp, sp)
                    best_model = model

            except Exception as e:
                log.error(f"Tuning error: {e}")
                continue

        return best_model, best_params, best_mae
    
    def modelPipeline(self):
        for product in range(1, 13):
            filePath = os.path.join(self.input_dir, f"{product}.csv")
            
            try:
                log.info(f"Loading data from {filePath}")
                df = self._loadData(filePath)

                model, best_params, best_score = self._tune(df)
                cp, sp = best_params

                log.info(
                    f"Product {product} | MAE: {best_score:.2f} | cp={cp}, sp={sp}"
                )

                # Save model
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
