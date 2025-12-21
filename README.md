<div align="left">
    <img src="assets/infra.png" width="100%" align="center" style="margin-right: 15px"/>
    <div style="display: inline-block;">
        <h1 style="display: inline-block; vertical-align: middle; margin-top: 0;">REALTIME POS SYSTEM STREAMING AND DATA FORECASTING PIPELINE</h1>
        <p>
</p>
        <p>
	<!-- Shields.io badges disabled, using skill icons. --></p>
        <p>Built with the tools and technologies:</p>
        <p>
        <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="docker">
        <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="python">
        <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="airflow">
        <img src="https://custom-icon-badges.demolab.com/badge/Power%20BI-F1C912?style=for-the-badge&logo=power-bi&logoColor=white?" alt="power-bi">
        <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=Apache%20Kafka&logoColor=white" alt="kafka">
        <img src="https://img.shields.io/badge/postgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="postgresql">
        <img src="https://img.shields.io/badge/Data_Build_Tool_(DBT)-red?style=for-the-badge" alt="DBT">
        <img src="https://img.shields.io/badge/Prophet-white?style=for-the-badge" alt="DBT">
	</p>
    </div>
</div>
<!-- <br clear="left"/> -->

##  Project Overview
This project simulates the complete data flow inside a typical Fast-Moving Consumer Goods (FMCG) company. It is designed to represent how real-world FMCG businesses manage retail demand forecasting, inventory tracking, supply-chain decisions, and operational reporting through data pipelines.

The repository demonstrates a full data engineering + analytics + modeling environment, starting from raw data ingestion all the way to ETL transformation, data warehousing, forecasting models, containerized infrastructure, and business analytics.

The goal is to provide a realistic system that mimics how FMCG companies operate daily using data-driven decisions. 

###  Background
FMCG companies deal with high-volume, fast-moving products that require accurate forecasting, tight inventory control, and robust operational pipelines. Even small fluctuations in consumer demand can cause costly outcomes:

- Stockouts → lost sales and customer dissatisfaction
- Overstocking → capital loss & product expiry
- Inefficient replenishment → logistic inefficiencies

This project captures those real challenges and builds a simulated ecosystem that mirrors real FMCG data workflows.
It includes:

- Raw data ingestion (sales, stores, items, supply-chain data)
- Batch and/or streaming transformations
- Data modeling & business logic
- Warehouse-ready tables and marts
- Forecasting and analytics layers
- Infrastructure setup using Docker and service orchestration

##  Project Goal
1. Build a complete end-to-end data pipeline that simulates real FMCG operations.
2. Forecast retail product demand using predictive modeling.
3. Transform raw data into analytics-ready models through a structured ETL/ELT workflow.
4. Provide actionable business insights through dashboards and analytical reporting.
5. Deploy a fully containerized, production-like data environment for realistic FMCG simulations.

##  Project Structure
<details open>
	<summary><b><code>DE-FMGC/</code></b></summary>
	<details> <!-- __root__ Submodule -->
		<summary><b>__root__</b></summary>
		<blockquote>
			<table>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/docker-compose.yaml'>docker-compose.yaml</a></b></td>
				<td><code>❯ Build image and container for the project</code></td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dockerfile'>dockerfile</a></b></td>
				<td><code>❯ Custom image for airflow to support DBT</code></td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/requirements.txt'>requirements.txt</a></b></td>
				<td><code>❯ Project Depedencies</code></td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbSetup.py'>dbSetup.py</a></b></td>
				<td><code>❯ Initial setup for postgres database</code></td>
			</tr>
			</table>
		</blockquote>
	</details>
	<details> <!-- etl Submodule -->
		<summary><b>etl</b></summary>
		<blockquote>
			<details>
				<summary><b>dags</b></summary>
				<blockquote>
					<details>
						<summary><b>weekly</b></summary>
						<blockquote>
							<table>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/etl/dags/weekly/weekly_salesForecast.py'>weekly_salesForecast.py</a></b></td>
								<td><code>❯ Weekly scheduled jobs</code></td>
							</tr>
							</table>
						</blockquote>
					</details>
					<details>
						<summary><b>yearly</b></summary>
						<blockquote>
							<table>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/etl/dags/yearly/placeholder.txt'>placeholder.txt</a></b></td>
								<td><code>❯ Yearly scheduled jobs</code></td>
							</tr>
							</table>
						</blockquote>
					</details>
					<details>
						<summary><b>daily</b></summary>
						<blockquote>
							<table>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/etl/dags/daily/daily_salesAggregation.py'>daily_salesAggregation.py</a></b></td>
								<td><code>❯ Daily scheduled jobs</code></td>
							</tr>
							</table>
						</blockquote>
					</details>
					<details>
						<summary><b>monthly</b></summary>
						<blockquote>
							<table>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/etl/dags/monthly/placehorder.txt'>placehorder.txt</a></b></td>
								<td><code>❯ Monthly scheduled jobs</code></td>
							</tr>
							</table>
						</blockquote>
					</details>
				</blockquote>
			</details>
		</blockquote>
	</details>
	<details> <!-- dbt_project Submodule -->
		<summary><b>dbt_project</b></summary>
		<blockquote>
			<table>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/.user.yml'>.user.yml</a></b></td>
				<td><code>❯ Auto generated key for DBT</code></td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/dbt_project.yml'>dbt_project.yml</a></b></td>
				<td><code>❯ Models setup and schema</code></td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/package-lock.yml'>package-lock.yml</a></b></td>
				<td><code>❯ Auto write installed depedencies</code></td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/profiles.yml'>profiles.yml</a></b></td>
				<td><code>❯ DBT profiles, used for airflow to recognize DBT module</code></td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/packages.yml'>packages.yml</a></b></td>
				<td><code>❯ DBT project depedencies to install</code></td>
			</tr>
			</table>
			<details>
				<summary><b>macros</b></summary>
				<blockquote>
					<table>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/macros/aggregation.sql'>aggregation.sql</a></b></td>
						<td><code>❯ Custom macro for colums to count lags</code></td>
					</tr>
					</table>
					<details>
						<summary><b>custom_tests</b></summary>
						<blockquote>
							<table>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/macros/custom_tests/test_not_negative.sql'>test_not_negative.sql</a></b></td>
								<td><code>❯ Test case to make sure no negative number in columns</code></td>
							</tr>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/macros/custom_tests/test_date_not_tommorow.sql'>test_date_not_tommorow.sql</a></b></td>
								<td><code>❯ Test case to make sure date is not bigger than today</code></td>
							</tr>
							</table>
						</blockquote>
					</details>
				</blockquote>
			</details>
			<details>
				<summary><b>models</b></summary>
				<blockquote>
					<table>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/schema.yml'>schema.yml</a></b></td>
						<td><code>❯ Models test case and description</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/sources.yml'>sources.yml</a></b></td>
						<td><code>❯ Data source definition</code></td>
					</tr>
					</table>
					<details>
						<summary><b>stg</b></summary>
						<blockquote>
							<table>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/stg/stg_pos.sql'>stg_pos.sql</a></b></td>
								<td><code>❯ Staging table for POS</code></td>
							</tr>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/stg/stg_area.sql'>stg_area.sql</a></b></td>
								<td><code>❯ Staging table for areas</code></td>
							</tr>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/stg/stg_stores.sql'>stg_stores.sql</a></b></td>
								<td><code>❯ Staging table for stores</code></td>
							</tr>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/stg/stg_products.sql'>stg_products.sql</a></b></td>
								<td><code>❯ Staging table for producst</code></td>
							</tr>
							</table>
						</blockquote>
					</details>
					<details>
						<summary><b>marts</b></summary>
						<blockquote>
							<details>
								<summary><b>sales</b></summary>
								<blockquote>
									<table>
									<tr>
										<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/marts/sales/marts_daily_sales.sql'>marts_daily_sales.sql</a></b></td>
										<td><code>❯ Marts to aggregate daily stores sales</code></td>
									</tr>
									<tr>
										<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/marts/sales/marts_products_daily_sales.sql'>marts_products_daily_sales.sql</a></b></td>
										<td><code>❯ Marts to aggregate daily products sales across stores</code></td>
									</tr>
									</table>
								</blockquote>
							</details>
							<details>
								<summary><b>core</b></summary>
								<blockquote>
									<table>
									<tr>
										<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/marts/core/marts_date.sql'>marts_date.sql</a></b></td>
										<td><code>❯ Dates dimension table</code></td>
									</tr>
									</table>
								</blockquote>
							</details>
						</blockquote>
					</details>
					<details>
						<summary><b>dbo</b></summary>
						<blockquote>
							<table>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/dbo/dbo_pos.sql'>dbo_pos.sql</a></b></td>
								<td><code>❯ DBO table for POS</code></td>
							</tr>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/dbo/dbo_products.sql'>dbo_products.sql</a></b></td>
								<td><code>❯ DBO table for products</code></td>
							</tr>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/dbo/dbo_stores.sql'>dbo_stores.sql</a></b></td>
								<td><code>❯ DBO table for stores</code></td>
							</tr>
							<tr>
								<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/dbt_project/models/dbo/dbo_area.sql'>dbo_area.sql</a></b></td>
								<td><code>❯ DBO table for areas</code></td>
							</tr>
							</table>
						</blockquote>
					</details>
				</blockquote>
			</details>
		</blockquote>
	</details>
	<details> <!-- models Submodule -->
		<summary><b>models</b></summary>
		<blockquote>
			<details>
				<summary><b>model</b></summary>
				<blockquote>
					<table>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_10.joblib'>model_10.joblib</a></b></td>
						<td><code>❯ Prediction model for product id = 10</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_1.joblib'>model_1.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 1</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_12.joblib'>model_12.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 12</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_5.joblib'>model_5.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 5</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_3.joblib'>model_3.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 3</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_9.joblib'>model_9.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 9</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_2.joblib'>model_2.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 2</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_11.joblib'>model_11.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 11</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_4.joblib'>model_4.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 4</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_6.joblib'>model_6.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 6</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_7.joblib'>model_7.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 7</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/model/model_8.joblib'>model_8.joblib</a></b></td>
						<td><code>❯  Prediction model for product id = 8</code></td>
					</tr>
					</table>
				</blockquote>
			</details>
			<details>
				<summary><b>src</b></summary>
				<blockquote>
					<table>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/src/predict.py'>predict.py</a></b></td>
						<td><code>❯ Inference scripts to forecast products sales for the next 7 days</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/src/train.py'>train.py</a></b></td>
						<td><code>❯ Train model with new weekly data</code></td>
					</tr>
					<tr>
						<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/models/src/extractData.py'>extractData.py</a></b></td>
						<td><code>❯ Extracting training data from database</code></td>
					</tr>
					</table>
				</blockquote>
			</details>
		</blockquote>
	</details>
	<details> <!-- stream Submodule -->
		<summary><b>stream</b></summary>
		<blockquote>
			<table>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/stream/consumerDB.py'>consumerDB.py</a></b></td>
				<td><code>❯ Send recieved data from kafka to database</code></td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/stream/mockData.py'>mockData.py</a></b></td>
				<td><code>❯ Send mock POS data with FAST API to consumerDB</code></td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/neinAlkem/de-fmgc/blob/master/stream/consumer.py'>consumer.py</a></b></td>
				<td><code>❯ Test script</code></td>
			</tr>
			</table>
		</blockquote>
	</details>
</details>

##  Data Flow
![data flow](assets/data_flow.png)
##  Visualization Report
![dashboard](assets/dashboard_pbi.png)
##  Getting Started
###  Prerequisites

Before getting started with de-fmgc, ensure your runtime environment meets the following requirements:

- **Programming Language:** SQL, Python
- **Package Manager:** Pip or UV
- **Container Runtime:** Docker

###  Installation

**Build from source:**

1. Clone the de-fmgc repository:
```sh
❯ git clone https://github.com/neinAlkem/de-fmgc
```

2. Navigate to the project directory:
```sh
❯ cd de-fmgc
```

3. Create and activate virtual envirovment:
```sh
❯ python -m venv .venv && source .venv/Scrips/activate
```

4. Install Depedencies:
```sh
❯ pip install -r requirements.txt
```

5. Build and run dockerfile:
```sh
❯ docker compose up -d --build
```

6. Run initial setup for database:
```sh
❯ python dbSetup.py
```

7. Start stream services:
```sh
❯ uvicorn stream.mockData:app --port 8001 --reload
❯ uvicorn stream.consumerDB:app --port 8002 --reload ( new +1 terminal )
❯ http://localhost:8001/start ( new +1 terminal )
```

8. Install DBT deps and run models:
```sh
❯ cd dbt_project 
❯ dbt deps && dbt run
```

9. Use airflow (opern in browser - use given user and pass from dockerfile):
```sh
❯ localhost:8080 
```

10. Check inserted data in postgres ( POS table ):
```sh
❯ docker exect -it de-fmgc-postgres-1 psql -U airflow
❯ \c fmgc
❯ select * from dev_dbo.dbo_pos;
```

##  Project Roadmap
- [X] **`Task 1`**: <strike>Build end to end data pipeline.</strike>
- [X] **`Task 2`**: <strike>Implement forecasting model with Prophet model.</strike>
- [X] **`Task 3`**: <strike>Implement apache airflow for orchestration.</strike>
- [X] **`Task 4`**: <strike>Create power BI Dashboard for visualization and auto report.</strike>

