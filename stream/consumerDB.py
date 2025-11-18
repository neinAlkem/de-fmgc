from confluent_kafka import Consumer
import simplejson as json
import psycopg2
import sys
import logging
import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager

logging.basicConfig(format='%(asctime)s %(message)s',
                    level=logging.INFO)
log = logging.getLogger()

# ---------------------------------------------------------------------------- #
#                       SETUP CONSUMER AND DB CONNECTION                       #
# ---------------------------------------------------------------------------- #
conn = None
dbParams = {
    'host': 'localhost',
    'database': 'fmgc',
    'user': 'airflow',
    'password': 'airflow',
    'port': '5435'
}

kafkaConf = {
    'bootstrap.servers' : 'localhost:9092',
    'group.id' : 'transaction',
    'auto.offset.reset' : 'earliest'
}


def connectDb():
    try:
        conn = psycopg2.connect(**dbParams)
        return conn
    except (Exception, psycopg2.Error) as e:
        log.error(f'Error while connecting to DB: {e}')

def consumeData(conn, data):
    insert= """
        INSERT INTO staging.pos 
        (
            transaction_id, 
            date_purchased,
            area_code,
            store_id,
            product_id,
            quantity,
            unit_price,
            total_price,
            cogs,
            inventory_latest,
            inventory_after
            )
        VALUES 
        (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
    """
    
    values= (
        data['Transaction Id'], data['Date Purchased'], data['Area Code'], data['Store ID'], data['Product Id'], data['Quantity'], data['Unit Price'], data['Total Price'], data['Cogs'], data['Inventory Latest'], data['Inventory After']
    )
    
    try:
        cursor = conn.cursor()
        cursor.execute(insert, values)
        conn.commit()
        log.info(f"Inserting {data['Transaction Id']} to Database")
    except (Exception, psycopg2.Error) as e:
        log.error(f'Error while inserting data: {e}')
        conn.rollback()

# ---------------------------------------------------------------------------- #
#                                FAST API SETUL                                #
# ---------------------------------------------------------------------------- #
consumer = Consumer(kafkaConf)
consumer.subscribe(['posTransaction'])

@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = connectDb()
    task = asyncio.create_task(main(conn))
    yield
    consumer.close()
    conn.close()
    task.cancel()
    
app = FastAPI(lifespan=lifespan)

async def main(conn):
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            elif msg.error():
                print(f"Error: {msg.error()}")
                continue
            else:
                data = json.loads(msg.value().decode('utf-8'))
                consumeData(conn, data)
            await asyncio.sleep(0)
    
    except KeyboardInterrupt:
        print("Stopping...")
        
    finally:
        conn.close()
        consumer.close()
        
@app.get("/")
def root():
    return {"status": "Consumer is running"}


