from confluent_kafka import Consumer
import simplejson as json
import psycopg2
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
    #     The `connectDb` function attempts to establish a connection to a PostgreSQL database using the
    #     provided parameters and logs an error message if the connection attempt fails.
    #     :return: The `connectDb()` function is returning a connection object `conn` if the connection to the
    #     database is successful.
    
    try:
        conn = psycopg2.connect(**dbParams)
        return conn
    except (Exception, psycopg2.Error) as e:
        log.error(f'Error while connecting to DB: {e}')

def consumeData(conn, data):    
 
        # The function `consumeData` inserts data into a PostgreSQL database table `raw_pos` using the
        # provided connection and data dictionary.
        
        # :param conn: The `conn` parameter in the `consumeData` function is typically a connection object
        # that represents a connection to a database. This connection object is used to communicate with the
        # database in order to insert data into the specified table
        # :param data: The `consumeData` function is designed to insert data into a database table named
        # `raw_pos`. The function takes two parameters: `conn`, which represents the database connection, and
        # `data`, which is a dictionary containing the following keys and their corresponding values:

    
    insert= """
        INSERT INTO dev_raw.pos  
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
#                                FAST API SETUP                                #
# ---------------------------------------------------------------------------- #
consumer = Consumer(kafkaConf)
consumer.subscribe(['posTransaction'])

    # The code defines a FastAPI application with an asynchronous context manager for managing the
    # lifespan of a database connection and a Kafka consumer for consuming messages.
    
    # :param conn: The `conn` parameter in your code represents a connection to a database. It is created
    # using the `connectDb()` function and is used to interact with the database within your `main`
    # coroutine. This connection is essential for consuming data from messages received by the `consumer`
    # and passing that data to corresponding database

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


