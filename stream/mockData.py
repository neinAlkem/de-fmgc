from confluent_kafka import Producer
import simplejson as json
from faker import Faker
import uuid
import random
from datetime import datetime
from fastapi import FastAPI, Query
import time

products = [
    (1, 'Coffee', 'Ameratto', 'Regular', 5.0, 2.0),
    (2, 'Coffee', 'Columbian', 'Regular', 4.0, 1.5),
    (3, 'Coffee', 'Decaf Irish Cream', 'Decaf', 8.0, 3.0),
    (4, 'Espresso', 'Caffe Latte', 'Regular', 7.0, 2.5),
    (5, 'Espresso', 'Caffe Mocha', 'Regular', 7.0, 3.0),
    (6, 'Espresso', 'Decaf Espresso', 'Decaf', 5.0, 1.0),
    (7, 'Espresso', 'Regula Espresso', 'Regular', 4.0, 0.5),
    (8, 'Herbal Tea', 'Chamomile', 'Decaf', 8.0, 4.0),
    (9, 'Herbal Tea', 'Lemon', 'Decaf', 10.0, 4.0),
    (10, 'Herbal Tea', 'Mint', 'Decaf', 12.0, 6.0),
    (11, 'Tea', 'Darjeeling', 'Regular', 6.0, 1.0),
    (12, 'Tea', 'Earl Grey', 'Regular', 7.0, 2.0),
    (12, 'Tea', 'Green Tea', 'Regular', 9.0, 3.0)
]

store = [
    ('California', 'West', 'Major Market', 209),
    ('Colorado', 'Central', 'Major Market', 303),
    ('Connecticut', 'East', 'Small Market', 203),
    ('Florida', 'East', 'Major Market', 239),
    ('Illnois', 'Central', 'Major Market', 217),
    ('Iowa', 'Central', 'Small Market', 319),
    ('Louisiana', 'South', 'Small Market', 225),
    ('Massachusetts', 'East', 'Major Market', 339),
    ('Missouri', 'Central', 'Small Market', 314),
    ('Nevada', 'West', 'Small Market', 702),
    ('New Hampshire', 'East', 'Small Market', 603),
    ('New Mexico', 'South', 'Small Market', 505),
    ('New York', 'East', 'Major Market', 212),
    ('Ohio', 'Central', 'Major Market', 216),
    ('Oklahoma', 'South', 'Small Market', 405),
    ('Oregon', 'West', 'Small Market', 503),
    ('Texas', 'West', 'Major Market', 210),
    ('Utah', 'West', 'Small Market', 435),
    ('Washington', 'West', 'Small Market', 206),
    ('Wisconsin', 'Central', 'Small Market', 262)
]

inventoryStores = {}
fake = Faker()
def generateData():
    productId, productType, product, types, price, cogs= random.choice(products)
    state, market, marketSize, areaCode = random.choice(store)
    
    startDate = datetime(2010, 1, 1)
    endDate = datetime.today()
    
    quantity = random.randint(1,10)
    totalPrice = quantity * price
    cogsPrice = cogs * quantity
    
    storeKey = (state, market, marketSize, areaCode)
    inventoryKey = (storeKey, productId)
    
    if inventoryKey not in inventoryStores:
        inventoryStores[inventoryKey] = random.randint(100, 500)
        
    
    inventoryBefore = inventoryStores[inventoryKey]
    inventoryAfter = inventoryBefore - quantity
    if inventoryAfter < 0 :
        inventoryAfter = 0
        
    inventoryStores[inventoryKey] = inventoryAfter
    
    return{
        'Transaction Id': f'T-{uuid.uuid4()}',
        'Date Purchased': fake.date_time_between_dates(datetime_start=startDate, datetime_end=endDate).isoformat(),
        'Area Code': areaCode,
        'State': state,
        'Market': market,
        'Market Size': marketSize,
        'Product Id': productId,
        'Product Type': productType,
        'Product': product,
        'Type': types,
        'Quantity': quantity,
        'Unit Price': price,
        'Total Price': totalPrice,
        'Cogs' : cogsPrice,
        'Inventory Latest': inventoryBefore,
        'Inventory After': inventoryAfter
    }

app = FastAPI()
producerConfig = {
    'bootstrap.servers' : 'localhost:9092'
}

producer = Producer(producerConfig)

@app.post('/producer')
async def publish(topic: str = Query('posTransaction'),count: int = Query(100), interval: float = Query(5.0)):
    topic = topic.strip()
    result = []
    for _ in range(count):  
        value = generateData()
        message = json.dumps(value).encode('utf-8')
        result.append(value)
        
        producer.produce(topic, message)
        producer.flush()
        time.sleep(interval)
     
    return {'Status' : 'Sent', 'Topic' : topic, 'Message' : result}
