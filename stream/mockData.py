from confluent_kafka import Producer
import simplejson as json
from faker import Faker
import uuid
import random
from datetime import datetime
from fastapi import FastAPI, Query
import threading
import logging
import time

logging.basicConfig(format='%(asctime)s %(message)s',
                    level=logging.INFO)
log = logging.getLogger()

# ---------------------------------------------------------------------------- #
#                                GLOBAL SETTING                                #
# ---------------------------------------------------------------------------- #
running = False
producer_thread = None

# ---------------------------------------------------------------------------- #
#                              POS MOCK DATA FAKER                             #
# ---------------------------------------------------------------------------- #
stores = [
    ("S-000001", "Daily Grind Cafe", 314),
    ("S-000002", "Rustic Bean Roastery", 212),
    ("S-000003", "Rustic Bean Brew Bar", 503),
    ("S-000004", "Amber & Oak Espresso Lab", 314),
    ("S-000005", "Dawn & Dusk Coffee Works", 505),
    ("S-000006", "Rustic Bean Roastery", 239),
    ("S-000007", "Iron Bloom Coffee Bar", 405),
    ("S-000008", "Peak Coffee House", 239),
    ("S-000009", "Peak Coffee Cafe", 206),
    ("S-000010", "Hidden Roastery", 339),
    ("S-000011", "BrewHub Roastery", 339),
    ("S-000012", "Peak Coffee Espresso Lab", 217),
    ("S-000013", "Crafted CupVelvet Bloom Espresso Lab", 435),
    ("S-000014", "Iron Bloom Coffee Co.", 339),
    ("S-000015", "Blue Ember Brew Bar", 210),
    ("S-000016", "DripHouse Espresso Lab", 239),
    ("S-000017", "Golden Crest Coffee Bar", 209),
    ("S-000018", "Nordic Pour Coffee Works", 216),
    ("S-000019", "Urban Roast Coffee Bar", 262),
    ("S-000020", "Golden Crest Coffee Bar", 505),
    ("S-000021", "Roast District Coffee Works", 209),
    ("S-000022", "Cinder Oak Roastery", 405),
    ("S-000023", "BrewHub Cafe", 505),
    ("S-000024", "Steam & Pour Coffee Co.", 210),
    ("S-000025", "Midnight Drip Coffee Works", 206),
    ("S-000026", "Daily Grind Coffee Works", 225),
    ("S-000027", "Roast District Brew Bar", 435),
    ("S-000028", "Dawn & Dusk Espresso Lab", 203),
    ("S-000029", "Hidden Brew Bar", 702),
    ("S-000030", "BeanCraft Coffee Bar", 503),
    ("S-000031", "Hearthstone  Coffee Co.", 225),
    ("S-000032", "Iron Bloom Roastery", 225),
    ("S-000033", "Midnight Drip Roastery", 216),
    ("S-000034", "Hearthstone  Roastery", 435),
    ("S-000035", "Hollow Grind House", 239),
    ("S-000036", "Everbrew Brew Bar", 435),
    ("S-000037", "Daily Grind Cafe", 217),
    ("S-000038", "Iron Bloom Cafe", 339),
    ("S-000039", "BrewHub Coffee Co.", 212),
    ("S-000040", "Java Collective Coffee Co.", 262),
    ("S-000041", "Nordic Pour Brew Bar", 603),
    ("S-000042", "Hearthstone  Brew Bar", 314),
    ("S-000043", "Everbrew Roastery", 206),
    ("S-000044", "Java Collective Roastery", 225),
    ("S-000045", "Golden Crest Brew Bar", 702),
    ("S-000046", "Dawn & Dusk Brew Bar", 262),
    ("S-000047", "Dawn & Dusk Espresso Lab", 262),
    ("S-000048", "Wildmill Cafe", 303),
    ("S-000049", "Urban Roast House", 206),
    ("S-000050", "Crafted CupVelvet Bloom Coffee Works", 603),
    ("S-000051", "Peak Coffee House", 435),
    ("S-000052", "Hidden Cafe", 603),
    ("S-000053", "Nordic Pour Roastery", 206),
    ("S-000054", "Copper Haven Cafe", 503),
    ("S-000055", "Daily Grind Cafe", 339),
    ("S-000056", "DripHouse Coffee Co.", 217),
    ("S-000057", "Nordic Pour Cafe", 209),
    ("S-000058", "Hidden Brew Bar", 503),
    ("S-000059", "Rustic Bean Coffee Bar", 225),
    ("S-000060", "Cinder Oak Coffee Bar", 435),
    ("S-000061", "Roast District Roastery", 217),
    ("S-000062", "Hidden Espresso Lab", 339),
    ("S-000063", "Everbrew Brew Bar", 225),
    ("S-000064", "Golden Crest Espresso Lab", 239),
    ("S-000065", "Peak Coffee Cafe", 303),
    ("S-000066", "Peak Coffee Coffee Works", 319),
    ("S-000067", "Dawn & Dusk Espresso Lab", 435),
    ("S-000068", "Steam & Pour Coffee Co.", 262),
    ("S-000069", "Golden Crest Cafe", 339),
    ("S-000070", "Silver Steam Espresso Lab", 217),
    ("S-000071", "Hollow Grind Espresso Lab", 212),
    ("S-000072", "Midnight Drip Coffee Co.", 239),
    ("S-000073", "Hollow Grind Espresso Lab", 303),
    ("S-000074", "Silver Steam Espresso Lab", 212),
    ("S-000075", "Crafted CupVelvet Bloom Espresso Lab", 603),
    ("S-000076", "Crafted CupVelvet Bloom Coffee Works", 206),
    ("S-000077", "Steam & Pour Cafe", 303),
    ("S-000078", "Hollow Grind Roastery", 217),
    ("S-000079", "Java Collective Roastery", 503),
    ("S-000080", "Iron Bloom Cafe", 212),
    ("S-000081", "Everbrew Brew Bar", 405),
    ("S-000082", "BrewHub Coffee Works", 435),
    ("S-000083", "Silver Steam Cafe", 209),
    ("S-000084", "BrewHub Coffee Co.", 216),
    ("S-000085", "Everbrew House", 503),
    ("S-000086", "Hidden Coffee Works", 210),
    ("S-000087", "Blue Ember Cafe", 603),
    ("S-000088", "Copper Haven Roastery", 702),
    ("S-000089", "Crafted CupVelvet Bloom Coffee Bar", 262),
    ("S-000090", "Golden Crest Espresso Lab", 603),
    ("S-000091", "Dawn & Dusk Brew Bar", 210),
    ("S-000092", "Wildmill Coffee Works", 203),
    ("S-000093", "RoastLab Coffee Co.", 239),
    ("S-000094", "Blue Ember Brew Bar", 314),
    ("S-000095", "Crafted CupVelvet Bloom House", 314),
    ("S-000096", "Wildmill Coffee Works", 262),
    ("S-000097", "Java Collective Brew Bar", 217),
    ("S-000098", "Everbrew Espresso Lab", 217),
    ("S-000099", "Hearthstone  House", 319),
    ("S-000100", "Iron Bloom Coffee Works", 216),
    ("S-000101", "Roast District Coffee Bar", 603),
    ("S-000102", "Hidden Roastery", 206),
    ("S-000103", "Fable Coffee Bar", 503),
    ("S-000104", "Wildmill Coffee Co.", 435),
    ("S-000105", "RoastLab Brew Bar", 212),
    ("S-000106", "Barista Republic Cafe", 702),
    ("S-000107", "Iron Bloom Roastery", 209),
    ("S-000108", "Copper Haven Brew Bar", 225),
    ("S-000109", "Blue Ember Roastery", 702),
    ("S-000110", "Crafted CupVelvet Bloom House", 339),
    ("S-000111", "Golden Crest House", 319),
    ("S-000112", "Golden Crest Roastery", 262),
    ("S-000113", "RoastLab Coffee Bar", 209),
    ("S-000114", "Fable Coffee Works", 206),
    ("S-000115", "Midnight Drip Roastery", 216),
    ("S-000116", "Silver Steam Brew Bar", 505),
    ("S-000117", "Amber & Oak Espresso Lab", 216),
    ("S-000118", "Fable Coffee Works", 210),
    ("S-000119", "Dawn & Dusk Espresso Lab", 217),
    ("S-000120", "Fable Cafe", 435),
    ("S-000121", "BrewHub Cafe", 216),
    ("S-000122", "Golden Crest House", 216),
    ("S-000123", "Wildmill Coffee Co.", 225),
    ("S-000124", "Iron Bloom Coffee Bar", 206),
    ("S-000125", "Hollow Grind Brew Bar", 225),
    ("S-000126", "Silver Steam Coffee Co.", 225),
    ("S-000127", "Roast District Espresso Lab", 209),
    ("S-000128", "Crafted CupVelvet Bloom Roastery", 239),
    ("S-000129", "Silver Steam Espresso Lab", 405),
    ("S-000130", "Blue Ember Roastery", 216),
    ("S-000131", "DripHouse House", 209),
    ("S-000132", "Hidden Coffee Co.", 225),
    ("S-000133", "Crafted CupVelvet Bloom Roastery", 225),
    ("S-000134", "Iron Bloom Roastery", 210),
    ("S-000135", "DripHouse Roastery", 210),
    ("S-000136", "Wildmill Roastery", 239),
    ("S-000137", "Crafted CupVelvet Bloom Coffee Works", 702),
    ("S-000138", "Golden Crest Espresso Lab", 339),
    ("S-000139", "Hearthstone  Coffee Bar", 319),
    ("S-000140", "Wildmill Brew Bar", 314),
    ("S-000141", "Wildmill Coffee Co.", 216),
    ("S-000142", "Peak Coffee Brew Bar", 503),
    ("S-000143", "Fable Coffee Works", 262),
    ("S-000144", "Wildmill Espresso Lab", 212),
    ("S-000145", "Cinder Oak Coffee Works", 210),
    ("S-000146", "Crafted CupVelvet Bloom House", 209),
    ("S-000147", "Wildmill Espresso Lab", 206),
    ("S-000148", "Roast District Roastery", 212),
    ("S-000149", "Everbrew Coffee Co.", 217),
    ("S-000150", "Golden Crest Roastery", 225),
    ("S-000151", "Silver Steam Cafe", 503),
    ("S-000152", "Amber & Oak Coffee Co.", 503),
    ("S-000153", "Steam & Pour Coffee Co.", 225),
    ("S-000154", "Fable Roastery", 225),
    ("S-000155", "Wildmill Cafe", 210),
    ("S-000156", "Peak Coffee Roastery", 212),
    ("S-000157", "Cinder Oak House", 603),
    ("S-000158", "BeanCraft Coffee Bar", 603),
    ("S-000159", "Everbrew Espresso Lab", 339),
    ("S-000160", "RoastLab Brew Bar", 314),
    ("S-000161", "Daily Grind Roastery", 319),
    ("S-000162", "Hearthstone  Roastery", 206),
    ("S-000163", "Dawn & Dusk Coffee Works", 603),
    ("S-000164", "RoastLab House", 314),
    ("S-000165", "Peak Coffee Brew Bar", 319),
    ("S-000166", "Hidden Brew Bar", 505),
    ("S-000167", "BeanCraft House", 212),
    ("S-000168", "RoastLab Cafe", 319),
    ("S-000169", "Hollow Grind Brew Bar", 225),
    ("S-000170", "Dawn & Dusk Espresso Lab", 225),
    ("S-000171", "Peak Coffee Brew Bar", 339),
    ("S-000172", "Rustic Bean Coffee Bar", 303),
    ("S-000173", "Steam & Pour Coffee Bar", 405),
    ("S-000174", "Peak Coffee Brew Bar", 262),
    ("S-000175", "Hearthstone  House", 314),
    ("S-000176", "Golden Crest Coffee Co.", 702),
    ("S-000177", "Cinder Oak Cafe", 303),
    ("S-000178", "Hidden Coffee Co.", 216),
    ("S-000179", "Hollow Grind Coffee Co.", 206),
    ("S-000180", "Hidden Brew Bar", 303),
    ("S-000181", "Wildmill Roastery", 216),
    ("S-000182", "BrewHub Coffee Works", 435),
    ("S-000183", "Blue Ember House", 225),
    ("S-000184", "Hollow Grind House", 210),
    ("S-000185", "Hollow Grind Roastery", 206),
    ("S-000186", "Dawn & Dusk Coffee Works", 209),
    ("S-000187", "BeanCraft Espresso Lab", 603),
    ("S-000188", "DripHouse Coffee Co.", 206),
    ("S-000189", "BrewHub Coffee Bar", 203),
    ("S-000190", "RoastLab Cafe", 209),
    ("S-000191", "Iron Bloom Coffee Works", 209),
    ("S-000192", "BeanCraft Cafe", 503),
    ("S-000193", "Urban Roast Roastery", 314),
    ("S-000194", "Everbrew Coffee Co.", 239),
    ("S-000195", "Peak Coffee Cafe", 303),
    ("S-000196", "BeanCraft Espresso Lab", 405),
    ("S-000197", "Peak Coffee Espresso Lab", 225),
    ("S-000198", "Nordic Pour Coffee Bar", 603),
    ("S-000199", "Java Collective Coffee Works", 405),
    ("S-000200", "Crafted CupVelvet Bloom Brew Bar", 505),
]

products = [
    (1, 'Coffee', 'Ameratto', 'Regular', 5.0, 2.0),
    (2, 'Coffee', 'Columbian', 'Regular', 4.0, 1.5),
    (3, 'Coffee', 'Decaf Irish Cream', 'Decaf', 8.0, 3.0),
    (4, 'Espresso', 'Caffe Latte', 'Regular', 7.0, 2.5),
    (5, 'Espresso', 'Caffe Mocha', 'Regular', 7.0, 3.0),
    (6, 'Espresso', 'Decaf Espresso', 'Decaf', 5.0, 1.0),
    (7, 'Espresso', 'Regular Espresso', 'Regular', 4.0, 0.5),
    (8, 'Herbal Tea', 'Chamomile', 'Decaf', 8.0, 4.0),
    (9, 'Herbal Tea', 'Lemon', 'Decaf', 10.0, 4.0),
    (10, 'Herbal Tea', 'Mint', 'Decaf', 12.0, 6.0),
    (11, 'Tea', 'Darjeeling', 'Regular', 6.0, 1.0),
    (12, 'Tea', 'Earl Grey', 'Regular', 7.0, 2.0),
    (12, 'Tea', 'Green Tea', 'Regular', 9.0, 3.0)
]

# ---------------------------------------------------------------------------- #
#                              MOCK DATA GENERATOR                             #
# ---------------------------------------------------------------------------- #
inventoryStores = {}
fake = Faker()
def generateData():
    """
        The `generateData` function creates simulated transaction data for products and stores, updating
        inventory levels accordingly.
        :return: The `generateData` function returns a dictionary containing transaction details such as
        Transaction Id, Date Purchased, Area Code, Store ID, Product ID, Quantity, Unit Price, Total Price,
        Cost of Goods Sold (COGS), Inventory Latest, and Inventory After.
    """
    
    productId, productType, product, type, price, cogs= random.choice(products)
    storeId, storeName, areaCode = random.choice(stores)
    
    startDate = datetime(2010, 1, 1)
    endDate = datetime.today()
    
    quantity = random.randint(1,10)
    totalPrice = quantity * price
    cogsPrice = cogs * quantity
    
    storeKey = (storeId, storeName, areaCode)
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
        'Store ID': storeId,
        'Product Id': productId,
        'Quantity': quantity,
        'Unit Price': price,
        'Total Price': totalPrice,
        'Cogs' : cogsPrice,
        'Inventory Latest': inventoryBefore,
        'Inventory After': inventoryAfter
    }
    
# ---------------------------------------------------------------------------- #
#                                  KAKFKA PRODUCER                             #
# ---------------------------------------------------------------------------- #

#     The code defines a function to produce messages to a Kafka topic with error handling for delivery
#     reports.
    
#     :param err: The `err` parameter in the `deliveryReport` function is used to capture any error that
#     may occur during the delivery of a message by the producer. If the `err` parameter is not `None`, it
#     indicates that there was an error in delivering the message, and the function logs an error
#     :param msg: The `msg` parameter in the `deliveryReport` function represents the message that was
#     attempted to be delivered by the producer. It contains information such as the key, topic,
#     partition, and offset of the message

producerConfig = {'bootstrap.servers' : 'localhost:9092'}
producer = Producer(producerConfig)

def deliveryReport(err, msg):
    if err is not None:
        log.error(f'Delivery failed {msg.key()} : {err}')
    else:
        log.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        
def produce(topic):
    topic = topic.strip()
    global running
    while running:
            value = generateData()
            message = json.dumps(value).encode('utf-8')
            producer.produce(topic, message, callback=deliveryReport)
            producer.poll(0)
            time.sleep(0.5)
    producer.flush()
    
# ---------------------------------------------------------------------------- #
#                               FASTAPI ENDPOINT                               #
# ---------------------------------------------------------------------------- #
app = FastAPI()

#     The code defines two FastAPI endpoints to start and stop a producer thread for a specified topic.
    
#     :param topic: The `topic` parameter in the `startProducer` function is a string parameter that
#     represents the topic to which messages will be produced. In this case, the default value for the
#     `topic` parameter is set to 'posTransaction', defaults to posTransaction
#     :type topic: str (optional)
#     :return: The `startProducer` function returns a JSON response with the status of the producer,
#     either 'already running' if the producer is already running or 'producer started' if the producer is
#     successfully started.
    
#     :sample usage: 
#         - curl -X POST http://localhost:{port}/start
#         - curl -X POST http://localhost:{port}/stop

@app.post('/start')
def startProducer(topic: str = 'posTransaction'):
    global running
    if running:
        return {'status': 'already running'}
    running = True
    threading.Thread(target=produce, args=(topic,), daemon=True).start()
    return {'status': 'producer started'}

@app.post("/stop")
def stop_producer():
    global running
    running = False
    return {"status": "producer stopped"}