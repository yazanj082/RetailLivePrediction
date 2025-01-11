from kafka import KafkaProducer
import pandas as pd
from json import dumps
import time
from time import sleep

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
df = pd.read_excel('Online Retail.xlsx')

for index, row in df.iterrows():
    producer.send('transactions', value=row)
    time.sleep(0.1)  # Simulates real-time streaming

