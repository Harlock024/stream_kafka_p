from kafka import KafkaProducer
import json
import pandas as pd


url = "https://raw.githubusercontent.com/Harlock024/stream_kafka_p/refs/heads/master/results/data.json"


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_send_success(record_metadata):
   print(record_metadata.topic)
   print(record_metadata.partition)
   print(record_metadata.offset)

def on_send_error(excp):
   print('I am an errback')

df =pd.read_json(url,orient="columns")

for index,value in df.head(100).iterrows():
    dict_data = dict(value)
    producer.send('spotify', value=dict_data)
    print(dict_data[0])

producer.close()
