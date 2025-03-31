from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os


load_dotenv()

uri = os.getenv("DATABASE_MONGO_URL")

try:
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.people
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")


consumer = KafkaConsumer('spotify',bootstrap_servers=[
     'localhost:9092'
     ])

for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    try:
        meme_rec = {'data': record}
        print(record)
        record_id = db.spotify.insert_one(meme_rec)
        print("data inserted with record id:", record_id)
    except:
        print("Error inserting data")
