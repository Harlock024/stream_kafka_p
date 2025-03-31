from kafka import KafkaConsumer
import json
from pandas.core.internals.blocks import re
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

uri = os.getenv('DATABASE_PG_URL')

print('connecting pg ...')
try:
    with psycopg2.connect(uri) as conn:
    # conn = psycopg2.connect(database = "",
    #                     user = "postgres",
    #                     host= 'localhost',
    #                     password = "1234",
    #                     port = 5432)
        cur = conn.cursor()
    print("PosgreSql Connected successfully!")
except:
    print("Could not connect to PosgreSql")




consumer = KafkaConsumer('spotify',bootstrap_servers=['localhost:9092'])


for msg in consumer:
    try:
        record = json.loads(msg.value.decode('utf-8'))

        if isinstance(record, dict) and "0" in record:
            raw_json = json.loads(record["0"])


            track_title = raw_json.get("track_title", "Unknown")
            artists = raw_json.get("artists", "Unknown")
            duration_ms = int(raw_json.get("duration_ms", 0))

            sql = "INSERT INTO tracks (track_title, artists, duration_ms) VALUES (%s, %s, %s)"
            cur.execute(sql, (track_title, artists, duration_ms))
            conn.commit()

            print(f"Inserted: {track_title} - {artists} ({duration_ms}ms)")
    except json.JSONDecodeError:
            print("Error: Invalid JSON received")
    except KeyError as e:
            print(f"Missing key in JSON: {e}")
    except Exception as e:
            print(f"Error inserting into PostgreSQL: {e}")

conn.close()
print("Connection closed")
