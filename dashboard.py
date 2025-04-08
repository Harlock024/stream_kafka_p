import streamlit as st
import pandas as pd
import requests
import pymongo
import psycopg2
import json


@st.cache_resource
def init_connection():
    return pymongo.MongoClient(st.secrets["mongo"]["uri"])
client = init_connection()


@st.cache_resource
def init_connection_postgres():
    return psycopg2.connect(st.secrets["postgres"]["uri"])
conn = init_connection_postgres()

@st.cache_data(ttl=600)
def get_data_mongo():
    db = client.people
    items = db.spotify.find()
    items = list(items)
    return items

def get_spark_results(url_results):
    if not url_results.startswith("http"):
        st.error("La URL no es válida. Introduce una URL HTTP/HTTPS.")
        return

    response = requests.get(url_results)

    if response.status_code == 200:
        try:
            json_lines = response.text.strip().split("\n")
            data = [json.loads(line) for line in json_lines]

            data = data[:100]

            df = pd.DataFrame(data)
            st.dataframe(df)

        except json.JSONDecodeError as e:
            st.error(f"Error al decodificar JSON: {e}")
    else:
        st.error(f"Error {response.status_code}: No se pudieron obtener los datos.")

def migrate_to_postgres():
    url = "https://kafka-producer-postgres.onrender.com/trigger-producer"
    try:
        response = requests.post(url)
        if response.status_code == 200:
            st.success("El proceso de Kafka ha sido activado correctamente.")
        else:
            st.error(f"Error al enviar a Kafka: {response.status_code}")
    except Exception as e:
        st.error(f"Ocurrió un error al intentar conectar con Kafka: {e}")


def post_spark_job(user, repo, job, token, code_url, dataset_url):
    url = f'https://api.github.com/repos/{user}/{repo}/dispatches'

    payload = {
        "event_type": job,
        "client_payload": {
            "codeurl": code_url,
            "dataseturl": dataset_url
        }
    }
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-type': 'application/json'
    }

    st.write(f"URL: {url}")
    st.write(f"Payload: {payload}")
    st.write(f"Headers: {headers}")

    response = requests.post(url, json=payload, headers=headers)

    st.write(response)

st.title("Spark & Streamlit")

st.header("spark-submit Job")

github_user  = st.text_input('Github user', value='harlock024')
github_repo  = st.text_input('Github repo', value='stream_kafka_p')
spark_job    = st.text_input('Spark job', value='spark')
github_token = st.text_input('Github token', value='***')
code_url = st.text_input('Code url', value='')
dataset_url = st.text_input('Database url', value='')

if st.button("POST spark submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)


st.header("Get Spark Results")

url_results = st.text_input('Spark results url', value='')

if st.button("GET spark results"):
    get_spark_results(url_results)

if st.button("Migrate data to Postgres"):
    migrate_to_postgres()

if st.button("Query mongodb collection"):
    items = get_data_mongo()
    for item in items:
        st.write(item)

postgres_uri = st.secrets["postgres"]["uri"]

@st.cache_data(ttl=600)
def get_data_postgres():
    df = pd.read_sql_query("SELECT * FROM tracks", conn)
    return df

if st.button("Query postgresql database"):
    df = get_data_postgres()
    st.write(df)
