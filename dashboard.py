from numpy import c_
import streamlit as st
import pandas as pd
import requests
import pymongo
import psycopg2
from sqlalchemy import create_engine

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
