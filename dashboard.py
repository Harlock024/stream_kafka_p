import streamlit as st
import pandas as pd

# Cargar el dataset desde GitHub
@st.cache_data

def load_data():
    url = "dataset.csv"

    df = pd.read_csv(url,nrows=100)
    return df

df = load_data()

# Mostrar los datos en tabla
st.title("📊 Dashboard de Datos")
st.dataframe(df)



st.success("✅ Dashboard listo con datos CSV")
