import streamlit as st
import pandas as pd
import requests

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
   url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
   payload = {
     "event_type": job,
     "client_payload": {
        "codeurl": codeurl,
        "dataseturl": dataseturl
      }
   }
   headers = {'Authorization': f'token {token}'}
   response = requests.post(url, json=payload, headers=headers)
   if response.status_code == 200:
       st.success("Spark job triggered successfully!")
   else:
       st.error(f"Failed to trigger job: {response.status_code}")

st.header("spark-submit Job")

github_user  =  st.text_input('Github user', value='harlock024')
github_repo  =  st.text_input('Github repo', value='stream_kafka_p')
spark_job    =  st.text_input('Spark job', value='spark')
github_token =  st.text_input('Github token', value='***')
code_url     =  st.text_input('Code URL', value='')
dataset_url  =  st.text_input('Dataset URL', value='')
if st.button("POST spark submit"):
   post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)
