from pyspark.sql import SparkSession
import pandas as pd
import json

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Música Processing") \
        .getOrCreate()

    print("Leyendo dataset.csv ...")
    path_music = "dataset.csv"
    df_music = spark.read.csv(path_music, header=True, inferSchema=True)

    df_music = df_music.withColumnRenamed("track_name", "track_title")

    df_warning_tracks = df_music.filter(df_music.artists.contains("The Warning"))

    pandas_df = df_warning_tracks.toPandas()

    results = pandas_df.to_dict(orient="records")


    output_file = 'results/data.json'
    with open(output_file, 'w') as file:
        json.dump(results, file)

    print(f"Archivo guardado en {output_file}")

    spark.stop()
