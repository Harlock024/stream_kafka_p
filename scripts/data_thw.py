from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("Música Processing") \
        .getOrCreate()

    print("Leyendo dataset.csv ...")
    path_music = "dataset.csv"
    df_music = spark.read.csv(path_music, header=True, inferSchema=True)

    # Renombrar columna para uniformidad
    df_music = df_music.withColumnRenamed("track_name", "track_title")

    # Filtrar canciones de la banda 'The Warning'
    df_warning_tracks = df_music.filter(df_music.artists.contains("The Warning"))

    # Mostrar las canciones filtradas
    df_warning_tracks.show(20)

    # Guardar los resultados en un archivo JSON
    df_warning_tracks.coalesce(1).write.mode("overwrite").json("results/data.json")
    spark.stop()
