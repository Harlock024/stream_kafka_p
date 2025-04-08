from pyspark.sql import SparkSession
import json
import os

if __name__ == "__main__":
    # Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("Música Processing") \
        .getOrCreate()

    print("Leyendo dataset.csv ...")
    path_music = "dataset.csv"
    df_music = spark.read.csv(path_music, header=True, inferSchema=True)

    # Crear una vista temporal para realizar consultas SQL
    df_music.createOrReplaceTempView("music")

    # Query para obtener todas las canciones de "The Warning"
    query = """SELECT * FROM music WHERE artists = 'The Warning'"""
    df_warning_tracks = spark.sql(query)

    # Mostrar los primeros resultados de la consulta
    df_warning_tracks.show()

    # Coalesce a una sola partición para evitar múltiples archivos de salida
    df_warning_tracks = df_warning_tracks.coalesce(1)

    # Escribir los resultados en formato JSON en la carpeta "results"
    df_warning_tracks.write.mode("overwrite").json("results")

    # Ahora leemos el archivo generado
    files = [f for f in os.listdir("results") if f.endswith(".json")]
    if files:
        # Se asume que hay un solo archivo .json generado
        with open(f"results/{files[0]}", "r") as f:
            results = json.load(f)

        # Guardar los resultados en el formato adecuado como "data.json"
        with open('results/data.json', 'w') as file:
            json.dump(results, file)

    spark.stop()
