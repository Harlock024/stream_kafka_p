from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    # Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("Música Processing") \
        .getOrCreate()

    print("Leyendo dataset.csv ...")
    path_music = "dataset.csv"
    df_music = spark.read.csv(path_music, header=True, inferSchema=True)

    df_music.createOrReplaceTempView("music")

    query = """SELECT * FROM music WHERE artists = 'The Warning'"""
    df_warning_tracks = spark.sql(query)

    df_warning_tracks.show()

    df_warning_tracks = df_warning_tracks.coalesce(1)

    df_warning_tracks.write.mode("overwrite").json("results")

    results = []
    for line in open("results/part-00000-*.json", "r"):
        results.append(json.loads(line))

    with open('results/data.json', 'w') as file:
        json.dump(results, file)

    spark.stop()
