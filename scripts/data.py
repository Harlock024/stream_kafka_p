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

    df_music = df_music.withColumnRenamed("track_name", "track_title")

    df_music.createOrReplaceTempView("music")

    query = 'DESCRIBE music'
    spark.sql(query).show(20)

    query = """SELECT track_title, artists, popularity FROM music WHERE popularity > 80 ORDER BY popularity DESC"""
    df_popular_tracks = spark.sql(query)
    df_popular_tracks.show(20)

    query = """SELECT track_title, artists, track_genre FROM music WHERE track_genre = 'Pop' ORDER BY track_title"""
    df_pop_tracks = spark.sql(query)
    df_pop_tracks.show(20)

    query = 'SELECT track_title, artists, duration_ms FROM music WHERE duration_ms BETWEEN 180000 AND 300000'
    df_tracks_duration = spark.sql(query)
    df_tracks_duration.show(20)

    results = df_tracks_duration.toJSON().collect()
    df_tracks_duration.write.mode("overwrite").json("results")
    with open('results/data.json', 'w') as file:
        json.dump(results, file)

    query = 'SELECT track_genre, COUNT(*) FROM music GROUP BY track_genre'
    df_genre_count = spark.sql(query)
    df_genre_count.show()

    spark.stop()
