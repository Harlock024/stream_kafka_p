from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Procesamiento con Spark") \
    .getOrCreate()

df = spark.read.csv("dataset.csv", header=True, inferSchema=True)

df_filtered = df.filter(df['edad'] > 30)

df_filtered.write.json("results/processed_data.json")

print("✅ Procesamiento completado y resultados guardados.")
