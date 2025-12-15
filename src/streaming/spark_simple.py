"""
spark_final.py - Spark pour Option 1 ET Option 2
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json

def main():
    print("=" * 70)
    print("🏢 SMART BUILDING - SPARK FINAL")
    print("✅ Compatible: Option 1 (Dataset réel) ET Option 2 (Simulé)")
    print("=" * 70)
    
    # 1. Session Spark
    spark = SparkSession.builder \
        .appName("SmartBuildingFinal") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("✅ Session Spark créée")
    
    # 2. Lire depuis Kafka (kafka:9092 dans Docker)
    print("📡 Connexion à Kafka: kafka:9092")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "building-sensors") \
        .option("startingOffsets", "latest") \
        .load()
    
    print("✅ Connecté à Kafka")
    
    # 3. Convertir JSON en texte
    json_df = df.select(
        col("value").cast("string").alias("json_data"),
        col("timestamp").alias("kafka_time")
    )

    # 4. Parser le JSON
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("building_id", StringType()),
        StructField("sensor_type", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("room", StringType()),
        StructField("floor", IntegerType()),
        StructField("source", StringType())
    ])

    parsed_df = json_df.withColumn("data", from_json(col("json_data"), schema)) \
                    .select("data.*", "kafka_time")

    # 5. Filtres et alertes
    alerts_df = parsed_df.filter(
        (col("sensor_type") == "temperature") & (col("value") > 25) |
        (col("sensor_type") == "electricity") & (col("value") > 5000)
    )

    # 6. Agrégations
    agg_df = parsed_df.groupBy("building_id", "sensor_type") \
                    .agg(
                        avg("value").alias("avg_value"),
                        max("value").alias("max_value"),
                        count("*").alias("message_count")
                    )
    
    # 7. Afficher les données brutes
    print("📊 Données reçues:")
    
    query = json_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 5) \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()