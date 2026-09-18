"""
spark_simple.py - Spark pour Option 1 ET Option 2
CORRIGÉ : Version avec téléchargement manuel du package
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import os
import subprocess
import sys
import pymongo                      # nouvel import
from datetime import datetime       # nouvel import

import builtins

from ml_models import SmartBuildingML

ml_model = SmartBuildingML()  # instance globale, partagée entre tous les batchs

def write_raw_to_mongo(batch_df, batch_id):
    """Écrit les données brutes enrichies ML dans MongoDB (remplace kafka_consumer.py)"""
    if batch_df.isEmpty():
        return
    
    client = pymongo.MongoClient(
        "mongodb://admin:password@localhost:27018/",
        authSource="admin"
    )
    db = client.smart_buildings
    
    rows = [row.asDict() for row in batch_df.collect()]
    docs = []
    
    for row in rows:
        sensor_type = row.get("sensor_type")
        value = row.get("value")
        
        doc = {**row, "processed_at": datetime.now(), "kafka_timestamp": datetime.now()}

        
        if sensor_type in ["temperature", "co2", "humidity"]:
            is_anomaly = ml_model.detect_anomaly(sensor_type, value)
            if is_anomaly:
                doc["ml_anomaly"] = True
                doc["ml_confidence"] = 0.95
            
            prediction = ml_model.predict_next(sensor_type)
            if prediction is not None:
                doc["ml_prediction"] = builtins.round(float(prediction), 2)
        
        docs.append(doc)
    
    if docs:
        db.raw_sensor_data.insert_many(docs)
        print(f"✅ Batch {batch_id}: {len(docs)} documents ML écrits dans raw_sensor_data")
    
    client.close()


def write_alerts_to_mongo(batch_df, batch_id):
    """Écrit chaque micro-batch d'alertes dans MongoDB"""
    if batch_df.isEmpty():
        return
    
    client = pymongo.MongoClient(
    "mongodb://admin:password@localhost:27018/",
    authSource="admin"
    )

    db = client.smart_buildings
    
    rows = [row.asDict() for row in batch_df.collect()]
    for row in rows:
        row["spark_batch_id"] = batch_id
        row["processed_at"] = datetime.now()
    
    if rows:
        db.spark_alerts.insert_many(rows)
        print(f"✅ Batch {batch_id}: {len(rows)} alertes écrites dans MongoDB")
    
    client.close()


def write_aggregations_to_mongo(batch_df, batch_id):
    """Écrit chaque micro-batch d'agrégations dans MongoDB (upsert)"""
    if batch_df.isEmpty():
        return
    
    client = pymongo.MongoClient(
    "mongodb://admin:password@localhost:27018/",
    authSource="admin"
    )
    
    db = client.smart_buildings
    
    rows = [row.asDict() for row in batch_df.collect()]
    for row in rows:
        db.spark_aggregations.update_one(
            {"building_id": row["building_id"], "sensor_type": row["sensor_type"]},
            {"$set": {**row, "spark_batch_id": batch_id, "last_update": datetime.now()}},
            upsert=True
        )
    
    print(f"✅ Batch {batch_id}: {len(rows)} agrégations mises à jour dans MongoDB")
    client.close()


def main():
    print("=" * 70)
    print("🏢 SMART BUILDING - SPARK FINAL")
    print("✅ Compatible: Option 1 (Dataset réel) ET Option 2 (Simulé)")
    print("=" * 70)
    
    # Télécharger manuellement le package si nécessaire
    print("📦 Vérification du package Kafka-Spark...")
    
    # 1. Session Spark avec package téléchargé localement
    spark = SparkSession.builder \
        .appName("SmartBuildingFinal") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.4.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("✅ Session Spark créée")
    
    # 2. Lire depuis Kafka - ESSAYER DEUX ADRESSES DIFFÉRENTES
    print("📡 Connexion à Kafka...")
    
    try:
        # Essayer d'abord localhost:29095 (depuis l'hôte)
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:29095") \
            .option("subscribe", "building-sensors") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        print("✅ Connecté à Kafka via localhost:29095")
    except Exception as e1:
        print(f"⚠️ Échec localhost:29095: {e1}")
        try:
            # Essayer kafka:9092 (depuis le réseau Docker)
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9092") \
                .option("subscribe", "building-sensors") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            print("✅ Connecté à Kafka via kafka:9092")
        except Exception as e2:
            print(f"❌ Échec des deux connexions:")
            print(f"   localhost:29095: {e1}")
            print(f"   kafka:9092: {e2}")
            print("\n🔧 Solution 1: Démarrer Spark avec ce commande:")
            print("   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_simple.py")
            print("\n🔧 Solution 2: Télécharger manuellement le JAR:")
            print("   wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar")
            print("   spark-submit --jars spark-sql-kafka-0-10_2.12-3.5.0.jar spark_simple.py")
            return
    
    # 3. Convertir JSON en texte
    json_df = df.select(
        col("value").cast("string").alias("json_data"),
        col("timestamp").alias("kafka_time")
    )

    # 4. Parser le JSON
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

    schema = StructType([
    StructField("timestamp", StringType()),
    StructField("kafka_timestamp", StringType()),
    StructField("building_id", StringType()),
    StructField("room_id", StringType()),   
    StructField("sensor_type", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
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
    
    # 7. Écrire les données brutes enrichies ML vers MongoDB
    query_raw = parsed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_raw_to_mongo) \
        .queryName("raw_to_mongo") \
        .start()

    # 8. Écrire les alertes vers MongoDB
    query_alerts = alerts_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_alerts_to_mongo) \
        .queryName("alerts_to_mongo") \
        .start()

    # 9. Écrire les agrégations vers MongoDB
    query_agg = agg_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_aggregations_to_mongo) \
        .queryName("aggregations_to_mongo") \
        .start()

    # Attendre que toutes les requêtes tournent en parallèle
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()