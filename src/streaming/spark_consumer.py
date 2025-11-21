import os
import sys
import time

# CONFIGURATION HADOOP
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SimpleSparkConsumer:
    def __init__(self):
        self.kafka_servers = "localhost:9092"
        
    def create_spark_session(self):
        """Session Spark simple"""
        print("🚀 Création session Spark...")
        
        spark = SparkSession.builder \
            .appName("KafkaTest") \
            .master("local[1]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config("spark.sql.streaming.checkpointLocation", "/dev/null") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")  # Moins de logs
        return spark
    
    def start_micro_batch(self):
        """Approche micro-batch au lieu de streaming continu"""
        spark = self.create_spark_session()
        
        try:
            print("📡 Démarrage en mode micro-batch...")
            
            while True:
                try:
                    # Lecture BATCH (pas streaming)
                    df = spark \
                        .read \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", self.kafka_servers) \
                        .option("subscribe", "building-sensors") \
                        .option("startingOffsets", "latest") \
                        .load() \
                        .limit(10)  # Limite pour éviter trop de données
                    
                    if df.count() > 0:
                        print(f"📨 {df.count()} messages reçus:")
                        df.select(col("value").cast("string")).show(truncate=False)
                    else:
                        print("⏳ En attente de données...")
                    
                    # Pause de 2 secondes
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"⚠️  Erreur batch: {e}")
                    time.sleep(5)
                    
        except KeyboardInterrupt:
            print("\n🛑 Arrêt demandé...")
        finally:
            spark.stop()
            print("🔚 Session fermée")

def main():
    print("=" * 50)
    print("🚀 SPARK KAFKA CONSUMER - MICRO BATCH")
    print("=" * 50)
    print("💡 Lancez data_producer.py dans un autre terminal")
    print("⏹️  Ctrl+C pour arrêter")
    
    consumer = SimpleSparkConsumer()
    consumer.start_micro_batch()

if __name__ == "__main__":
    main()