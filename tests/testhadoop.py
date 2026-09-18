import os
os.environ['HADOOP_HOME'] = "C:\\hadoop"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Test") \
    .getOrCreate()

print("🎉 SUCCÈS! Spark fonctionne avec Hadoop 3.2.2") #3.3.6
spark.stop()