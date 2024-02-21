#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
from pyspark.sql.functions import *



def init_spark():
    sp=SparkSession.builder \
        .master('spark://spark-master:7077') \
        .appName('Submit-App') \
        .config('spark.driver.memory', '512M')  \
        .config('spark.executor.memory', '512M')  \
        .config('spark.executor.instances', '1')  \
        .config('spark.executor.cores', '1')  \
        .config("spark.cores.max", "1") \
        .config("spark.jars", "/spark-apps/postgresql-42.6.0.jar")\
        .getOrCreate()
    sc = sp.sparkContext
    return sp,sc
spark,sc = init_spark()


def main():
  global   spark,sc   
  url = "jdbc:postgresql://postgres-server:5432/mta_data"
  properties = {
    "user": "Test",
    "password": "Test123",
    "driver": "org.postgresql.Driver"
  }
  file = "/spark-data/MTA-Bus-Time_.2014-08-01.txt"

  df = spark.read.load(file,format = "csv", inferSchema="true", sep="\t", header="true") \
      .withColumn("report_hour",date_format(col("time_received"),"yyyy-MM-dd HH:00:00")) \
      .withColumn("report_date",date_format(col("time_received"),"yyyy-MM-dd"))
  
  print("Number of total rows :", df.count())
  print("-="*30)

  new_df=df.where("latitude <= 90 AND latitude >= -90 AND longitude <= 180 AND longitude >= -180")\
    .where("latitude != 0.000000 OR longitude !=  0.000000 ")
    
  # Debugging: print the number of rows after filtering
  print("Number of rows after filtering:", new_df.count())
  new_df.show(5)
  print("-="*30)
  
  if new_df.count() > 0:
        new_df.write.jdbc(url=url, table="mta_reports", mode='append', properties=properties)
        print("Data written to the database.")
  else:
        print("No valid data to write to the database.")
    
if __name__ == '__main__':
  main()
