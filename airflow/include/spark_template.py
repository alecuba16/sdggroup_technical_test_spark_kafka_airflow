import sys, csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit,array_except,array,when,array_except,current_timestamp

spark = SparkSession.builder.appName("spark_demo").getOrCreate()
