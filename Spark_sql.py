from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType,BooleanType,DateType, NumericType,DecimalType,ArrayType,StringType
from pyspark.sql.functions import  spark_partition_id, collect_list, col, struct
from datetime import date, datetime
import dateutil.parser


#Spark configuration
config = SparkConf().setAll([('spark.debug.maxToStringFields', '100'),('spark.executor.memory', '8g'), ('spark.executor.cores', '3'),
                             ('spark.cores.max', '3'), ('spark.driver.memory', '8g'),
                             ("spark.jars", "C:\\spark-3.1.2-bin-hadoop3.2\\jars\\postgresql-42.2.23.jar")])

print("spark session start ")

#Spark session starting
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config(conf=config) \
    .getOrCreate()

'''/***************************************************************************************************************/'''

print("spark session started, database connection is starting......")

url = "jdbc:postgresql://localhost:5432/postgres"
properties ={"user":"postgres", "password":"**** ", "driver":"org.postgresql.Driver"}


df_orders = spark.read.jdbc(url=url,table="public.orders",properties=properties)

df_customer = spark.read.jdbc(url=url, table="public.customer", properties=properties)

df_join = df_orders.alias("ord").join(df_customer.alias("cust"),
      (col("ord.customer_id") == col("cust.customer_id")), "left")\
    .filter(to_date(col("cust.customer_create_date")) > '2021-10-11')\
    .select(col("cust.customer_id").cast(DecimalType(precision=20, scale=0)),
    concat_ws(" ", "first_name", "last_name").alias("name"),
    "phone_number",
    "customer_create_date",
    "order_id",
    "order_channel",
    "order_item_count",
    "order_price",
    "order_city",
    "order_price_currency",
    "order_transaction_type")


df_agg = df_join.groupBy("order_channel","order_city", "order_price_currency", "order_transaction_type")\
    .agg(sum(coalesce("order_price", lit(0))).alias("sum_price"))


df_last = df_agg.select(
    "order_channel",
    "order_city",
    "order_price_currency",
    "order_transaction_type",
    "sum_price",
    when(col("sum_price")<500, "LOW ITEM")
    .when((col("sum_price")>=500) & (col("sum_price")<2000), "MEDIUM ITEM")
    .when(col("sum_price")>=2000, "HIGH ITEM").alias("sum_price_status")).sort("order_channel","order_city")

df_last.show(truncate=False)
df_last.printSchema()


#df_last sonucu oluşan datanın json dosya formatında data klasörü altında yazılması
#writing df data to json file
df_last.write.mode("overwrite").json("data")

'''/***************************************************************************************************************/'''
import os
import json

directory = "C:\\Users\\esra.yildiz\\PycharmProjects\\Spark\\data\\"
file_list = os.listdir(directory)
data = []
for file in file_list:
    if file.endswith(".json"):
        if os.path.getsize(directory+file):
            for line in open(directory + file, 'r'):
                file_data = json.loads(line)
                data.append(file_data)



print(data,"len-->" ,len(data))


'''/***************************************************************************************************************/'''

#mongodb connection
from pymongo import MongoClient

try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client.spark_project
except Exception as err:
    print("no connection", str(err))

collection = db.order

db.order.drop()

if isinstance(data, list):
    print("many")
    print("data loading to mongodb")
    collection.insert_many(data)
else:
    print("one")
    collection.insert_one(data)
    print("data loading to mongodb")




