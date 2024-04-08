from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import *

spark = SparkSession.builder.appName("finding count").getOrCreate()
schema1 = StructType([StructField("customer", StringType(), nullable=False),
                      StructField("product_model", StringType(), nullable=False)
                      ])
data1 = [("1", "iphone13"),
         ("1", "dell i5 core"),
         ("2", "iphone13"),
         ("2", "dell i5 core"),
         ("3", "iphone13"),
         ("3", "dell i5 core"),
         ("1", "dell i3 core"),
         ("1", "hp i5 core"),
         ("1", "iphone14"),
         ("3", "iphone14"),
         ("4", "iphone13")]
schema2 = StructType([
    StructField("product_model", StringType(), nullable=False)])
data2 = [("iphone13",),
         ("dell i5 core",),
         ("dell i3 core",),
         ("hp i5 core",),
         ("iphone14",)]
purchase_data_df= spark.createDataFrame(data=data1, schema=schema1)

product_data_df= spark.createDataFrame(data=data2, schema=schema2)
def only_iphone13():
    filtered_purchase_df = purchase_data_df.groupBy("customer").count()
    count = filtered_purchase_df.filter("count = 1")
    res_df = count.join(purchase_data_df, "customer").join(product_data_df, "product_model").filter(
    "product_model = 'iphone13'").select("customer")
    return res_df
def purchase_all_products():
    count1 =purchase_data_df.groupBy("customer").agg(countDistinct("product_model").alias("distinct_count"))
    count2 = product_data_df.count()
    res1=count1.filter(count1.distinct_count==count2).select("customer")
    return res1

def upgraded_customers():
    coun=purchase_data_df.filter(purchase_data_df ["product_model"]=="iphone13").select("customer")#fetching only iphone13 customers
    count3=purchase_data_df.filter(purchase_data_df["product_model"]=="iphone14").select("customer")#fetching only iphone14 customers
    res2=coun.intersect(count3).alias("upgraded_customers")
    return res2
#
