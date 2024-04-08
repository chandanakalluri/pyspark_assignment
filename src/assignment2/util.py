from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType


# create Spark session
def create_spark_session():
    return SparkSession.builder.appName("Assignment 2 - Credited card ").getOrCreate()


# 1) create DataFrame
def create_dataframe(spark):
    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)]
    custom_schema = StructType([
        StructField("card_number", StringType(), True)])
    return spark.createDataFrame(data, schema=custom_schema)


# 2) print number of partitions
def print_partitions(df):
    initial_partition = df.rdd.getNumPartitions()
    print("Number of partitions :", initial_partition)


# 3) Increase the partition size to 5
def increase_partitions(df, num_partitions):
    return df.repartition(df.rdd.getNumPartitions() + num_partitions)


# 4) Decrease the partition size
def decrease_partitions(df, num_partitions):
    return df.coalesce(num_partitions)


# 5)Create a UDF to print only the last 4 digits marking the remaining digits as *Eg: ************4567
def mask_card_numbers(df):
    def hide(card_number):
        return "*" * (len(card_number) - 4) + card_number[-4:]

    hide_udf = udf(hide, StringType())
    return df.withColumn("masked_card_number", hide_udf(df["card_number"]))
