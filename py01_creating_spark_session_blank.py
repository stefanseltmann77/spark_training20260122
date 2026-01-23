"""This shows two minimal examples for creating a PySpark session

ATTENTION, this will only work with pyspark installed and not databricks-connect or spark-connect.
When working on Databricks itself, "spark" is already created.
"""
import pyspark
from pyspark.sql import SparkSession

spark_conf = (pyspark.SparkConf().
              setAppName("HelloSpark").
              setMaster("local[*]"))
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# or just
spark = SparkSession.builder.getOrCreate()

print(spark.version)
