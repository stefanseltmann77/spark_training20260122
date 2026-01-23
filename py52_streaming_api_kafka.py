from pyspark.sql import functions as sf
from pyspark.sql.types import StringType, StructType, StructField, TimestampType, IntegerType, FloatType, ArrayType

from py57_fake_stream_kafka import TOPIC_NAME
from spark_setup_spark3 import get_spark

spark = get_spark()
spark.conf.set("spark.sql.shuffle.partitions", "20")

# %% Analyze Kafka stationary
df_raw = spark.read.format("kafka"). \
    option("kafka.bootstrap.servers", 'ip-172-31-44-96.eu-central-1.compute.internal:9092'). \
    option("subscribe", TOPIC_NAME).load()

df_raw.count()  # with every count the number

# value/payload ins still binay
df_raw.show(truncate=False)


# %% parsing
# first parse it to a string
df_raw.select(df_raw.value.cast(StringType())).show(truncate=False)


df = df_raw.select(df_raw.value.cast(StringType()).alias("msg"))
df.show()
# then cast it to a structure using from_json
schema = StructType([StructField("drinks", StringType())])

df_parsed = df.withColumn('msg', sf.from_json(sf.col('msg'), schema))
df_parsed.show()
# incomplete schema leads to incomplete parsing
df_parsed.show(truncate=False)
df_parsed.printSchema()

# lets extend the schema
schema_complete = StructType([StructField("drinks", ArrayType(StructType([StructField("beer_id", IntegerType()),
                                                                          StructField("volume", FloatType())]))),
                              StructField("trink_dts", TimestampType()),
                              StructField("consumer", StringType())])
df_parsed = df.withColumn('msg', sf.from_json(sf.col('msg'), schema_complete))
df_parsed.printSchema()

# check the results
df_parsed.show(truncate=False)
# select only the message
df_parsed.select("msg.*").show()
# select only the drinks
df_parsed.select("msg.drinks").show()

df_clean = df_parsed.select(sf.col('msg.consumer').alias('consumer'),
                            sf.col('msg.trink_dts').alias('trink_dts'),
                            sf.posexplode("msg.drinks")). \
    withColumn("beer_id", sf.col('col')['beer_id']). \
    withColumn("volume", sf.col('col')['volume'])
df_clean.show()

# %%  lets put in all in one function

def process_kafka_df(df_input, schema_input):
    df_tmp = df_input.withColumn('msg', sf.from_json(sf.col('value').cast(StringType()), schema_input))
    df_processed = df_tmp.select(sf.col('msg.consumer').alias('consumer'),
                                 sf.col('msg.trink_dts').alias('trink_dts'),
                                 sf.posexplode("msg.drinks")). \
        withColumn("beer_id", sf.col('col')['beer_id']). \
        withColumn("volume", sf.col('col')['volume']). \
        drop("col")
    return df_processed


# %% write stream to csv:
df_stream = spark.readStream.format("kafka"). \
    option("kafka.bootstrap.servers", 'localhost:9092'). \
    option("subscribe", TOPIC_NAME).load()

df_stream_processed = process_kafka_df(df_stream, schema_complete)

kafka_stream = df_stream_processed.writeStream.outputMode("append"). \
    option("checkpointLocation", 'data'). \
    format("csv").option("path", "drink_dump_").start()
kafka_stream.stop()

# %% write stream to console:
df_stream = spark.readStream.format("kafka"). \
    option("kafka.bootstrap.servers", 'localhost:9092'). \
    option("subscribe", TOPIC_NAME).load()

df_stream_processed = process_kafka_df(df_stream, schema_complete)

kafka_stream = df_stream_processed.writeStream.outputMode("update"). \
    format("console").start()
kafka_stream.stop()
