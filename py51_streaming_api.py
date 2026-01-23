# %%  setup ############################################################################################################
from pyspark.sql import DataFrame, Column
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import window

from py56_fake_stream import STREAM_PATH
from spark_setup_spark3 import get_spark

spark = get_spark()
spark.conf.set("spark.sql.shuffle.partitions", "20")

#########################################################
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType
stream_path = str(STREAM_PATH)
# for streams the data has to be precisely defined up front
schema = StructType([StructField("BeerID", LongType(), False),
                     StructField("DrinkingTime", TimestampType(), False),
                     StructField("DrinkingVolume", DoubleType(), False),
                     StructField("Consumer", StringType(), False)]
                    )

print(schema)

# %% ###################################################################################################################
# lets read the source for a stream, csv-based

stream_handle_static = spark.read.csv(schema=schema, path=stream_path, sep="\t")
stream_handle_static.show()
stream_handle_static.describe().show()

# normal dataframe operation, still no streaming
stream_handle_static.isStreaming

# everytime, we run it, it gets bigger

# %% Analyse the stream in "complete" mode #############################################################################
# let's define some analysis on it
# change read to readStream
stream_handle = spark.readStream.csv(schema=schema, path=stream_path, sep="\t")
stream_handle.isStreaming
# --> now it's true

# lets define a function for the stream:
def calc_consumer_frequs(df: DataFrame) -> DataFrame:
    return df.groupby("Consumer").count()

# the function works with static data
calc_consumer_frequs(stream_handle_static).show()

# the function also works with streaming data
stream_query: StreamingQuery = calc_consumer_frequs(stream_handle) \
    .writeStream.format("console") \
    .outputMode("complete") \
    .start()

stream_query.id
stream_query.runId
stream_query.recentProgress
stream_query.lastProgress


# works perfectly on "complete mode"
stream_query.stop()
# query.awaitTermination()   // block until query is terminated, with stop() or with error

# %% lets try the "update" mode ######################################################################################
stream_query: StreamingQuery = calc_consumer_frequs(stream_handle) \
    .writeStream.format("console") \
    .outputMode("update") \
    .start()
# works perfectly on update mode as well. Sometimes not all drinkers are reported.
stream_query.stop()


# %% lets try the "append" mode ######################################################################################
stream_query: StreamingQuery = calc_consumer_frequs(stream_handle) \
    .writeStream.format("console") \
    .outputMode("append") \
    .start()
## THROWS EXCEPTION on append mode. Needs watermarking.


# %% Adding Watermarking ###############################################################################################
# lets read the source for a stream, csv-based
# try a different query with watermarking

# define a function for the stream.
def calc_consumer_frequs_last_interval(df: DataFrame) -> DataFrame:
    return df.groupBy("Consumer", window("DrinkingTime", "1 minute")).count()


stream_with_watermark = stream_handle.withWatermark("DrinkingTime", "5 minute")

stream_query: StreamingQuery = calc_consumer_frequs_last_interval(stream_with_watermark) \
    .writeStream.format("console").option("truncate", False) \
    .outputMode("update") \
    .start()

stream_query.stop()

# %%#####################################################################################################################
# lets read while control the time interval
stream_query: StreamingQuery = calc_consumer_frequs(stream_handle) \
    .writeStream.format("console") \
    .outputMode("complete") \
    .trigger(processingTime='1 minute') \
    .start()

# 1 minute fixed pause

stream_query.stop()

# %%#####################################################################################################################
# reading and writing the whole stream in one pass:
stream_handle = spark. \
    readStream.csv(schema=schema, path=stream_path, sep="\t"). \
    groupby("Consumer"). \
    count(). \
    writeStream.format("console"). \
    outputMode("update"). \
    trigger(processingTime='2 seconds'). \
    start()
stream_handle.stop()

# %%#####################################################################################################################
# checkpointing saves the progress:

stream_handle = spark.readStream.csv(schema=schema, path=stream_path, sep="\t")


def calc_consumer_frequs_last_interval(df: DataFrame):
    return df.groupBy("Consumer", window("DrinkingTime", "1 minute")).count()


stream_with_watermark = stream_handle.withWatermark("DrinkingTime", "5 minute")
stream_query: StreamingQuery = calc_consumer_frequs_last_interval(stream_with_watermark) \
    .writeStream.format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "myloc1") \
    .start()

stream_query.stop()

# %% Route stream directly to temptable ################################################################################
# Output-Sinks to memory allows sql on top
from pyspark.sql.functions import window

stream_handle = spark.readStream.csv(schema=schema, path=stream_path, sep="\t")


def calc_consumer_frequs_last_interval(df: DataFrame):
    return df.groupBy("Consumer", window("DrinkingTime", "1 minute")).count()


stream_with_watermark = stream_handle.withWatermark("DrinkingTime", "5 minute")
stream_query: StreamingQuery = calc_consumer_frequs_last_interval(stream_with_watermark) \
    .writeStream.format("memory") \
    .queryName("beer_summary") \
    .outputMode("complete") \
    .start()

spark.sql("SELECT count(*) FROM beer_summary").show(truncate=False)
spark.sql("SELECT * FROM beer_summary").show(truncate=False)


# %% Spark 3.1 new streaming directly to metastore table ###############################################################

stream_handle = spark.readStream.csv(schema=schema, path=stream_path, sep="\t")


def calc_consumer_frequs_last_interval(df: DataFrame):
    return df.groupBy("Consumer", window("DrinkingTime", "1 minute")).count()

beer_table_name = "beer_summary_table_"

stream_with_watermark = stream_handle.withWatermark("DrinkingTime", "5 minute")
stream_query: StreamingQuery = calc_consumer_frequs_last_interval(stream_with_watermark) \
    .writeStream \
    .option("checkpointLocation", "myloc_") \
    .toTable(beer_table_name)

spark.read.table(beer_table_name).show()

spark.sql(f"SELECT count(*) FROM {beer_table_name}").show(truncate=False)
