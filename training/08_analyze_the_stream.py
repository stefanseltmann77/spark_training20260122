from pyspark.sql import functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from spark_setup_spark3 import get_spark, path_trainingdata

spark = get_spark()
spark.conf.set("spark.sql.shuffle.partitions", 20)

#########################################################
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

stream_path = "data/stream"
schema = StructType([StructField("BeerID", LongType(), False),
                     StructField("DrinkingTime", TimestampType(), False),
                     StructField("DrinkingVolume", DoubleType(), False),
                     StructField("Consumer", StringType(), False)]
                    )

streamHandleStatic = spark.read.csv(schema=schema, path=stream_path, sep="\t")
streamHandleStatic.describe().show()

df_beers = spark.read.csv("./data/recipeData.csv", inferSchema=True, header=True)

# normal dataframe operation, still no streaming
streamHandleStatic.isStreaming
# everytime, we run it, it gets bigger




# %% Exercise 01: ######################################################################################################
# Create an STATIC aggregation that shows the amount of drank beer based on the beerID

# %% Exercise 02: ######################################################################################################
# Join the df_beers to your aggregation using the BeerID, and display the volumne based on the Beer Name as a new df



# %% Exercise 03: ######################################################################################################
# Stream the result to console
# define a new streamHandle that is not static with readStream
# apply the aggregation and join to it
# use writeStream with outputmode "complete"
# Optional A: Do it only every Minute
# Optional B: Do it only for the last 5 Minutes  (GroupBy Window)

