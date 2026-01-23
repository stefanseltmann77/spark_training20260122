# %% SETUP ############################################################################################################

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, LongType, MapType, StringType

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

# %% Loading the dataframe #############################################################################################
df: DataFrame = spark.read.json((path_demodata / 'countries.geo.json').as_posix(), multiLine=True)
df.printSchema()
df.show()
## the structure is complex with many arrays
df.show(truncate=False)

# %% explore ###########################################################################################################

# first get the feature
df.printSchema()
df.select(sf.col("features")[0])

# then go deeper, next ist a struct
df.select(sf.col("features")[0]['geometry'])

## then go deeper, next ist a struct ...
## and so on
df.select(sf.col("features")[0]['geometry']['coordinates'][0][0][0]).show()

# or go down the tree directly
df.printSchema()
df.select(sf.col("features")['geometry']['type']).show(truncate=False)

# reformat it to rows
df.printSchema()
df.select(sf.posexplode(sf.col("features")['geometry']['type'])).show(truncate=False)

# %% other example with dicts to map ###################################################################################
import datetime

schema_json = df.schema.json()
some_df = spark.createDataFrame([{"ID": 1,
                                  "msg": {"msg_text": "Hello, this is Kafka", "msg_dts": datetime.datetime.now()}}],
                                schema=StructType([StructField('ID', LongType()),
                                                   StructField('msg', MapType(StringType(), StringType()))]))
some_df.printSchema()
some_df.schema
some_df.select("msg.msg_text").show(truncate=False)
some_df.select("msg.msg_dts").show(truncate=False)

# other example with structs
df = spark.read.csv("./data/recipeData.csv", header=True, inferSchema=True)
df.printSchema()

# Nesting of columns
df_nested = df.select(
    sf.struct("BeerID", "Name", sf.struct("BoilSize", "BoilTime", "BoilGravity").alias("BoilData")).alias("BeerData"))
df_nested.select("BeerData.BoilData")  # selection via "."
df_nested.printSchema()
df_nested.show(truncate=False)
# flattening of structs
df_nested.select("BeerData.BoilData.*").show()
