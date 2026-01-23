# %% SETUP ############################################################################################################
import datetime
from collections.abc import Callable

import numpy as np
from numpy import ndarray
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

file_path_style = (path_demodata / 'styleData.csv').as_posix()
file_path_recipe = (path_demodata / 'recipeData.csv').as_posix()

# disable AQE and Broadcast jons in order to simulate large data amounts
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


def get_durations(func_call: Callable[[], None], iterations: int) -> ndarray:
    durations = []
    for i in range(iterations):
        start_duration = datetime.datetime.now()
        func_call()
        end_duration = datetime.datetime.now()
        durations.append(end_duration - start_duration)
    return np.median(durations)


def get_durations_dec(func_call):
    def call_spark():
        durations = []
        for i in range(5):
            start_duration = datetime.datetime.now()
            func_call()
            end_duration = datetime.datetime.now()
            durations.append(end_duration - start_duration)
        return np.median(durations)

    return call_spark


csv_reader = spark.read.format('csv').options(inferSchema=True, header=True, encoding="utf-8")


# %% simple approach without optimisation ##############################################################################

@get_durations_dec
def calc_result_simple():
    df, df_styles = csv_reader.load(file_path_recipe), csv_reader.load(file_path_style)
    df_joined: DataFrame = df.join(df_styles, on="StyleID")
    df_joined.select(sf.countDistinct("BeerID")).show()
    # 4 stages!  2 x shuffle exchange, 1 x Join

print(calc_result_simple())
print(spark.sparkContext.uiWebUrl)


# --> very poor performance, a lot of shuffles. 200 partitions.


# %% our tables are keyed by StyleID, let's use this for our JOINS. ####################################################

@get_durations_dec
def get_result_preshuffled() -> None:
    df, df_styles = csv_reader.load(file_path_recipe), csv_reader.load(file_path_style)

    # lets repartition to 4 partitions
    partition_cnt = 4
    df = df.repartition(partition_cnt, "StyleID")  # repartition after reading on JOIN-Key
    df_styles = df_styles.repartition(partition_cnt, "StyleID")  # repartition after reading on JOIN-Key

    df_joined: DataFrame = df.join(df_styles, on="StyleID")
    df_joined.select(sf.countDistinct("BeerID")).show()


print(get_result_preshuffled())


# less shuffling! and less stages, because Spark remembers the partitions.
# still 200 shuffle partitions for grouping
# Decide on your data layout early on!

# %% in addition adjust the number of shuffle partitions 4. ############################################################

@get_durations_dec
def get_result_preshuffled_4parts():
    partition_cnt = 4
    # global setting for shuffles
    spark.conf.set("spark.sql.shuffle.partitions", str(partition_cnt))

    df, df_styles = csv_reader.load(file_path_recipe), csv_reader.load(file_path_style)
    df = df.repartition(partition_cnt, "StyleID")
    df_styles = df_styles.repartition(partition_cnt, "StyleID")
    df_joined: DataFrame = df.join(df_styles, on="StyleID")
    df_joined.select(sf.countDistinct("BeerID")).show()
    spark.conf.set("spark.sql.shuffle.partitions", str(200))  # reset again


print(get_result_preshuffled_4parts())


# less shuffling! and less stages, because Spark remembers the partitions.
# no exchange with 200 partitions but only with 4
# Decide on your data layout early on!

# %% if you set the shuffle partitions, you don't need to set the repartion with the same number, this will happen
# implicitly

@get_durations_dec
def get_result_nopreshuffled_4parts():
    spark.conf.set("spark.sql.shuffle.partitions", str(4))
    # further no preshuffeling
    df, df_styles = csv_reader.load(file_path_recipe), csv_reader.load(file_path_style)
    df_joined: DataFrame = df.join(df_styles, on="StyleID")
    df_joined.select(sf.countDistinct("BeerID")).show()
    spark.conf.set("spark.sql.shuffle.partitions", str(200))  # reset again


print(get_result_nopreshuffled_4parts())


# %% if you continue working with a dimension that has been shuffled, no futher shuffle is required.

# --> now we do not group on BeerID but on StyleID, which is the same as the join key.
@get_durations_dec
def get_result_nopreshuffled_4parts_style_distinct():
    spark.conf.set("spark.sql.shuffle.partitions", str(4))
    df, df_styles = csv_reader.load(file_path_recipe), csv_reader.load(file_path_style)
    df_joined: DataFrame = df.join(df_styles, on="StyleID")
    df_joined.select(sf.countDistinct("StyleID")).show()  # ATTENTION not BEER_ID anymore
    spark.conf.set("spark.sql.shuffle.partitions", str(200))  # reset again

print(get_result_nopreshuffled_4parts_style_distinct())

# %% lets try it as a broadcast, to avoid one side of the shuffle
from pyspark.sql.functions import broadcast


@get_durations_dec
def get_result_with_broadcast():
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    df, df_styles = csv_reader.load(file_path_recipe), csv_reader.load(file_path_style)
    df_joined: DataFrame = df.join(broadcast(df_styles), on="StyleID")
    df_joined.select(sf.countDistinct("StyleID")).show()
    spark.conf.set("spark.sql.shuffle.partitions", "200")


print(get_result_with_broadcast())
## --> the execution plan does not need a join for the broadcast is no need


# %% Reading prepartitioned data, will this speed things up?
## lets save it first and try again with prepartitioned data:

partition_file_name = "recipe_part4_dd"

df: DataFrame = spark.read.csv(file_path_recipe, inferSchema=True, header=True, encoding="utf-8")
df = df.withColumnRenamed("Size(L)", "Size_L")
df.write.saveAsTable(partition_file_name, mode="overwrite", partitionBy="StyleID")

# let's read it again
df_mod = spark.table(partition_file_name)

# check the partitions
df_mod.rdd.getNumPartitions()

df_styles = spark.read.csv(file_path_style, inferSchema=True, header=True, encoding="utf-8")
df_joined: DataFrame = df_mod.join(df_styles, on="StyleID")
df_joined.select(sf.countDistinct("StyleID")).show()

# the result still needs shuffling.


# %% bucketing will save the day!
## Remark 20260118  BUCKETING IS FINALLY COMPLETELY DEPRECATED WITH LIQUID CLUSTERING
## lets save it first and try again with bucketing:

bucket_file_name = "recipe_bucket_demo"

df: DataFrame = spark.read.csv(file_path_recipe, inferSchema=True, header=True, encoding="utf-8")
df = df.withColumnRenamed("Size(L)", "Size_L")
df.write.bucketBy(20, 'StyleID').saveAsTable(bucket_file_name, mode="overwrite")

# load it again
df_buck = spark.table(bucket_file_name)

# get_partitions
df_buck.rdd.getNumPartitions()

# notice the difference of explain plans
df_buck.groupby("StyleID").count().show()
df.groupby("StyleID").count().show()

df_buck.groupby("StyleID").count().explain()
df.groupby("StyleID").count().explain()

# let's look at the old way
df_styles = spark.read.csv(file_path_style, inferSchema=True, header=True, encoding="utf-8")
df_joined: DataFrame = df.join(df_styles, on="StyleID")
df_joined.select(sf.countDistinct("StyleID")).show()

# and the new way
df_styles = spark.read.csv(file_path_style, inferSchema=True, header=True, encoding="utf-8")
df_joined: DataFrame = df_buck.join(df_styles, on="StyleID")
df_joined.select(sf.countDistinct("StyleID")).show()

# no difference, regarding shuffling or similar.


###########################################################
## lets bucket both parts

bucket_file2_name = "styleData_bucket_demo"

df_styles = spark.read.csv(file_path_style, inferSchema=True, header=True, encoding="utf-8")
df_styles.write.bucketBy(20, 'StyleID').saveAsTable(bucket_file2_name, mode="overwrite")

df_buck = spark.table(bucket_file_name)
df_buck_style = spark.table(bucket_file2_name)

df_joined: DataFrame = df_buck.join(df_buck_style, on="StyleID")
df_joined.select(sf.countDistinct("StyleID")).show()

# no shuffling for the join!


# and try the filtering
df_joined.where("StyleID = 1").show()
df_joined.where("StyleID = 1").explain()

# %% Epilog, what about Spark 3.*
spark.conf.set("spark.sql.adaptive.enabled", "true")

df, df_styles = csv_reader.load(file_path_recipe), csv_reader.load(file_path_style)
df_joined: DataFrame = df.join(df_styles, on="StyleID")
df_joined.select(sf.countDistinct("BeerID")).show()

df_joined.select(sf.countDistinct("BeerID")).explain()
df_joined.select(sf.countDistinct("BeerID")).show()


print(spark.sparkContext.uiWebUrl)
