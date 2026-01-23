# %% SETUP #############################################################################################################
from contextlib import suppress

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

# read dataframe schema autoinference
df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True)
df.printSchema()
df.rdd.getNumPartitions()

# %% WRITING DATAFRAMES ################################################################################################
with suppress(AnalysisException):
    df.write.parquet("./tmp_data/recipeData.whatever", mode="overwrite")
    # ERROR due to forbidden column names !!! for Spark <= 3.2

# fixing column names by renaming works
df = df.withColumnRenamed("Size(L)", "Size")
df.write.parquet("./tmp_data/recipeData.whatever", mode="overwrite")
# notice the number of files matches the number of rdds

# partition with folder structure
df.write.parquet("./tmp_data/recipeData.partitioned", partitionBy="BrewMethod", mode="overwrite")
df_read = spark.read.parquet("./tmp_data/recipeData.partitioned").where("BrewMethod == 'All Grain'")
df_read.explain(mode="formatted")

# you can also add to a table in append mode:
# example append mode
df.coalesce(1).write.parquet("./tmp_data/recipeData.appends", mode="overwrite")
df.coalesce(1).write.parquet("./tmp_data/recipeData.appends", mode="append")
df.coalesce(1).write.parquet("./tmp_data/recipeData.appends", mode="append")
df.coalesce(1).write.parquet("./tmp_data/recipeData.appends", mode="append")

# read it and check that it has 4 times the size of the original
df_parqet_apps = spark.read.parquet("./tmp_data/recipeData.appends")
assert df_parqet_apps.count() == 4 * df.count()
# drop the duplicates and see, that it has the original count again
df_parqet_apps.drop_duplicates().count()

# finally write part of the data prepartitioned (in memory) to disk, in this way control the number of files
df.repartition(3, "BrewMethod").write.parquet("./tmp_data/recipeData.prepart", mode="overwrite")
# a hash partitioning happens that tries to evenly distribute the contents.


# %% PARTITION SWAPPING ################################################################################################
# there is no real partition swapping. you have to carefully overwrite existing partitions
df = df.withColumnRenamed("Size(L)", "Size")


def reset_experiment() -> DataFrame:
    """In order to show the different effects, we will write a clean complete prepartitioned table
    in addition the subsample for 'All Grain' will be returned.
    """
    df.write.parquet("./tmp_data/recipeData.partitioned",
                     mode="overwrite", partitionBy="BrewMethod")
    df_allgrain = df.where(df.BrewMethod == "All Grain")
    return df_allgrain

# reset
df_allgrain = reset_experiment()

spark.read.parquet("./tmp_data/recipeData.partitioned").count()  # 73861 records
# naive approach: lets overwrite by partition
df_allgrain.write.parquet("./tmp_data/recipeData.partitioned",
                          mode="overwrite", partitionBy="BrewMethod")
spark.read.parquet("./tmp_data/recipeData.partitioned").count()
# ONLY 49692 records, the rest got DROPPED
# all other partitions are dropped!!


# reset again
df_allgrain = reset_experiment()
# state the correct insert partition file path. don't use partition by, but mode overwrite
df_allgrain.write.parquet("./tmp_data/recipeData.partitioned/BrewMethod=All Grain",
                          mode="overwrite")
spark.read.parquet("./tmp_data/recipeData.partitioned").count()  # All 73861 accounted for!
# but not very nice

# the new undocumented way
df_allgrain = reset_experiment()
spark.conf.get("spark.sql.sources.partitionOverwriteMode")  # should be static per default
# set param to dynamic!
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
df_allgrain.write.parquet("./tmp_data/recipeData.partitioned", mode="overwrite", partitionBy="BrewMethod")
spark.read.parquet("./tmp_data/recipeData.partitioned").groupby("BrewMethod").count().show()
# then it works fine again.
