# %% SETUP #############################################################################################################

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.utils import AnalysisException

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

### Loading the dataframe
df: DataFrame = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True,
                               encoding="utf-8")
df.printSchema()

df.select("Name").show(truncate=False)

# %% #################################################################################################################
# which words are contained in the name column?
try:
    df.select(sf.explode("Name"))
except AnalysisException as e:
    print(e)
# --> throws an exception because explode is only applicable to a kind of array

# lets split the name by whitespace
df_names = df.select("Name", sf.split("Name", " ").alias("name_chunks"))
df_names.show(truncate=False)
df_names.printSchema()  # the result is an array of strings!

# what is the first element?
df_names.select("Name", sf.col("name_chunks")[0]).show()
# I can't figure out how to get the last element. At least it throws no error.
df_names.select(sf.col("name_chunks")[4]).show()

# we can check the content
df_names.select(sf.array_contains("name_chunks", "Zombie")).show()

# we can sort it
df_names.select(sf.sort_array("name_chunks")).show()

# What to to with it?  We can make it flat!
df_names_flat = df_names.select(sf.explode("name_chunks").alias("name_chunks"))
df_names_flat.show()

# what ar the most frequent words?
df_names_flat.groupby("name_chunks").count().orderBy(sf.desc("count")).show()

# how do we get it back?
df_names_flat.select(sf.collect_list("name_chunks")).show()
# or without dupes
df_names_flat.select(sf.collect_set("name_chunks")).show()

# but I wanted to keep my rows!
df_names_flat = df_names \
    .select(sf.monotonically_increasing_id().alias("someRowID"), "name_chunks") \
    .select("someRowID", sf.explode("name_chunks").alias("name_chunks"))
df_names_flat.show()

# and back again as a grouping
df_names_flat.groupby("someRowID").agg(sf.collect_set("name_chunks")).show()

# or identify a row with the original name content
df_with_id = df_names.select("Name", sf.explode("name_chunks").alias("name_chunks"))
df_with_id.show()
df_with_id.groupby("Name").agg(sf.collect_list("name_chunks")).show()

# %% #################################################################################################################
# Usefull in some usecases: The postion within the array:
df_names = df.select("Name", sf.split("Name", " ").alias("name_chunks"))
df_names_flat_pos = df_names.select("Name", sf.posexplode("name_chunks").alias("row", "name_chunks"))
df_names_flat_pos.show()

# %% #################################################################################################################
# What are the most frequent words in "name" in one pass
df.select("Name", sf.explode(sf.split("Name", " ")).alias("name_chunks")). \
    groupby("name_chunks").agg(sf.count("name_chunks").alias("count")). \
    orderBy(sf.desc("count")).show()
