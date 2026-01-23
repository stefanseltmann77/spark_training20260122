# %% SETUP #############################################################################################################
from pyspark.sql import functions as sf
from pyspark.sql.utils import AnalysisException

from spark_setup_spark3 import get_spark, path_demodata
import pandas as pd

spark = get_spark()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# %% Simple Join, but with duplicate column names #####################################################################
df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8").cache()
df_styles = spark.read.csv((path_demodata / 'styleData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8").cache()

df.printSchema()
df_styles.printSchema()

df_joined = df.join(df_styles, on="StyleID", how="left")
df_joined.show()

# Warning!! the name column "Style" is contained twice
pd.Series(df_joined.columns).value_counts()[0:5]

try:
    df_joined.write.parquet("df_joined", mode="overwrite")
except AnalysisException as e:
    print(e)

df_joined.select("Style")
# pyspark.sql.utils.AnalysisException: "Reference 'Style' is ambiguous, could be: Style#766, Style#849.;"

# even after the join, the columns can be called by their origin dataframe, because it hasn't happened yet
df_joined.explain()
df_joined.select(df.Style).show()
df_joined.select(df_styles.Style).show()
df_joined.select(df.Style, df_styles.Style).show()

# call the explain plan to trace the origin of columns
df_joined.explain()

# %% Workarounds A #####################################################################################################
# rename the columns early
df_styles = df_styles.withColumnRenamed("Style", "Style_joined")
df_joined = df.join(df_styles, on="StyleID", how="left")
df_joined.show()

df_joined.select("Style")  # no errors
df_joined.withColumnRenamed("Size(L)", "Size").write.parquet("df_joined", mode="overwrite")

# %% Workarounds B #####################################################################################################
# call all columns by their name
df_styles = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8").cache()
df_joined = df.join(df_styles, on="StyleID", how="left")

df_columns = df.columns
df_columns.remove("Style")
df_joined.select(*df_columns,
                 df.Style.alias("StyleOld"),
                 df_styles.Style)
# WARNING! This is just an demonstration example! Try, never to use "*" or similar in selects.
# Keep it explicit at all times.

# better:
df_joined.select(df.BeerID,
                 df.Name,
                 df.URL,
                 # df.Style  # omitted to avoid clash
                 df_styles.Style)

#  %% Workarounds C ###################################################################################################
# In my opinion BEST PRACTICE
# explicitly state the used columns in advance!
df_styles = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8").cache()
df = df.select("BeerID",
               "Name",
               "URL",
               "StyleID")

df_styles = df_styles.select("StyleID",
                             "Style")

df_joined = df.join(df_styles, on="StyleID", how="left")
df_joined.show()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df_joined.explain()


# %% #################################################################################
# Join condiditions can be stated without actual data
from pyspark.sql.functions import col
from pyspark.sql.column import Column
df_styles = spark.read.csv((path_demodata / 'styleData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8")
df_styles = df_styles.withColumnRenamed("StyleID", "StyleID_join")

join_condition = col("StyleID") == col("StyleID_join")

df_joined = df.join(df_styles, on=join_condition, how="left")



# %%  #################################################################################
# Join conditions can be stated with data in advance
df_styles = spark.read.csv((path_demodata / 'styleData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8")
join_condition = df.StyleID == df_styles.StyleID
df_joined = df.join(df_styles, on=join_condition, how="left")
df_joined = df.join(df_styles, on=df.StyleID == df_styles.StyleID, how="left")



# %% #################################################################################
# Meet the semi join
df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8")
df_styles = spark.read.csv((path_demodata / 'styleData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8")

# lets reduce the styles a bit and induce duplicates
df_styles_sample = df_styles.sample(True, 0.5, 42)
df_styles_sample.count()

# we clearly have a lot of duplicates
df_styles_sample.groupby("StyleID").count().orderBy("count", ascending=False).show()

complete_count = df.count()
# the usual way to filter on the styles involves duplication
df_filtered = df.join(df_styles_sample.select("StyleID").distinct(), on="StyleID", how="inner").select("*")
df_filtered_count = df_filtered.count()
df_filtered_count

# the semi-join makes it easier
df_semi_filtered = df.join(df_styles_sample, on="StyleID", how="leftsemi").select("*")
df_semi_filtered_count = df_semi_filtered.count()
assert(df_filtered_count==df_semi_filtered_count)


# the anti-join delivers the opposite
df_antifiltered = df.join(df_styles_sample, on="StyleID", how="leftanti").select("*")
df_anti_filtered_count = df_antifiltered.count()
assert(df_anti_filtered_count+df_filtered_count ==  complete_count)


# %% #######################################################
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100000000")

df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8")
df_styles = spark.read.csv((path_demodata / 'styleData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8")
df.printSchema()
df_styles.printSchema()

# try to set the broadcast deliberately to make your code more explicit.
df_joined = df.join(sf.broadcast(df_styles), on="StyleID", how="left")
df_joined.explain()
df_joined.count()


# -> with Spark3.+ you should get broadcast joins automatically due to AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")  # make, sure it is enabled
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100000")   # legacy, raise if too low
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "100000")   # raise if too low

df_joined = df.join(df_styles, on="StyleID", how="left")
df_joined.explain()
df_joined.count()

