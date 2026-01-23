# %% Setup
## ALWAYS IMPORT AS SPARK FUNCTIONS WITH AN ALIAS! NEVER pyspark.sql.functions.*
from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

# %% Loading and basic introspection ###########################################
df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8").cache()

# what are the datatypes?
df.dtypes

# show the schema
df.printSchema()

# what are my columns
df.columns  # return is a list
# ['BeerID', 'Name', 'URL', 'Style', 'StyleID', 'Size(L)', 'OG', 'FG', 'ABV', 'IBU', 'Color', 'BoilSize', 'BoilTime',
# 'BoilGravity', 'Efficiency', 'MashThickness', 'SugarScale', 'BrewMethod', 'PitchRate', 'PrimaryTemp', 'PrimingMethod',
# 'PrimingAmount']

# how many rows
df.count()

# data summary
df.describe().show()

# %% selecting columns #########################################################
# lets select just a few columns:
from pyspark.sql.functions import col

df.select("BeerID", "Name", "Style")

df.select("BeerID",  # as a string
          df["Name"],  # as a dataframe attribute
          df.Name,  # as a dataframe attribute again
          col("Style").alias("StyleAs")  # as a column object,
          ).show()

# or just pass the list of columns
my_columns = ["BeerID", "Name", "Style"]
df.select(my_columns)

# the 'select' acts like a view
df.select("BeerID", "Name", "Style").explain()

df.selectExpr("BeerID",
              "CASE WHEN BeerID > 0 THEN 1 ELSE 0 END as col2 ").explain()

# %% further analysis ##########################################################
# are the BeerIDs really distinct?
from pyspark.sql import functions as sf

df.agg(sf.count(df.BeerID),
       sf.countDistinct(df.BeerID)).show()
# seems unique.

# also in spark 3.2 +
# the same with python coding style
df.agg(sf.count(df.BeerID),
       sf.count_distinct(df.BeerID)).show()

# %% basic groupings ###########################################################
# How many beers for each Brewmethod are there?
df.groupby("BrewMethod").count().show()

df_by_brewMethod = df.groupby("BrewMethod")

df_grouped = df_by_brewMethod.agg(sf.count(df.BeerID),
                                  sf.countDistinct(df.BeerID))

df_grouped_renamed = df_grouped.withColumnRenamed("BrewMethod", "Braumethode")
df_grouped_renamed.show()

# or
df.select("BrewMethod").distinct().show()

# without action, it's just a definition of a grouping
df.groupBy("BrewMethod").count().show()
# count on group is just short for df.groupBy("BrewMethod").agg(sf.count("*")), which is also lazy
# results will be printed with show
df.groupBy("BrewMethod").count().show()
# the order is still wrong

df.groupBy("BrewMethod"). \
    count(). \
    orderBy("count", ascending=False). \
    show()

# what if I only want the most frequent one
df.groupBy("BrewMethod").count().orderBy("count", ascending=False).limit(1).show()

# the same works with sums ##
df.groupBy("BrewMethod").sum().show()
df.groupBy("BrewMethod").max().show()
df.groupBy("BrewMethod").min().show()

df.groupBy("BrewMethod").agg(sf.sum("StyleId"),
                             sf.max("StyleId"))

df.show(truncate=False)

# %% retrieving data ###########################################################
# fetching the results locally in three flavours
df.take(10)
df.collect()
df_pandas = df.toPandas()

df.take(10)[0].URL
df.take(10)[0][2]

# get the data locally:
df.take(1)[0].BrewMethod

# get all to pandas
pandas_df = df.toPandas()
pandas_df.shape

# %% complex aggregations #####################################################
# futher analysis
# group by Brewmethod and Style
df.groupby("BrewMethod", "Style").count().show()

# what are the most frequent styles within "All Grain"
df_allgrain = df.where(df.BrewMethod == "All Grain")
df_allgrain.groupby("BrewMethod", "Style").count().orderBy("count", ascending=False).show()

## more complex aggregations
# avg on two double values

df.agg(sf.round(sf.mean("BoilSize"), 2).alias("BoilSize_avg"),
       sf.round(sf.mean("Efficiency")).alias("Efficiency_avg")).show()

# or the same the old way:
df.agg(sf.expr("round(avg(BoilSize),2) as BoilSize_avg"),
       sf.expr("round(avg(Efficiency),2) as Efficiency_avg")).show()

# similar to pandas
df.agg({"BoilSize": "avg"}).show()

# grouping and aggregation can be defined independently
grouping = df.groupby("BrewMethod")
aggregation = [sf.round(sf.mean("BoilSize"), 2).alias("BoilSize_avg"),
               sf.round(sf.mean("Efficiency")).alias("Efficiency_avg")]

grouping.agg(*aggregation).show()
grouping.agg(*aggregation, sf.max("BoilSize"), sf.min("BoilSize")).show()

# %% # Analytical functions are also available #################################
# analytical functions with window
from pyspark.sql import Window
from pyspark.sql.functions import col

part_spec = Window.partitionBy(col("BrewMethod")).orderBy(col("BoilSize"))

df.select(df.BrewMethod,
          df.Efficiency,
          df.BoilSize,
          sf.sum(df.Efficiency).over(part_spec),
          sf.row_number().over(part_spec)).show()

# %% Crosstabs ################################################################
# DATAFRAME-Functions
# crosstabs are available on the dataframe
df.crosstab("Style", "BrewMethod").show()

# correlations, only Pearson
df.corr("BoilTime", "Efficiency")

# scan on frequent items, which have a support of 40%
df.freqItems(["Style", "BrewMethod"], 0.4).show(truncate=False)
