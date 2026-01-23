# %% SETUP #############################################################################################################
from pyspark.sql import DataFrame

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

# %% Loading the dataframe ############################################################################################
df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8")
df.printSchema()

# alternative for sql also possible
# df = spark.sql("SELECT * FROM csv.`./data/recipeData.csv` ")

# what are my columns?
print(df.columns)
# ['BeerID', 'Name', 'URL', 'Style', 'StyleID', 'Size(L)', 'OG', 'FG', 'ABV',
# 'IBU', 'Color', 'BoilSize', 'BoilTime',
# 'BoilGravity', 'Efficiency', 'MashThickness', 'SugarScale', 'BrewMethod',
# 'PitchRate', 'PrimaryTemp', 'PrimingMethod',
# 'PrimingAmount']

# %% Do analytics using SQL ###########################################################################################
### let's cache and register the table
df.cache()  # more on that later ...
df.printSchema()

# register table as a view
df.createOrReplaceTempView("beers")

# show me the data
spark.sql("SELECT * FROM beers").show()

# what are the BrewMethod by frequency?
spark.sql("""
    SELECT BrewMethod, count(*) 
    FROM beers 
    GROUP BY BrewMethod 
    ORDER BY 2 desc
    """).show()

# results from querys are DataFrames again
result = spark.sql("SELECT BrewMethod, count(*) FROM beers GROUP BY BrewMethod ORDER BY 2 desc")
assert isinstance(result, DataFrame)
result.show()

# Example for join with temporary table
df_boiltimes: DataFrame = spark.sql("SELECT BrewMethod, mean(BoilTime) BoilTime_avg "
                                    "FROM beers GROUP BY BrewMethod ORDER BY 2 desc")
df_boiltimes.show()
df_boiltimes.createOrReplaceTempView("boiltimes")  # register temp result as new view
spark.sql("SELECT * FROM beers INNER JOIN boiltimes USING(BrewMethod)").show(truncate=False)
# UI shows the loading stage is skipped.


# %% Getting metadata on tables #######################################################################################
spark.catalog.setCurrentDatabase("default")
spark.catalog.currentDatabase()  # default
spark.catalog.listDatabases()  # metadata locations
