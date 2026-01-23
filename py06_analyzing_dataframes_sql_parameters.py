# %% SETUP #############################################################################################################

from contextlib import suppress

from pyspark.errors.exceptions.captured import ParseException

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

# %% Loading the dataframe ############################################################################################
df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8")
df.createOrReplaceTempView("beers")
df.printSchema()


# %% Do analytics using SQL ###########################################################################################
# spark.sql("SELECT * FROM beers").show()

style_id = 45

# working for simple data types
spark.sql(f"SELECT * FROM beers WHERE StyleID = {style_id}").show()

beer_name = "Vanilla Cream Ale"
# needs quotes for strings, ... not quite dynamic any more
spark.sql(f"SELECT * FROM beers WHERE Name = '{beer_name}'").show()

beer_name2 = "Cali' Creamin' Clone"
with suppress(ParseException):
    spark.sql(f"SELECT * FROM beers WHERE Name = '{beer_name2}'").show()
    # fails without proper preprocessing!


# %% Correct way:
beer_name2 = "Cali' Creamin' Clone"
spark.sql(f"SELECT * FROM beers WHERE Name = :beer_name", args={'beer_name': beer_name2}).show()
# use proper parameter that does sanitizing for you

