# %% SETUP ############################################################################################################

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

# %% Load Data #################################################################
df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8").cache()
df.printSchema()

# %% Select Columns, 4 different Styles + 1 SQL variant## ############################################################
# old style, like sql
df.createOrReplaceTempView("df_tmp")
spark.sql("SELECT Style, BrewMethod FROM df_tmp")

# most simple, with string
df.select("Style", "BrewMethod")

# advanced with columns
from pyspark.sql.functions import col

df.select(col("Style"), col("BrewMethod"))

# explicit from Dataframe
df.select(df.Style, df.BrewMethod)
# or
df.select(df['Style'], df['BrewMethod'])

# %% rename Columns #############################################################
df = df.withColumnRenamed("Size(L)", "Size_L")

# if run twice, ... no error
df = df.withColumnRenamed("Size(L)", "Size_L")
df = df.withColumnRenamed("Size(L)", "Size_L")

# %% new columns ################################################################
# SELECT BoilTime * 60 as BoilTime_Second FROM ...
df = df.withColumn("BoilTime_Second", df.BoilTime * 60)

# idempotent, ... repetition raises no error
df = df.withColumn("BoilTime_Second", df.BoilTime * 60)

# %% dropping columns ###########################################################
# create a dummy
df = df.withColumn("superfluous_column", df.BoilTime)
# drop it again
df = df.drop("superfluous_column")
# repeated runs do not throw an error
df = df.drop("superfluous_column")

# %% casting columns ###########################################################
# cast is a method of columns.
# use the string for the new type ...
df.withColumn("beer_id_string", df.BeerID.cast("String"))
# or better the type object
from pyspark.sql.types import StringType

df.withColumn("beer_id_string", df.BeerID.cast(StringType()))

# %% using functions ############################################################
# if you are lazy, resuse the like sql
df.registerTempTable("df_tmp")
spark.sql("SELECT BoilTime*60 BoilTime_Seconds, 1 as SomeNumber, cast(StyleID as String) FROM df_tmp ").show()

# as string columns
df.selectExpr("BoilTime*60 BoilTime_Seconds",
              "1 as SomeNumber",
              "cast(StyleID as String)")

# the proper way as spark column objects
from pyspark.sql.functions import lit

df.select((df.BoilTime * 60).alias("BoilTime_Seconds"),
          lit(1).alias("SomeNumber"),
          col("StyleID").cast("String"))

# %% filtering data ############################################################
# where conditions
df.show()

df_ale = df.where(df.Style == "Cream Ale")  # just an alias for filter
df_ale.count()
df_ale = df.filter(df.Style == "Cream Ale")
df_ale.count()
df_ale = df.filter("Style = 'Cream Ale'")  # carefull, just one "="
df_ale.count()

# %% handling missings or replacing values #####################################
# replacement of values
df = df.withColumn("Style2", df.Style)
df.replace("Cream Ale", "Creamy Beer")  # replacement in the complete dataset!

# replaces only two columns
df.replace("Cream Ale", "Creamy Beer", ['Style', 'Style2'])  # replaces only two columns

# replaces only two columns, and two values at once
df.replace({"Cream Ale": "Creamy Beer",
            "Stale Ale": "Stale Beer"}, subset=['Style', 'Style2'])  # ersetzt nur in zwei Spalten

# replace missings
df.fillna("???")  # reclaces all missing in the dataset
cnt_columns = [col for col in df.columns if col.endswith("cnt")]
df.fillna(0, subset=cnt_columns)  # replaces only one columns

# drop missing
# drop records with at least one missing in one of the three columns
df.dropna(how="any", subset=["Style", "BrewMethod", "BeerID"])
df.dropna(thresh=2)  # keep only records with at least 2 valid

# deduplicate
df.distinct()  # get distinct on all rows
df.drop_duplicates(subset=['BeerID'])  # get distinct on subset of rows
# WARNING! ... no real guarantee on the order of records.

# %% the CASE WHEN statement:
import pyspark.sql.functions as sf

# SELECT ... CASE WHEN abv > 15 THEN 1 ELSE 0 END is_liqueur ...
df = df.withColumn("is_liqueur", sf.when(df.ABV > 15, 1).otherwise(0))
df.select(sf.sum("is_liqueur")).show()

# %% Grouping a first glance: ######################################################################################
import pyspark.sql.functions as sf
from pyspark.sql.group import GroupedData

grouping: GroupedData = df.groupby(['Style'])

grouping.agg(sf.count(df.Style),
         sf.count_distinct(df.Style),
         sf.max(df.ABV)).show()

grouping.count().show()
grouping.pivot("BrewMethod").mean().show()

# welches Bier hat am meisten Umdrehungen?
grouping.pivot("BrewMethod").agg(sf.max("ABV")).show(truncate=False)
