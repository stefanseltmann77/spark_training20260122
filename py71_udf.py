# %% SETUP #############################################################################################################
from pandas import Series
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

df: DataFrame = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True)
df.createOrReplaceTempView("recipes")
df.printSchema()

# SIMPLE EXAMPLE URL EXTRACTION  ######################################################################################
# %% old style spark 1-2  DEPRECATED ##################################################################################
df.show(truncate=False)

# Imagine, you want to extract the ID from the URL
def extract_id_from_url(url: str) -> int:
    return int(url.split("/")[4])

assert extract_id_from_url("/homebrew/recipe/view/10092/hop-notch-clone") == 10092

from pyspark.sql.types import IntegerType

# register for SQL
spark.udf.register("extract_id", extract_id_from_url, IntegerType())

spark.sql("SELECT extract_id(t.URL) FROM recipes t").show(truncate=False)

# register for API
extract_id = sf.udf(extract_id_from_url, IntegerType())
df.select(extract_id(sf.col("URL"))).show()


# %% new style spark 3+ ###############################################################################################

###############
# Use pandas_udf to define a Pandas UDF, with decorator
from pyspark.sql.functions import pandas_udf, PandasUDFType

# old version from Spark 2.4  # DEPRECATED
@pandas_udf('string', PandasUDFType.SCALAR)
def extract_id_from_url(url):
    return int(url.split("/")[4])
# old version from Spark 2.4  # DEPRECATED


# new version from Spark3.0
# pass and recieve a series. state the type of each element, as if you would be working with Pandas
@pandas_udf(IntegerType())
def extract_id_from_url(url: Series) -> Series:
    return url.str.split("/").str[4].astype("int")
df.select(extract_id_from_url(sf.col("URL"))).show(truncate=False)


# Use pandas_udf to define a Pandas UDF, with function registration
from pyspark.sql.functions import pandas_udf


def pandas_times_two(v):
    return v * 2.0

make_double = pandas_udf(pandas_times_two, returnType=IntegerType())

df.select(sf.col("ABV"), make_double(sf.col("ABV"))).show()

