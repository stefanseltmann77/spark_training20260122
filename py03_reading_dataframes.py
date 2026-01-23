# %% SETUP #############################################################################################################
from spark_setup_spark3 import get_spark, path_demodata

file_path = (path_demodata / 'recipeData.csv').as_posix()
spark = get_spark()


# %% SIMPLE CSV READING ################################################################################################

df = spark.read.csv(file_path)
df.printSchema()
df.take(5)
df.show(5)
# the result works but is missing column names. The column names are in the first row.

# %% READ DATAFRAME WITH SCHEMA AUTOINFERENCE ##########################################################################
df = spark.read.csv(file_path, inferSchema=True, header=True)
df.printSchema()
df.rdd.getNumPartitions()

# let's see whats insinde
df.describe().show()

# %% READ DATAFRAME WITH PREDEFINED SCHEMA #############################################################################
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([StructField("BeerID", IntegerType(), True),
                     StructField("Name", StringType(), False),
                     StructField("URL", StringType(), False),
                     StructField("Style", StringType(), False),
                     StructField("StyleID", IntegerType(), False),
                     StructField("Size (L)", DoubleType(), True),
                     StructField("OG", DoubleType(), True),
                     StructField("FG", DoubleType(), True),
                     StructField("ABV", DoubleType(), True),
                     StructField("IBU", DoubleType(), True),
                     StructField("Color", DoubleType(), True),
                     StructField("BoilSize", DoubleType(), True),
                     StructField("BoilTime", IntegerType(), True),
                     StructField("BoilGravity", StringType(), True),
                     StructField("Efficiency", DoubleType(), True),
                     StructField("MashThickness", StringType(), True),
                     StructField("SugarScale", StringType(), True),
                     StructField("BrewMethod", StringType(), True),
                     StructField("PitchRate", StringType(), True),
                     StructField("PrimaryTemp", StringType(), True),
                     StructField("PrimingMethod", StringType(), True),
                     StructField("PrimingAmount", StringType(), True)])
df = spark.read.csv(file_path, schema=schema, header=True)
df.printSchema()

df.head()

# The schema can be derived from existing dataframes, even in json-Form
# as Repr-String:
df.schema

df.show()

# or in json form
df.schema.json()

# %% READER OBJECTS ####################################################################################################
# if you want to read different files with the same settings, use a dataframe reader

reader = spark.read.format("csv").options(**{'header': True, 'inferSchema': True, 'encoding': "utf-8"})

print(reader)  # <pyspark.sql.readwriter.DataFrameReader object at xxxxx>

df = reader.load(file_path)
df_2 = reader.load(file_path)

# %% READ DIRECTLY FROM FILE ###########################################################################################

df_sql = spark.sql(f"SELECT * FROM csv.`{file_path}`")
df_sql.show()
# but how do you pass arguments for parsing?
