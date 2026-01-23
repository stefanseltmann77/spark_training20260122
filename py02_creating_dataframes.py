# %% SETUP #############################################################################################################
import string

from pyspark.sql import DataFrame

from spark_setup_spark3 import get_spark

spark = get_spark()

# generate some dummy data based on the basic letters and an autoincrement
dummy_data = [[i, char] for i, char in enumerate(string.ascii_letters)]
# [[0, 'a'], [1, 'b'], ...

# %% CREATE DATAFRAMES FROM LIST-LIKE DATA #############################################################################
# data looks like [[0, 'a'], ...]
df: DataFrame = spark.createDataFrame(dummy_data)

# what's in the dataframe?
# look at the human-readable form, the column names are automatically generated
df.show()

# raw form, 10 rows, the format is still without column names
df.take(10)

# raw form, take all back to driver
df.collect()
df.head(5)

# schema in human readable form
df.printSchema()

# schema in code form
df.schema

# %% CREATE DATAFRAMES FROM PANDAS DATAFRAMES ##########################################################################
import pandas as pd

# create pandas dataframe based on the dummy data
pandas_df = pd.DataFrame(dummy_data, columns=['index', 'character'])

# cast pandas dataframe to spark dataframe, column names are adopted
df = spark.createDataFrame(pandas_df)
df.show()

# column names are inherited!
assert (df.columns == list(pandas_df.columns))  # asserts successfully

# %% CREATE DATAFRAMES FROM LIST-LIKE DATA with schema #################################################################
df: DataFrame = spark.createDataFrame(dummy_data)  # without schema ...
df.show()  # ... column names are missing!

# create it with simple listing of column names
df: DataFrame = spark.createDataFrame(dummy_data, schema=['index', 'character'])
df.show()
df.columns  # column names are present!

# even better:
# the most precise way is defining a schema
from pyspark.sql.types import StructField, StringType, StructType, ByteType

schema = StructType([StructField("index", ByteType(), False),
                     StructField("character", StringType(), False)])

# A schema collision will FAIL FAST at reading the data!
# therefore it is the best way for solid data ingest.
df: DataFrame = spark.createDataFrame(dummy_data, schema=schema)
df.show()

df.columns  # column names are present and the extended schema
df.printSchema()

# %% CREATE DATAFRAMES FROM LIST-LIKE DATA with ROWS ##################################################################
from pyspark.sql import Row

test_rows = [Row(company="ACME", product="Toxic Waste", product_id=123, stock_value=92.32),
             Row(company="LUMOS", product="Macro Data", product_id=345, stock_value=99.32)]
df_by_row = spark.createDataFrame(test_rows)
df_by_row.show(truncate=False)

# Rows are nice as well. They act as a record
# get the value per label
test_row = test_rows[0]
assert test_row.company == 'ACME'
# or cast it as a dict
assert test_row.asDict() == {'company': 'ACME', 'product': 'Toxic Waste', 'product_id': 123, 'stock_value': 92.32}

# all options shown here are useful in order to create test data
