from pyspark.sql import functions as sf
from spark_setup_spark3 import get_spark, path_trainingdata

spark = get_spark()

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

# %% Exercise 01: How many rows of data are there?

# %% Exercise 02: Look at the Schema. Extract the datatypes,
# that are or type integer.
# Make a list of the column names of integers

# %% Exercise 03: select the columns isbn13 and original_publication_year.
# What types are they? Cast them to a more appropriate formats

# %% Exercise 04: Drop the two columns that contain urls

# %% Exercise 05: Create new columns,
# - Set the current date in a column: work_dt
# - top_rating_flg, if average_rating above 2.0 or not
# - contemporary_flg, if original_publication_year greater than 2000
# commands: "case when" ->  when().otherwise()
from pyspark.sql.functions import when, lit

# %% Exercise 06: Check if there are any missings

# %% Exercise 07: How many rows are left, if we discard all missings?

# %% Exercise 08: Which books have no language_code? What are the original titles.

