from pyspark.sql import functions as sf
from spark_setup_spark3 import get_spark, path_trainingdata

spark = get_spark()

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True, escape="\"")

# %% Exercise 01: Save the data as a plain parquet file.
# Lookup the number of resulting files in the filesystem.

# %% Exercise 02: Save the data partitioned by language, choose a different name.
# Explore the folder containing the data. How does the file structure look like?
# how many files are there now?

# %% Exercise 03: Read the partitioned parquet into a new DataFrame.

# %% Exercise 04: Print the schema.

# %% Exercise 05: Save the file again
# distributed by language in 5 partitions.
