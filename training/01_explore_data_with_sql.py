from pyspark.sql import functions as sf
from spark_setup_spark3 import get_spark, path_trainingdata

spark = get_spark()

# %% Exercise 01: Read the date from  "./training_data/books.csv"
# Use the SparkSession "spark" and read a csv as a DataFrame called "df".
# The file has a header. The schema can be inferred.

# %% Exercise 02: Get the columns of the DataFrame and/or Print its schema.

# %% Exercise 03: Register the Dataframe as a table called "books"
# dataframe -> createOrReplaceTempView

# %% Exercise 04: How many books are in the dataset
# spark -> sql

# %% Exercise 05: Make a "limited" Select of just 100 rows

# %% Exercise 06: Which languages are there (column language_code)
# and which are the most frequent ones?

# %% Exercise 07: What are the titles of the german books?

# %% Exercise 08: Make a summary of ratings (column average_rating) by publication year (max, min, mean)

# %% Exercise 09: Save the result of the summary to a new csv file, with header
# commands: [some_dataframe].write.csv


