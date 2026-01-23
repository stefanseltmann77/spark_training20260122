from spark_setup_spark3 import get_spark, path_trainingdata

spark = get_spark()

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

# %% Exercise 01: Read the ratings.csv, tags.csv, book_tags.csv and get familiar with the columns.
# What columns are there in each dataset?

# %% Exercise 02: Check all four DataFrames for columns to join with.

# %% Exercise 03: Take the book table and join it first with the ratings. What is the count?

# %% Exercise 04: Which User (by user_id) rated the most books?

# %% Exercise 05: What are the frequent tags for the books of this user?
# Display the labels


