from pyspark.sql import functions as sf

from spark_setup_spark3 import get_spark, path_trainingdata

spark = get_spark()

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

# df.where(df['authors'].like('%Iain%Banks%')).orderBy('original_publication_year').show()

# %% Exercise 01: How many distinct authors are there?
# Please use an aggregation and not the df.distinct() method.

# %% Exercise 02: which 10 authors have the most books in the sample

# %% Exercise 03: What is the mean, maximum and minimum average_rating
# for these authors and the earliest original_publication_year.


