from spark_setup_spark3 import get_spark, path_trainingdata

spark = get_spark()

df = spark.read.csv("./data/bs140513_032310.csv", header=True, inferSchema=True, quote="'")

# %% Exercise 01: How many rows of data are there?

# %% Exercise 02: What columns are there and which datatypes?

# %% Exercise 03: cast the zipcodeOri to string


# %% Exercise 04: Build a new flag (0/1) that signifies if the customer is female

# %% Exercise 05: How many distinct customers are there?


# %% Exercise 06: How many fraud cases are there?


# %% Exercise 07: How many distinct female fraud customers are there. Use the new flag for is_female?
