from spark_setup_spark3 import get_spark, path_trainingdata

spark = get_spark()

df = spark.read.csv("./data/bs140513_032310.csv", header=True, inferSchema=True, quote="'")

# %% Exercise 01: ######################################################################################################
# Create several separate DataFrames from the data using selects:
# One dimension for the Customers containing customer, age, gender and zipcodeOri, unique of course.
# One for the Merchants containing merchant, zipMerchant, also unique.
# One fact-table for the Transactions with category, amount and fraud, ... and the columns to join both the merchant
# and the customer back in again.

# %% Exercise 02: ######################################################################################################
# How many customers are there? How many merchants and how many transactions?
# Use your new DataFrames

# %% Exercise 03: #############################################################
# Which category of transaction causes the biggest total loss in frauds?
# What amount is lost?
# Use your new table for transactions

# %% Exercise 04: ######################################################################################################
# Which category has the highest single fraud case by amount?
# Use your new table for transactions

# %% Exercise 05: ######################################################################################################
# How old is the customer that has the most fraud cases in travel.
# Use your new DataFrames instead of the imported table via a join.

# %% Exercise 06: ###############################################################
# Try calculating a share from the total loss per category that every
# fraud is causing.
# For example x% from all losses related to es_travel, and so on.
# use windowing functions. see pyspark.sql Window

