from pyspark.sql import functions as sf
from spark_setup_spark3 import get_spark, path_trainingdata
from pyspark.sql import Window
from pyspark.sql import WindowSpec

spark = get_spark()

df = spark.read.csv("./data/bs140513_032310.csv", header=True, inferSchema=True, quote="'")

df_customers = df.select("customer", "age", "gender", "zipcodeOri").distinct()
df_merchants = df.groupby('merchant').agg(sf.first('zipMerchant'))
df_transactions = df.select('category', 'amount', 'fraud', 'merchant', 'customer')

result = df_transactions.where(sf.col("fraud") == 1).groupby("category", "fraud") \
    .agg(sf.round(sf.max("amount"), 0).alias("amount")) \
    .orderBy(sf.desc("amount"))

window_term_fraud: WindowSpec = Window().partitionBy("category", "fraud")
window_term_total: WindowSpec = Window().partitionBy("category")
amended_transcactions = df_transactions.select(sf.monotonically_increasing_id().alias("transaction_id"),
                                               "category",
                                               "amount",
                                               "fraud",
                                               sf.sum("amount").over(window_term_fraud).alias("amount_category_fraud"),
                                               sf.sum("amount").over(window_term_total).alias("amount_category_total"))
amended_transcactions.printSchema()

# %% Exercise 01: ######################################################################################################
# Try to make a dynamic aggregation.
# Define the avg, max and min for every numerical column (see dtypes) in amended_transactions,
# except the transaction_id. Try not to reference directly to the dataframe, except when retrieving the column names.
# group by category and add the predefined columns to the aggregation


# %% Exercise 02: ######################################################################################################
# Imagine, you are testing with just a few records of dummy-data
# Create a Test-Dataframes for amended_transactions with 3 rows.
# Do it by creating rows and by creating a dataframe of the rows.
# or by creating creating a dataframe directly from raw data by using a schema.

