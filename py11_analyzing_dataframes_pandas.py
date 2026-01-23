# %% SETUP ############################################################################################################
from pyspark.sql import functions as sf
from spark_setup_spark3 import get_spark, path_demodata
import pandas as pd
import matplotlib.pyplot as plt

spark = get_spark()

df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8").cache()
df.printSchema()


# %% Switch to Pandas ##################################################################################################
pdf = df.toPandas()

pdf.StyleID.value_counts()

pdf.describe()
boil_times = pdf.groupby("StyleID").agg({"BoilTime": ['mean', 'max']})

boil_times.plot()
plt.show()


# %% And back to Spark: ################################################################################################
spark.createDataFrame(boil_times)
