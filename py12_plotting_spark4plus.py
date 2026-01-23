# %% SETUP ############################################################################################################
from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True,
                    encoding="utf-8").cache()
df.printSchema()

# %% New Feature only in Spark 4.0+
# needs plotly
fig = df.groupby('StyleID').count().plot()
fig.show()
