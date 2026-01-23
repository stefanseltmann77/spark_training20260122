# %% SETUP #############################################################################################################
from pyspark.sql import functions as sf

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# Spark3.2
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")  # raise if too low
spark.conf.set("spark.sql.adaptive.enabled", "true")  # make, sure it is enabled

df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8")
df_styles = spark.read.csv((path_demodata / 'styleData.csv').as_posix(), inferSchema=True, header=True,
                           encoding="utf-8")
df.printSchema()
df_styles.printSchema()
df_styles.show()

df_joined = df.join(df_styles, on="StyleID", how="left")

df_joined.collect()

df_joined.repartition(10, 'StyleID')
# try to set the broadcast deliberately to make your code more explicit.
df_joined = df.join(sf.broadcast(df_styles), on="StyleID", how="left")
df_joined.explain()
df_joined.count()

# -> with Spark3.+ you should get broadcast joins automatically due to AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")  # make, sure it is enabled
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100000")  # legacy, raise if too low
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "100000")  # raise if too low

df_joined = df.join(df_styles, on="StyleID", how="left")
df_joined.explain()
df_joined.count()
