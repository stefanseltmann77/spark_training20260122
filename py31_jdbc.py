# %% setup #############################################################################################################
import sqlite3

from pyspark.sql import SparkSession

from spark_setup_spark3 import get_spark

spark: SparkSession = get_spark()
db_name = 'beers.db'
db = sqlite3.connect(db_name)
df = spark.read.csv("./data/recipeData.csv", header=True, inferSchema=True)

# %% write_to_db ########################################################################################################
df.rdd.getNumPartitions()  # the file has 4 partitions
spark.sparkContext.uiWebUrl

# write to database
(df.coalesce(1).write.option("driver", "org.sqlite.JDBC").
 jdbc(url=f"jdbc:sqlite:{db_name}", table="beers", mode="overwrite"))
# the level of parallelism is the same as the number of partitions.

# lets repartition to 10
(df.repartition(10).write.option("driver", "org.sqlite.JDBC").
 jdbc(url=f"jdbc:sqlite:{db_name}", table="beers", mode="overwrite"))
# THIS WILL THROW AN ERROR, because sqlite does not allow parallel connections.
# parallelism = 10

# %% read_from_db #######################################################################################################

df_beers = spark.read.option("driver", "org.sqlite.JDBC").jdbc(url=f"jdbc:sqlite:{db_name}", table="beers")
df_beers.count()
df_beers.rdd.getNumPartitions()  # just 1 partition by default

# use boundaries and predefined partitions to parallize read operations.
df_beers.selectExpr("max(BeerID)").show()
df_rep = (spark.read.option("driver", "org.sqlite.JDBC").
          jdbc(url=f"jdbc:sqlite:{db_name}", table="beers", column="BeerID",
               lowerBound=0, upperBound=80000, numPartitions=10))
df_rep.count()

# uses 10 partitions!
