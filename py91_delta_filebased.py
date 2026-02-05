# %% SETUP #############################################################################################################
# WARNING THIS EXAMPLES ONLY WORKS WITH A FILESYSTEM PRESENT, E.G. A LOCAL SPARK EXECUTION
from delta.tables import DeltaTable

from spark_setup_spark3 import get_spark, path_demodata

PATH = "delta/recipe_2023"

spark = get_spark()

df = spark.read.option("encoding", "utf-8") \
    .csv((path_demodata / 'recipeData.csv').as_posix(),
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")

# %% Use Delta right away ##############################################################################################
# simply safe the table to the new format
# write to a location unmanaged!
# df.write.format('delta').save(PATH,  mode='overwrite')
# or
df.write.save(PATH, 'delta', mode='overwrite')

# every_time you write new data the old will be kept
df.write.save(PATH, "delta", mode='overwrite')
df.write.save(PATH, "delta", mode='overwrite')
df.write.save(PATH, "delta", mode='overwrite')

# %% ACID: Read and write from the same table: #########################################################################
# What if we read and write from the same parquet table.
df_csv = spark.read.option("encoding", "utf-8") \
    .csv((path_demodata / 'recipeData.csv').as_posix(),
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")
df_csv.write.parquet('tmp/parquet', mode='overwrite')

df_parquet = spark.read.parquet('tmp/parquet')
try:
    df_parquet.write.parquet('tmp/parquet', mode='overwrite')
except:
    print("Reading and writing from the same parquet is not allowed!")

# Let's try the same with delta
df = spark. \
    read. \
    load(PATH, 'delta')
df.write.save(PATH, 'delta', mode='append')
# works without error

# %% Reading Delta as Parquet ##########################################################################################
df_csv = spark.read.option("encoding", "utf-8") \
    .csv((path_demodata / 'recipeData.csv').as_posix(),
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")
tmp_delta_path = 'tmp/delta'

df_csv.write.save(tmp_delta_path, 'delta', mode='overwrite')
df_csv.write.save(tmp_delta_path, 'delta', mode='overwrite')

try:
    assert spark.read.format("parquet").load(path=tmp_delta_path).count() == \
           spark.read.format("delta").load(path=tmp_delta_path).count()
except AssertionError:
    print("Assertion error, because reading delta as plain parquet, will also read deleted rows.")
deltatab = DeltaTable.forPath(spark, tmp_delta_path)

