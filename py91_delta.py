# %% SETUP #############################################################################################################
import datetime
import os
from pprint import pprint

import pyspark.sql.functions as sf
from delta.tables import DeltaTable

import delta
from spark_setup_spark3 import get_spark, path_demodata

PATH = "delta/recipe_2023"

spark = get_spark()

df = spark.read.option("encoding", "utf8") \
    .csv((path_demodata / 'recipeData.csv').as_posix(),
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")

# %% Use Delta right away ##############################################################################################
# simply safe the table to the new format

df.show()

# write to a location unmanaged!
# df.write.format('delta').save(PATH,  mode='overwrite')
# or
df.write.save(PATH, 'delta', mode='overwrite')

# write as managed table
df.write.saveAsTable('dt_recipedata', format='delta')
# see table in metadata
spark.catalog.listTables()

# every_time you write new data the old will be kept
df.write.save(PATH, "delta", mode='overwrite')
df.write.save(PATH, "delta", mode='overwrite')
df.write.save(PATH, "delta", mode='overwrite')

# %% how can I check the revisions? ####################################################################################

# register the table

deltatab = DeltaTable.forPath(spark, PATH)

deltatab.history()

# show me version history
deltatab.history().show(truncate=False)
# or just the most recent one
deltatab.history(1).show()
# show the operational metrics
deltatab.history().select("operationMetrics").show(truncate=False)

df = spark.read.format('delta').load(PATH)
df.count()

# %% how can I check the table metadata? ###############################################################################

# for the unmanaged:
deltatab = DeltaTable.forPath(spark, PATH)
deltatab.detail().show(truncate=False, vertical=True)  # the location is our path

# now for the managed:
deltatab_managed = DeltaTable.forName(spark, 'dt_recipedata')
deltatab_managed.detail().show(truncate=False, vertical=True)  # the location is in a spark warehouse


# %% ACID: Read and write from the same table: #########################################################################
# What if we read and write from the same parquet table.
df_csv = spark.read.option("encoding", "utf8") \
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
df_csv = spark.read.option("encoding", "utf8") \
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

# %% load the data depending on time or version ########################################################################

# based on the version number
df = spark. \
    read. \
    format("delta"). \
    option("versionAsOf", 0). \
    load(PATH)

# based on the time => timetravel
df = spark. \
    read. \
    format("delta"). \
    option("timestampAsOf", '2023-03-31 07:40:00'). \
    load(PATH)

# cleanup of old versions
deltatab.history().show()
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
deltatab.vacuum(retentionHours=0)
spark.sparkContext.uiWebUrl

# %% try deletes and updates

deltatab.delete(sf.col("BrewMethod") == "extract")
deltatab.update(sf.col("BrewMethod") == "BIAB", {"BrewMethod": sf.lit("B.I.A.B")})

# you can see the update
deltatab.history().show(truncate=False)

# %% Merging and updating complete tables

df = spark.read.option("encoding", "utf8") \
    .csv("./data/recipeData.csv",
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")

df_initial = df.sample(withReplacement=False, fraction=0.5, seed=42)
df_update = df.sample(withReplacement=False, fraction=0.5, seed=21)

df_initial.write.format("delta").save("delta/recipe_merge", mode='overwrite')

df_initial.count()
df_update.count()

deltadf_merge = DeltaTable.forPath(spark, "delta/recipe_merge")

# starting conditions
deltadf_merge.toDF().count()  # 37055
df_update.count()  # 36827

deltadf_merge.alias("root"). \
    merge(source=df_update.alias("updates"),
          condition="root.BeerID == updates.BeerID"). \
    whenNotMatchedInsertAll(). \
    execute()

# merged count as aspected lower as the sum of both.
deltadf_merge.toDF().count()  # 55580

deltadf_merge.alias("root"). \
    merge(source=df_update.alias("updates"),
          condition="root.BeerID == updates.BeerID"). \
    whenNotMatchedInsertAll(). \
    execute()

# count is unchanged after second merge
deltadf_merge.toDF().count()  # 55580
deltadf_merge.history().show(truncate=False)

# %% SCD2

PATH_SCD2 = 'delta/beers_scd2'
df = spark.read.option("encoding", "utf8") \
    .csv("./data/recipeData.csv",
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")

valid_to_dts_max = sf.lit(datetime.datetime(2199, 12, 31))

# existing data
df_initial = df.sample(withReplacement=False, fraction=0.5, seed=42)
# update data, ... expecting a small overlapp, some new rows, and having a new column value for old data
df_update = (df.sample(withReplacement=False, fraction=0.5, seed=21).
             withColumn("BrewMethod", sf.lit("SpicyBrew")))

# create the starting table, e.g. a customer hub.
df_initial = df_initial. \
    withColumns({"valid_from_dts": sf.lit(datetime.datetime.now()),
                 "valid_to_dts": valid_to_dts_max})
df_initial.write.save(PATH_SCD2, "delta", mode='overwrite')
df_initial.show()

delta_scd2 = DeltaTable.forPath(spark, PATH_SCD2)

update_dts = sf.lit(datetime.datetime.now())
df_main =  delta_scd2.toDF()

# determine which rows are already present (by key) and have to be changed
df_update_existing = df_update.join(df_main,
                                    ((df_main.valid_to_dts > datetime.datetime.now()) &
                                     (df_main.BrewMethod != df_update.BrewMethod) &
                                     (df_main.BeerID == df_update.BeerID)), how='leftsemi')

df_staging = (df_update_existing.withColumn("merge_key", sf.lit(None)).
              union(df_update.withColumn("merge_key", sf.col("BeerID"))))

(delta_scd2.merge(df_staging, df_staging.merge_key == delta_scd2.toDF().BeerID).
 whenMatchedUpdate( set={'valid_to_dts': update_dts}).
 whenNotMatchedInsert(values={'valid_from_dts': update_dts,
                              'valid_to_dts': valid_to_dts_max,
                              **{key: sf.col(key) for key in df_update.columns}}).execute())

delta_scd2.toDF().orderBy('valid_to_dts', ascending=True).show()

delta_scd2.toDF().where('BeerID = 274').show(truncate=False)

# %% Optimize #########################################################################################################

df = spark.read.option("encoding", "utf8") \
    .csv("./data/recipeData.csv",
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")
PATH_FRAGMENTED = 'delta/fragemented'

# fragment a dataset into 100 chunks
df.repartition(100).write.format('delta').save(path=PATH_FRAGMENTED, mode='overwrite')

# check the number of files
pprint(os.listdir(PATH_FRAGMENTED))
pprint(len(os.listdir(PATH_FRAGMENTED)))
# way to much!!!

# compact the files again within a partition
table = delta.DeltaTable.forPath(spark, PATH_FRAGMENTED)
table.optimize().executeCompaction().show(truncate=False)

# check number of files again [... still too many]
pprint(os.listdir(PATH_FRAGMENTED))
pprint(len(os.listdir(PATH_FRAGMENTED)))

# cleanup old versions
table.vacuum(0)

# now only small number of files
pprint(os.listdir(PATH_FRAGMENTED))
pprint(len(os.listdir(PATH_FRAGMENTED)))

# %% Optimize ZOrdering ################################################################################################

table = delta.DeltaTable.forPath(spark, PATH_FRAGMENTED)
table.optimize().executeZOrderBy('BeerID')  # physically order by BeerId

# can also be done if partitioned.
# table.optimize().where("BrewMethod='All Grain'").executeZOrderBy('BeerID')


# %% Check Correct Verions  ############################################################################################


deltatab = DeltaTable.forPath(spark, PATH)
deltatab.detail().show(vertical=True)

deltatab.history().show()
