from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import window
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType
from pyspark.sql import functions as sf

from py56_fake_stream import STREAM_PATH
from spark_setup_spark3 import get_spark

spark = get_spark()
spark.conf.set("spark.sql.shuffle.partitions", 20)

stream_path: Path = STREAM_PATH
schema = StructType([StructField("BeerID", LongType(), False),
                     StructField("DrinkingTime", TimestampType(), False),
                     StructField("DrinkingVolume", DoubleType(), False),
                     StructField("Consumer", StringType(), False)]
                    )

# %%#####################################################################################################################
# lets read the source for a stream, csv-based
stream_handle = spark.readStream.csv(schema=schema, path=str(stream_path), sep="\t")

# adding partition_column
stream_handle = stream_handle.withColumn('part_minute', sf.date_format("DrinkingTime", 'yyyyMMddHHmm'))

sink_path = str(Path(stream_path.parent, 'delta_sink_part'))
# write it back to
stream_query: StreamingQuery = stream_handle \
    .writeStream.option("checkpointLocation", "tmp/delta_check_part") \
    .start(sink_path, partitionBy='part_minute', format='delta', outputMode='append')

# 1 minute fixed pause

stream_query.stop()


# %%#####################################################################################################################
# lets read the source for a stream, csv-based
stream_handle_aggr = spark.read.format('delta').load(sink_path).show()

# adding partition_column
stream_handle = stream_handle.withColumn('part_minute', sf.date_format("DrinkingTime", 'yyyyMMddHHmm'))

sink_path = str(Path(stream_path.parent, 'delta_sink_part'))
# write it back to
stream_query: StreamingQuery = stream_handle \
    .writeStream.option("checkpointLocation", "tmp/delta_check_part") \
    .start(sink_path, partitionBy='part_minute', format='delta', outputMode='append')


# take the result from delta
df_aggr = spark.readStream.format('delta').load(sink_path).withWatermark("DrinkingTime", "5 minute")
df_aggr = df_aggr.groupBy("Consumer", window("DrinkingTime", "5 minute")).count()


# save the second stream to an aggregation table
df_aggr.writeStream.option("checkpointLocation", "tmp/delta_check_aggr") \
    .start(str(Path(stream_path.parent, 'delta_aggr')),
           format='delta', outputMode='append')


# end the streams
stream_query.stop()
df_aggr.stop()



# analyze the statistics
from delta.tables import DeltaTable
df_analyse = spark.read.format('delta').load(str(Path(stream_path.parent, 'delta_aggr')))
df_analyse.show()

deltatab = DeltaTable.forPath(spark, str(Path(stream_path.parent, 'delta_aggr')))
deltatab.history().select('operationMetrics.numOutputRows').groupBy('numOutputRows').count().show()

df_sink_analyse = spark.read.format('delta').load(sink_path)
df_sink_analyse.show()

deltatab_sink = DeltaTable.forPath(spark, sink_path)
deltatab_sink.history().select('operationMetrics.numOutputRows').groupBy('numOutputRows').count().show()

deltatab.history().show()