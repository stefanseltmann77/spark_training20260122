# %% SETUP #############################################################################################################
import datetime

import pandas as pd
import numpy as np
from pyspark.sql import DataFrame

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()


def benchmark_show_describe_xtimes(df: DataFrame, repetitions: int = 10) -> list[datetime.timedelta]:
    run_times = []
    for i in range(repetitions):
        run_start_dts = datetime.datetime.now()
        df.describe().show()
        run_end_dts = datetime.datetime.now()
        run_times.append(run_end_dts - run_start_dts)
    return run_times


df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), header=True, inferSchema=True)
df.printSchema()
# print(spark.sparkContext.uiWebUrl)

# %% BENCHMARK without caching  #######################################################################################
times = benchmark_show_describe_xtimes(df, 10)
print(np.median(times))
print(pd.Series(times))

# %% BENCHMARK with caching of source  ##############################################################################
df.cache()
times = benchmark_show_describe_xtimes(df, 10)
print(np.median(times))
print(pd.Series(times))
# no effect on first run
# faster execution on second and all other runs

# %% BENCHMARK with caching of result  ##############################################################################
summary = df.describe()
summary.cache()
times = []
for i in range(10):
    start_dts = datetime.datetime.now()
    summary.show()
    end_dts = datetime.datetime.now()
    times.append(end_dts - start_dts)
print(np.median(times))
print(pd.Series(times))

# Check in SparkUI at "Storage" what's persisted.
df.unpersist()
df.describe().show()
# back to old performance

# check persistence in runtime
df.cache()
df.is_cached
df.unpersist()
df.is_cached
