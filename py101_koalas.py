# %% setup

# os.environ["PYSPARK_PYTHON"] = '/usr/local/miniconda/envs/spark3/bin/python3'
from pyspark.sql import SparkSession

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()
spark = SparkSession.builder.getOrCreate()

# %% exploration with koalas ##########################################################################################

# %% with spark < 3.2 !!! DEPRECATED NOW  #############################################################################
import databricks.koalas as ks

# import pandas as pd

# read like a pandas dataframe
# s = pd.read_csv("./data/recipeData.csv")
s = ks.read_csv((path_demodata / 'recipeData.csv').as_posix())

# show the columns
s.columns

# do the usual transformations
s['new_column'] = 'abc'
s.groupby(['Style', 'new_column']).aggregate({'ABV': 'mean'})

means = s.groupby(['Style', 'new_column']). \
    aggregate({'ABV': 'mean'})

# compute correlations
s.corr()

# transform it to real pandas
means.to_pandas()

# push it back to genuine spark
means.to_spark()

# try a join
beer_id_counts = s.groupby("StyleID")['BeerID'].count()

# do this to allow joins.
ks.set_option('compute.ops_on_diff_frames', True)

s.join(beer_id_counts, on='StyleID', rsuffix='_join')

# # # write it back to spark
# s.to_spark().write. \
#     format("delta"). \
#     save('path', mode='overwrite')


# %% with spark >= 3.2 #################################################################################################

import pyspark.pandas as ps
ps.options.display.max_rows = 10

df_ps = ps.read_csv((path_demodata / 'recipeData.csv').as_posix())

# do frequencies
df_ps.Name.value_counts()

# plotting works differently --> Plotly
df_ps.Name.value_counts().plot()

# how much liquers?
(df_ps.loc[df_ps.Name == 'IPA'].ABV > 0.15).sum()


# switch to real pandas
df_pd = df_ps.to_pandas()
df_pd.value_counts().plot()

