import os
import sys
from importlib.util import find_spec
from pathlib import Path
from typing import Final

import pyspark
from pyspark.sql import SparkSession


def dbconnect_enabled() -> bool:
    if find_spec('databricks'):
        print("Databricks Connect detected")
        return True
    else:
        print("Assuming local Spark")
        return False


PATH_JARS = Path(__file__).absolute().parent / 'jars'

################################################
######### check individually!
################################################
DBCONNECT_PROFILE: Final[str] = 'playground'
if dbconnect_enabled():
    path_trainingdata = Path("/Volumes/spark_training_stese/training/training_data/")
    path_demodata = Path("/Volumes/spark_training_stese/training/data/")
    path_demodata = Path("/Volumes/workspace/default/data/")
else:
    path_trainingdata = Path('./training_data')
    path_demodata = Path('./data')

################################################
################################################


def get_spark(app_name: str = "spark_training_spark3", master: str = "local[2]",
              profile_name: str = 'training') -> SparkSession:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    spark_conf = pyspark.SparkConf().setAppName(app_name).setMaster(master)
    spark_conf = spark_conf.set("spark.jars.packages",
                                ','.join(["org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
                                          "io.delta:delta-spark_2.13:4.0.0",
                                          "org.xerial:sqlite-jdbc:3.43.0.0"]))
    # spark_conf = spark_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark_conf = spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    if dbconnect_enabled():
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.profile(profile_name).getOrCreate()
        spark.sql("SELECT 'dbconnect established'").show(truncate=False)
    else:
        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
        assert (spark.sparkContext.pythonVer == "3.12")
        print(spark.sparkContext.uiWebUrl)
    return spark
