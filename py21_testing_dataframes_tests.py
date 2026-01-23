"""Example for testing class using unittest,
unittests is the older approach. please switch to pytest."""
import unittest
from unittest.mock import MagicMock

from py20_testing_dataframes_api import BreweryControllingETL
from py22_testing_dummy_data import beer_rows, schema_beer
from pyspark.sql import Column, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from spark_setup_spark3 import get_spark

spark = get_spark("unittests", master="local")  # testing has to be local

df_beer_raw = spark.createDataFrame(beer_rows, schema=schema_beer)


class TestBreweryControllingETL(unittest.TestCase):

    def setUp(self):
        self.etl = BreweryControllingETL(spark)

    def test_load_recipes(self):
        self.etl._load_recipes_raw = MagicMock(return_value=df_beer_raw)
        df = self.etl.load_recipes()
        assert ("Size" in df.columns)
        # just assert that there is no errow

    def test_prepare_aggregations(self):
        column_names = ['a', 'b', 'c']
        aggs = self.etl.prepare_aggregations(column_names)
        self.assertTrue(isinstance(aggs[0], Column))
        self.assertEqual(len(aggs), len(column_names))

    def test_run_beer_etl(self):
        self.etl._load_recipes_raw = MagicMock(return_value=df_beer_raw)
        df = self.etl.run_beer_etl()
        df.count()

    def test_get_numeric_fields(self):
        schema_test = StructType([StructField("Integer", IntegerType(), True),
                                  StructField("String", StringType(), True),
                                  StructField("Double", DoubleType(), True)])
        data_test = [Row(Integer=1, String="abc", Double=1.23)]
        df_test = spark.createDataFrame(data_test, schema_test)
        num_fields = self.etl.get_numeric_fields(df_test)
        self.assertEqual(num_fields, ["Integer", "Double"])


if __name__ == "__main__":
    spark.stop()
