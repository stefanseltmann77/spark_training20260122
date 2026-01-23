"""Example for testing class using pytest,
pytest is the more modern approach"""

import pytest
from pyspark.pandas import DataFrame

from py20_testing_dataframes_api import BreweryControllingETL
from py22_testing_dummy_data import beer_rows, schema_beer
from pyspark.sql import Column, Row, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from spark_setup_spark3 import get_spark

spark = get_spark("unittests", master="local")  # testing has to be local


@pytest.fixture(scope="session")
def spark():
    spark = get_spark("unittests", master="local")  # testing has to be local
    return spark


@pytest.fixture(scope="session")
def df_beer_raw(spark: SparkSession) -> DataFrame:
    df_beer_raw = spark.createDataFrame(beer_rows, schema=schema_beer)
    return df_beer_raw


@pytest.fixture(scope="function")
def etl(spark, df_beer_raw, monkeypatch):
    etl = BreweryControllingETL(spark)

    def fake_return():
        return df_beer_raw

    monkeypatch.setattr(etl, "_load_recipes_raw", fake_return)

    return etl


class TestBreweryControllingETL:

    def test_load_recipes(self, etl):
        df = etl.load_recipes()
        assert ("Size" in df.columns)
        # just assert that there is no errow

    def test_prepare_aggregations(self, etl):
        column_names = ['a', 'b', 'c']
        aggs = etl.prepare_aggregations(column_names)
        assert isinstance(aggs[0], Column)
        assert len(aggs) == len(column_names)

    def test_run_beer_etl(self, etl):
        df = etl.run_beer_etl()
        assert df.count() == 1234

    def test_get_numeric_fields(self, etl, spark):
        schema_test = StructType([StructField("Integer", IntegerType(), True),
                                  StructField("String", StringType(), True),
                                  StructField("Double", DoubleType(), True)])
        data_test = [Row(Integer=1, String="abc", Double=1.23)]
        df_test = spark.createDataFrame(data_test, schema_test)
        num_fields = etl.get_numeric_fields(df_test)
        assert num_fields == ["Integer", "Double"]
