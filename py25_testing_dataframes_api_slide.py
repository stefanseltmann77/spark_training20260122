import unittest
from unittest.mock import MagicMock
from pyspark.sql import Row, DataFrame
from spark_setup_spark3 import get_spark
from some_etl import SomeEtl  # example, won't work

spark = get_spark("unittests", master="local")  # TESTING HAS TO BE LOCAL

some_dummy_row = Row(some_id=1, some_float=0.123, some_int=123, some_char='ABC')
dummy_data = spark.createDataFrame([some_dummy_row, some_dummy_row, some_dummy_row])

class TestSomeETL(unittest.TestCase):

    def test_some_etl_middle_step(self):
        etl = SomeEtl(spark)
        result: DataFrame = etl.do_some_transformation(df=dummy_data)
        # test something here or ensure there is no error

    def test_some_etl_start_step(self):
        etl = SomeEtl(spark)
        etl._load_raw_data = MagicMock(return_value=dummy_data)
        result: DataFrame = etl.load_data()
        # test something here or ensure there is no error

if __name__ == "__main__":
    spark.stop()
