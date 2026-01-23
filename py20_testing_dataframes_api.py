"""Small demo etl
just for testing purposes"""
from collections.abc import Sequence

import pyspark.sql.functions as sf

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark()

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import IntegerType, DoubleType


class BreweryControllingETL:
    """This is a basic Demo-ETL in Pyspark"""

    spark: SparkSession

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _load_recipes_raw(self) -> DataFrame:
        """internal function, as an opportunity for IO mocking"""
        return self.spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), inferSchema=True, header=True, encoding="utf-8")

    def load_recipes(self) -> DataFrame:
        df_raw = self._load_recipes_raw()
        df = df_raw.withColumnRenamed("Size(L)", "Size")
        return df

    @staticmethod
    def build_aggregations(df: DataFrame, aggs: Sequence[Column]) -> DataFrame:
        """implement predefined aggregations in a grouping"""
        return df.groupby("BrewMethod").agg(*aggs)

    @staticmethod
    def get_numeric_fields(df: DataFrame) -> list[str]:
        numeric_fields = [field.name for field in df.schema.fields
                          if field.dataType == IntegerType()
                          or field.dataType == DoubleType()]
        return numeric_fields

    @staticmethod
    def prepare_aggregations(numeric_fields: Sequence[str]) -> list[Column]:
        """dynamically create aggregation definitions"""
        return [sf.max(sf.col(field_name)).alias(field_name + "_max") for field_name in numeric_fields]

    def run_beer_etl(self):
        """create the complete ETL without actual running it as a action"""
        df = self.load_recipes()
        df_agg = self.build_aggregations(df, self.prepare_aggregations(self.get_numeric_fields(df)))
        return df_agg

    def run_all(self):
        df_agg = self.run_beer_etl()
        df_agg.write.csv("beer_etl.csv")


if __name__ in ("__main__", "builtins"):
    bcetl = BreweryControllingETL(spark)
    df_beers = bcetl.run_beer_etl()
    df_beers.show()
