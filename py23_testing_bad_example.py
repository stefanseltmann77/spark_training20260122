from pyspark.sql import SparkSession, DataFrame

from spark_setup_spark3 import get_spark


class SomeBadEtl:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def clean_input_data(self):
        df: DataFrame = self.spark.read.csv("./data/recipeData.csv",
                                            inferSchema=True, header=True,
                                            encoding="utf-8")
        for column_name in df.columns:
            df = df.withColumnRenamed(column_name, column_name.lower())
        df: DataFrame = df.withColumnRenamed("size(l)", "size")
        df = df.dropDuplicates()
        df.write.parquet("./data/recipeData_clean.csv", mode="overwrite")


class SomeBetterEtl:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _load_input_data_raw(self) -> DataFrame:
        return self.spark.read.csv("./data/recipeData.csv", inferSchema=True, header=True, encoding="utf-8")

    @staticmethod
    def _store_input_data_clean(df):
        df.write.parquet("./data/recipeData_clean.csv", mode="overwrite")

    @staticmethod
    def _clean_input_data(df: DataFrame) -> DataFrame:
        for column_name in df.columns:
            df = df.withColumnRenamed(column_name, column_name.lower())
        return df.dropDuplicates()

    def clean_input_data(self):
        df = self._load_input_data_raw()
        df = self._clean_input_data(df)
        self._store_input_data_clean(df)


if __name__ in ("__main__", "builtins"):
    spark = get_spark()
    etl = SomeBadEtl(spark)
    etl.clean_input_data()
