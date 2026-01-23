
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.utils import AnalysisException

from spark_setup_spark3 import get_spark, path_demodata

spark = get_spark(profile_name='databricks_free')
# spark.catalog.setCurrentCatalog('spark_training_stese')
# spark.catalog.setCurrentDatabase('training')
spark.catalog.setCurrentCatalog('workspace')
spark.catalog.setCurrentDatabase('default')


df_recipe = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), header=True, inferSchema=True)
df_recipe = df_recipe.dropna().withColumnRenamed('Size(L)', 'Size')
spark.catalog.listCatalogs()
df_recipe.show()

## only
# spark.catalog.setCurrentCatalog('spark_training_stese')

(df_recipe.withColumnRenamed('Size(L)', 'Size').
 write.saveAsTable('fct_recipe', mode="overwrite", overwriteSchema=True))
spark.sql("ALTER TABLE fct_recipe ALTER COLUMN BeerID SET NOT NULL;")
spark.sql("ALTER TABLE fct_recipe ADD CONSTRAINT beer_pk PRIMARY KEY (BeerID) RELY")

df_style =  df_recipe.select("StyleID", "Style").distinct()

(df_style.write.saveAsTable('dim_style', mode="overwrite", overwriteSchema=True))
spark.sql("ALTER TABLE dim_style ALTER COLUMN StyleID SET NOT NULL;")
spark.sql("ALTER TABLE dim_style ADD CONSTRAINT style_pk PRIMARY KEY (StyleID) RELY")
spark.sql("ALTER TABLE fct_recipe ADD CONSTRAINT style_fk FOREIGN KEY (StyleID) REFERENCES dim_style(StyleID)")


spark.sql("select * from dim_style").show()