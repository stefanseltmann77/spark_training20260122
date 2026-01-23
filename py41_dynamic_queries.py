# %% imports ###########################################################################################################
from pyspark.sql import SparkSession

from spark_setup_spark3 import get_spark, path_demodata

# %% setup #############################################################################################################
spark: SparkSession = get_spark()
df = spark.read.csv((path_demodata / 'recipeData.csv').as_posix(), header=True, inferSchema=True)

# %% Simple_query ###################################################################### ################################

df.describe().show()
df.createOrReplaceTempView("beers")

## lets get some aggregated result per BrewMethod:
group_result = spark.sql("""
    SELECT  BrewMethod,
    mean(OG),
    mean(FG),
    mean(ABV),
    mean(IBU),
    mean(BoilSize),
    mean(BoilTime),
    mean(Efficiency),
    min(OG),
    min(FG),
    min(ABV),
    min(IBU),
    min(BoilSize),
    min(BoilTime),
    min(Efficiency),
    max(OG),
    max(FG),
    max(ABV),
    max(IBU),
    max(BoilSize),
    max(BoilTime),
    max(Efficiency)
    FROM beers
    GROUP BY BrewMethod
""")
print(group_result)

## that is a lot of manual code. I also want the labels to be more readable and have these columns computed
# for multiple PrimingMethods. these are the priming methods:
df.groupby("PrimingMethod").count().orderBy("count", ascending=False).show()

# %% More complex query ################################################################################################
# lets split all this by Dextrose

group_result = spark.sql("""
    SELECT  
    BrewMethod,
    mean(OG),
    mean(FG),
    mean(ABV),
    mean(IBU),
    mean(BoilSize),
    mean(BoilTime),
    mean(Efficiency),
    min(OG),
    min(FG),
    min(ABV),
    min(IBU),
    min(BoilSize),
    min(BoilTime),
    min(Efficiency),
    max(OG),
    max(FG),
    max(ABV),
    max(IBU),
    max(BoilSize),
    max(BoilTime),
    max(Efficiency),
    mean(CASE WHEN PrimingMethod = "Dextrose" THEN OG END),
    mean(CASE WHEN PrimingMethod = "Dextrose" THEN FG END),
    mean(CASE WHEN PrimingMethod = "Dextrose" THEN ABV END),
    mean(CASE WHEN PrimingMethod = "Dextrose" THEN IBU END),
    mean(CASE WHEN PrimingMethod = "Dextrose" THEN BoilSize END),
    mean(CASE WHEN PrimingMethod = "Dextrose" THEN BoilTime END),
    mean(CASE WHEN PrimingMethod = "Dextrose" THEN Efficiency END),
    min(CASE WHEN PrimingMethod = "Dextrose" THEN OG END),
    min(CASE WHEN PrimingMethod = "Dextrose" THEN FG END),
    min(CASE WHEN PrimingMethod = "Dextrose" THEN ABV END),
    min(CASE WHEN PrimingMethod = "Dextrose" THEN IBU END),
    min(CASE WHEN PrimingMethod = "Dextrose" THEN BoilSize END),
    min(CASE WHEN PrimingMethod = "Dextrose" THEN BoilTime END),
    min(CASE WHEN PrimingMethod = "Dextrose" THEN Efficiency END),
    max(CASE WHEN PrimingMethod = "Dextrose" THEN OG END),
    max(CASE WHEN PrimingMethod = "Dextrose" THEN FG END),
    max(CASE WHEN PrimingMethod = "Dextrose" THEN ABV END),
    max(CASE WHEN PrimingMethod = "Dextrose" THEN IBU END),
    max(CASE WHEN PrimingMethod = "Dextrose" THEN BoilSize END),
    max(CASE WHEN PrimingMethod = "Dextrose" THEN BoilTime END),
    max(CASE WHEN PrimingMethod = "Dextrose" THEN Efficiency END)
    FROM beers
    GROUP BY BrewMethod
""")
group_result.show()

##### that is one ugly query.

# %% Refactor to dynamic queries ########################################################################################

# let's get all numerics without ids
columns_numeric = [col_name for col_name, col_type in df.dtypes
                   if col_type in ("int", "double") and not col_name.endswith("ID")]

print(columns_numeric)

# then define the aggregations:
import pyspark.sql.functions as sf

# test what it would look like
for column in columns_numeric:
    print(sf.mean(sf.col(column)).alias(f"{column}_mean"))
    print(sf.min(sf.col(column)).alias(f"{column}_min"))
    print(sf.max(sf.col(column)).alias(f"{column}_max"))

# collect all the computations
aggs = []
for column in columns_numeric:
    aggs.append(sf.mean(sf.col(column)).alias(f"{column}_mean"))
    aggs.append(sf.min(sf.col(column)).alias(f"{column}_min"))
    aggs.append(sf.max(sf.col(column)).alias(f"{column}_max"))
df.groupby("BrewMethod").agg(*aggs).show()

# lets split this by Sugar AND Dextrose
filter_conditions = {"total": sf.lit(True)}
filter_conditions.update({method: sf.col("PrimingMethod") == method
                          for method in ['Sugar', 'Dextrose']})
print(filter_conditions)
aggs = []
for column in columns_numeric:
    for filter_label, filter_con in filter_conditions.items():
        aggs.append(sf.mean(sf.when(filter_con, sf.col(column))).alias(f"{column}_mean_{filter_label}"))
        aggs.append(sf.min(sf.when(filter_con, sf.col(column))).alias(f"{column}_min_{filter_label}"))
        aggs.append(sf.max(sf.when(filter_con, sf.col(column))).alias(f"{column}_max_{filter_label}"))
df.groupby("BrewMethod").agg(*aggs).show()

# lets put this to the extreme even further

filter_conditions = {"total": sf.lit(True),
                     **{method: sf.col("PrimingMethod") == method
                        for method in ['Sugar', 'Dextrose']}}
agg_functions = {"avg": sf.mean, "min": sf.min, "max": sf.max}
aggs = [agg_fun(sf.when(filter_con, sf.col(column))).alias(f"{column}_{agg_label}_{filter_label}")
        for column in columns_numeric
        for agg_label, agg_fun in agg_functions.items()
        for filter_label, filter_con in filter_conditions.items()]
print(aggs)
df.groupby("BrewMethod").agg(*aggs).show()
