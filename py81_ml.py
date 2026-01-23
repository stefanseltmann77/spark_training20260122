# %% SETUP #############################################################################################################
from matplotlib import pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import RFormula, VectorAssembler, QuantileDiscretizer, StandardScaler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.tuning import CrossValidatorModel

from spark_setup_spark3 import get_spark

spark = get_spark()

df = spark.read.option("encoding", "utf8") \
    .csv("./data/recipeData.csv",
         inferSchema=True,
         header=True).cache()

# %% ##################################################################################################################
# test a simple linear regression and predict the alcohol beverage value

regr = LinearRegression(labelCol='ABV'). \
    setMaxIter(100). \
    setElasticNetParam(0.4)

# build a featurevector
feature_vec = VectorAssembler(inputCols=['BoilTime', 'Efficiency', 'BrewMethod_cat'],
                              outputCol='features')

# create an index from a string column
brewMethCat = StringIndexer(inputCol='BrewMethod', outputCol='BrewMethod_cat')

# apply the transformers
df.show()

data_df = brewMethCat.fit(df).transform(df)
data_df = feature_vec.transform(data_df)

# the feature vector, which is the input for the estimator is appended at the end.
data_df.show()

data_df.select("features").show()

# fit the regression
regr_fit = regr.fit(data_df)
df_result = regr_fit.transform(data_df)

print(regr_fit.summary.tValues)
df_result.select(   "features", "ABV", "prediction").show()

regr.

# evaluate the result
RegressionEvaluator(labelCol='ABV', metricName='rmse').evaluate(df_result)
RegressionEvaluator(labelCol='ABV', metricName='r2').evaluate(df_result)

df_result.select("ABV", "prediction").toPandas().plot.scatter(x='ABV', y='prediction')

plt.show()

#########################################################################
# test a simple linear regression and predict the alcohol beverage value
# with RFormula

regr = LinearRegression(labelCol='ABV'). \
    setMaxIter(100). \
    setElasticNetParam(0.4)
data = RFormula().setFormula("ABV~BoilTime + Efficiency + BrewMethod").fit(df).transform(df)

df_result = regr.fit(data).transform(data)

df_result.show()

evaluator = RegressionEvaluator(labelCol='ABV', metricName='r2')
evaluator.evaluate(df_result)

#########################################################################
# test a more complex regressor
# with RFormula

data = RFormula().setFormula("ABV~BoilTime + Efficiency + BrewMethod").fit(df).transform(df)

rforest = RandomForestRegressor()
rforest_fit = rforest.fit(data)
rforest_result = rforest_fit.transform(data)
rforest_result.select("features", "label", "prediction").show()

evaluator = RegressionEvaluator(metricName="r2")
evaluator.evaluate(dataset=rforest_result)
# the result is not yet satisfactory

paramGrid = ParamGridBuilder() \
    .addGrid(RandomForestRegressor.maxDepth, [2, 3, 6]) \
    .addGrid(RandomForestRegressor.numTrees, [10, 20, 50]) \
    .build()

cv = CrossValidator(estimator=rforest,
                    estimatorParamMaps=paramGrid,
                    evaluator=evaluator, parallelism=10)

result_cv: CrossValidatorModel = cv.fit(data)
result_cv.getEstimator()
result_cv.getEstimatorParamMaps()

best_model: RandomForestRegressionModel = result_cv.bestModel

# what were the best parameters?
best_model.getParam('numTrees')

df_result = best_model.transform(data)
evaluator.evaluate(dataset=df_result)

#########################################################################
## let's get to the best practice, a pipeline

# as an example, we do many steps manually from before

# build a featurevector
feature_vec = VectorAssembler(inputCols=['BoilTime', 'Efficiency', 'BrewMethod_cat',
                                         'OG_buck'],
                              outputCol='features')
brewMethCat = StringIndexer(inputCol='BrewMethod', outputCol='BrewMethod_cat')
rforest = RandomForestRegressor(labelCol='ABV')
og_bins = QuantileDiscretizer(numBuckets=10, inputCol="OG", outputCol="OG_buck")
ibu_scaled = StandardScaler(inputCol="IBU", outputCol="ibu_scal",
                            withStd=True, withMean=True)

evaluator = RegressionEvaluator(metricName="r2", labelCol='ABV')

paramGrid = ParamGridBuilder() \
    .addGrid(RandomForestRegressor.maxDepth, [1, 3, 6, 10]) \
    .addGrid(RandomForestRegressor.numTrees, [1, 10, 20, 50]) \
    .build()

# we combine all steps to a pipeline object
pipeline = Pipeline(stages=[brewMethCat,
                            og_bins,
                            feature_vec,
                            rforest])

best_pipe = CrossValidator(estimator=pipeline,
                           estimatorParamMaps=paramGrid,
                           evaluator=evaluator).fit(df)
best_pipe.transform(df).show()

# best_pipe can now be stored or reloaded.
