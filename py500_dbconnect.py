import logging

from databricks.connect import DatabricksSession

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    spark = DatabricksSession.builder.profile('playground_shared').getOrCreate()
    spark.sql("SELECT 1 ").show()



    from databricks.connect import DatabricksSession
    # just call directly if using env vars
    spark = DatabricksSession.builder.getOrCreate()
    # or use profile name when using config
    spark = DatabricksSession.builder.profile('playground').getOrCreate()

