#######################################################################################################################
#######################################################################################################################
#### SETUP THAT SHOULD BE SET ON OPERATING SYSTEM
# # following configs are an example for manual configuration of paths on a target system,
# # fortunately we don't need it here.
# config = configparser.ConfigParser()
# config.read("paths.ini")
#
# # links to local install pyspark instance for module import
# sys.path.append(config['PYSPARK']['PYSPARK_PATH'])
# sys.path.append(config['PYSPARK']['PY4J_PATH'])
#
# # link to spark instance for spark session
# os.environ['SPARK_HOME'] = config['SPARK']['SPARK_HOME']
# # link to spark instance for spark session
# os.environ['PYSPARK_PYTHON'] = config['PYSPARK']['PYSPARK_PYTHON']
#######################################################################################################################
#######################################################################################################################
# %% Minimum imports
import pyspark
from pyspark.sql import SparkSession


# %% The Simplest alternative for spark session with default config.
spark = SparkSession.builder.getOrCreate()  # factory function for singleton pattern.


# %% Alternative with customized config and spark context:
spark_conf = (pyspark.SparkConf().
              setAppName("HelloSpark").  # set a meaningful name that will be displayed in Yarn e.g.
              setMaster("local[*]").  # local for localmode or yarn for yarn mode
              set("spark.logConf", True)  # set to true, in order to have your config documented
              )
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()  # factory function for singleton pattern.
# derive the low level SparkContext from the SparkSession
sc = spark.sparkContext

# %% do some instrospection into new objects
print(sc)  # <SparkContext master=local[*] appName=pyspark-shell>
print(spark.version)  # 'e.g. '3.3."x"'
print(sc.master)  # 'local[*]'
print(sc.pythonVer)  # 3.10 e.g.
print(sc.pythonExec)  # Python Executable
print(sc.uiWebUrl)  # Address to SparkUI
for row in sc.getConf().getAll():
    print(row)

# reduce the level of noise
sc.setLogLevel("ERROR")
