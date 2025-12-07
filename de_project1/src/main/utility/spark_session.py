import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *

def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("Tanuj_spark")\
        .config("spark.driver.extraClassPath", "/Users/tanuj.rana/Downloads/mysql-connector-j-9.5.0/mysql-connector-j-9.5.0.jar") \
        .getOrCreate()
    logger.info("spark session %s",spark)
    # spark.sparkContext.setLogLevel("Error")
    return spark