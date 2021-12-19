"""
Spark related functions for "Project: Capstone Project"
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark import SparkFiles


def get_spark():
    """Creates the spark session"""
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()
    return spark
