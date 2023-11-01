from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder.config("spark.driver.memory", "5g").getOrCreate()
    return spark
