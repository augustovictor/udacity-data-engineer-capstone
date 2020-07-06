from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType


@udf(TimestampType())
def get_datetime_from(long_value):
    """
    Converts timestamp of type Long to datetime
    """

    return datetime.fromtimestamp(long_value / 1000.0)


def create_spark_session() -> SparkSession:
    """
    Creates and configures spark session
    """

    spark_session = SparkSession() \
        .builder \
        .appName("lol_data_transformer") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark_session.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark_session


def process_summoners_data(
        spark: SparkSession,
        s3_raw_data_path: str,
        s3_destination_path: str
):
    """
    Processes summoners data
    Loads data into DataLake in parquet format
    """
    pass


def process_champions_data(
        spark: SparkSession,
        s3_raw_data_path: str,
        s3_destination_path: str
):
    """
    Processes champions data
    Loads data into DataLake in parquet format
    """
    pass


def process_items_data(
        spark: SparkSession,
        s3_raw_data_path: str,
        s3_destination_path: str
):
    """
    Processes items data
    Loads data into DataLake in parquet format
    """
    pass


def process_matches_data(
        spark: SparkSession,
        s3_raw_data_path: str,
        s3_destination_path: str
):
    """
    Processes matches data
    Loads data into DataLake in parquet format
    """
    pass


def main():
    """
    Orchestrates execution
    """
    spark = create_spark_session()


if __name__ == '__main__':
    main()
