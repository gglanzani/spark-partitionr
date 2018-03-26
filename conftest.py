import shutil

import pytest

from spark_partitionr import create_spark_session


@pytest.fixture(scope="session", autouse=True)
def spark():
    """
    Fixture to create a SparkSession.

    :returns: the Spark session
    """
    try:
        shutil.rmtree('metastore_db')
    except:
        pass
    spark = create_spark_session(database="test", table="run")
    spark.sparkContext.setLogLevel('ERROR')
    yield spark
    spark.stop()


@pytest.fixture(scope="session", autouse=True)
def sample_df(spark):
    df = spark.read.json("tests/formatted-issue.json", multiLine=True)
    return df