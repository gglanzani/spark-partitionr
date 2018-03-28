import pytest
import pyspark.sql.functions as sf

import spark_partitionr as spr


def test_create_default_spark(spark):
    assert True  # sanity check with the fixture


def test_load_data(spark):
    df = spr.load_data(spark, "tests/formatted-issue.json",
                       format='json', multiLine=True)
    assert df.count() == 5


def test_add_partition_existing_column(sample_df):
    partitioned_df = spr.add_partition_column(sample_df, partition_col='created')
    assert partitioned_df.subtract(sample_df).count() == 0


def test_add_partition_new_column(sample_df):
    partition_col = "dt"
    partition_with = sf.from_unixtime(sf.unix_timestamp(sf.col('created'),
                                                        format="yyyy-MM-dd"),
                                      format="yyyyMMdd")
    with_partition = spr.add_partition_column(sample_df, partition_col=partition_col,
                                              partition_with=partition_with)
    assert partition_col in with_partition.columns
    partition_list = [row.dt for row in with_partition.select(partition_col).collect()]
    assert 5 == len(partition_list)
    assert set(partition_list) == {'20130423', '20130418', '20130417'}


def test_add_non_existing_partition(sample_df):
    partition_col = "dt"
    with pytest.raises(ValueError):
        spr.add_partition_column(sample_df, partition_col=partition_col)


def test_external(spark, sample_df):
    database = 'test'
    table = 'is_external'
    spark.sql("CREATE DATABASE IF NOT EXISTS " + database)
    schema = spr.schema.create_schema(sample_df, database, table,
                                      output_path='/tmp/is_external',
                                      external=True)
    spark.sql(schema)
    assert spr.is_table_external(spark, database, table)