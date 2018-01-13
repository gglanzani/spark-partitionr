import pytest
import pyspark.sql.functions as sf

import spark_partitionr as spr


def test_create_default_spark_session(spark_session):
    assert True  # sanity check with the fixture


def test_load_data(spark_session):
    df = spr.load_data(spark_session, "tests/formatted-issue.json",
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


def test_create_schema_faulty_output(sample_df):
    with pytest.raises(KeyError):
        spr.create_schema(sample_df, '', '', format_output="wrong")


def test_create_schema_happy_flow(sample_df):
    result = """CREATE TABLE IF NOT EXISTS test_db.test_table (
author struct<active:boolean,avatarUrls:struct<16x16:string,48x48:string>,displayName:string,emailAddress:string,name:string,self:string>,
created string,
id string,
items array<struct<field:string,fieldtype:string,from:string,fromString:string,to:string,toString:string>>
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs:///test/folder'"""
    assert result == spr.create_schema(sample_df, "test_db", "test_table",
                                   partition_col="dt", format_output='parquet',
                                   output_path='hdfs:///test/folder')


def test_create_schema_output_path(sample_df):
    location = 'hdfs:///test/folder'
    location_schema = spr.create_schema(sample_df, "test_db", "test_table",
                                     output_path=location)
    default_schema = spr.create_schema(sample_df, "test_db", "test_table")
    assert f"LOCATION '{location}'" in location_schema
    assert "LOCATION" not in default_schema
    assert location not in default_schema


def test_create_schema_partition_col(sample_df):
    non_present_partition = "dt"
    present_partition = "created"
    non_present_schema = spr.create_schema(sample_df, "test_db", "test_table",
                                           partition_col=non_present_partition)
    present_schema = spr.create_schema(sample_df, "test_db", "test_table",
                                       partition_col=present_partition)
    non_partitioned_schema = spr.create_schema(sample_df, "test_db", "test_table")
    assert f"PARTITIONED BY ({non_present_partition} STRING)" in non_present_schema
    assert f"PARTITIONED BY ({present_partition} STRING)" in present_schema
    assert f"{present_partition} string," not in present_schema
    assert f"{present_partition} string," in non_partitioned_schema



