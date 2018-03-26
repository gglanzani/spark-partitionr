import json

import pytest
import pyspark.sql.functions as sf

from spark_partitionr import schema


def test_create_schema_faulty_output(sample_df):
    with pytest.raises(KeyError):
        schema.create_schema(sample_df, '', '', format_output="wrong")


def test_create_schema_happy_flow(sample_df):
    result = """CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table (
author struct<active:boolean,avatarUrls:struct<16x16:string,48x48:string>,displayName:string,emailAddress:string,name:string,self:string>,
created string,
id string,
items array<struct<field:string,fieldtype:string,from:string,fromString:string,to:string,toString:string>>
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs:///test/folder'"""
    assert result == schema.create_schema(sample_df, "test_db", "test_table",
                                          partition_col="dt", format_output='parquet',
                                          output_path='hdfs:///test/folder', external=True)


def test_create_schema_output_path(sample_df):
    location = 'hdfs:///test/folder'
    location_schema = schema.create_schema(sample_df, "test_db", "test_table",
                                           output_path=location)
    default_schema = schema.create_schema(sample_df, "test_db", "test_table")
    assert f"LOCATION '{location}'" in location_schema
    assert "LOCATION" not in default_schema
    assert location not in default_schema


def test_create_schema_partition_col(sample_df):
    non_present_partition = "dt"
    present_partition = "created"
    non_present_schema = schema.create_schema(sample_df, "test_db", "test_table",
                                              partition_col=non_present_partition)
    present_schema = schema.create_schema(sample_df, "test_db", "test_table",
                                          partition_col=present_partition)
    non_partitioned_schema = schema.create_schema(sample_df, "test_db", "test_table")
    assert f"PARTITIONED BY ({non_present_partition} STRING)" in non_present_schema
    assert f"PARTITIONED BY ({present_partition} STRING)" in present_schema
    assert f"{present_partition} string," not in present_schema
    assert f"{present_partition} string," in non_partitioned_schema


def test_evolve_schema(sample_df):
    new_df = sample_df.withColumn('author', sf.struct('author.*', 'id'))
    assert not schema.are_schemas_equal(new_df, sample_df)

    new_df = sample_df.withColumn('new_id', sf.col('id'))
    assert not schema.are_schemas_equal(new_df, sample_df)


def test__compare_fields():
    new_field = {'metadata': {}, 'name': 'id', 'nullable': True, 'type': 'string'}
    old_field = {'metadata': {'name': 'is'}, 'name': 'id', 'nullable': True, 'type': 'string'}
    assert schema._compare_fields(new_field, old_field)


def test_compare_fields_equal_simple():
    new_field = {'metadata': {}, 'name': 'created', 'nullable': True, 'type': 'string'}
    old_field = {'metadata': {}, 'name': 'created', 'nullable': True, 'type': 'string'}
    assert schema.compare_fields(new_field, old_field)


def test_compare_fields_inequal_type_simple():
    new_field = {'metadata': {}, 'name': 'created', 'nullable': True, 'type': 'string'}
    old_field = {'metadata': {}, 'name': 'created', 'nullable': True, 'type': 'float'}
    assert not schema.compare_fields(new_field, old_field)


def test_compare_fields_inequal_simple():
    new_field = {'metadata': {}, 'name': 'id', 'nullable': True, 'type': 'string'}
    old_field = {'metadata': {}, 'name': 'created', 'nullable': True, 'type': 'string'}
    assert not schema.compare_fields(new_field, old_field)


def test_compare_fields_added_field(sample_df):
    old_schema = json.loads(sample_df.schema.json())
    new_schema = json.loads(sample_df.schema.json())
    new_schema['fields'].append({'metadata': {}, 'name': 'new_field',
                                 'nullable': True, 'type': 'string'})
    assert schema.are_schemas_compatible(new_schema, old_schema)


def test_compare_fields_added_not_nullable_field(sample_df):
    old_schema = json.loads(sample_df.schema.json())
    new_schema = json.loads(sample_df.schema.json())
    new_schema['fields'].append({'metadata': {}, 'name': 'new_field',
                                 'nullable': False, 'type': 'string'})
    assert not schema.are_schemas_compatible(new_schema, old_schema)


def test_compare_fields_added_in_complex_field(sample_df):
    old_schema = json.loads(sample_df.schema.json())
    new_schema = json.loads(sample_df.schema.json())
    new_schema['fields'][0]['type']['fields'].append({'metadata': {}, 'name': 'new_field',
                                                      'nullable': False, 'type': 'string'})
    assert not schema.are_schemas_compatible(new_schema, old_schema)


def test_compare_fields_different_complex_field_type(sample_df):
    old_schema = json.loads(sample_df.schema.json())
    new_schema = json.loads(sample_df.schema.json())
    new_schema['fields'][0]['type']['type'] = 'array'
    assert not schema.are_schemas_compatible(new_schema, old_schema)


def test_are_field_complex(sample_df):
    _schema = json.loads(sample_df.schema.json())
    struct = _schema['fields'][0]
    array = _schema['fields'][3]
    assert schema.are_fields_complex(struct, array)


def test_are_field_complex_with_none(sample_df):
    _schema = json.loads(sample_df.schema.json())
    struct = _schema['fields'][0]['type']
    assert not schema.are_fields_complex(struct, None)


def test_compare_fields_remove_from_old(sample_df):
    old_schema = json.loads(sample_df.schema.json())
    new_schema = json.loads(sample_df.schema.json())
    old_schema['fields'].append({'metadata': {}, 'name': 'dt',
                                 'nullable': False, 'type': 'string'})
    assert schema.are_schemas_compatible(new_schema, old_schema, remove_from_old='dt')


def test_read_different_order(spark):
    """
    Here we add a new column before the old one, to check for

    """
    df1 = (spark
           .range(100)
           .withColumn('p', sf.lit(1))
           )

    df2 = (spark
           .range(100)
           .withColumn('new_id', sf.col('id'))
           .drop('id')
           .withColumn('id', sf.col('new_id'))
           .withColumn('p', sf.lit(2))
           )
    path = '/tmp/test'
    spark.sql('create database test')

    df1.write.partitionBy('p').saveAsTable('test.df', format='parquet', path=path, mode='overwrite')
    _schema = schema.create_schema(df2, 'test', 'df', external=True, output_path=path,
                               partition_col='p')
    spark.sql('drop table test.df')
    spark.sql(_schema)
    df2.write.format('parquet').mode('append').partitionBy('p').save(path)
    spark.sql('msck repair table test.df')
    assert set(spark.sql("SELECT DISTINCT p FROM test.df")
               .rdd
               .map(lambda row: int(row['p'])).collect()) == {1, 2}
    # if the order doesn't matter, then new_id for p = 1 should always be NULL, so the count should
    # be zero. If the order count (i.e. the new_id column will be filled with data from id in p = 1)
    # then the count won't be 0
    assert spark.sql("""select distinct new_id from test.df
                     where p = 1 and new_id is not null""").count() == 0

