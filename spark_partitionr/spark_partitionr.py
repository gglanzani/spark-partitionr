"""
Main module to load the data
"""
# pylint: disable=C0330
import re
import random
import uuid
import os

from spark_partitionr import schema


INVALID_HIVE_CHARACTERS = re.compile("[^A-Za-z0-9_]")

storage = {'parquet': 'STORED AS PARQUET',
           'com.databricks.spark.csv': ("ROW FORMAT SERDE"
                                        "'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n"
                                        "STORED AS TEXTFILE")}


def add_partition_column(df, partition_col='dt', partition_with=None):
    """
    Return a new dataframe with a column added, to be used as partition column

    If `partition_col` already exists and `partition_with` is `None`, the dataframe is unmodified

    :param df: A dataframe to add a partition columnn
    :type df: A Spark dataframe
    :param str partition_col: On which column should it be partitioned
    :param partition_with: A Spark Column expression for the `partition_col`. If not present,
                          `partition_col` should already be in the input data
    :returns: A Spark dataframe with the partition column added

    """
    if partition_with is None:
        if partition_col and partition_col not in df.columns:
            raise ValueError(("The partition_function can't be None "
                              "if partition_col is not part of the dataframe"))
        else:
            return df
    return df.withColumn(partition_col, partition_with)


def repair_partitions(spark, database, table):
    """
    Create the partitions on the metastore (not on "disk").

    This is necessary as when writing the data on "disk", the metastore is not yet aware that
    partitions exists.

    :param spark: A SparkSession
    :type spark: :class:`pyspark.sql.SparkSession`
    :param df: The dataframe that has been written partitioned on "disk"
    :type df: A Spark dataframe
    :param str database: To which database does the table belong
    :param str table: On which tables has this been written to
    :param str partition_col: On which column should it be partitioned
    """
    query = """MSCK REPAIR TABLE {database}.{table}""".format(database=database,
                                                              table=table)
    spark.sql(query)


def load_data(spark, path, **kwargs):
    r"""
    Load the data in `path` as a Spark DataFrame

    :param spark: A SparkSession object
    :type spark: pyspark.sql.SparkSession
    :param str path: The path where the data is
    :return: A Spark DataFrame

    :Keywords Argument:

    * *format* (``str``) --
      The format the data will be in. All options supported by Spark. Parquet is the default.
    * *header* (``bool``) --
      If reading a csv file, this will tell if the header is present (and use the schema)
    * *schema* (``pyspark.sql.types.StructType``)
      The input schema
    * *key* (``str``)
      In principle all `values` if they `key` is accepted by `spark.read.options` or by
      `findspark.init()`

    """
    readr = spark.read.options(**kwargs)
    for key, value in kwargs.items():
        try:
            readr = getattr(readr, key)(value)
        except AttributeError:
            pass
    df = (readr.load(path))
    return df


def create_spark_session(database='not provided', table='not provided', **kwargs):
    r"""
    Returns a Spark session.

    :param str database: The database name. Only used to name the SparkSession
    :param str table: The table name. Only used to name the SparkSession

    :Keyword Arguments:

    * *key* (``str``) --
      All arguments valid for SparkSession.builder, such as `master=local`
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        import findspark
        findspark.init(spark_home=kwargs.get('spark_home'),
                       python_path=kwargs.get('python_path'))
        from pyspark.sql import SparkSession

    builder = SparkSession.builder
    for key, value in kwargs.items():
        builder = builder.config(key, value)
    spark = (builder.enableHiveSupport()
                    .appName(" ".join([__file__, "Database:", database, "Table:", table]))
                    .getOrCreate())
    return spark


def get_output_path(spark, database, table):
    """
    Return the path where data should be written using database and table as argument
    """
    result = (spark.sql("describe formatted %s.%s" % (database, table))
                   .where("col_name='Location:'")
                   .collect())
    location = result[0]['data_type']
    return location


def write_data(df, format_output, mode_output, partition_col, output_path, **kwargs):
    """
    pass
    """
    if kwargs.get('repartition', False):
        result = df.repartition(partition_col)
    else:
        result = df
    (result.write
           .format(format_output)
           .mode(mode_output)
           .partitionBy(partition_col)
           .save(output_path))


def is_table_external(spark, database='default', table=None):
    """
    Check whether a Hive table is external or not

    :param spark: Spark context
    :param str database: Database
    :param table: Table
    :return: True when external
    :rtype: bool
    """
    count = (spark.sql('describe formatted {database}.{table}'.format(database=database,
                                                                      table=table))
             .where("col_name='Type'")
             .where("data_type = 'EXTERNAL'")
             .count())
    return bool(count)


def sanitize_table_name(table_name):
    return re.sub(INVALID_HIVE_CHARACTERS, "_", table_name)


def check_compatibility(df, old_df, format_output, partition_col):
    """
    Check if `df` and `old_df` have compatible schemas.

    Only the check for `parquet` has been implemeted, due to field ordering

    :param df: A Spark dataframe, the most recent one
    :param old_df: A Spark dataframe, the oldest
    :param str format_output: Which output format the data will be written to
    :param str partition_col: The way old data is partitioned
    :return: Whether the schemas are equal and compatible
    :rtype: tuple[bool, bool]
    """
    schema_equal = schema.are_schemas_equal(df, old_df, partition_col=partition_col)

    if not schema_equal and format_output != 'parquet':
        raise NotImplementedError("Only `parquet` schema evolution is supported")
    elif not schema_equal:
        new_schema = df.schema.jsonValue()
        old_schema = old_df.schema.jsonValue()
        schema_compatible = schema.are_schemas_compatible(new_schema, old_schema)
    else:
        schema_compatible = True
    return schema_equal, schema_compatible


def check_external(spark, database, table, schema_equal, schema_compatible):
    """
    If the table is external, return the table name with and without a random int appended

    New data can be saved in the version with the random int appendend. Once that is done, the old
    table can be dropped, and the new table moved

    :param spark: Spark context
    :param str database: Hive database
    :param str table: Hive table
    :param bool schema_equal: Whether the new schema is equal to the old one
    :param bool schema_compatible: Whether the new schema is compatible with the old one
    :param schema_reverse_compatible: Whether the old schema is "backward" compatible with the new
    :return: `table` and `tableN`, where `N` is a `randint`
    :rtype: tuple[str, str]
    """
    table_external = is_table_external(spark, database, table)
    if not schema_equal and table_external and schema_compatible:
        old_table_name = table
        table = table + str(random.randint(0, 1000))
        return old_table_name, table
    elif not table_external:
        raise ValueError("The schema has changed, but the table is internal")
    elif not schema_compatible:
        raise ValueError("The schema is not compatible")
    return None, table


def move_table(spark, *, from_database, from_table, to_database, to_table):
    """
    Rename Hive table

    :param spark: Spark context
    :param str from_database: Source db
    :param str from_table: Source table
    :param str to_database: Target db
    :param str to_table: Target table
    :return: None
    """
    spark.sql('DROP TABLE {}.{}'.format(to_database, to_table))
    spark.sql("""
            ALTER TABLE  {from_database}.{from_table} RENAME TO {to_database}.{to_table} 
            """.format(from_database=from_database,
                       to_database=to_database,
                       from_table=from_table,
                       to_table=to_table))


def main(input, format_output, database='default', table_name='', output_path=None,
         mode_output='append', partition_col='dt',
         partition_with=None, spark=None, **kwargs):
    r"""
    :param input: Either the location location for the data to load, which will be passed to `.load` in Spark.
                    Or the dataframe that contains the data.
    :param format_output str: One of `parquet` and `com.databricks.spark.csv` at the moment
    :param database str: The Hive database where to write the output
    :param table str: The Hive table where to write the output
    :param output_path str: The table location
    :param mode_output str: Anything accepted by Spark's `.write.mode()`.
    :param partition_col str: The partition column
    :param partition_function: A Spark Column expression for the `partition_col`. If not present,
                               `partition_col` should already be in the input data

    :Keyword Arguments:
      * *spark_config* (``dict``) --
        This dictionaries contains options to be passed when building a `SparkSession` (for example
        `{'master': 'yarn'}`)
      * *format* (``str``) --
        The format the data will be in. All options supported by Spark. Parquet is the default.
      * *header* (``bool``) --
        If reading a csv file, this will tell if the header is present (and use the schema)
      * *schema* (``pyspark.sql.types.StructType``)
        The input schema
      * *master* (``str``) --
        Specify which `master` should be used
      * *repartition* (``bool``) --
        Whether to partition the data by partition column beforer writing. This reduces the number
        of small files written by Spark
      * *key* (``str``)
        In principle all `key` if accepted by `spark.read.options`, by `findspark.init()`, or by
        `SparkSession.builder.config`

    :Example:

    >>> import pyspark.sql.functions as sf
    >>> column = 'a_column_with_unixtime'
    >>> partition_function = lambda column: sf.from_unixtime(sf.col(column), fmt='yyyy-MM-dd')

    >>> from spark_partitionr import main
    >>> main('hdfs:///data/some_data', 'parquet', 'my_db', 'my_tbl', mode_output='overwrite',
    ...      partition_col='dt', partition_with=partition_function('a_col'),
    ...      master='yarn', format='com.databricks.spark.csv',
    ...      header=True, to_unnest=['deeply_nested_column'])
    """
    sanitized_table = sanitize_table_name(table_name)
    if not spark:
        spark = create_spark_session(database, sanitized_table, **kwargs)

    if isinstance(input, str):
        df = load_data(spark, input, **kwargs)
    else:
        df = input


    try:
        old_df = spark.read.table("{}.{}".format(database, sanitized_table))
        new = False
    except Exception as e: # spark exception
        new = True


    if not new:
        try:
            schema_equal, schema_compatible = check_compatibility(df, old_df,
                                                                  format_output,
                                                                  partition_col)

            old_table_name, sanitized_table = check_external(spark, database, sanitized_table,
                                                             schema_equal,
                                                             schema_compatible)
        except schema.SchemaError as e:
            # we set this as no table movement should take place

            schema_compatible = False
            _, schema_backward_compatible = check_compatibility(old_df, df, format_output)
            if schema_backward_compatible:
                df = try_impose_schema(spark, input, old_df.schema, **kwargs)
            else:
                raise schema.SchemaError('Schemas are not compatible in both direction')

    if not new and schema_compatible:
        df_schema = schema.create_schema(df, database, sanitized_table, partition_col,
                               format_output, output_path, **kwargs)
        spark.sql(df_schema)

    partitioned_df = add_partition_column(df, partition_col, partition_with)
    if not output_path:
        output_path = get_output_path(spark, database, sanitized_table)

    write_data(partitioned_df, format_output, mode_output, partition_col, output_path, **kwargs)
    repair_partitions(spark, database, sanitized_table)

    if not new and schema_compatible:
        move_table(spark, from_database=database, from_table=sanitized_table,
                   to_database=database, to_table=old_table_name)


def try_impose_schema(spark, input, schema, **kwargs):
    """
    This will impose the schema from old_df on a dataframe

    :param spark: SparkContext
    :param str input: Location of the data
    :param old_df:
    :param kwargs:
    :return:
    """
    if isinstance(input, str):
        df = load_data(spark, input, schema=schema, **kwargs)
    else:
        raise ValueError("Can only impose schema if `input` is not a DataFrame")
    return df


def try_healing(spark, df, old_df, partition_col):
    alias_partition_col = 'partition_col_df'
    partition_values_df = df.distinct(partition_col).alias(alias_partition_col)
    partition_values_df.cache()
    partition_values_df.count()
    random_string = uuid.uuid4().hex
    path = os.path.join('/tmp', random_string)
    (old_df
     .sample(0.01)
     .join(partition_values_df, on=partition_values_df[alias_partition_col] ==
                                   old_df[partition_col], how='left')
     .filter(alias_partition_col + ' IS NULL')
     .drop(alias_partition_col)
     .write
     .partitionBy(partition_col)
     .json(path, mode='overwrite')
     )

    df.write.partitionBy(partition_col).json(path, mode='append')

    new_df = (spark
              .read
              .option('mergeSchema', 'true')
              .json(path))

    return (new_df
            .join(partition_values_df, on=partition_values_df[alias_partition_col] ==
                                          new_df[partition_col], how='inner')
            .drop(alias_partition_col))


