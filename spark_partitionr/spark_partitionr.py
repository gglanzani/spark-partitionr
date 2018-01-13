"""
Main module to load the data
"""
# pylint: disable=C0330
import re
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


def sanitize(key):
    """
    Sanitize column names (they cannot begin with '_') by surrounding them with backticks (`)
    """
    if key[0] == "_":
        return "`%s`" % key
    else:
        return key


def create_schema(df, database, table, partition_col='dt',
                  format_output='parquet', output_path=None, external=False, **kwargs):
    """
    Create the schema (as a SQL string) for the dataframe in question

    The `format_output` is needed as this has to be specified in the create statement

    :param df: The dataframe that has been written partitioned on "disk"
    :type df: A Spark dataframe
    :param database str: To which database does the table belong
    :param table str: On which tables has this been written to
    :param partition_col str: On which column should it be partitioned
    :param format_output str: What format should the table use.
    :param output_path: Where the table should be written (if not in the metastore managed folder).
    """
    if format_output and format_output not in storage:
        raise KeyError("Unrecognized format_output %s. Available values are %s" % (format_output,
                                                                                   list(storage.keys())))
    external = "EXTERNAL" if external else ""
    init_string = ("CREATE {external} TABLE "
                  "IF NOT EXISTS {database}.{table} ".format(external=external,
                                                             database=database,
                                                             table=table))
    fields_string = "(\n" + ",\n".join([sanitize(key) + " " + value
                                for key, value in df.dtypes
                                if key != partition_col]) + "\n)"
    if partition_col:
        partition_string = "\nPARTITIONED BY (%s STRING)" % partition_col
    else:
        partition_string = ""

    format_string = "\n%s" % storage.get(format_output, "")
    if output_path:
        location = "\nLOCATION '%s'" % output_path
    else:
        location = ""
    return init_string + fields_string + partition_string + format_string + location


def create_partitions(spark, database, table):
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
    query = """MSCK REPAIR TABLE %(database)s.%(table)s""".format(database=database,
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

def sanitize_table_name(table_name):
    return re.sub(INVALID_HIVE_CHARACTERS, "_", table_name)


def are_schemas_equal(df, *, spark, database, table, partition_col):
    old_df = spark.read.table("{}.{}".format(database, table))
    old_dtypes = dict(old_df.dtypes)
    new_dtypes = dict(df.dtypes)
    new_keys = new_dtypes.keys() - old_dtypes.keys() - set(partition_col)
    if new_keys:
        return False
    else:
        return all(value == old_dtypes[key]
                   for key, value in new_dtypes.items if key != partition_col)


def main(input, format_output, database, table_name, output_path=None, mode_output='append', partition_col='dt',
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
      * *to_unnest* (``list``) --
        Which Struct's, if any, should be unnested as columns. This is helpful for the cases when
        a field is too deeply nested that it exceeds the maximum length supported by Hive
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

    to_unnest = kwargs.get('to_unnest')
    if to_unnest:
        for el in to_unnest:
            df = df.select('%s.*' % el, *df.columns).drop(el)

    recreate_table = are_schemas_equal(df, spark=spark,
                                       database=database, table=sanitized_table,
                                       partition_col=partition_col)
    if recreate_table:
        spark.sql('DROP TABLE {}.{}'.format(database, sanitized_table))

    schema = create_schema(df, database, sanitized_table, partition_col,
                           format_output, output_path, **kwargs)
    spark.sql(schema)
    partitioned_df = add_partition_column(df, partition_col, partition_with)
    create_partitions(spark, database, sanitized_table)
    if not output_path:
        output_path = get_output_path(spark, database, sanitized_table)
    write_data(partitioned_df, format_output, mode_output, partition_col, output_path, **kwargs)
