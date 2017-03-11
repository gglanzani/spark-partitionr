Spark Partitionr
================

This project aims at making it easy to load a dataset supported by Spark and create a Hive table
partitioned by a specific column. The output is written using one of the output format supported by
Spark.

Usage
-----

To use it

.. code:: python

    >>> import pyspark.sql.functions as sf
    >>> column = 'a_column_with_unixtime'
    >>> partition_function = lambda column: sf.from_unixtime(sf.col(column), fmt='yyyy-MM-dd')

    >>> from spark_partitionr import main
    >>> main('hdfs:///data/some_data', format_output='parquet',
             database='my_db', table='my_tbl', mode_output='overwrite',
             partition_col='dt', partition_with=partition_function('a_col'),
             master='yarn', format='com.databricks.spark.csv',
             header=True)

There are a number of additional arguments, among which `spark=spark` if you want
to pass your own `SparkSession` (very helpful when you need to process lots of datasets this way)
