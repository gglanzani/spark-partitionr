import logging

logger = logging.getLogger()

storage = {'parquet': 'STORED AS PARQUET',
           'com.databricks.spark.csv': ("ROW FORMAT SERDE"
                                        "'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n"
                                        "STORED AS TEXTFILE")}


def sanitize(key):
    """
    Sanitize column names (they cannot begin with '_') by surrounding them with backticks (`)
    """
    if key[0] == "_":
        return "`%s`" % key
    else:
        return key


def are_schemas_equal(df, old_df, *, partition_col=None):
    if not partition_col:
        partition_col_set = set()
    else:
        partition_col_set = {partition_col}
    old_dtypes = dict(old_df.dtypes)
    new_dtypes = dict(df.dtypes)
    new_keys = new_dtypes.keys() - old_dtypes.keys() - partition_col_set
    if new_keys:
        return False
    else:
        return all(value == old_dtypes[key]
                   for key, value in new_dtypes.items() if key != partition_col)


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
        raise KeyError(
            "Unrecognized format_output %s. Available values are %s" % (format_output,
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


def list_to_dict(lst, attr):
    return {elem[attr]: elem for elem in lst}


def are_fields_complex(new_field, old_field):
    return type(new_field['type']) == dict and old_field and type(old_field['type']) == dict


def _compare_fields(new_field, old_field):
    return all(new_value == old_field.get(new_key)
               for new_key, new_value in new_field.items() if new_key != 'metadata')


def compare_complex_fields(new_field, old_field):
    complex_new_field = new_field['type']
    complex_old_field = old_field['type']
    if complex_new_field['type'] == complex_old_field['type'] == 'struct':
        new_schema = complex_new_field
        old_schema = complex_old_field
    elif complex_new_field['type'] == complex_old_field['type'] == 'array':
        new_schema = complex_new_field['elementType']
        old_schema = complex_old_field['elementType']
    else:
        return False
    return are_schemas_compatible(new_schema, old_schema)


def compare_fields(new_field, old_field):
    if are_fields_complex(new_field, old_field):
        return compare_complex_fields(new_field, old_field)
    elif old_field and new_field['type'] != old_field['type']:
        return False
    elif old_field and new_field['type'] == old_field['type']:
        return _compare_fields(new_field, old_field)
    else:
        return new_field.get('nullable')


def are_schemas_compatible(new_schema, old_schema, remove_from_old=None):
    new_schema = list_to_dict(new_schema['fields'], 'name')
    old_schema = list_to_dict(old_schema['fields'], 'name')

    if (isinstance(remove_from_old, str)  # this fails when remove_from_old=None
            and old_schema.get(remove_from_old)):
        old_schema.pop(remove_from_old)
    if (isinstance(remove_from_old, str)
            and not old_schema.get(remove_from_old)):
        logger.warning(
            'The `remove_from_old`={} key was not found in `old_schema`'.format(remove_from_old))
        logger.warning("Available keys are {}".format(old_schema.keys()))

    return all(compare_fields(new_value, old_schema.get(new_key))
               for new_key, new_value in new_schema.items())
