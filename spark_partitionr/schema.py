import logging
from collections import Counter

logger = logging.getLogger()

storage = {'parquet': 'STORED AS PARQUET',
           'com.databricks.spark.csv': ("ROW FORMAT SERDE"
                                        "'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n"
                                        "STORED AS TEXTFILE")}

# key we can ignore when comparing if two fields are equal
METADATA = 'metadata'
# key where type information or nested fields are stored
TYPE = 'type'
# key where array type information of nested fields is stored
ARRAYTYPE = 'elementType'
# key where nullability is stored
NULLABLE = 'nullable'
# key where the fields are stored
FIELDS = 'fields'
# key where the name of the fields are stored
NAME = 'name'

STRUCT = 'struct'
ARRAY = 'array'


class SchemaError(Exception):
    pass


def sanitize(key):
    """
    Sanitize column names (they cannot begin with '_') by surrounding them with backticks (`)
    """
    if key[0] == "_":
        return "`%s`" % key
    else:
        return key


def are_schemas_equal(new_df, old_df, *, partition_col=None):
    """
    Check if two dataframe schemas are exactly the same, modulo the partition_col
    :param new_df: The new Spark DataFrame
    :param old_df: The old Spark DataFrame, that can contain `partition_col`
    :param str partition_col: The name of a column that might be only in `old_df`
    :return: A boolean indicating if the schemas are equal
    """
    if not partition_col:
        partition_col_set = set()
    else:
        partition_col_set = {partition_col}
    old_dtypes = dict(old_df.dtypes)
    new_dtypes = dict(new_df.dtypes)
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
    """
    Convert a list of dictionaries into a dictionary

    :param list[dict] lst: A list of dictionaries, all containing the `attr` key
    :param attr: The key to indicate the element in the resulting dict
    :return: A dictionary of dictionaries
    """
    if Counter(elem[attr] for elem in lst).most_common(1)[0][1] > 1:
        raise ValueError("""
        The dictionary can't be created unambigously. 
        More than one element contains the same {}""".format(attr))
    return {elem[attr]: elem for elem in lst}


def are_fields_complex(new_field, old_field):
    """
    Check if the fields are complex (and if the old one exists)

    :param any new_field: The new field to check. Can't be None
    :param any old_field: The old field to check. Can be None
    :return: A boolean
    """
    return type(new_field[TYPE]) == dict and old_field and type(old_field[TYPE]) == dict


def _compare_fields(new_field, old_field):
    """
    Compare if all elements, besides METADATA, exists

    :param dict new_field: The new field to check
    :param dict old_field: The old field to check
    :return: A boolean indicating if they are compatible
    """
    return all(new_value == old_field.get(new_key)
               for new_key, new_value in new_field.items() if new_key != METADATA)


def compare_complex_fields(new_field, old_field):
    """
    Compare if two complex fields are compatible

    :param dict new_field: The new field to check
    :param dict old_field: The old field to check
    :return: A boolean indicating if they are compatible
    """
    # The complex fields are nested
    complex_new_field = new_field[TYPE]
    complex_old_field = old_field[TYPE]
    if complex_new_field[TYPE] == complex_old_field[TYPE] == STRUCT:
        new_schema = complex_new_field
        old_schema = complex_old_field
    elif complex_new_field[TYPE] == complex_old_field[TYPE] == ARRAY:
        # somehow, for array, the fields are stored in ARRAYTYPE
        new_schema = complex_new_field[ARRAYTYPE]
        old_schema = complex_old_field[ARRAYTYPE]
        # the next happens for json sometimes:
        # old data: [(a: 1), (a: 5)] <-- array of structs
        # new data: [] <-- array of string, but it's empty! thank you json
        if ((old_schema and type(old_schema) != str) and type(new_schema) == str):
            logger.warning("""New schema is backward incompatible. Old schema is {},
                           new is {}""".format(old_schema, new_schema))
            raise SchemaError("Found array of strings instead of array of structs")
    else:
        # When the new one is a STRUCT, and the old one an ARRAY, or vice versa
        return False
    return are_schemas_compatible(new_schema, old_schema)


def compare_fields(new_field, old_field):
    """
    Compare two schema fields

    :param dict new_field: The new field to check
    :param dict old_field: The old field to check
    :return: A boolean indicating if they are compatible
    """
    if are_fields_complex(new_field, old_field):
        return compare_complex_fields(new_field, old_field)
    elif old_field and new_field[TYPE] != old_field[TYPE]:
        # this could be more accurante, as some numeric type are compatible (int -> float)
        return False
    elif old_field and new_field[TYPE] == old_field[TYPE]:
        return _compare_fields(new_field, old_field)
    else:
        # this happens when old_field is None. In that case the new field should be NULLABLE
        return new_field.get(NULLABLE)


def are_schemas_compatible(new_schema, old_schema, remove_from_old=None):
    """
    Check for schema compatibility

    The schema should be dict as returned by df.schema.jsonValue()
    :param dict new_field: The new field to check
    :param dict old_field: The old field to check
    :param remove_from_old: The (optional) field to remove from the old_schema
    :return: A boolean indicating if they are compatible
    """
    new_schema = list_to_dict(new_schema[FIELDS], NAME)
    old_schema = list_to_dict(old_schema[FIELDS], NAME)

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
