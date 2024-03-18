import json

SAMPLE_SIZE = 10000
INITIAL_SAMPLE_SIZE = 100000


def infer_columns_from_json_by_sampling(
    client, json_columns, table_ref, should_unnest_objects
):
    """Return mapping from old column names to metadata dict describing new columns

    Returns for example:
    {
        some_json_array_column: {
            None: {
                "data_type": "JSON",
                "mode": "NULLABLE",
                "special_data_type": None,
                "create_subtable": True,
                "subcolumns": {
                    "key1": {
                        "data_type": "INT64",
                        "mode": "NULLABLE",
                        "special_data_type": None,
                    },
                },
            }
        }
        some_json_object_column: {
            key_a: {
                "data_type": "STRING",
                "mode": "NULLABLE",
                "special_data_type": "csv",
            }
        }
    }

    """
    if not json_columns:
        return {}

    schemas = {}
    for json_column in json_columns:
        col_schema = infer_schema_for_column(
            client, json_column, table_ref, should_unnest_objects
        )
        schemas[json_column] = col_schema

    return schemas


def infer_schema_for_column(client, json_column, table_ref, should_unnest_objects):
    query = f"""
        WITH initial_sample AS (
          SELECT `{json_column}`
          FROM `{table_ref}`
          WHERE `{json_column}` is not null
          LIMIT {INITIAL_SAMPLE_SIZE}
        )

        SELECT *
        FROM initial_sample
        ORDER BY RAND()
        LIMIT {SAMPLE_SIZE};
    """
    rows = client.query(query)

    schema = {}
    for row in rows:
        value = row.get(json_column)
        try:
            schema = analyze_json_value(
                json_column, value, schema, should_unnest_objects
            )
        except SkipAnalyzing:
            continue

    return schema


def infer_schema_from_json_values(json_column, values, should_unnest_objects):
    schema = {}
    for value in values:
        try:
            schema = analyze_json_value(
                json_column, value, schema, should_unnest_objects
            )
        except SkipAnalyzing:
            continue
    return schema


class SkipAnalyzing(Exception):
    pass


def analyze_json_value(field_name, field_value, schema, should_unnest_objects):
    """Return mapping from keys to BigQuery data types and modes"""
    if field_value is None:
        return schema

    obj = json.loads(field_value)

    if isinstance(obj, dict):
        if not should_unnest_objects:
            raise SkipAnalyzing()
        return analyze_dict(obj, schema)
    elif isinstance(obj, list):
        return analyze_list(obj, schema)
    elif obj is None:
        # Original value was JSON null instead of SQL null.
        return schema
    elif isinstance(obj, str):
        # For example Pipedrive's persons.next_activity_time has json string
        # values such as "13:30:00" for some reason. TODO: Maybe we should
        # convert these kind of columns to regular strings?
        return schema

    raise RuntimeError(f"Unexpected JSON value in {field_name}.")


def analyze_dict(obj, schema):
    if isinstance(obj, list):
        # Sometimes a column with dicts can have also some lists. Skip these cases.
        raise SkipAnalyzing()

    for key, val in obj.items():
        this_type = get_bigquery_type(val)
        previous_type = schema.get(key, {}).get("data_type", "STRING")
        new_type = get_better_type(this_type, previous_type)

        schema[key] = {
            "data_type": new_type,
            "mode": "NULLABLE",
            "special_data_type": None,
        }
    return schema


def analyze_list(obj, previous_schema):
    json_column_schema = {}
    types = set([get_bigquery_type(val) for val in obj if val is not None])
    if not types:
        return previous_schema

    if types == {"STRING"}:
        new_schema = _get_string_array_schema()
        if previous_schema and new_schema != previous_schema:
            previous_data_type = previous_schema.get(None, {}).get("data_type")
            if not previous_data_type in ("INT64", "FLOAT64"):
                raise RuntimeError("Schema conflict")
        return new_schema

    elif types == {"JSON"}:
        subcolumns = previous_schema.get(None, {}).get("subcolumns", {})

        for val in obj:
            if val is None:
                continue

            subcolumns = analyze_dict(val, subcolumns)

        json_column_schema[None] = {
            "data_type": "JSON",
            "mode": "NULLABLE",
            "special_data_type": None,
            "create_subtable": True,
            "subcolumns": subcolumns,
        }

    elif len(types) == 1:
        data_type = types.pop()
        new_schema = {
            None: {
                "data_type": data_type,
                "mode": "NULLABLE",
                "create_subtable": True,
                "special_data_type": None,
            }
        }
        if previous_schema and new_schema != previous_schema:
            previous_data_type = previous_schema.get(None, {}).get("data_type")
            if previous_data_type == "STRING":
                return _get_string_array_schema()
            raise RuntimeError("Schema conflict")
        return new_schema
    else:
        print("Unhandled type:", types)
        raise RuntimeError("Unhandled type.")

    return json_column_schema


def _get_string_array_schema():
    return {
        None: {
            "data_type": "STRING",
            "mode": "NULLABLE",
            "special_data_type": "csv",
            "create_subtable": True,
        }
    }


def get_bigquery_type(value):
    if isinstance(value, bool):
        return "BOOL"
    elif isinstance(value, int):
        return "INT64"
    elif isinstance(value, float):
        return "FLOAT64"
    elif isinstance(value, str):
        return "STRING"
    elif value is None:
        return "STRING"  # Assuming null values are represented as strings in JSON
    elif isinstance(value, dict):
        # Nested JSON
        return "JSON"
    elif isinstance(value, list):
        return "JSON"
    else:
        return "STRING"  # Default to STRING if type is not recognized


type_precedence = ["FLOAT64", "BOOL", "INT64", "JSON", "STRING"]
type_precedence_dict = {value: index for index, value in enumerate(type_precedence)}


def get_better_type(value1, value2):
    position1 = type_precedence_dict.get(value1, float("inf"))
    position2 = type_precedence_dict.get(value2, float("inf"))
    result = value1 if position1 < position2 else value2
    return result
