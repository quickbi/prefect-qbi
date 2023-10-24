import json

SAMPLE_SIZE = 3000


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
    json_columns_str = ", ".join(json_columns)

    query = f"""
        WITH batched_rows AS (
          (SELECT {json_columns_str} FROM `{table_ref}` LIMIT {SAMPLE_SIZE * 2} OFFSET 0)
          UNION ALL
          (SELECT {json_columns_str} FROM `{table_ref}` LIMIT {SAMPLE_SIZE * 2} OFFSET 10000)
          UNION ALL
          (SELECT {json_columns_str} FROM `{table_ref}` LIMIT {SAMPLE_SIZE * 2} OFFSET 100000)
        )

        SELECT *
        FROM batched_rows
        ORDER BY RAND()
        LIMIT {SAMPLE_SIZE};
    """
    query_job = client.query(query)
    rows = list(query_job)

    # Loop through the sampled rows to infer the schema.
    json_column_schemas = {}
    for row in rows:
        for field_name, field_value in row.items():
            schema = json_column_schemas.get(field_name, {})
            try:
                json_column_schemas[field_name] = analyze_json_value(
                    field_name, field_value, schema, should_unnest_objects
                )
            except SkipAnalyzing:
                continue

    return json_column_schemas


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

    raise RuntimeError(f"Unexpected JSON value in {field_name}.")


def analyze_dict(obj, schema):
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
    types = set([get_bigquery_type(val) for val in obj])
    if not types:
        return previous_schema

    if types == {"STRING"}:
        new_schema = {
            None: {
                "data_type": "STRING",
                "mode": "NULLABLE",
                "special_data_type": "csv",
                "create_subtable": True,
            }
        }
        if previous_schema and new_schema != previous_schema:
            raise RuntimeError("Schema conflict")
        return new_schema

    elif types == {"JSON"}:
        subcolumns = previous_schema.get("subcolumns", {})

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
            }
        }
        if previous_schema and new_schema != previous_schema:
            raise RuntimeError("Schema conflict")
        return new_schema
    else:
        print("Unhandled type:", types)
        raise RuntimeError("Unhandled type.")

    return json_column_schema


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
