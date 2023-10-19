import json

SAMPLE_SIZE = 1000


def infer_columns_from_json_by_sampling(client, json_columns, table_ref):
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
            field_schema = json_column_schemas.get(field_name, {})
            json_column_schemas[field_name] = analyze_json_value(
                field_name, field_value, field_schema
            )

    return json_column_schemas


def analyze_json_value(field_name, field_value, previous_mapping):
    """Return mapping from keys to BigQuery data types and modes"""
    subcolumns = {}

    try:
        obj = json.loads(field_value)
    except (json.decoder.JSONDecodeError, TypeError):
        # Doesn't seem like a json value so don't analyze it further. Let's
        # just keep the original column.
        # TODO: Probably some error should be raised and analyzing aborted.
        return {}

    if isinstance(obj, dict):
        for key, val in obj.items():
            this_type = get_bigquery_type(val)
            previous_type = previous_mapping.get(key, {}).get("data_type", "STRING")
            new_type = get_better_type(this_type, previous_type)

            subcolumns[key] = {
                "data_type": new_type,
                "mode": "NULLABLE",
                "special_data_type": None,
            }

    # TODO: conflict handling, testing
    # elif isinstance(obj, list):
    #     types = set([get_bigquery_type(val) for val in obj])
    #     if types == {"STRING"}:
    #         subcolumns[None] = {
    #             "data_type": "STRING",
    #             "mode": "NULLABLE",
    #             "special_data_type": "csv",
    #         }
    #         if previous_mapping and subcolumns != previous_mapping:
    #             # TODO: Probably some error should be raised and analyzing aborted.
    #             return {}

    return subcolumns


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
        return "RECORD"
    # TODO: Handle list type.
    else:
        return "STRING"  # Default to STRING if type is not recognized


type_precedence = ["FLOAT64", "BOOL", "INT64", "RECORD", "STRING"]
type_precedence_dict = {value: index for index, value in enumerate(type_precedence)}


def get_better_type(value1, value2):
    position1 = type_precedence_dict.get(value1, float("inf"))
    position2 = type_precedence_dict.get(value2, float("inf"))
    result = value1 if position1 < position2 else value2
    return result
