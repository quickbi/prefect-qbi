import json

from google.cloud import bigquery

from .utils import convert_to_snake_case

CUSTOM_RENAMINGS = {
    "_airbyte_extracted_at": "_row_extracted_at",
}
SAMPLE_SIZE = 1000


def _rename(name):
    snake_cased = convert_to_snake_case(name)
    customized = CUSTOM_RENAMINGS.get(snake_cased, snake_cased)
    return customized


def transform_schema(source_schema, client, project_id, source_dataset_id, table_name):
    schema = []
    select_list = []

    # First, infer how each field should be mapped to new field(s).
    # mapping is in format:
    # [
    #   (field_1, [...]),
    #   (field_2, [...]),
    #   ...
    # ]
    mapping = get_schema_mapping(
        source_schema, client, project_id, source_dataset_id, table_name
    )

    # Then, get actual schema as simple list of fields and generate SQL for selecting
    # those fields.
    for original_field, new_fields in mapping:
        select_list.append(original_field.name)

        for field_item in new_fields:
            json_key = field_item["json_key"]
            field = field_item["field"]
            schema.append(field)

            if original_field.field_type == "JSON" and json_key:
                selection = f"JSON_EXTRACT({original_field.name}, '$.{json_key}')"
                type_conversion_func = (
                    f"LAX_{field.field_type}"
                    if field.field_type in ("INT64", "BOOL", "FLOAT64", "STRING")
                    else None
                )
                if type_conversion_func:
                    selection = f"{type_conversion_func}({selection})"
                select_list.append(selection)

    select_list_str = ", \n".join(select_list)
    return schema, select_list_str


def get_schema_mapping(
    source_schema, client, project_id, source_dataset_id, table_name
):
    """Return mapping from original fields to new fields

    One field can be mapped to multiple fields.
    """
    table_ref = f"{project_id}.{source_dataset_id}.{table_name}"
    filtered_schema = [
        field
        for field in source_schema
        if (
            not field.name.startswith("_airbyte")
            or field.name == "_airbyte_extracted_at"
        )
    ]
    # Move remaining "_airbyte*" fields to the end.
    schema = sorted(
        filtered_schema, key=lambda field: field.name.startswith("_airbyte")
    )

    # If there are JSON columns, sample data and infer schema.
    json_columns = [field.name for field in schema if field.field_type == "JSON"]
    json_column_schemas = infer_columns_from_json_by_sampling(
        client, json_columns, table_ref
    )

    schema_mapping = []
    for field in schema:
        field_mapping = map_field(field, json_column_schemas)
        schema_mapping.append(field_mapping)

    return schema_mapping


def transform_field(field):
    if field.field_type == "RECORD":
        transformed_subfields = [transform_field(f) for f in field.fields]
        return bigquery.SchemaField(
            name=_rename(field.name),
            field_type=field.field_type,
            mode=field.mode,
            fields=transformed_subfields,
        )
    else:
        return bigquery.SchemaField(
            name=_rename(field.name),
            field_type=field.field_type,
            mode=field.mode,
        )


def map_field(field, json_column_schemas):
    new_field = transform_field(field)
    new_fields = [
        {
            "field": new_field,
            "json_key": None,
        }
    ]

    if field.name in json_column_schemas:
        for key, data_type in json_column_schemas[field.name].items():
            raw_name = field.name if key is None else f"{field.name}__{key}"
            name = _rename(raw_name)
            new_fields.append(
                {
                    "field": bigquery.SchemaField(name, data_type, mode="NULLABLE"),
                    "json_key": key,
                }
            )

    return (field, new_fields)


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
                field_value, field_schema
            )

    return json_column_schemas


def analyze_json_value(value, previous_mapping):
    """Return mapping from keys to BigQuery data types"""
    subcolumns = {}

    try:
        obj = json.loads(value)
    except (json.decoder.JSONDecodeError, TypeError) as e:
        # Just keep the original column.
        return {}

    if isinstance(obj, dict):
        for key, val in obj.items():
            this_type = get_bigquery_type(val)
            previous_type = previous_mapping.get(key, "STRING")
            new_type = get_better_type(this_type, previous_type)

            subcolumns[key] = new_type

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
    else:
        return "STRING"  # Default to STRING if type is not recognized


type_precedence = ["FLOAT64", "BOOL", "INT64", "RECORD", "STRING"]
type_precedence_dict = {value: index for index, value in enumerate(type_precedence)}


def get_better_type(value1, value2):
    position1 = type_precedence_dict.get(value1, float("inf"))
    position2 = type_precedence_dict.get(value2, float("inf"))
    result = value1 if position1 < position2 else value2
    return result
