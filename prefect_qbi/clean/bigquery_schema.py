import json

from google.cloud import bigquery

from .json_columns import infer_columns_from_json_by_sampling
from .utils import convert_to_snake_case

CUSTOM_RENAMINGS = {
    "_airbyte_extracted_at": "_row_extracted_at",
}


def transform_table_schema(
    source_schema, client, project_id, source_dataset_id, table_name
):
    """Return field list and SQL column selections for given table

    Field list is a list of SchemaFields.

    Column selections are strings used in SQL select statement like
    "some_regular_column,"
    or
    "JSON_EXTRACT(some_json_column, '$.some_key'),"
    """

    schema = []
    select_list = []

    # First, infer how each field should be mapped to new field(s).
    mapping = get_schema_mapping(
        source_schema, client, project_id, source_dataset_id, table_name
    )

    # Then, get actual schema as simple list of fields and generate SQL for selecting
    # those fields.
    for original_field, new_fields in mapping:
        schema_fields, selects = get_new_fields_from_mapping(original_field, new_fields)
        schema.extend(schema_fields)
        select_list.extend(selects)

    select_list_str = ", \n".join(select_list)
    return schema, select_list_str


# ----------------------
# Getting schema mapping
# ----------------------


def get_schema_mapping(
    source_schema, client, project_id, source_dataset_id, table_name
):
    """Return mapping from original fields to new fields (list of tuples)

    One field can be mapped to multiple fields.

    Mapping is in format:
    [
        (field_1, [...]),
        (field_2, [...]),
      ...
    ]
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
    sorted_schema = sorted(
        filtered_schema, key=lambda field: field.name.startswith("_airbyte")
    )

    # If there are JSON columns, infer schema by sampling data.
    json_columns = [field.name for field in sorted_schema if field.field_type == "JSON"]
    json_column_schemas = infer_columns_from_json_by_sampling(
        client, json_columns, table_ref
    )

    schema_mapping = [
        (field, map_to_new_fields(field, json_column_schemas))
        for field in sorted_schema
    ]

    return schema_mapping


def map_to_new_fields(field, json_column_schemas):
    original_field_transformed = transform_original_field(field)
    new_fields = [
        {
            "field": original_field_transformed,
            "json_key": None,
            "original_name": field.name,
            "special_data_type": None,
        }
    ]

    # In case of JSON column, possilby add multiple extra fields.
    if field.name in json_column_schemas:
        for key, schema_item in json_column_schemas[field.name].items():
            data_type = schema_item["data_type"]
            mode = schema_item["mode"]
            special_data_type = schema_item["special_data_type"]

            if special_data_type == "csv":
                raw_name = f"{field.name}_as_csv"
            else:
                raw_name = field.name if key is None else f"{field.name}__{key}"
            name = _rename(raw_name)

            new_fields.append(
                {
                    "field": bigquery.SchemaField(name, data_type, mode=mode),
                    "json_key": key,
                    "original_name": field.name,
                    "special_data_type": special_data_type,
                }
            )

    return new_fields


def transform_original_field(field):
    if field.field_type == "RECORD":
        transformed_subfields = [transform_original_field(f) for f in field.fields]
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


def _rename(name):
    snake_cased = convert_to_snake_case(name)
    customized = CUSTOM_RENAMINGS.get(snake_cased, snake_cased)
    return customized


# ---------------
# Process mapping
# ---------------


def get_new_fields_from_mapping(original_field, new_fields):
    schema_fields = []
    selects = []

    for field_item in new_fields:
        json_key = field_item["json_key"]
        field = field_item["field"]

        # Temporarily skip RECORD types as error
        # "Field <field_name> is type RECORD but has no schema" is raised.
        if field.field_type == "RECORD":
            continue

        schema_fields.append(field)

        if json_key:
            selection = f"JSON_EXTRACT(`{original_field.name}`, '$.{json_key}')"
            type_conversion_func = (
                f"LAX_{field.field_type}"
                if field.field_type in ("INT64", "BOOL", "FLOAT64", "STRING")
                else None
            )
            if type_conversion_func:
                selection = f"{type_conversion_func}({selection})"
        elif field_item["special_data_type"] == "csv":
            selection = f"ARRAY_TO_STRING(JSON_EXTRACT_STRING_ARRAY(`{original_field.name}`, '$'), ',')"
        else:
            selection = f"`{original_field.name}`"
        selects.append(selection)

    return schema_fields, selects
