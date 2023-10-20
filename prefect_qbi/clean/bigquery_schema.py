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
    new_schema = get_new_schema(
        source_schema, client, project_id, source_dataset_id, table_name
    )

    fields = [field["field"] for field in new_schema]
    select_list = [field["select_str"] for field in new_schema]
    select_list_str = ", \n".join(select_list)
    return fields, select_list_str


def get_new_schema(source_schema, client, project_id, source_dataset_id, table_name):
    """Return list of dicts containing new fields and some metadata"""
    table_ref = f"{project_id}.{source_dataset_id}.{table_name}"
    filtered_schema = [
        field
        for field in source_schema
        if (
            not field.name.startswith("_airbyte")
            or field.name == "_airbyte_extracted_at"
        )
    ]

    # If there are JSON columns, infer schema by sampling data.
    json_columns = [
        field.name for field in filtered_schema if field.field_type == "JSON"
    ]
    json_column_schemas = infer_columns_from_json_by_sampling(
        client, json_columns, table_ref
    )

    new_schema = []
    for field in filtered_schema:
        new_fields = map_to_new_fields(field, json_column_schemas)

        # Temporarily skip RECORD types as error
        # "Field <field_name> is type RECORD but has no schema" is raised.
        new_fields = [f for f in new_fields if f["field"].field_type != "RECORD"]

        new_schema.extend(new_fields)

    new_schema_sorted = sorted(new_schema, key=_get_field_sort_key)
    return new_schema_sorted


def _get_field_sort_key(field):
    if field["field"].field_type == "JSON":
        return 1
    if field["field"].name == "_row_extracted_at":
        return 2
    if field["field"].name.startswith("_airbyte"):
        return 3
    return 0


def map_to_new_fields(original_field, json_column_schemas):
    original_field_transformed = transform_original_field(original_field)
    select_str = get_select_str(original_field.name, None, None, None)
    new_fields = [
        {
            "field": original_field_transformed,
            "json_key": None,
            "original_name": original_field.name,
            "special_data_type": None,
            "select_str": select_str,
        }
    ]

    # In case of JSON column, possilby add multiple extra fields.
    if original_field.name in json_column_schemas:
        for json_key, schema_item in json_column_schemas[original_field.name].items():
            # Get name.
            special_data_type = schema_item["special_data_type"]
            if special_data_type == "csv":
                raw_name = f"{original_field.name}_as_csv"
            else:
                raw_name = (
                    original_field.name
                    if json_key is None
                    else f"{original_field.name}__{json_key}"
                )
            name = _clean_name(raw_name)

            # Get select_str.
            data_type = schema_item["data_type"]
            select_str = get_select_str(
                original_field.name,
                data_type,
                json_key,
                special_data_type,
            )

            mode = schema_item["mode"]
            new_fields.append(
                {
                    "field": bigquery.SchemaField(name, data_type, mode=mode),
                    "json_key": json_key,
                    "original_name": original_field.name,
                    "special_data_type": special_data_type,
                    "select_str": select_str,
                }
            )

    return new_fields


def transform_original_field(field):
    if field.field_type == "RECORD":
        # Handling of records is actually maybe not needed as BigQuery destination v2
        # uses JSON columns instead of records. So this could maybe be removed.
        transformed_subfields = [transform_original_field(f) for f in field.fields]
        return bigquery.SchemaField(
            name=_clean_name(field.name),
            field_type=field.field_type,
            mode=field.mode,
            fields=transformed_subfields,
        )
    else:
        return bigquery.SchemaField(
            name=_clean_name(field.name),
            field_type=field.field_type,
            mode=field.mode,
        )


def _clean_name(name):
    snake_cased = convert_to_snake_case(name)
    customized = CUSTOM_RENAMINGS.get(snake_cased, snake_cased)
    return customized


def get_select_str(original_field_name, json_field_type, json_key, special_data_type):
    if json_key:
        selection = f"JSON_EXTRACT(`{original_field_name}`, '$.{json_key}')"
        type_conversion_func = (
            f"LAX_{json_field_type}"
            if json_field_type in ("INT64", "BOOL", "FLOAT64", "STRING")
            else None
        )
        if type_conversion_func:
            return f"{type_conversion_func}({selection})"
    elif special_data_type == "csv":
        return f"ARRAY_TO_STRING(JSON_EXTRACT_STRING_ARRAY(`{original_field_name}`, '$'), ',')"

    return f"`{original_field_name}`"
