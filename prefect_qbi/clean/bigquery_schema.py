from google.cloud import bigquery

from .json_columns import infer_columns_from_json_by_sampling
from .utils import convert_to_snake_case

CUSTOM_RENAMINGS = {
    "_airbyte_extracted_at": "_row_extracted_at",
}


def transform_table_schema(
    source_schema,
    client,
    project_id,
    source_dataset_id,
    table_name,
    should_unnest_objects,
):
    """Return transformed schema metadata for given table

    Metadata includes among other things:
    - "fields": a list of SchemaFields.
    - "select_list": a list of strings used in SQL select statement like:
        "some_regular_column,"
        or
        "JSON_EXTRACT(some_json_column, '$.some_key'),"
    """
    new_schema, subtables = get_new_schema(
        source_schema,
        client,
        project_id,
        source_dataset_id,
        table_name,
        should_unnest_objects,
    )

    sub = []
    if subtables:
        for subtable_name, subtable_data in subtables.items():
            cleaned_subtable_name = _clean_name(subtable_name)
            sub.append(
                {
                    "table_name": f"{table_name}__{cleaned_subtable_name}",
                    "json_column_name": subtable_name,
                    "fields": [field["field"] for field in subtable_data],
                    "select_list": [field["select_str"] for field in subtable_data],
                }
            )

    result_schema = {
        "table_name": table_name,
        "fields": [field["field"] for field in new_schema],
        "select_list": [field["select_str"] for field in new_schema],
        "subtables": sub,
    }

    result_schema = _add_extra_fields(result_schema, table_name)
    return result_schema


def get_new_schema(
    source_schema,
    client,
    project_id,
    source_dataset_id,
    table_name,
    should_unnest_objects,
):
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
        client, json_columns, table_ref, should_unnest_objects
    )

    new_schema = []
    subtables = {}
    for field in filtered_schema:
        new_fields, field_subtables = map_to_new_fields(field, json_column_schemas)
        new_schema.extend(new_fields)
        subtables.update(field_subtables)

    new_schema_sorted = sorted(new_schema, key=_get_field_sort_key)
    return new_schema_sorted, subtables


def _get_field_sort_key(field):
    if field["field"].name == "_row_extracted_at":
        return 1
    if field["field"].name.startswith("_airbyte"):
        # There probably isn't "_airbyte*" fields anymore but if there are,
        # they should be (almost) in the end.
        return 2
    if field["field"].field_type == "JSON":
        return 3
    return 0


def map_to_new_fields(original_field, json_column_schemas):
    new_fields = []
    subtables = {}

    original_field_transformed = transform_original_field(original_field)

    include_original_field = True
    json_column_schema = json_column_schemas.get(original_field.name, {})
    json_array_schema = json_column_schema.get(None, {})
    if json_array_schema.get("data_type") == "JSON" and json_array_schema.get(
        "create_subtable"
    ):
        include_original_field = False

    if include_original_field:
        select_str = get_select_str(original_field.name, None, None, None)
        new_fields.append(
            {
                "field": original_field_transformed,
                "json_key": None,
                "original_name": original_field.name,
                "special_data_type": None,
                "select_str": select_str,
                "create_subtable": False,
                "subtable_columns": None,
            }
        )

    # In case of JSON column, possilby add multiple extra fields.
    if original_field.name in json_column_schemas:
        for json_key, metadata in json_column_schemas[original_field.name].items():
            # Get name.
            special_data_type = metadata["special_data_type"]
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
            data_type = metadata["data_type"]
            select_str = get_select_str(
                original_field.name,
                data_type,
                json_key,
                special_data_type,
            )

            mode = metadata["mode"]
            item = {
                "field": bigquery.SchemaField(name, data_type, mode=mode),
                "json_key": json_key,
                "original_name": original_field.name,
                "special_data_type": special_data_type,
                "select_str": select_str,
                "create_subtable": metadata.get("create_subtable") is True,
                "subtable_columns": metadata.get("subcolumns"),
            }

            is_array = json_key is None
            if is_array:
                if special_data_type == "csv":
                    new_fields.append(item)

                if data_type == "JSON":
                    subtables[original_field.name] = [
                        {
                            "field": bigquery.SchemaField(
                                _clean_name(col_name),
                                col_metadata["data_type"],
                                mode=col_metadata["mode"],
                            ),
                            "json_key": col_name,
                            "select_str": _get_json_extract_select_str(
                                "array_item", col_name, col_metadata["data_type"]
                            ),
                        }
                        for col_name, col_metadata in metadata.get(
                            "subcolumns", {}
                        ).items()
                    ]
                elif data_type == "STRING":
                    subtables[original_field.name] = [
                        {
                            "field": bigquery.SchemaField(
                                _clean_name(original_field.name),
                                metadata["data_type"],
                                mode=metadata["mode"],
                            ),
                            "select_str": "LAX_STRING(array_item)",
                            "json_key": original_field.name,
                        }
                    ]

            else:
                new_fields.append(item)

    return new_fields, subtables


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
        select_str = _get_json_extract_select_str(
            original_field_name, json_key, json_field_type
        )
        if select_str:
            return select_str
    elif special_data_type == "csv":
        return f"ARRAY_TO_STRING(JSON_EXTRACT_STRING_ARRAY(`{original_field_name}`, '$'), ',')"

    return f"`{original_field_name}`"


def _get_json_extract_select_str(original_field_name, json_key, json_field_type):
    selection = f"JSON_EXTRACT(`{original_field_name}`, '$.{json_key}')"
    type_conversion_func = (
        f"LAX_{json_field_type}"
        if json_field_type in ("INT64", "BOOL", "FLOAT64", "STRING")
        else None
    )
    if type_conversion_func:
        return f"{type_conversion_func}({selection})"

    return selection


def _add_extra_fields(schema, table_name):
    if schema["subtables"]:
        _add_join_key(schema, table_name)

        for subtable in schema["subtables"]:
            _add_join_key(subtable, table_name)
            _add_row_extracted_at(subtable)

    return schema


def _add_join_key(schema_obj, table_name):
    schema_obj["fields"] = [
        bigquery.SchemaField(
            f"_quickbi_{table_name}_join_key", "STRING", mode="REQUIRED"
        )
    ] + schema_obj["fields"]
    schema_obj["select_list"] = ["_airbyte_raw_id"] + schema_obj["select_list"]
    return schema_obj


def _add_row_extracted_at(schema_obj):
    schema_obj["fields"] = schema_obj["fields"] + [
        bigquery.SchemaField(f"_row_extracted_at", "TIMESTAMP", mode="REQUIRED")
    ]
    schema_obj["select_list"] = schema_obj["select_list"] + ["_airbyte_extracted_at"]
    return schema_obj
