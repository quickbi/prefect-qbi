from google.cloud import bigquery

from .bigquery_schema import transform_table_schema
from .bigquery_utils import (
    create_dataset_with_location,
    create_table_with_schema,
    delete_table,
    get_dataset_location,
    get_dataset_table_names,
    get_table_schema,
    insert_query_result_to_table,
    rename_table,
)
from .utils import convert_to_snake_case, get_unique_temp_table_name


# TODO: instead of looking at dataset name this should be able to get source system name.
def are_subtables_enabled(source_dataset_id):
    return True


# TODO: instead of looking at dataset name this should be able to get source system name.
def _should_unnest_objects(source_dataset_id):
    # Disable unnesting objects from HubSpot because Airbyte connector already
    # does it (https://docs.airbyte.com/integrations/sources/hubspot#unnesting-top-level-properties).
    return "hub" not in source_dataset_id


def transform_dataset(
    client,
    project_id,
    source_dataset_id,
    destination_dataset_id,
    table_prefix,
):
    source_location = get_dataset_location(client, project_id, source_dataset_id)
    create_dataset_with_location(
        client, project_id, destination_dataset_id, source_location
    )

    for source_table_name in get_dataset_table_names(
        client, project_id, source_dataset_id
    ):
        transform_table(
            client,
            project_id,
            source_dataset_id,
            destination_dataset_id,
            source_table_name,
            table_prefix,
        )


def transform_table(
    client,
    project_id,
    source_dataset_id,
    destination_dataset_id,
    source_table_name,
    table_prefix,
):
    # Infer how schema should be transformed.
    source_schema = get_table_schema(
        client, project_id, source_dataset_id, source_table_name
    )
    should_unnest_objects = _should_unnest_objects(source_dataset_id)
    transformed_schema = transform_table_schema(
        source_schema,
        client,
        project_id,
        source_dataset_id,
        source_table_name,
        should_unnest_objects,
    )

    table_mappings = []
    add_join_key = bool(transformed_schema.get("subtables"))  # TODO: unused?

    try:
        # Create temp version of main table and insert data to it.
        temp_destination_table_name, destination_table_name = _create_temp_table(
            table_prefix=table_prefix,
            transformed_schema=transformed_schema,
            project_id=project_id,
            destination_dataset_id=destination_dataset_id,
            client=client,
        )
        table_mappings.append((temp_destination_table_name, destination_table_name))
        insert_query_result_to_table(
            client,
            project_id,
            destination_dataset_id,
            temp_destination_table_name,
            _build_query(
                project_id, source_dataset_id, source_table_name, transformed_schema
            ),
        )

        _add_demo_tables(
            project_id,
            source_table_name,
            table_prefix,
            destination_dataset_id,
            client,
            source_dataset_id,
            source_table_name,
        )

        if (
            "m_files" in source_dataset_id
            and source_table_name == "objects_files_contents"
        ):
            from .m_files_transform import transform_json_column_to_tables

            transform_json_column_to_tables(
                client,
                project_id,
                source_dataset_id,
                destination_dataset_id,
                source_table_name,
                ["ObjectType", "ObjectID", "FileID"],
                "FileName",
                "ContentJson",
                table_prefix,
            )

        # Create temp version of possible subtables and insert data to them.
        for subtable_schema in transformed_schema["subtables"]:
            if not are_subtables_enabled(source_dataset_id):
                break

            temp_destination_table_name, destination_table_name = _create_temp_table(
                table_prefix=table_prefix,
                transformed_schema=subtable_schema,
                project_id=project_id,
                destination_dataset_id=destination_dataset_id,
                client=client,
            )
            table_mappings.append((temp_destination_table_name, destination_table_name))
            insert_query_result_to_table(
                client,
                project_id,
                destination_dataset_id,
                temp_destination_table_name,
                _build_query_subtable(
                    project_id, source_dataset_id, source_table_name, subtable_schema
                ),
            )

        # Remove original tables.
        for _, destination_table_name in table_mappings:
            delete_table(
                client,
                project_id,
                destination_dataset_id,
                destination_table_name,
            )

        # Rename temp tables to original tables.
        for temp_destination_table_name, destination_table_name in table_mappings:
            rename_table(
                client,
                project_id,
                destination_dataset_id,
                temp_destination_table_name,
                destination_table_name,
            )

        print(f"Table '{source_table_name}' transformed.")

    except Exception as e:
        # Delete the temporary tables in case of an error.
        for temp_destination_table_name, _ in table_mappings:
            delete_table(
                client,
                project_id,
                destination_dataset_id,
                temp_destination_table_name,
            )

        print(f"Error processing table '{source_table_name}': {e}")
        raise e


def _add_demo_tables(
    project_id,
    table_name,
    table_prefix,
    destination_dataset_id,
    client,
    source_dataset_id,
    source_table_name,
):
    if not (
        project_id in ("quickbi-demoexte2168", "quickbi-eerontok6534")
        and table_name == "users"
    ):
        return

    transformed_schemas = [
        {
            "fields": [
                bigquery.SchemaField(
                    "instagram_business_account",
                    "STRING",
                    "NULLABLE",
                    None,
                    None,
                    (),
                    None,
                ),
                bigquery.SchemaField(
                    "page", "STRING", "REQUIRED", None, None, (), None
                ),
                bigquery.SchemaField(
                    "_row_extracted_at",
                    "TIMESTAMP",
                    "REQUIRED",
                    None,
                    None,
                    (),
                    None,
                ),
            ],
            "select_list": [
                "'quickbiofficial'",
                "'Quickbi'",
                "`_airbyte_extracted_at`",
            ],
            "subtables": [],
            "table_name": "business_accounts_and_linked_pages",
        },
        {
            "fields": [
                bigquery.SchemaField(
                    "facebook_page", "STRING", "REQUIRED", None, None, (), None
                ),
                bigquery.SchemaField(
                    "_row_extracted_at",
                    "TIMESTAMP",
                    "REQUIRED",
                    None,
                    None,
                    (),
                    None,
                ),
            ],
            "select_list": [
                "'Quickbi'",
                "`_airbyte_extracted_at`",
            ],
            "subtables": [],
            "table_name": "facebook_pages",
        },
    ]
    for transformed_schema in transformed_schemas:
        temp_table_name, table_name_final = _create_temp_table(
            table_prefix=table_prefix,
            transformed_schema=transformed_schema,
            project_id=project_id,
            destination_dataset_id=destination_dataset_id,
            client=client,
        )
        insert_query_result_to_table(
            client,
            project_id,
            destination_dataset_id,
            temp_table_name,
            _build_query(
                project_id, source_dataset_id, source_table_name, transformed_schema
            ),
        )

        delete_table(
            client,
            project_id,
            destination_dataset_id,
            table_name_final,
        )
        rename_table(
            client,
            project_id,
            destination_dataset_id,
            temp_table_name,
            table_name_final,
        )


def transform_table_name(source_table_name, table_prefix):
    table_name_snake_case = convert_to_snake_case(source_table_name)
    destination_table_name = f"{table_prefix}__{table_name_snake_case}"
    temp_destination_table_name = get_unique_temp_table_name(destination_table_name)
    return temp_destination_table_name, destination_table_name


def _create_temp_table(
    table_prefix,
    transformed_schema,
    project_id,
    destination_dataset_id,
    client,
):
    temp_destination_table_name, destination_table_name = transform_table_name(
        transformed_schema["table_name"], table_prefix
    )
    create_table_with_schema(
        client,
        project_id,
        destination_dataset_id,
        temp_destination_table_name,
        schema=transformed_schema["fields"],
    )
    return temp_destination_table_name, destination_table_name


def _build_query(project_id, source_dataset_id, source_table_name, transformed_schema):
    source_table_ref = f"{project_id}.{source_dataset_id}.{source_table_name}"
    select_list_str = ", \n".join(transformed_schema["select_list"])
    return f"""
        SELECT
            {select_list_str}
        FROM `{source_table_ref}`
    """


def _build_query_subtable(
    project_id, source_dataset_id, source_table_name, transformed_schema
):
    source_table_ref = f"{project_id}.{source_dataset_id}.{source_table_name}"
    select_list_str = ", \n".join(transformed_schema["select_list"])
    json_column_name = transformed_schema["json_column_name"]
    return f"""
        SELECT
            {select_list_str}
        FROM `{source_table_ref}`
        CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(`{json_column_name}`)) AS array_item
    """
