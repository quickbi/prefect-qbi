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
from .m_files_transform import transform_json_column_to_tables
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
    client: bigquery.Client,
    project_id: str,
    source_dataset_id: str,
    destination_dataset_id: str,
    table_prefix: str,
):
    # Create destination dataset with source dataset's location.
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
    client: bigquery.Client,
    project_id: str,
    source_dataset_id: str,
    destination_dataset_id: str,
    source_table_name: str,
    table_prefix: str,
):
    table_mappings = []

    try:
        # Create and populate temp tables.
        for destination_table_spec in _iter_destination_table_specs(
            client,
            project_id,
            source_dataset_id,
            source_table_name,
            destination_dataset_id,
        ):
            destination_table_name = f"{table_prefix}__{destination_table_spec['name']}"
            temp_destination_table_name = get_unique_temp_table_name(
                destination_table_name
            )
            table_mappings.append((temp_destination_table_name, destination_table_name))
            _make_temp_destination_table(
                client,
                project_id,
                destination_dataset_id,
                temp_destination_table_name,
                destination_table_spec,
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

        # Remove previous versions of the final tables.
        for _, destination_table_name in table_mappings:
            delete_table(
                client,
                project_id,
                destination_dataset_id,
                destination_table_name,
            )

        # Rename temp tables to be the latest final tables.
        # Note: This is done only after all tables have been removed (previous step),
        # to prevent a state, in which both old and new tables exist simultaneously.
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
        # Remove temp tables on error. The final tables should be left unchanged.
        for temp_destination_table_name, _ in table_mappings:
            delete_table(
                client,
                project_id,
                destination_dataset_id,
                temp_destination_table_name,
            )

        print(f"Error processing table '{source_table_name}': {e}")
        raise e


def _iter_destination_table_specs(
    client,
    project_id,
    source_dataset_id,
    source_table_name,
    destination_dataset_id,
):
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

    source_table_ref = f"{project_id}.{source_dataset_id}.{source_table_name}"

    # Main table.
    yield {
        "name": convert_to_snake_case(transformed_schema["table_name"]),
        "schema_list": transformed_schema["fields"],
        "query_select_list": transformed_schema["select_list"],
        "query_from": f"""
            `{source_table_ref}`
        """,
    }

    # Subtables.
    if are_subtables_enabled(source_dataset_id):
        for subtable_schema in transformed_schema["subtables"]:
            json_column_name = subtable_schema["json_column_name"]
            yield {
                "name": convert_to_snake_case(subtable_schema["table_name"]),
                "schema_list": subtable_schema["fields"],
                "query_select_list": subtable_schema["select_list"],
                "query_from": f"""
                    `{source_table_ref}`
                    CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(`{json_column_name}`)) AS array_item
                """,
            }

    # M-Files file content tables.
    if "m_files" in source_dataset_id and source_table_name == "objects_files_contents":
        yield from transform_json_column_to_tables(
            client,
            project_id,
            source_dataset_id,
            destination_dataset_id,
            source_table_name,
            ["ObjectType", "ObjectID", "FileID"],
            "FileName",
            "ContentJson",
        )


def _make_temp_destination_table(
    client,
    project_id,
    destination_dataset_id,
    temp_destination_table_name,
    destination_table_spec,
):
    create_table_with_schema(
        client,
        project_id,
        destination_dataset_id,
        temp_destination_table_name,
        schema=destination_table_spec["schema_list"],
    )
    query_select = ", \n".join(destination_table_spec["query_select_list"])
    query_from = destination_table_spec["query_from"]
    destination_table_query = f"""
        SELECT {query_select}
        FROM {query_from}
    """
    insert_query_result_to_table(
        client,
        project_id,
        destination_dataset_id,
        temp_destination_table_name,
        destination_table_query,
    )


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
        table_name_snake_case = convert_to_snake_case(transformed_schema["table_name"])
        table_name_final = f"{table_prefix}__{table_name_snake_case}"

        temp_table_name = get_unique_temp_table_name(table_name_final)
        create_table_with_schema(
            client,
            project_id,
            destination_dataset_id,
            temp_table_name,
            schema=transformed_schema["fields"],
        )
        source_table_ref = f"{project_id}.{source_dataset_id}.{source_table_name}"
        select_list_str = ", \n".join(transformed_schema["select_list"])
        query = f"""
            SELECT
                {select_list_str}
            FROM `{source_table_ref}`
        """
        insert_query_result_to_table(
            client,
            project_id,
            destination_dataset_id,
            temp_table_name,
            query,
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
