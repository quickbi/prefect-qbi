from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from .bigquery_schema import clean_name, transform_table_schema
from .bigquery_utils import create_dataset_with_location, get_dataset_location
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
    source_dataset_ref = f"{project_id}.{source_dataset_id}"
    source_tables = client.list_tables(source_dataset_ref)

    for table in source_tables:
        transform_table(
            client,
            project_id,
            source_dataset_id,
            destination_dataset_id,
            table.table_id,
            source_location,
            table_prefix,
        )


def transform_table(
    client,
    project_id,
    source_dataset_id,
    destination_dataset_id,
    table_name,
    source_location,
    table_prefix,
):
    # Infer how schema should be transformed.
    source_table_ref = f"{project_id}.{source_dataset_id}.{table_name}"
    source_table = client.get_table(source_table_ref)
    source_schema = source_table.schema
    should_unnest_objects = _should_unnest_objects(source_dataset_id)
    transformed_schema = transform_table_schema(
        source_schema,
        client,
        project_id,
        source_dataset_id,
        table_name,
        should_unnest_objects,
    )

    created_temp_table_refs = []
    table_mappings = []
    add_join_key = bool(transformed_schema.get("subtables"))

    try:
        # Create temp version of main table and insert data to it.
        temp_table_ref, table_name_final = _create_temp_table(
            table_prefix=table_prefix,
            transformed_schema=transformed_schema,
            project_id=project_id,
            destination_dataset_id=destination_dataset_id,
            source_table=source_table,
            client=client,
        )
        created_temp_table_refs.append(temp_table_ref)
        table_mappings.append((temp_table_ref, table_name_final))
        _insert_data(
            transformed_schema, temp_table_ref, source_table_ref, client, add_join_key
        )

        _add_demo_tables(
            project_id,
            table_name,
            table_prefix,
            destination_dataset_id,
            source_table,
            client,
            source_table_ref,
        )

        if "m_files" in source_dataset_id and table_name == "objects_files_contents":
            from .m_files_transform import transform_json_column_to_tables

            transform_json_column_to_tables(
                client,
                project_id,
                source_dataset_id,
                destination_dataset_id,
                table_name,
                ["ObjectType", "ObjectID", "FileID"],
                "FileName",
                "ContentJson",
                table_prefix,
            )

        # Create temp version of possible subtables and insert data to them.
        for subtable_schema in transformed_schema["subtables"]:
            if not are_subtables_enabled(source_dataset_id):
                break

            temp_table_ref, table_name_final = _create_temp_table(
                table_prefix=table_prefix,
                transformed_schema=subtable_schema,
                project_id=project_id,
                destination_dataset_id=destination_dataset_id,
                source_table=source_table,
                client=client,
            )
            created_temp_table_refs.append(temp_table_ref)
            table_mappings.append((temp_table_ref, table_name_final))
            _insert_data_to_subtable(
                subtable_schema, temp_table_ref, source_table_ref, client
            )

        # Remove original tables.
        for temp_table_ref, table_name_final in table_mappings:
            _delete_table(project_id, destination_dataset_id, table_name_final, client)

        # Rename temp tables to original tables.
        for temp_table_ref, table_name_final in table_mappings:
            _rename_temp_to_original(
                temp_table_ref, table_name_final, project_id, client
            )

        print(f"Table '{table_name}' transformed.")

    except Exception as e:
        # Delete the temporary tables in case of an error.
        for table_ref in created_temp_table_refs:
            try:
                client.delete_table(table_ref)
            except NotFound:
                pass

        print(f"Error processing table '{table_name}': {e}")
        raise e


def _add_demo_tables(
    project_id,
    table_name,
    table_prefix,
    destination_dataset_id,
    source_table,
    client,
    source_table_ref,
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
        temp_table_ref, table_name_final = _create_temp_table(
            table_prefix=table_prefix,
            transformed_schema=transformed_schema,
            project_id=project_id,
            destination_dataset_id=destination_dataset_id,
            source_table=source_table,
            client=client,
        )
        _insert_data(
            transformed_schema, temp_table_ref, source_table_ref, client, False
        )
        _delete_table(project_id, destination_dataset_id, table_name_final, client)
        _rename_temp_to_original(temp_table_ref, table_name_final, project_id, client)


def _create_temp_table(
    table_prefix,
    transformed_schema,
    project_id,
    destination_dataset_id,
    source_table,
    client,
):
    table_name = transformed_schema["table_name"]
    table_name_snake_case = convert_to_snake_case(table_name)
    table_name_final = f"{table_prefix}__{table_name_snake_case}"
    temp_table_id = get_unique_temp_table_name(table_name_final)
    destination_temp_table_ref = (
        f"{project_id}.{destination_dataset_id}.{temp_table_id}"
    )
    destination_schema = transformed_schema["fields"]

    # Create table.
    destination_temp_table = bigquery.Table(
        destination_temp_table_ref, schema=destination_schema
    )
    client.create_table(destination_temp_table)

    return destination_temp_table_ref, table_name_final


def _insert_data(
    transformed_schema, temp_table_ref, source_table_ref, client, add_join_key
):
    select_list_str = _get_select_list(transformed_schema)
    query = f"""
        INSERT INTO `{temp_table_ref}`
        SELECT {select_list_str}
        FROM `{source_table_ref}`
    """
    client.query(query).result()


def _insert_data_to_subtable(
    transformed_schema, temp_table_ref, source_table_ref, client
):
    select_list_str = _get_select_list(transformed_schema)
    json_column_name = transformed_schema["json_column_name"]
    query = f"""
        INSERT INTO `{temp_table_ref}`
        SELECT {select_list_str}
        FROM `{source_table_ref}`
        CROSS JOIN UNNEST(json_extract_array(`{json_column_name}`)) AS array_item
    """
    client.query(query).result()


def _get_select_list(transformed_schema):
    return ", \n".join(transformed_schema["select_list"])


def _delete_table(project_id, destination_dataset_id, table_name_final, client):
    destination_table_ref = f"{project_id}.{destination_dataset_id}.{table_name_final}"
    try:
        client.delete_table(destination_table_ref)
    except NotFound:
        pass


def _rename_temp_to_original(temp_table_ref, table_name_final, project_id, client):
    rename_query = f"""
        ALTER TABLE `{temp_table_ref}`
        RENAME TO `{table_name_final}`
    """
    client.query(rename_query).result()
