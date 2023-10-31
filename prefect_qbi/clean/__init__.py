from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from .bigquery_schema import transform_table_schema
from .bigquery_utils import create_dataset_with_location, get_dataset_location
from .utils import convert_to_snake_case, get_unique_temp_table_name

JOIN_KEY_SOURCE_COLUMN = "_airbyte_raw_id"


# TODO: instead of looking at dataset name this should be able to get source system name.
def are_subtables_enabled(source_dataset_id):
    # Temporarily disable for Facebook and Instagram.
    if "facebook" in source_dataset_id or "instagram" in source_dataset_id:
        return False
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
            table_prefix,
            transformed_schema,
            project_id,
            destination_dataset_id,
            source_table,
            client,
            add_join_key,
        )
        created_temp_table_refs.append(temp_table_ref)
        table_mappings.append((temp_table_ref, table_name_final))
        _insert_data(
            transformed_schema, temp_table_ref, source_table_ref, client, add_join_key
        )

        # Create temp version of possible subtables and insert data to them.
        for subtable_schema in transformed_schema["subtables"]:
            if not are_subtables_enabled(source_dataset_id):
                break

            temp_table_ref, table_name_final = _create_temp_table(
                table_prefix,
                subtable_schema,
                project_id,
                destination_dataset_id,
                source_table,
                client,
                True,
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


def _create_temp_table(
    table_prefix,
    transformed_schema,
    project_id,
    destination_dataset_id,
    source_table,
    client,
    add_join_key,
):
    table_name = transformed_schema["table_name"]
    table_name_snake_case = convert_to_snake_case(table_name)
    table_name_final = f"{table_prefix}__{table_name_snake_case}"
    temp_table_id = get_unique_temp_table_name(table_name_final)
    destination_temp_table_ref = (
        f"{project_id}.{destination_dataset_id}.{temp_table_id}"
    )

    # Define the schema for the new table.
    extra_fields = (
        [
            bigquery.SchemaField(
                f"_quickbi_{source_table.table_id}_join_key", "STRING", mode="REQUIRED"
            )
        ]
        if add_join_key
        else []
    )
    destination_schema = extra_fields + transformed_schema["fields"]

    # Create table.
    destination_temp_table = bigquery.Table(
        destination_temp_table_ref, schema=destination_schema
    )
    client.create_table(destination_temp_table)

    return destination_temp_table_ref, table_name_final


def _insert_data(
    transformed_schema, temp_table_ref, source_table_ref, client, add_join_key
):
    select_list_str = _get_select_list(transformed_schema, add_join_key)
    query = f"""
        INSERT INTO `{temp_table_ref}`
        SELECT {select_list_str}
        FROM `{source_table_ref}`
    """
    client.query(query).result()


def _insert_data_to_subtable(
    transformed_schema, temp_table_ref, source_table_ref, client
):
    select_list_str = _get_select_list(transformed_schema, True)
    json_column_name = transformed_schema["json_column_name"]
    query = f"""
        INSERT INTO `{temp_table_ref}`
        SELECT {select_list_str}
        FROM `{source_table_ref}`
        CROSS JOIN UNNEST(json_extract_array(`{json_column_name}`)) AS array_item
    """
    client.query(query).result()


def _get_select_list(transformed_schema, add_join_key):
    selects = (
        [JOIN_KEY_SOURCE_COLUMN] + transformed_schema["select_list"]
        if add_join_key
        else transformed_schema["select_list"]
    )
    return ", \n".join(selects)


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
