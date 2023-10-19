from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from .bigquery_schema import transform_table_schema
from .bigquery_utils import create_dataset_with_location, get_dataset_location
from .utils import convert_to_snake_case, get_unique_temp_table_name


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
    table_name_snake_case = convert_to_snake_case(table_name)
    table_name_final = f"{table_prefix}__{table_name_snake_case}"
    source_table_ref = f"{project_id}.{source_dataset_id}.{table_name}"
    source_table = client.get_table(source_table_ref)
    source_schema = source_table.schema

    # Infer how schema should be transformed.
    destination_schema, select_list = transform_table_schema(
        source_schema, client, project_id, source_dataset_id, table_name
    )

    # Create the temporary destination table with the transformed schema.
    temp_table_id = get_unique_temp_table_name(table_name_final)
    destination_temp_table_ref = (
        f"{project_id}.{destination_dataset_id}.{temp_table_id}"
    )
    destination_temp_table = bigquery.Table(
        destination_temp_table_ref, schema=destination_schema
    )
    client.create_table(destination_temp_table)

    try:
        # Copy data.
        query = f"""
            INSERT INTO `{destination_temp_table_ref}`
            SELECT {select_list}
            FROM `{source_table_ref}`
        """
        client.query(query).result()

        # Delete the original destination table if it exists.
        destination_table_ref = (
            f"{project_id}.{destination_dataset_id}.{table_name_final}"
        )
        try:
            client.delete_table(destination_table_ref)
        except NotFound:
            pass

        # Rename the temporary table to the original table's name.
        rename_query = f"""
            ALTER TABLE `{destination_temp_table_ref}`
            RENAME TO `{table_name_final}`
        """
        client.query(rename_query).result()

        print(f"Table '{table_name}' copied and renamed.")

    except Exception as e:
        # Delete the temporary table in case of an error.
        try:
            client.delete_table(destination_temp_table_ref)
        except NotFound:
            pass

        print(f"Error processing table '{table_name}': {e}")
