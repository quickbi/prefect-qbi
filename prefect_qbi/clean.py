import random
import re
import time

from google.cloud import bigquery
from google.api_core.exceptions import Conflict, NotFound
from prefect_gcp import GcpCredentials


def get_dataset_location(client, project_id, dataset_id):
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = client.get_dataset(dataset_ref)
    return dataset.location


def convert_to_snake_case(text):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", text).lower()


def transform_schema(schema_fields):
    transformed_fields = []
    for field in schema_fields:
        if field.field_type == "RECORD":
            transformed_subfields = transform_schema(field.fields)
            transformed_fields.append(
                bigquery.SchemaField(
                    name=convert_to_snake_case(field.name),
                    field_type=field.field_type,
                    mode=field.mode,
                    description=field.description,
                    fields=transformed_subfields,
                )
            )
        else:
            transformed_fields.append(
                bigquery.SchemaField(
                    name=convert_to_snake_case(field.name),
                    field_type=field.field_type,
                    mode=field.mode,
                    description=field.description,
                )
            )
    return transformed_fields


def create_dataset_with_location(client, project_id, dataset_id, location):
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    try:
        client.create_dataset(dataset)
    except Conflict as e:
        if "Already Exists" not in str(e):
            raise e


def process_and_copy_table(
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
    schema = source_table.schema

    destination_schema = transform_schema(schema)

    # Generate a unique temporary table name with timestamp and random number
    timestamp = int(time.time())
    random_suffix = random.randint(1000, 9999)
    temp_table_id = f"{table_name_final}__temp_{timestamp}_{random_suffix}"

    # Create the temporary destination table with the transformed schema
    destination_temp_table_ref = (
        f"{project_id}.{destination_dataset_id}.{temp_table_id}"
    )
    destination_temp_table = bigquery.Table(
        destination_temp_table_ref, schema=destination_schema
    )
    client.create_table(destination_temp_table)

    try:
        # Construct and execute the SQL query to copy data
        query = f"""
            INSERT INTO `{destination_temp_table_ref}`
            SELECT *
            FROM `{source_table_ref}`
        """

        client.query(query).result()

        # Delete the original destination table if it exists
        destination_table_ref = (
            f"{project_id}.{destination_dataset_id}.{table_name_final}"
        )
        try:
            client.delete_table(destination_table_ref)
        except NotFound as e:
            pass

        # Rename the temporary table to the original table's name using SQL
        rename_query = f"""
            ALTER TABLE `{destination_temp_table_ref}`
            RENAME TO `{table_name_final}`
        """
        client.query(rename_query).result()

        print(f"Table '{table_name}' copied and renamed.")

    except Exception as e:
        # Delete the temporary table in case of an error
        try:
            client.delete_table(destination_temp_table_ref)
        except NotFound as e:
            pass

        print(f"Error processing table '{table_name}': {e}")


def copy_tables_with_transform(
    customer,
    gcp_credentials_block_name,
    source_dataset_id,
    destination_dataset_id,
    table_prefix,
):
    gcp_credentials_block = GcpCredentials.load(gcp_credentials_block_name)
    client = gcp_credentials_block.get_bigquery_client()
    project_id = gcp_credentials_block.project
    assert project_id, "No project found"

    source_location = get_dataset_location(client, project_id, source_dataset_id)
    create_dataset_with_location(
        client, project_id, destination_dataset_id, source_location
    )

    source_dataset_ref = f"{project_id}.{source_dataset_id}"
    source_tables = client.list_tables(source_dataset_ref)

    for table in source_tables:
        process_and_copy_table(
            client,
            project_id,
            source_dataset_id,
            destination_dataset_id,
            table.table_id,
            source_location,
            table_prefix,
        )
