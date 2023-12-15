from google.cloud import bigquery


def insert_query_result_to_table(client, project_id, dataset_id, table_name, query):
    table_ref = f"{project_id}.{dataset_id}.{table_name}"

    insert_query = f"""
        INSERT INTO `{table_ref}`
        {query}
    """
    client.query(insert_query).result()

    # TODO: maybe
    """
    client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            destination=table_ref,
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_EMPTY",
        ),
    ).result()
    """


def create_table_with_schema(client, project_id, dataset_id, table_name, schema):
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_ref, schema)
    client.create_table(table)


def delete_table(client, project_id, dataset_id, table_name):
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    client.delete_table(table_ref, not_found_ok=True)


def rename_table(client, project_id, dataset_id, old_table_name, new_table_name):
    query = f"""
        ALTER TABLE `{project_id}.{dataset_id}.{old_table_name}`
        RENAME TO `{new_table_name}`
    """
    client.query(query).result()


def get_table_schema(client, project_id, dataset_id, table_name):
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    table = client.get_table(table_ref)
    return table.schema


def get_dataset_table_names(client, project_id, dataset_id):
    dataset_ref = f"{project_id}.{dataset_id}"
    for table in client.list_tables(dataset_ref):
        yield table.table_id


def get_dataset_location(client, project_id, dataset_id):
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = client.get_dataset(dataset_ref)
    return dataset.location


def create_dataset_with_location(client, project_id, dataset_id, location):
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)
