from google.cloud import bigquery


def _extract_table(client, project_id, dataset_id, table_id, location, bucket_name):
    destination_uri = "gs://{}/{}".format(
        bucket_name, f"{dataset_id}__{table_id}__backup.csv"
    )

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(table_ref, destination_uri, location=location)
    extract_job.result()
    print(
        "Exported {}:{}.{} to {}".format(
            project_id, dataset_id, table_id, destination_uri
        )
    )


def dataset(client, project_id, dataset_id, location, bucket_name):
    tables = client.list_tables(dataset_id)

    for table in tables:
        _extract_table(
            client, project_id, dataset_id, table.table_id, location, bucket_name
        )


def table(client, project_id, dataset_id, table_id, location, bucket_name):
    _extract_table(client, project_id, dataset_id, table_id, location, bucket_name)
