from google.cloud import dataform_v1beta1
from prefect import task
from prefect_gcp import GcpCredentials

from . import backup, clean, dataform


@task
def backup_dataset(gcp_credentials_block_name, dataset_id, location, bucket_name):
    gcp_credentials_block = GcpCredentials.load(gcp_credentials_block_name)
    client = gcp_credentials_block.get_bigquery_client()
    project_id = gcp_credentials_block.project
    assert project_id, "No project found"

    backup.dataset(client, project_id, dataset_id, location, bucket_name)


@task
def backup_table(
    gcp_credentials_block_name, dataset_id, table_id, location, bucket_name
):
    gcp_credentials_block = GcpCredentials.load(gcp_credentials_block_name)
    client = gcp_credentials_block.get_bigquery_client()
    project_id = gcp_credentials_block.project
    assert project_id, "No project found"

    backup.table(client, project_id, dataset_id, table_id, location, bucket_name)


@task
def clean_dataset(
    gcp_credentials_block_name,
    source_dataset,
    destination_dataset,
    table_prefix,
):
    gcp_credentials_block = GcpCredentials.load(gcp_credentials_block_name)
    client = gcp_credentials_block.get_bigquery_client()
    project_id = gcp_credentials_block.project
    assert project_id, "No project found"

    clean.transform_dataset(
        client,
        project_id,
        source_dataset,
        destination_dataset,
        table_prefix,
    )


@task
def run_dataform(
    gcp_credentials_block_name,
    location,
    repository,
):
    gcp_credentials_block = GcpCredentials.load(gcp_credentials_block_name)
    credentials = gcp_credentials_block.get_credentials_from_service_account()
    project = gcp_credentials_block.project
    assert project, "No project found"

    with dataform_v1beta1.DataformClient(credentials=credentials) as client:
        dataform.run(client, project, location, repository)
