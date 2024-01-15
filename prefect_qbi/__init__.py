from prefect import task
from prefect_gcp import GcpCredentials

from . import clean, dataform


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
    assert project_id, "No project found"

    with dataform_v1beta1.DataformClient(credentials=credentials) as client:
        dataform.run(client, project, location, repository)
