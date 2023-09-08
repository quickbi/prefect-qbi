from prefect import task
from prefect_gcp import GcpCredentials

from .clean import transform_dataset


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

    transform_dataset(
        client,
        project_id,
        source_dataset,
        destination_dataset,
        table_prefix,
    )
