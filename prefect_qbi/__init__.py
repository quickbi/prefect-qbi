from prefect import task

from .clean import transform_dataset


@task
def clean_dataset(
    gcp_credentials_block_name,
    source_dataset,
    destination_dataset,
    table_prefix,
):
    transform_dataset(
        gcp_credentials_block_name,
        source_dataset,
        destination_dataset,
        table_prefix,
    )
