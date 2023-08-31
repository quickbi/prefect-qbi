from prefect import task

from .clean import copy_tables_with_transform


@task
def clean_dataset(
    customer,
    gcp_credentials_block_name,
    source_dataset,
    destination_dataset,
    table_prefix,
):
    copy_tables_with_transform(
        customer,
        gcp_credentials_block_name,
        source_dataset,
        destination_dataset,
        table_prefix,
    )
