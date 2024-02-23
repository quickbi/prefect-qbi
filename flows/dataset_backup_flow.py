from prefect import flow


@flow
def dataset_backup_flow(gcp_credentials_block_name, dataset_id, location, bucket_name):
    # Import inside the function to prevent error
    # when `prefect_qbi` is not available during deployment.
    from prefect_qbi import backup_dataset

    backup_dataset(gcp_credentials_block_name, dataset_id, location, bucket_name)
