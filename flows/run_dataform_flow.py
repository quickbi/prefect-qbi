from prefect import flow


@flow
def run_dataform_flow(gcp_credentials_block_name, repository_location, repository_name):
    # Import inside the function to prevent error
    # when `prefect_qbi` is not available during deployment.
    from prefect_qbi import run_dataform

    run_dataform(gcp_credentials_block_name, repository_location, repository_name)
