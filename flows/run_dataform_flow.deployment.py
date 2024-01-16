import sys

from prefect.deployments import Deployment
from prefect.filesystems import GCS
from prefect.infrastructure.container import DockerContainer
from run_dataform_flow import run_dataform_flow


def deploy(
    env,
    customer_id,
    gcp_credentials_block_name,
    repository_location,
    repository_name,
):
    gcs_block = GCS.load("qbi-prefect-storage")
    docker_container_block = DockerContainer.load("prefect-qbi")
    work_queue_name = {
        "prod": "infra-elt-vm-prod2",
        "staging": "infra-elt-vm-staging",
    }[env]

    deployment = Deployment.build_from_flow(
        flow=run_dataform_flow,
        name=customer_id,
        storage=gcs_block,
        infrastructure=docker_container_block,
        work_queue_name=work_queue_name,
        tags=[f"customer:{customer_id}"],
        path="prefect-qbi",
        parameters={
            "gcp_credentials_block_name": gcp_credentials_block_name,
            "repository_location": repository_location,
            "repository_name": repository_name,
        },
    )
    deployment.apply()


if __name__ == "__main__":
    args = sys.argv[1:]
    deploy(*args)
