import sys
from pathlib import Path

from prefect.deployments import Deployment
from prefect.filesystems import GCS
from prefect.infrastructure.container import DockerContainer

from dataset_backup_flow import dataset_backup_flow


def deploy(
    env,
    customer_id,
    gcp_credentials_block_name,
    dataset_id,
    location,
    bucket_name,
):
    assert Path.cwd() == Path(__file__).parent
    gcs_block = GCS.load("qbi-prefect-storage")
    docker_container_block = DockerContainer.load("prefect-qbi")
    work_queue_name = {
        "prod": "infra-elt-vm-prod2",
        "staging": "infra-elt-vm-staging",
    }[env]

    deployment = Deployment.build_from_flow(
        flow=dataset_backup_flow,
        name=f"{customer_id}-{dataset_backup_flow.name}",
        storage=gcs_block,
        infrastructure=docker_container_block,
        work_queue_name=work_queue_name,
        tags=[f"customer:{customer_id}"],
        path="prefect-qbi",
        parameters={
            "gcp_credentials_block_name": gcp_credentials_block_name,
            "dataset_id": dataset_id,
            "location": location,
            "bucket_name": bucket_name,
        },
    )
    deployment.apply()


if __name__ == "__main__":
    args = sys.argv[1:]
    deploy(*args)
