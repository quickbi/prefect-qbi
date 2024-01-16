import sys

from google.cloud import dataform_v1beta1

from prefect_qbi import dataform


def test_run_dataform(project, location, repository):
    with dataform_v1beta1.DataformClient() as client:
        dataform.run(client, project, location, repository)


if __name__ == "__main__":
    args = sys.argv[1:]
    test_run_dataform(*args)
