import sys

from google.cloud import bigquery

from prefect_qbi.backup import dataset


def test_dataset_backup(project_id, dataset_id, location, bucket_name):
    client = bigquery.Client(project=project_id)
    dataset(
        client,
        project_id,
        dataset_id,
        location,
        bucket_name,
    )


if __name__ == "__main__":
    args = sys.argv[1:]
    test_dataset_backup(*args)
