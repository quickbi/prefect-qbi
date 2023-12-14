"""
Run this to easily test cleaning dataset locally. You need to be authenticated
to Google Cloud (default authentication is used).

Example:
    python test_clean_dataset.py my-project my_src_dataset my_destination_dataset test

"""

import sys

from google.cloud import bigquery

from prefect_qbi.clean import transform_dataset


def test_transform_dataset(
    project_id, source_dataset_id, destination_dataset_id, table_prefix
):
    client = bigquery.Client(project=project_id)
    transform_dataset(
        client,
        project_id,
        source_dataset_id,
        destination_dataset_id,
        table_prefix,
    )


if __name__ == "__main__":
    args = sys.argv[1:]
    test_transform_dataset(*args)
