from google.api_core.exceptions import Conflict
from google.cloud import bigquery


def get_dataset_location(client, project_id, dataset_id):
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = client.get_dataset(dataset_ref)
    return dataset.location


def create_dataset_with_location(client, project_id, dataset_id, location):
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    try:
        client.create_dataset(dataset)
    except Conflict as e:
        if "Already Exists" not in str(e):
            raise e
