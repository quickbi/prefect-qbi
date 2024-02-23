# prefect-qbi

## Deploying

- `develop` branch is automatically deployed to staging (Docker image is pushed to Artifact Registry with "staging" tag)
- `main` branch is automatically deployed to production (Docker image is pushed to Artifact Registry with "production" tag)

## Testing

```sh
python test_dataset_backup.py "<project>" "<dataset-id>" "<location>" "<bucket_name>"
```

```sh
python test_clean_dataset.py "<project>" "<source-dataset>" "<destination-dataset>" "<destination-table-prefix>"
```

```sh
python test_run_dataform.py "<project>" "<dataform-repository-location>" "<dataform-repository-name>"
```

## Deploying

Deploy Dataform run flow to Prefect Cloud. The deployment can then be scheduled to run through the user interface.

```sh
cd flows
python run_dataform_flow.deployment.py "<staging/prod>" "<customer-id>" "<gcp-credentials-block-name>" "<dataform-repository-location>" "<dataform-repository-name>"
```
