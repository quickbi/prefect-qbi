# prefect-qbi

## Deploying

- `develop` branch is automatically deployed to staging (Docker image is pushed to Artifact Registry with "staging" tag)
- `main` branch is automatically deployed to production (Docker image is pushed to Artifact Registry with "production" tag)

## Testing

```sh
python test_clean_dataset.py "<project>" "<source-dataset>" "<destination-dataset>" "<destination-table-prefix>"
```

```sh
python test_run_dataform.py "<project>" "<dataform-repository-location>" "<dataform-repository-name>"
```
