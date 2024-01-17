from google.cloud import bigquery

from .bigquery_schema import clean_name
from .json_columns import infer_schema_from_json_values
from .utils import get_unique_temp_table_name


def transform_json_column_to_tables(
    client,
    project_id,
    source_dataset_id,
    destination_dataset_id,
    source_table_name,
    source_index_columns,
    source_name_column,
    source_value_column,
):
    # Get values for each column specified in `source_index_columns`.
    json_object_keys_function = '''
        CREATE TEMP FUNCTION JSON_OBJECT_KEYS(json_str STRING)
        RETURNS ARRAY<STRING>
        LANGUAGE js
        AS r"""
            const json_dict = JSON.parse(json_str);
            return Array.isArray(json_dict) ? [] : Object.keys(json_dict);
        """
    '''
    index_query = f"""
        {json_object_keys_function};
        SELECT
            `{'`, `'.join(source_index_columns)}`,
            `{source_name_column}`,
            JSON_TYPE(PARSE_JSON(`{source_value_column}`, wide_number_mode=>'round')) AS `{source_value_column}_type`,
            JSON_OBJECT_KEYS(`{source_value_column}`) AS `{source_value_column}_object_keys`
        FROM `{project_id}.{source_dataset_id}.{source_table_name}`
        WHERE `{source_value_column}` IS NOT NULL
    """

    for index in client.query(index_query):
        json_type = index[f"{source_value_column}_type"]
        json_object_keys = index[f"{source_value_column}_object_keys"]

        if json_type == "object":
            # The `source_value_column` for this row should be an object,
            # whose values are arrays of objects.
            # Each value (array) is converted to their own table,
            # named based on the file and the key associated with the array.
            json_paths = [f'$."{key}"' for key in json_object_keys]
            destination_table_names = [
                f"{index[source_name_column]}__{key}" for key in json_object_keys
            ]
        elif json_type == "array":
            # The `source_value_column` for this row should be an array of objects.
            # The array is converted into one table, named only based on the file.
            json_paths = ["$"]
            destination_table_names = [index[source_name_column]]
        else:
            raise ValueError(f"invalid json type: {json_type}")

        for json_path, destination_table_name in zip(
            json_paths, destination_table_names
        ):
            # Get records from the column `source_value_column` for the row with the matching index.
            # The column specified by `source_value_column` should contain an array of records stored
            # in JSON-string form. The records are unnested, and the original order is preserved
            # by ordering the results by the row offset.
            value_query = f"""
                SELECT PARSE_JSON(`{source_value_column}__unnested`, wide_number_mode=>'round') AS `array_item`
                FROM `{project_id}.{source_dataset_id}.{source_table_name}`
                CROSS JOIN UNNEST(JSON_QUERY_ARRAY(`{source_value_column}`, ?)) AS `{source_value_column}__unnested`
                WITH OFFSET AS `{source_value_column}__offset`
                WHERE {' AND '.join(f'`{c}` = ?' for c in source_index_columns)}
                ORDER BY `{source_value_column}__offset`
            """
            query_parameters = [
                bigquery.ScalarQueryParameter(None, "STRING", json_path)
            ]
            column_types = {
                schema_field.name: schema_field.field_type
                for schema_field in client.get_table(
                    f"{project_id}.{source_dataset_id}.{source_table_name}"
                ).schema
            }
            for index_column in source_index_columns:
                query_parameters.append(
                    bigquery.ScalarQueryParameter(
                        None, column_types[index_column], index[index_column]
                    )
                )
            schema = infer_schema_from_json_values(
                source_value_column,
                (
                    row["array_item"]
                    for row in client.query(
                        value_query,
                        job_config=bigquery.QueryJobConfig(
                            query_parameters=query_parameters
                        ),
                    )
                ),
                True,
            )

            field_schemas = []
            field_selectors = []

            # Subtables are currently not supported, because Excel files cannot contain such data.

            for field_name, field_spec in schema.items():
                field_data_type = field_spec["data_type"]
                field_mode = field_spec["mode"]

                field_schema = bigquery.SchemaField(
                    clean_name(field_name),
                    field_data_type,
                    mode=field_mode,
                )
                field_schemas.append(field_schema)

                field_selector = f"""JSON_QUERY(`array_item`, '$."{field_name}"')"""
                if field_data_type in ("INT64", "BOOL", "FLOAT64", "STRING"):
                    field_selector = f"LAX_{field_data_type}({field_selector})"
                field_selectors.append(field_selector)

            unique_destination_table_name = clean_name(
                f"{destination_table_name}__{'_'.join(str(index[c]) for c in source_index_columns)}"
            )

            yield {
                "name": unique_destination_table_name,
                "schema_list": field_schemas,
                "query_select_list": field_selectors,
                "query_from": f"({value_query})",
                "query_parameters": query_parameters,
            }
