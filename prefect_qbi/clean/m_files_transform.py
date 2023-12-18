from textwrap import dedent, indent
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
    index_query = f"""
        SELECT `{'`, `'.join(source_index_columns)}`, `{source_name_column}`
        FROM `{project_id}.{source_dataset_id}.{source_table_name}`
        WHERE `{source_value_column}` IS NOT NULL
    """

    for index in client.query(index_query):
        value_query = f"""
            SELECT PARSE_JSON(`{source_value_column}__unnested`) AS `array_item`
            FROM `{project_id}.{source_dataset_id}.{source_table_name}`
            CROSS JOIN UNNEST(JSON_QUERY_ARRAY(`{source_value_column}`)) AS `{source_value_column}__unnested`
            WITH OFFSET AS `{source_value_column}__offset`
            WHERE {' AND '.join(f'`{c}` = {index[c]!r}' for c in source_index_columns)}
            ORDER BY `{source_value_column}__offset`
        """
        schema = infer_schema_from_json_values(
            source_value_column,
            (row["array_item"] for row in client.query(value_query)),
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

        destination_table_name = clean_name(
            f"{index[source_name_column]}__{'_'.join(str(index[c]) for c in source_index_columns)}"
        )

        yield {
            "name": destination_table_name,
            "schema_list": field_schemas,
            "query_select_list": field_selectors,
            "query_from": f"({value_query})",
        }
