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
    table_prefix,
):
    index_query = dedent(
        f"""\
        SELECT `{'`, `'.join(source_index_columns)}`, `{source_name_column}`
        FROM `{project_id}`.`{source_dataset_id}`.`{source_table_name}`
        WHERE `{source_value_column}` IS NOT NULL"""
    )
    rows = client.query(index_query)
    for row in rows:
        value_query = dedent(
            f"""\
            SELECT `{source_value_column}__unnested`
            FROM `{project_id}`.`{source_dataset_id}`.`{source_table_name}`
            JOIN UNNEST(JSON_QUERY_ARRAY(`{source_value_column}`, '$')) AS `{source_value_column}__unnested`
            WHERE {' AND '.join(f'`{c}` = {row[c]!r}' for c in source_index_columns)}"""
        )
        values = (
            row[f"{source_value_column}__unnested"] for row in client.query(value_query)
        )

        fields = []
        select_list = []

        schema = infer_schema_from_json_values(values, True)

        field_declarations = []
        field_selectors = []

        for key, value in schema.items():
            field_data_type = value["data_type"]
            field_mode = value["mode"]

            field_declaration = f"`{clean_name(key)}` {field_data_type}"
            if field_mode == "REQUIRED":
                field_declaration += " NOT NULL"
            field_declarations.append(field_declaration)

            field_selector = f"""SAFE_CAST(JSON_QUERY(`{source_value_column}__unnested`, '$."{key}"') AS {field_data_type})"""
            field_selector = f"{field_selector} AS `{clean_name(key)}`"
            field_selectors.append(field_selector)

        field_declarations = ", \n".join(field_declarations)
        field_selectors = ", \n".join(field_selectors)

        destination_table_name = clean_name(
            f"{table_prefix}__{row[source_name_column]}__{'_'.join(str(row[c]) for c in source_index_columns)}"
        )

        query = "\n".join(
            [
                f"CREATE OR REPLACE TABLE `{project_id}`.`{destination_dataset_id}`.`{destination_table_name}` (",
                indent(field_declarations, " " * 4),
                ") AS ",
                "SELECT",
                indent(field_selectors, " " * 4),
                "FROM (",
                indent(value_query, " " * 4),
                ");",
            ]
        )

        client.query(query).result()
