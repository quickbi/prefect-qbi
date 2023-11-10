import pytest

from prefect_qbi.clean.bigquery_schema import clean_name


class TestCleanName:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("_row_extracted_at", "_row_extracted_at"),
            ("properties__$set_once", "properties__set_once"),
            ("with.dot", "with_dot"),
            ("___too&_many_$under___scores", "__too_many_under__scores"),
        ],
    )
    def test_clean_name(self, name, expected):
        result = clean_name(name)
        assert result == expected
