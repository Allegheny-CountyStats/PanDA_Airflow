import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException

# Import the decorators from your package file
from PanDAirflow.sql import (
    sql_execute,
    sql_alter_columns,
    sql_alter_columns_dateformat
)


# ---------------------------------------------------------
# Helper: patches MsSqlHook.run so we never call a real DB
# ---------------------------------------------------------
@pytest.fixture
def mock_mssql_hook():
    with patch("your_package_file.MsSqlHook") as mock_hook_cls:
        mock_instance = MagicMock()
        mock_hook_cls.return_value = mock_instance
        yield mock_instance


# ---------------------------------------------------------
# TEST: sql_execute
# ---------------------------------------------------------
def test_sql_execute_valid(mock_mssql_hook):
    sql = "UPDATE test SET value=1;"

    result = sql_execute(sql=sql, conn_id="test_conn")

    mock_mssql_hook.run.assert_called_with("UPDATE test SET value=1")
    assert result is None


def test_sql_execute_invalid_empty_sql(mock_mssql_hook):
    with pytest.raises(AirflowException):
        sql_execute(sql="", conn_id="test_conn")


def test_sql_execute_invalid_no_statements(mock_mssql_hook):
    with pytest.raises(AirflowException):
        sql_execute(sql=";;; ;", conn_id="test_conn")


def test_sql_execute_hook_error(mock_mssql_hook):
    mock_mssql_hook.run.side_effect = Exception("DB error")

    with pytest.raises(AirflowException):
        sql_execute(sql="UPDATE test SET a=1;", conn_id="test_conn")


# ---------------------------------------------------------
# TEST: sql_alter_columns
# ---------------------------------------------------------
def test_sql_alter_columns_valid(mock_mssql_hook):
    alters = [
        {"column": "col1", "datatype": "varchar(255)"},
        {"column": "col2", "datatype": "int"},
    ]

    result = sql_alter_columns(
        database="DB",
        schema="dbo",
        table="mytable",
        alters=alters,
        conn_id="test_conn"
    )

    expected_calls = [
        "ALTER TABLE [DB].[dbo].[mytable] ALTER COLUMN col1 varchar(255);",
        "ALTER TABLE [DB].[dbo].[mytable] ALTER COLUMN col2 int;"
    ]

    # Ensure each SQL was executed
    mock_mssql_hook.run.assert_any_call(expected_calls[0])
    mock_mssql_hook.run.assert_any_call(expected_calls[1])

    assert result is None


def test_sql_alter_columns_invalid_list(mock_mssql_hook):
    with pytest.raises(AirflowException):
        sql_alter_columns(database="DB", schema="dbo", table="t", alters=None, conn_id="test_conn")


def test_sql_alter_columns_missing_keys(mock_mssql_hook):
    alters = [{"column": "col1"}]  # missing datatype

    with pytest.raises(AirflowException):
        sql_alter_columns(database="DB", schema="dbo", table="t", alters=alters, conn_id="test_conn")


def test_sql_alter_columns_hook_error(mock_mssql_hook):
    mock_mssql_hook.run.side_effect = Exception("DB error")

    alters = [{"column": "col1", "datatype": "varchar(255)"}]

    with pytest.raises(AirflowException):
        sql_alter_columns(database="DB", schema="dbo", table="t", alters=alters, conn_id="test_conn")


# ---------------------------------------------------------
# TEST: sql_alter_columns_dateformat
# ---------------------------------------------------------
def test_sql_alter_columns_dateformat_default(mock_mssql_hook):
    result = sql_alter_columns_dateformat(
        database="DB",
        schema="dbo",
        table="mytable",
        columns=["colA", "colB"],
        conn_id="test_conn"
    )

    expected_calls = [
        "ALTER TABLE [DB].[dbo].[mytable] ALTER COLUMN colA DATE;",
        "ALTER TABLE [DB].[dbo].[mytable] ALTER COLUMN colB DATE;"
    ]

    mock_mssql_hook.run.assert_any_call(expected_calls[0])
    mock_mssql_hook.run.assert_any_call(expected_calls[1])

    assert result is None


def test_sql_alter_columns_dateformat_datetime(mock_mssql_hook):
    result = sql_alter_columns_dateformat(
        database="DB",
        schema="dbo",
        table="mytable",
        columns=["created_at"],
        conn_id="test_conn",
        format_type="datetime"
    )

    mock_mssql_hook.run.assert_called_with(
        "ALTER TABLE [DB].[dbo].[mytable] ALTER COLUMN created_at DATETIME;"
    )

    assert result is None


def test_sql_alter_columns_dateformat_invalid_format(mock_mssql_hook):
    with pytest.raises(AirflowException):
        sql_alter_columns_dateformat(
            database="DB",
            schema="dbo",
            table="mytable",
            columns=["colA"],
            conn_id="test_conn",
            format_type="wrong_type"
        )


def test_sql_alter_columns_dateformat_no_columns(mock_mssql_hook):
    with pytest.raises(AirflowException):
        sql_alter_columns_dateformat(
            database="DB",
            schema="dbo",
            table="mytable",
            columns=[],
            conn_id="test_conn"
        )