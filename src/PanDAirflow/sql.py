from airflow.sdk import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.exceptions import AirflowException
from typing import Literal


@task
def sql_execute(sql: str, conn_id: str):
    hook = MsSqlHook(conn_id=conn_id)
    if not sql or not isinstance(sql, str):
        raise AirflowException("Invalid SQL: SQL must be a non-empty string")
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if not statements:
        raise AirflowException("Invalid SQL: No executable statements found")
    try:
        for stmt in statements:
            hook.run(stmt)
    except Exception as exc:
        raise AirflowException(f"SQL execution failed on conn_id={conn_id}: {exc}")
    return None


@task
def sql_alter_columns(database: str, schema: str, table: str, alters: list[dict], conn_id: str):
    if not alters or not isinstance(alters, list):
        raise AirflowException("The 'alters' argument must be a non-empty list.")
    if not database or not schema or not table:
        raise AirflowException("Database, schema, and table must be provided.")
    hook = MsSqlHook(conn_id=conn_id)
    table_id = f"[{database}].[{schema}].[{table}]"
    sql_statements = []
    for entry in alters:
        if "column" not in entry or "datatype" not in entry:
            raise AirflowException("Each alter entry must contain 'column' and 'datatype'.")
        col = entry["column"]
        dtype = entry["datatype"]
        sql_statements.append(f"ALTER TABLE {table_id} ALTER COLUMN {col} {dtype};")
    try:
        for stmt in sql_statements:
            hook.run(stmt)
    except Exception as exc:
        raise AirflowException(f"ALTER COLUMN failed: {exc}")
    return None


@task
def sql_alter_columns_dateformat(
    database: str,
    schema: str,
    table: str,
    columns: list[str],
    conn_id: str,
    format_type: Literal["date", "datetime"] = "date"
):
    if not columns or not isinstance(columns, list):
        raise AirflowException("'columns' must be a non-empty list.")
    if format_type not in ("date", "datetime"):
        raise AirflowException("format_type must be only 'date' or 'datetime'.")
    if not database or not schema or not table:
        raise AirflowException("Database, schema, and table must be provided.")

    datatype = "DATE" if format_type == "date" else "DATETIME"
    hook = MsSqlHook(conn_id=conn_id)
    table_id = f"[{database}].[{schema}].[{table}]"
    sql_statements = [f"ALTER TABLE {table_id} ALTER COLUMN {col} {datatype};" for col in columns]
    try:
        for stmt in sql_statements:
            hook.run(stmt)
    except Exception as exc:
        raise AirflowException(f"ALTER COLUMN failed: {exc}")
    return None
