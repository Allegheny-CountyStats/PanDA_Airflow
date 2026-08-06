import os
import pytest
from sqlalchemy import text

from PanDAirflow.sql import (
    sql_execute,
    sql_alter_columns,
    sql_alter_columns_dateformat,
)

def get_type(engine, table, column, schema="dbo"):
    sql = text("""
        SELECT DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema
          AND TABLE_NAME = :table
          AND COLUMN_NAME = :column
    """)
    with engine.begin() as conn:
        return conn.execute(sql, {
            "schema": schema,
            "table": table,
            "column": column
        }).scalar()

# ============================================================
# sql_execute integration test
# ============================================================
def test_sql_execute_integration(engine, patch_mssqlhook_with_sqlalchemy):
    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('ExecInt') IS NOT NULL DROP TABLE ExecInt;"))
        conn.execute(text("CREATE TABLE ExecInt (id INT, value VARCHAR(50));"))

    sql_execute(sql="INSERT INTO ExecInt VALUES (1, 'abc');", conn_id="test_conn")

    with engine.begin() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM ExecInt")).scalar()

    assert count == 1

# ============================================================
# sql_alter_columns integration test (changing INT → VARCHAR)
# ============================================================
def test_sql_alter_columns_integration(engine, patch_mssqlhook_with_sqlalchemy):
    db = os.getenv("WH_DB")

    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('AlterInt') IS NOT NULL DROP TABLE AlterInt;"))
        conn.execute(text("CREATE TABLE AlterInt (col1 INT, col2 INT);"))

    alters = [
        {"column": "col1", "datatype": "varchar(255)"},
        {"column": "col2", "datatype": "varchar(100)"}
    ]

    sql_alter_columns(
        database=db,
        schema="dbo",
        table="AlterInt",
        alters=alters,
        conn_id="test_conn"
    )

    assert get_type(engine, "AlterInt", "col1") == "varchar"
    assert get_type(engine, "AlterInt", "col2") == "varchar"

# ============================================================
# sql_alter_columns_dateformat (DATE)
# ============================================================
def test_sql_alter_columns_dateformat_date(engine, patch_mssqlhook_with_sqlalchemy):
    db = os.getenv("WH_DB")

    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('DateFmt') IS NOT NULL DROP TABLE DateFmt;"))
        conn.execute(text("CREATE TABLE DateFmt (colA VARCHAR(50), colB VARCHAR(50));"))

    sql_alter_columns_dateformat(
        database=db,
        schema="dbo",
        table="DateFmt",
        columns=["colA", "colB"],
        format_type="date",
        conn_id="test_conn"
    )

    assert get_type(engine, "DateFmt", "colA") == "date"
    assert get_type(engine, "DateFmt", "colB") == "date"

# ============================================================
# sql_alter_columns_dateformat (DATETIME)
# ============================================================
def test_sql_alter_columns_dateformat_datetime(engine, patch_mssqlhook_with_sqlalchemy):
    db = os.getenv("WH_DB")

    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('DateTimeFmt') IS NOT NULL DROP TABLE DateTimeFmt;"))
        conn.execute(text("CREATE TABLE DateTimeFmt (ts VARCHAR(50));"))

    sql_alter_columns_dateformat(
        database=db,
        schema="dbo",
        table="DateTimeFmt",
        columns=["ts"],
        format_type="datetime",
        conn_id="test_conn"
    )

    assert get_type(engine, "DateTimeFmt", "ts") == "datetime"

