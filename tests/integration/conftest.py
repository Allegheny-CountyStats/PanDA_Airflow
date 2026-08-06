import pytest
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from unittest.mock import patch

load_dotenv()


@pytest.fixture(scope="session")
def engine():
    """
    Real SQLAlchemy engine using Windows Trusted Connection
    and new WH_* env variable names.
    """
    server = os.getenv("WH_HOST")
    db = os.getenv("WH_DB")
    user = os.getenv("WH_USER")
    driver = os.getenv("TEST_DB_DRIVER", "ODBC+Driver+17+for+SQL+Server")

    if not all([server, db, user]):
        raise RuntimeError("Missing one or more WH_* environment variables")

    # Trusted Connection string (no password required)
    conn_str = (
        f"mssql+pyodbc://{user}@{server}/{db}"
        f"?driver={driver.replace(' ', '+')}"
        f"&Trusted_Connection=yes"
    )

    engine = create_engine(conn_str)
    return engine


@pytest.fixture
def patch_mssqlhook_with_sqlalchemy(engine):
    """
    Patch MsSqlHook so that Airflow decorators execute real SQL
    using SQLAlchemy instead of Airflow's MSSQL provider.
    """

    class SQLAlchemyHook:
        def __init__(self, conn_id):
            self.engine = engine

        def run(self, sql):
            with self.engine.begin() as conn:
                conn.execute(text(sql))

    # Patch path MUST match the location of MsSqlHook inside your package
    with patch("PanDAirflow.sql.MsSqlHook", SQLAlchemyHook):
        yield SQLAlchemyHook
