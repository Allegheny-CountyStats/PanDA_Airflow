"""
PanDAirflow Unit Test Template
--------------------------------

This template is designed for testing TaskFlow operators and sensors
inside PanDAirflow and other Airflow SDK-compatible code without needing:

  • Airflow runtime context
  • Airflow metadata DB
  • Airflow Variables or Connections
  • The scheduler or webserver

It supports:
  ✔ Local logic tests (mock-based)
  ✔ Remote interpreter tests (via PyCharm SSH)
  ✔ Optional real API calls (USE_REAL_API=true)
"""

import os
import pytest
from unittest.mock import patch


@patch("airflow.sdk.definitions.variable.Variable.get")
def test_template(mock_var_get):
    """
    Example test demonstrating:
      - Environment variable injection
      - Selective mocking
      - Optional real API toggle
    """

    # -------------------------------
    # Parameters from PyCharm Config
    # -------------------------------
    file_name = os.getenv("FILE_NAME", "testfile")
    folder_path = os.getenv("FOLDER_PATH", "/sample/path")
    raw_types = os.getenv("FILE_TYPES", ".csv")
    file_types = [t.strip() for t in raw_types.split(",")]
    use_real_api = os.getenv("USE_REAL_API", "false").lower() == "true"

    # -------------------------------
    # Mock Airflow Variables
    # -------------------------------
    mock_var_get.side_effect = lambda key: {
        "moveit_username": os.getenv("MI_USER", "dummy_user"),
        "moveit_password": os.getenv("MI_PASS", "dummy_pass"),
    }[key]

    # -------------------------------
    # Decide whether to mock API calls
    # -------------------------------
    if use_real_api:
        print("Running real API call mode...")
        from PanDAirflow.file_checkit import check_moveit
        result = check_moveit.__wrapped__(file_name, folder_path, file_types)

    else:
        print("Running mock mode...")
        with patch("PanDAirflow.file_checkit.auth_move_it") as mock_auth, \
             patch("PanDAirflow.file_checkit.available_files") as mock_files:

            mock_auth.return_value = {"token": "mock_token"}
            mock_files.return_value = [
                {"path": folder_path, "name": f"{file_name}{file_types[0]}"},
            ]

            from PanDAirflow.file_checkit import check_moveit
            result = check_moveit.__wrapped__(file_name, folder_path, file_types)

    # -------------------------------
    # Assertions
    # -------------------------------
    assert hasattr(result, "is_done")
    assert hasattr(result, "xcom_value")
    print("Test finished.")
    print(f"is_done: {result.is_done}")
    print(f"xcom_value: {result.xcom_value}")